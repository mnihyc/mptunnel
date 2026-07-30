use super::client::{
    ClientRelayDisconnectedState, ClientRelayState, ClientStreamAckContext,
    apply_client_stream_ack, apply_client_stream_ack_for_tcp_service,
    apply_client_stream_data_state, evaluate_client_data_ack_reinjection,
    update_request_path_staleness,
};
use super::diagnostics::log_unexpected_stream_relay_frame;
use super::flow::{ReliableRelayFlowDemandTracker, ReliableRelayFlowSignals};
use super::io::{
    ReadyStreamDataBatchBounds, ReadyStreamDataDirection, apply_and_write_ready_stream_data_batch,
    collect_ready_stream_data_batch, pending_stream_fin_ready, read_reliable_relay_payload,
    receive_stream_fin, resize_reliable_relay_buffer, stream_data_range_already_delivered,
    stream_terminal_fin_replay_required,
};
use super::lifecycle::{
    attach_reliable_relay_paths_with_recovery_exclusions, cancel_pending_additional_path_opens,
    drain_completed_additional_path_opens, handle_additional_path_open_result,
    recover_reliable_relay_after_path_failure, reliable_relay_can_send_pending_fin,
    reliable_relay_disconnected_retry_delay, reliable_relay_lane_changed,
    reliable_relay_product_stall_deadline,
    reliable_relay_product_stall_preserves_attached_path_set,
    reliable_relay_product_stall_should_try_alternate_attach,
    reliable_relay_queued_send_blocked_for_retry, reliable_relay_receive_hole_reinjection_active,
    reliable_relay_receive_hole_reinjection_deadline, reliable_relay_stall_progress_anchor,
    reliable_relay_stall_watch_active, spawn_reliable_relay_additional_path_opens,
    spawn_reliable_relay_disconnected_path_open, spawn_reliable_relay_recovery_path_open,
    switch_reliable_relay_to_best_path,
};
use super::open::{ReliableRelayOpenSpec, relay_error_is_tcp_path_failure};
use super::remote::ReliableRelayAttachMode;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_flush, lab_perf_record};
use crate::model::ack_clock::reliable_ack_clock_measurement_limit_bytes;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, adaptive_reliable_relay_chunk_bytes,
    adaptive_reliable_relay_chunk_bytes_with_frame_limit, adaptive_reliable_relay_inflight_bytes,
    reliable_relay_buffer_len, reliable_relay_sender_dispatch_budget,
    reliable_stream_initial_advertised_window_bytes,
};
use crate::model::tcp_service::{TcpServiceWithdrawalReason, TcpServiceWriterLifecycle};
use crate::model::timing::sender_service_retry_delay;
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream, StreamError};
use crate::performance::MppPerformanceConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{Frame, OffsetRange, ResetReason, StreamId};
use crate::runtime::error::{RuntimeError, reliable_path_error_is_migratable};
use crate::runtime::path::commands::reliable_stream_frame_queue;
use crate::runtime::path::{
    ClientPathContext, ClientRequestTcpServiceLifecycleState, PathDeliveryStats,
};
use crate::runtime::sender::{
    ClientQueuedDispatch, RelayRecvProgressSend, RelaySendCause, ReliableRelaySenderQueue,
    RequestSenderService, reliable_relay_can_read_product_source,
    reliable_relay_sender_queue_limit, reliable_relay_sender_queue_read_budget,
};
use crate::runtime::stream::{
    OpenedRemoteStream, ReliableRelayRemoteFrame, ReliableRelayRemotePath, ReliableRelayRemoteSet,
    RequestRelayActorEvent, RequestTcpServiceFrozenStream, StreamSendBufferReservation,
    wait_for_carrier_capacity_notifies,
};
use crate::runtime::stream::{
    reliable_relay_recv_progress_resend_active, reliable_stream_recv_progress_interval,
};
use crate::runtime::tcp_service::{
    RequestTcpServiceControl, RequestTcpServiceControlOutcome, RequestTcpServiceObserverInstall,
    RequestTcpServiceSnapshotRequest, TcpServiceObserverRemoval,
};
use crate::runtime::telemetry::ObservedProductIo;
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::future::Future;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

async fn wait_for_optional_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

struct ClientStreamAckOperation<'a> {
    state: &'a mut ClientRelayState,
    sender_queue: &'a mut ReliableRelaySenderQueue,
    context: &'a ClientPathContext,
    remotes: &'a ReliableRelayRemoteSet,
    send_stream: &'a mut ReliableSendStream,
    path_snapshot: Option<PathSnapshot>,
    relay_lane: TrafficClass,
    send_buffer_reservation: &'a mut StreamSendBufferReservation,
    stream_id: StreamId,
    complete: bool,
    ranges: Vec<OffsetRange>,
}

fn apply_ordinary_client_stream_ack(
    sender: &mut RequestSenderService,
    operation: ClientStreamAckOperation<'_>,
) -> Result<usize, StreamError> {
    let ClientStreamAckOperation {
        state,
        sender_queue,
        context,
        remotes,
        send_stream,
        path_snapshot,
        relay_lane,
        send_buffer_reservation,
        stream_id,
        complete,
        ranges,
    } = operation;
    let released = apply_client_stream_ack(
        ClientStreamAckContext {
            state,
            sender,
            sender_queue,
            context,
            remotes,
            send_stream,
            path_snapshot,
            relay_lane,
        },
        stream_id,
        complete,
        ranges,
    )?;
    send_buffer_reservation.release(released);
    Ok(released)
}

fn apply_observed_client_stream_ack(
    sender: &mut RequestSenderService,
    operation: ClientStreamAckOperation<'_>,
    lifecycle: TcpServiceWriterLifecycle,
) -> Result<usize, StreamError> {
    let ClientStreamAckOperation {
        state,
        sender_queue,
        context,
        remotes,
        send_stream,
        path_snapshot,
        relay_lane,
        send_buffer_reservation,
        stream_id,
        complete,
        ranges,
    } = operation;
    let released = apply_client_stream_ack_for_tcp_service(
        ClientStreamAckContext {
            state,
            sender,
            sender_queue,
            context,
            remotes,
            send_stream,
            path_snapshot,
            relay_lane,
        },
        stream_id,
        complete,
        ranges,
        lifecycle,
    )?;
    send_buffer_reservation.release(released);
    Ok(released)
}

fn snapshot_request_tcp_service_stream(
    state: &ClientRelayState,
    sender: &RequestSenderService,
    sender_queue: &ReliableRelaySenderQueue,
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    request: RequestTcpServiceSnapshotRequest,
) -> RequestTcpServiceControlOutcome<RequestTcpServiceFrozenStream> {
    let demand_generation = match state.request_tcp_service_demand_generation() {
        Ok(generation) => generation,
        Err(reason) => return RequestTcpServiceControlOutcome::Withdrawn(reason),
    };
    if sender_queue.data_bytes() == 0 {
        return RequestTcpServiceControlOutcome::Withdrawn(TcpServiceWithdrawalReason::DemandEnded);
    }
    let data_ack_horizon_bytes = reliable_ack_clock_measurement_limit_bytes(context.mux_limits);
    let frozen = match remotes.snapshot_tcp_service_stream(
        context,
        request,
        demand_generation,
        data_ack_horizon_bytes,
    ) {
        Ok(frozen) => frozen,
        Err(reason) => return RequestTcpServiceControlOutcome::Withdrawn(reason),
    };
    if !sender.tcp_service_accepted_set_has_original_flight(frozen.accepted()) {
        return RequestTcpServiceControlOutcome::Withdrawn(TcpServiceWithdrawalReason::DemandEnded);
    }
    RequestTcpServiceControlOutcome::Complete(frozen)
}

fn withdraw_request_tcp_service_installation(
    sender: &mut RequestSenderService,
    context: &ClientPathContext,
    frozen: &RequestTcpServiceFrozenStream,
    coordinator: &std::sync::Arc<crate::runtime::tcp_service::TcpServiceWriterCoordinator>,
    proposed: TcpServiceWithdrawalReason,
) -> TcpServiceWithdrawalReason {
    let lifecycle = coordinator.lifecycle();
    let accepted = context.withdraw_request_tcp_service_installation(frozen, coordinator, proposed);
    let state = context.request_tcp_service_lifecycle_state(lifecycle);
    let terminal = matches!(
        state,
        Some(
            ClientRequestTcpServiceLifecycleState::CleanupPending
                | ClientRequestTcpServiceLifecycleState::Withdrawn(_)
        )
    );
    if accepted.is_none() && !terminal {
        return proposed;
    }
    let reason = accepted
        .or_else(|| match state {
            Some(ClientRequestTcpServiceLifecycleState::Withdrawn(reason)) => Some(reason),
            _ => None,
        })
        .unwrap_or(proposed);
    let _ = sender.remove_tcp_service_observer(lifecycle);
    let _ =
        context.acknowledge_request_tcp_service_actor_cleanup(frozen.stream().stream_id, lifecycle);
    reason
}

fn apply_request_tcp_service_control(
    state: &ClientRelayState,
    sender: &mut RequestSenderService,
    sender_queue: &ReliableRelaySenderQueue,
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    control: RequestTcpServiceControl,
) {
    match control {
        RequestTcpServiceControl::Snapshot { request, receipt } => {
            let result = snapshot_request_tcp_service_stream(
                state,
                sender,
                sender_queue,
                context,
                remotes,
                request,
            );
            let _ = receipt.send(result);
        }
        RequestTcpServiceControl::Install { install, receipt } => {
            let RequestTcpServiceObserverInstall {
                frozen,
                coordinator,
                max_flight_records,
                max_ack_release_records,
            } = install;
            let revalidated = snapshot_request_tcp_service_stream(
                state,
                sender,
                sender_queue,
                context,
                remotes,
                frozen.snapshot_request(),
            );
            let installation = match revalidated {
                RequestTcpServiceControlOutcome::Complete(current) if current == frozen => {
                    let accepted = frozen
                        .accepted()
                        .iter()
                        .map(|binding| (binding.instance(), binding.carrier()))
                        .collect();
                    context.install_request_tcp_service_observer(&frozen, &coordinator, || {
                        sender.install_tcp_service_observer(
                            frozen.stream(),
                            accepted,
                            frozen.candidate(),
                            coordinator.clone(),
                            max_flight_records,
                            max_ack_release_records,
                        )
                    })
                }
                RequestTcpServiceControlOutcome::Complete(_) => {
                    Err(TcpServiceWithdrawalReason::FenceChanged)
                }
                RequestTcpServiceControlOutcome::Withdrawn(reason) => Err(reason),
            };
            let result = match installation {
                Ok(installation) => RequestTcpServiceControlOutcome::Complete(installation),
                Err(reason) => RequestTcpServiceControlOutcome::Withdrawn(
                    withdraw_request_tcp_service_installation(
                        sender,
                        context,
                        &frozen,
                        &coordinator,
                        reason,
                    ),
                ),
            };
            let _ = receipt.send(result);
        }
        RequestTcpServiceControl::Remove { lifecycle, receipt } => {
            let result = if context.request_tcp_service_lifecycle_state(lifecycle)
                == Some(ClientRequestTcpServiceLifecycleState::Current)
            {
                RequestTcpServiceControlOutcome::Withdrawn(
                    TcpServiceWithdrawalReason::InvalidEvidence,
                )
            } else {
                let removal = sender.remove_tcp_service_observer(lifecycle);
                context
                    .acknowledge_request_tcp_service_actor_cleanup(remotes.stream_id(), lifecycle);
                match removal {
                    TcpServiceObserverRemoval::DifferentLifecycle => {
                        RequestTcpServiceControlOutcome::Withdrawn(
                            TcpServiceWithdrawalReason::FenceChanged,
                        )
                    }
                    removal => RequestTcpServiceControlOutcome::Complete(removal),
                }
            };
            let _ = receipt.send(result);
        }
    }
}

async fn commit_pending_remote_fin<S>(
    local: &mut S,
    state: &mut ClientRelayState,
    recv_stream: &ReliableRecvStream,
    feedback_published: bool,
) -> Result<(), RuntimeError>
where
    S: AsyncWrite + Unpin,
{
    if feedback_published
        && pending_stream_fin_ready(recv_stream, state.endpoint.pending_remote_fin_offset)
    {
        local.shutdown().await.map_err(RuntimeError::Io)?;
        state.record_remote_finished();
    }
    Ok(())
}

fn record_final_recv_progress_enqueue(
    state: &mut ClientRelayState,
    sent: bool,
    path: Option<crate::scheduler::PathSnapshot>,
) {
    state.record_recv_progress_sent(sent);
    if !sent {
        // Forced final feedback always has work. A false result means every
        // carrier control queue was full, so keep FIN pending and retry on
        // writer capacity or the bounded sender retry timer.
        state.progress.sender_retry_at =
            Some(tokio::time::Instant::now() + sender_service_retry_delay(path));
    }
}

pub(in crate::runtime) fn reliable_relay_client_dispatch_payload_limit(
    adaptive_chunk_bytes: usize,
    remaining_pass_bytes: usize,
) -> usize {
    adaptive_chunk_bytes.min(remaining_pass_bytes).max(1)
}

fn reliable_relay_request_outstanding_headroom_bytes(
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    outstanding_limit_bytes: usize,
) -> usize {
    outstanding_limit_bytes.saturating_sub(
        send_stream
            .reinjection_bytes()
            .saturating_add(sender_queue.data_bytes()),
    )
}

fn reliable_relay_request_outstanding_limit_bytes(
    lane: crate::scheduler::TrafficClass,
    payload_bytes: usize,
    mux_limits: crate::mux::MuxLimits,
) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let resource_ceiling = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(stream_window)
        .max(1);
    if lane.is_bulk() {
        // Per-stream peer flow control remains path-independent. A separate
        // session owner bounds aggregate unique source bytes across streams.
        resource_ceiling
    } else {
        reliable_relay_buffer_len(mux_limits)
            .min(resource_ceiling)
            .max(payload_bytes.min(resource_ceiling))
            .max(1)
    }
}

#[derive(Debug, Clone, Copy)]
struct ClientOpportunisticReadBounds {
    sender_dispatch_byte_budget: usize,
    sender_dispatch_item_budget: usize,
    sender_queue_limit: usize,
    source_read_ceiling: usize,
    request_outstanding_limit: usize,
}

fn reliable_relay_client_opportunistic_read_budget(
    completed_reads: usize,
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    bounds: ClientOpportunisticReadBounds,
) -> usize {
    if completed_reads >= bounds.sender_dispatch_item_budget
        || !reliable_relay_can_read_product_source(
            true,
            false,
            send_stream,
            sender_queue,
            bounds.sender_queue_limit,
        )
    {
        return 0;
    }

    reliable_relay_sender_queue_read_budget(
        send_stream,
        sender_queue,
        bounds.sender_queue_limit,
        bounds.source_read_ceiling,
    )
    .min(
        bounds
            .sender_dispatch_byte_budget
            .saturating_sub(sender_queue.data_bytes()),
    )
    .min(reliable_relay_request_outstanding_headroom_bytes(
        send_stream,
        sender_queue,
        bounds.request_outstanding_limit,
    ))
}

async fn ready_at_entry<F>(future: F) -> Option<F::Output>
where
    F: Future,
{
    tokio::select! {
        biased;
        output = future => Some(output),
        _ = std::future::ready(()) => None,
    }
}

pub(in crate::runtime) async fn relay_migrating_tcp_stream<S>(
    local: S,
    context: &ClientPathContext,
    performance: MppPerformanceConfig,
    spec: ReliableRelayOpenSpec,
    remote: OpenedRemoteStream,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let initial_recv_max_offset = reliable_stream_initial_advertised_window_bytes(
        remote.stream().underlay,
        remote.stream().lane,
        context.mux_limits,
    );
    let mut remotes =
        ReliableRelayRemoteSet::new(remote, reliable_stream_frame_queue(context.mux_limits));
    let stream_id = remotes.stream_id();
    let telemetry_flow = context.telemetry.open_reliable_flow(
        Some(context.session_id),
        stream_id,
        spec.target.clone(),
    );
    let mut local = ObservedProductIo::new(local, telemetry_flow.counter());
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, context.mux_limits, 0);
    send_stream.update_max_offset(remotes.max_offset());
    let mut recv_stream = ReliableRecvStream::new_with_initial_max_offset(
        stream_id,
        context.mux_limits,
        initial_recv_max_offset,
    );
    let chunk_size =
        adaptive_reliable_relay_chunk_bytes(None, TrafficClass::Latency, context.mux_limits);
    let mut buf = bytes::BytesMut::with_capacity(chunk_size);
    let mut state = ClientRelayState::new();
    let tcp_service_writer_registration =
        context.register_tcp_service_writer(stream_id, remotes.tcp_service_writer())?;
    // Declaration order is cleanup authority: on cancellation or panic the
    // local observer is destroyed before its actor registration acknowledges
    // that no observer can remain.
    let mut sender = RequestSenderService::new_with_performance(stream_id, performance);
    let mut flow_demand = ReliableRelayFlowDemandTracker::new();
    let mut request_flow_demand = ReliableRelayFlowDemandTracker::new();
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let mut deferred_remote_frame = None::<ReliableRelayRemoteFrame>;
    let mut ready_remote_data = super::io::ReadyStreamDataBatch::new();
    let mut send_buffer_reservation = context.session_send_buffer.stream_reservation();
    let mut send_buffer_updates = context.session_send_buffer.subscribe();
    let (additional_path_open_tx, mut additional_path_open_rx) = mpsc::channel(
        context
            .tcp_paths
            .len()
            .saturating_add(context.udp_paths.len())
            .max(1),
    );
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_budget: Option<(TrafficClass, usize, usize)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_read_block: Option<(usize, usize, usize, usize, usize)> = None;
    let result = loop {
        if state.is_finished(&send_stream, &recv_stream, &sender_queue) {
            break Ok(state.delivery.total);
        }
        if remotes.is_empty() {
            let now = Instant::now();
            let now_async = tokio::time::Instant::now();
            let disconnected = state
                .recovery
                .disconnected
                .get_or_insert_with(|| ClientRelayDisconnectedState::new(now, now_async));
            if disconnected.expired(now, context.session_retention_timeout) {
                break Err(RuntimeError::SessionRetentionTimeout);
            }
            let retention_deadline =
                disconnected.retention_deadline(context.session_retention_timeout);
            let relay_lane = flow_demand.current_lane();
            if state.recovery.pending_additional_path_opens.is_empty()
                && now_async >= disconnected.retry_at
            {
                let spawned = spawn_reliable_relay_disconnected_path_open(
                    context,
                    &spec,
                    relay_lane,
                    &remotes,
                    &send_stream,
                    &mut disconnected.attempted_paths,
                    &mut state.recovery.pending_additional_path_opens,
                    &additional_path_open_tx,
                );
                if !spawned {
                    disconnected.retry_after(reliable_relay_disconnected_retry_delay());
                }
            }

            if state.recovery.pending_additional_path_opens.is_empty() {
                let retry_at = state
                    .recovery
                    .disconnected
                    .as_ref()
                    .expect("disconnected relay state")
                    .retry_at;
                tokio::select! {
                    event = remotes.recv_event() => {
                        match event {
                            Ok(RequestRelayActorEvent::TcpService(control)) => {
                                apply_request_tcp_service_control(
                                    &state,
                                    &mut sender,
                                    &sender_queue,
                                    context,
                                    &remotes,
                                    *control,
                                );
                            }
                            Ok(RequestRelayActorEvent::Frame(_)) => {}
                            Err(err) => break Err(err),
                        }
                        continue;
                    }
                    _ = tokio::time::sleep_until(retry_at) => continue,
                    _ = wait_for_optional_deadline(retention_deadline) => {
                        break Err(RuntimeError::SessionRetentionTimeout);
                    }
                }
            }

            tokio::select! {
                event = remotes.recv_event() => {
                    match event {
                        Ok(RequestRelayActorEvent::TcpService(control)) => {
                            apply_request_tcp_service_control(
                                &state,
                                &mut sender,
                                &sender_queue,
                                context,
                                &remotes,
                                *control,
                            );
                        }
                        Ok(RequestRelayActorEvent::Frame(_)) => {}
                        Err(err) => break Err(err),
                    }
                    continue;
                }
                additional_path_open = additional_path_open_rx.recv() => {
                    let Some(additional_path_open) = additional_path_open else {
                        cancel_pending_additional_path_opens(
                            stream_id,
                            &mut state.recovery.pending_additional_path_opens,
                        );
                        if let Some(disconnected) = state.recovery.disconnected.as_mut() {
                            disconnected.retry_after(reliable_relay_disconnected_retry_delay());
                        }
                        continue;
                    };
                    state
                        .recovery
                        .pending_additional_path_opens
                        .remove(&additional_path_open.key);
                    let attached = handle_additional_path_open_result(
                        context,
                        &mut sender,
                        stream_id,
                        &mut remotes,
                        &mut send_stream,
                        !state.endpoint.local_open,
                        additional_path_open,
                        state.recovery.pending_additional_path_opens.len(),
                        &mut state.progress.last_stream_at,
                    )
                    .await
                    .is_some();
                    if attached {
                        state.progress.sender_retry_at = None;
                        send_stream.update_max_offset(remotes.max_offset());
                        let path_snapshot = remotes.lowest_eta_path_snapshot(
                            context,
                            relay_lane,
                            PATH_OPEN_SCORE_BYTES,
                        );
                        let progress_ready = match sender
                            .send_recv_progress(
                                &mut remotes,
                                context,
                                &mut recv_stream,
                                &mut state.progress.recv_progress,
                                RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                            )
                            .await
                        {
                            Ok(sent) => {
                                record_final_recv_progress_enqueue(&mut state, sent, path_snapshot);
                                sent
                            }
                            Err(err) if reliable_path_error_is_migratable(&err) => false,
                            Err(err) => break Err(err),
                        };
                        if remotes.is_empty() {
                            if let Some(disconnected) = state.recovery.disconnected.as_mut() {
                                disconnected.retry_after(reliable_relay_disconnected_retry_delay());
                            }
                        } else {
                            state.recovery.disconnected = None;
                            if let Err(err) = commit_pending_remote_fin(
                                &mut local,
                                &mut state,
                                &recv_stream,
                                progress_ready,
                            )
                            .await
                            {
                                break Err(err);
                            }
                        }
                    } else if state.recovery.pending_additional_path_opens.is_empty()
                        && let Some(disconnected) = state.recovery.disconnected.as_mut()
                    {
                        disconnected.retry_after(reliable_relay_disconnected_retry_delay());
                    }
                    continue;
                }
                _ = wait_for_optional_deadline(retention_deadline) => {
                    break Err(RuntimeError::SessionRetentionTimeout);
                }
            }
        } else {
            state.recovery.disconnected = None;
        }
        if !state.recovery.pending_additional_path_opens.is_empty()
            && drain_completed_additional_path_opens(
                context,
                &mut sender,
                stream_id,
                &mut remotes,
                &mut send_stream,
                !state.endpoint.local_open,
                &mut state.recovery.pending_additional_path_opens,
                &mut additional_path_open_rx,
                &mut state.progress.last_stream_at,
            )
            .await
        {
            // Membership changes precede the next immutable scheduling view.
            state.progress.sender_retry_at = None;
            send_stream.update_max_offset(remotes.max_offset());
        }
        let timing_path_snapshot =
            remotes.lowest_eta_path_snapshot(context, TrafficClass::Latency, PATH_OPEN_SCORE_BYTES);
        let demand_update = flow_demand.refresh(
            ReliableRelayFlowSignals::new(
                send_stream
                    .next_offset()
                    .saturating_add(sender_queue.data_bytes() as u64),
                recv_stream.next_offset(),
            )
            .with_product_work(
                sender_queue.data_bytes(),
                send_stream
                    .reinjection_bytes()
                    .saturating_add(recv_stream.reorder_bytes()),
            ),
            timing_path_snapshot,
            context.mux_limits,
        );
        let relay_lane = demand_update.lane;
        let request_observed_bytes = send_stream
            .next_offset()
            .saturating_add(sender_queue.data_bytes() as u64);
        let request_demand_update = request_flow_demand.refresh(
            ReliableRelayFlowSignals::new(request_observed_bytes, 0)
                .with_product_work(sender_queue.data_bytes(), send_stream.reinjection_bytes()),
            timing_path_snapshot,
            context.mux_limits,
        );
        let request_lane = request_demand_update.lane;
        let request_lane_changed =
            reliable_relay_lane_changed(request_demand_update.previous_lane, request_lane);
        if request_lane_changed {
            sender.withdraw_tcp_service_observer(context, TcpServiceWithdrawalReason::DemandEnded);
            state.refresh_request_tcp_service_demand(request_lane);
        }
        let path_snapshot =
            remotes.lowest_eta_path_snapshot(context, relay_lane, PATH_OPEN_SCORE_BYTES);
        update_request_path_staleness(
            &mut state,
            &mut sender,
            &mut sender_queue,
            context,
            &remotes,
            &send_stream,
            &[],
            stream_id,
        );
        #[cfg(feature = "lab-diagnostics")]
        if request_lane_changed {
            lab_diagnostic(
                "client_request_lane_changed",
                format_args!(
                    "stream_id={} previous={:?} lane={:?} observed_bytes={} product_rate_mbps={:.3} byte_proven={} rate_proven={} buffered_data={} relay_lane={:?}",
                    stream_id.0,
                    request_demand_update.previous_lane,
                    request_lane,
                    request_demand_update.observed_bytes,
                    request_demand_update.product_rate_bps / 1_000_000.0,
                    request_demand_update.byte_proven_bulk,
                    request_demand_update.rate_proven_sustained_bulk,
                    request_demand_update.buffered_bulk,
                    relay_lane,
                ),
            );
        }
        for failed_instance in sender.unreported_missing_owner_instances(
            &remotes,
            transport_pto_from_snapshot(path_snapshot),
        ) {
            if sender.enqueue_failed_path_reinjections(
                &mut sender_queue,
                context,
                &remotes,
                &send_stream,
                failed_instance,
            ) {
                state.progress.sender_retry_at = None;
            }
        }
        let request_reinjection_retry_after =
            sender.request_reinjection_retry_after(context, &remotes);
        for stale_instance in
            sender.stale_paths_requiring_reinjection(&remotes, request_reinjection_retry_after)
        {
            if sender.enqueue_stale_path_reinjections(
                &mut sender_queue,
                context,
                &remotes,
                &send_stream,
                stale_instance,
            ) {
                state.progress.sender_retry_at = None;
            }
        }
        if reliable_relay_lane_changed(demand_update.previous_lane, relay_lane) {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "client_stream_lane_changed",
                format_args!(
                    "stream_id={} previous={:?} lane={:?} sent_offset={} received_offset={} reinjection_bytes={} reorder_bytes={} byte_proven={} rate_proven={} buffered_data={}",
                    stream_id.0,
                    demand_update.previous_lane,
                    relay_lane,
                    send_stream.next_offset(),
                    recv_stream.next_offset(),
                    send_stream.reinjection_bytes(),
                    recv_stream.reorder_bytes(),
                    demand_update.byte_proven_bulk,
                    demand_update.rate_proven_sustained_bulk,
                    demand_update.buffered_bulk,
                ),
            );
            for key in remotes.load_owned_path_keys() {
                context.change_relay_path_lane_load(
                    key.underlay,
                    key.index,
                    demand_update.previous_lane,
                    relay_lane,
                );
            }
            remotes.set_lane(relay_lane);
        }
        if demand_update.preopen_additional_paths && !relay_lane.is_bulk() {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "client_stream_additional_path_open_due",
                format_args!(
                    "stream_id={} observed_bytes={} product_rate_mbps={:.3} attached_paths={}",
                    stream_id.0,
                    demand_update.observed_bytes,
                    demand_update.product_rate_bps / 1_000_000.0,
                    remotes.path_keys().len(),
                ),
            );
            if spawn_reliable_relay_additional_path_opens(
                context,
                &spec,
                TrafficClass::Throughput,
                &remotes,
                &send_stream,
                &mut state.recovery.pending_additional_path_opens,
                &additional_path_open_tx,
            ) {
                state.progress.last_stream_at = Instant::now();
            }
        }
        if flow_demand.should_rebalance(demand_update) {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "client_stream_rebalance_due",
                format_args!(
                    "stream_id={} lane={:?} promoted={} observed_bytes={} product_rate_mbps={:.3} interval_ms={:.3} attached_paths={}",
                    stream_id.0,
                    relay_lane,
                    demand_update.promoted_to_throughput,
                    demand_update.observed_bytes,
                    demand_update.product_rate_bps / 1_000_000.0,
                    demand_update.rebalance_interval.as_secs_f64() * 1000.0,
                    remotes.path_keys().len(),
                ),
            );
            flow_demand.mark_rebalance_attempted();
            if relay_lane.is_bulk() {
                if spawn_reliable_relay_additional_path_opens(
                    context,
                    &spec,
                    relay_lane,
                    &remotes,
                    &send_stream,
                    &mut state.recovery.pending_additional_path_opens,
                    &additional_path_open_tx,
                ) {
                    state.progress.last_stream_at = Instant::now();
                }
            } else if let Err(err) = switch_reliable_relay_to_best_path(
                context,
                &mut sender,
                &spec,
                relay_lane,
                &mut remotes,
                &send_stream,
                !state.endpoint.local_open,
                ReliableRelayAttachMode::BulkStriping,
                &state.recovery.pending_additional_path_opens,
            )
            .await
            {
                crate::observability::process_event!(
                    Warn,
                    "reliable_relay",
                    "auto_path_attachment_failed",
                    "reliable auto path attachment failed: {err}"
                );
            } else {
                state.progress.last_stream_at = Instant::now();
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
        let request_outstanding_limit = reliable_relay_request_outstanding_limit_bytes(
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
                    "stream_id={} lane={:?} chunk_bytes={} inflight_bytes={} request_outstanding_limit_bytes={} session_send_buffer_used_bytes={} session_send_buffer_limit_bytes={} attached_paths={} path_snapshot={}",
                    stream_id.0,
                    relay_lane,
                    adaptive_chunk,
                    adaptive_inflight,
                    request_outstanding_limit,
                    context.session_send_buffer.used_bytes(),
                    context.session_send_buffer.limit_bytes(),
                    remotes.accepted_path_count(),
                    path_snapshot.is_some(),
                ),
            );
            last_reported_budget = Some((relay_lane, adaptive_chunk, adaptive_inflight));
        }
        let stall_watch_active = reliable_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            state.endpoint.remote_open,
            relay_lane,
            state.progress.interactive_response_pending,
            context.mux_limits,
        );
        let stall_progress_anchor = reliable_relay_stall_progress_anchor(
            state.progress.last_stream_at,
            state.progress.last_delivery_at,
            state.progress.last_response_stall_reinjection_at,
            &recv_stream,
            state.endpoint.remote_open,
            relay_lane,
            state.progress.interactive_response_pending,
            context.mux_limits,
        );
        let receive_hole_reinjection_active = reliable_relay_receive_hole_reinjection_active(
            &recv_stream,
            state.endpoint.remote_open,
        );
        let receive_hole_reinjection_deadline = reliable_relay_receive_hole_reinjection_deadline(
            state.progress.last_delivery_at,
            state.progress.last_receive_hole_reinjection_at,
            path_snapshot,
        );
        let stall_deadline = reliable_relay_product_stall_deadline(
            stall_progress_anchor,
            state.progress.last_product_stall_attempt_at,
            path_snapshot,
        );
        let recv_progress_deadline = tokio::time::Instant::from_std(
            state.progress.last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(path_snapshot),
        );
        let data_ack_reinjection_at = state.progress.data_ack_reinjection_at;
        if state
            .progress
            .sender_retry_at
            .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
        {
            state.progress.sender_retry_at = None;
        }
        sender.discard_unusable_tail_reinjections(&mut sender_queue, &remotes);
        if sender.discard_stale_persistent_ack_gap_reinjections(&mut sender_queue, &remotes) > 0 {
            state
                .progress
                .ack_gap_reinjection
                .release_reinjection_attempt();
            state.progress.sender_retry_at = None;
        }
        if sender.discard_resolved_stale_path_reinjections(&mut sender_queue, &remotes) > 0 {
            state.progress.sender_retry_at = None;
        }
        let inbound_frame_ready = deferred_remote_frame.is_some() || remotes.has_buffered_event();
        let queued_send_blocked = reliable_relay_queued_send_blocked_for_retry(
            sender_queue.is_empty(),
            state.progress.sender_retry_at,
        );
        let pending_remote_fin_ready =
            pending_stream_fin_ready(&recv_stream, state.endpoint.pending_remote_fin_offset);
        let final_feedback_retry_blocked = pending_remote_fin_ready
            && !remotes.is_empty()
            && state.progress.sender_retry_at.is_some();
        let carrier_retry_blocked = queued_send_blocked || final_feedback_retry_blocked;
        let queued_send_ready =
            !sender_queue.is_empty() && !queued_send_blocked && !inbound_frame_ready;
        let queued_send_retry_deadline = state
            .progress
            .sender_retry_at
            .unwrap_or_else(tokio::time::Instant::now);
        let carrier_capacity_notifies = if carrier_retry_blocked {
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
            state.endpoint.local_open,
            queued_send_blocked,
            &send_stream,
            &sender_queue,
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
                sender_queue_limit,
                source_read_ceiling,
            )
            .min(request_outstanding_headroom)
        } else {
            0
        };
        let can_read_local = !remotes.is_empty()
            && can_read_by_flow
            && prospective_read_budget > 0
            && !inbound_frame_ready;
        let can_send_pending_fin = reliable_relay_can_send_pending_fin(
            state.endpoint.pending_local_fin,
            sender_queue.is_empty(),
        );
        let terminal_fin_replay_ready = stream_terminal_fin_replay_required(
            state.endpoint.local_fin_sent,
            state.endpoint.terminal_fin_replayed,
            sender_queue.is_empty(),
        );
        #[cfg(feature = "lab-diagnostics")]
        {
            if state.endpoint.local_open && !can_read_local {
                let blocked_state = (
                    send_stream.reinjection_bytes(),
                    send_stream.send_credit_bytes(),
                    adaptive_inflight,
                    request_outstanding_limit,
                    request_outstanding_headroom,
                );
                if last_reported_read_block != Some(blocked_state) {
                    lab_diagnostic(
                        "relay_local_read_blocked",
                        format_args!(
                            "stream_id={} lane={:?} reinjection_bytes={} send_credit_bytes={} inflight_limit={} request_outstanding_limit={} request_outstanding_headroom={} sent_offset={} received_offset={}",
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
            _ = std::future::ready(()), if pending_remote_fin_ready
                && !remotes.is_empty()
                && state.progress.sender_retry_at.is_none() => {
                let feedback_published = match sender
                    .send_recv_progress(
                        &mut remotes,
                        context,
                        &mut recv_stream,
                        &mut state.progress.recv_progress,
                        RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                    )
                    .await
                {
                    Ok(sent) => {
                        record_final_recv_progress_enqueue(&mut state, sent, path_snapshot);
                        sent
                    }
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        state.progress.sender_retry_at = None;
                        continue;
                    }
                    Err(err) => break Err(err),
                };
                if let Err(err) = commit_pending_remote_fin(
                    &mut local,
                    &mut state,
                    &recv_stream,
                    feedback_published && !remotes.is_empty(),
                )
                .await
                {
                    break Err(err);
                }
            }
            _ = tokio::time::sleep_until(receive_hole_reinjection_deadline), if receive_hole_reinjection_active => {
                let recovery_path_open_spawned = spawn_reliable_relay_recovery_path_open(
                    context,
                    &spec,
                    relay_lane,
                    &remotes,
                    &send_stream,
                    &state.recovery.excluded_paths,
                    &mut state.recovery.pending_additional_path_opens,
                    &additional_path_open_tx,
                );
                state.progress.receive_hole_reinjection_attempts =
                    state.progress.receive_hole_reinjection_attempts.saturating_add(1);
                match sender
                    .send_recv_progress(
                        &mut remotes,
                        context,
                        &mut recv_stream,
                        &mut state.progress.recv_progress,
                        RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                    )
                    .await
                {
                    Ok(sent) => state.record_recv_progress_sent(sent),
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        state.progress.sender_retry_at = None;
                    }
                    Err(err) => break Err(err),
                }
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "receive_hole_reinjection_signal",
                    format_args!(
                        "stream_id={} lane={:?} reorder_bytes={} attempts={} recovery_path_open_spawned={} action=ack_progress_existing_paths",
                        stream_id.0,
                        relay_lane,
                        recv_stream.reorder_bytes(),
                        state.progress.receive_hole_reinjection_attempts,
                        recovery_path_open_spawned,
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = recovery_path_open_spawned;
                state.progress.last_receive_hole_reinjection_at = Instant::now();
            }
            _ = wait_for_optional_deadline(data_ack_reinjection_at), if data_ack_reinjection_at.is_some() => {
                state.progress.data_ack_reinjection_at = None;
                let reinjection = evaluate_client_data_ack_reinjection(
                    &mut state,
                    &mut sender,
                    &mut sender_queue,
                    context,
                    &remotes,
                    &send_stream,
                    path_snapshot,
                    relay_lane,
                    stream_id,
                );
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "data_ack_loss_timer",
                    format_args!(
                        "stream_id={} reinjection_frames={} ack_gap_reinjection_ready={} multipath_reinjection_alternative={} next_deadline_armed={}",
                        stream_id.0,
                        reinjection.frame_count,
                        reinjection.persistent_ready,
                        reinjection.has_multipath_alternative,
                        state.progress.data_ack_reinjection_at.is_some(),
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = reinjection;
            }
            _ = tokio::time::sleep_until(stall_deadline), if stall_watch_active => {
                let queued_existing_tail_reinjection = sender.enqueue_tail_reinjection(
                    &mut sender_queue,
                    context,
                    &remotes,
                    &send_stream,
                    state.progress.last_send_ack.ranges(),
                    state.progress.last_send_ack.complete(),
                    state.progress.last_send_ack.horizon(),
                    state.progress.last_send_ack_frontier,
                    relay_lane,
                );
                if queued_existing_tail_reinjection
                    || reliable_relay_product_stall_preserves_attached_path_set(&remotes)
                {
                    if queued_existing_tail_reinjection {
                        state.progress.sender_retry_at = None;
                    }
                    match sender.send_recv_progress(
                        &mut remotes,
                        context,
                        &mut recv_stream,
                        &mut state.progress.recv_progress,
                        RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                    )
                    .await
                    {
                        Ok(sent) => state.record_recv_progress_sent(sent),
                        Err(err) if reliable_path_error_is_migratable(&err) => {
                            state.progress.sender_retry_at = None;
                        }
                        Err(err) => break Err(err),
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "client_product_stall_keeps_attached_path_set",
                        format_args!(
                            "stream_id={} active_underlay={:?} attached_paths={} reinjection_bytes={} recv_reorder_bytes={} sent_offset={} cause=product_stall_only",
                            stream_id.0,
                            path_snapshot.map(|snapshot| snapshot.underlay),
                            remotes.path_keys().len(),
                            send_stream.reinjection_bytes(),
                            recv_stream.reorder_bytes(),
                            send_stream.next_offset(),
                        ),
                    );
                    state.progress.last_response_stall_reinjection_at = Instant::now();
                    state.progress.last_product_stall_attempt_at = Some(Instant::now());
                    continue;
                }
                if reliable_relay_product_stall_should_try_alternate_attach(&remotes) {
                    // Path establishment must not stop the live relay from
                    // consuming data or ACKs on its existing carrier.
                    let recovery_open_spawned = spawn_reliable_relay_recovery_path_open(
                        context,
                        &spec,
                        relay_lane,
                        &remotes,
                        &send_stream,
                        &state.recovery.excluded_paths,
                        &mut state.recovery.pending_additional_path_opens,
                        &additional_path_open_tx,
                    );
                    let attempted_at = Instant::now();
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        if recovery_open_spawned {
                            "client_product_stall_alternate_open_spawned"
                        } else {
                            "client_product_stall_keeps_sole_carrier"
                        },
                        format_args!(
                            "stream_id={} active_underlay={:?} attached_paths={} pending_opens={} reinjection_bytes={} sent_offset={} cause=product_stall_only",
                            stream_id.0,
                            path_snapshot.map(|snapshot| snapshot.underlay),
                            remotes.path_keys().len(),
                            state.recovery.pending_additional_path_opens.len(),
                            send_stream.reinjection_bytes(),
                            send_stream.next_offset(),
                        ),
                    );
                    state.progress.last_response_stall_reinjection_at = attempted_at;
                    state.progress.last_product_stall_attempt_at = Some(attempted_at);
                    if recovery_open_spawned {
                        state.progress.last_stream_at = attempted_at;
                    }
                    continue;
                }
                match sender.send_recv_progress(
                    &mut remotes,
                    context,
                    &mut recv_stream,
                    &mut state.progress.recv_progress,
                    RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                )
                .await
                {
                    Ok(sent) => state.record_recv_progress_sent(sent),
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        state.progress.sender_retry_at = None;
                        if remotes.is_empty() {
                            continue;
                        }
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &mut sender,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !state.endpoint.local_open,
                            ReliableRelayAttachMode::Any,
                            &mut state.recovery.excluded_paths,
                            &state.recovery.pending_additional_path_opens,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                send_stream.update_max_offset(remotes.max_offset());
                                match sender
                                    .send_recv_progress(
                                        &mut remotes,
                                        context,
                                        &mut recv_stream,
                                        &mut state.progress.recv_progress,
                                        RelayRecvProgressSend::new(
                                            path_snapshot,
                                            relay_lane,
                                            true,
                                        ),
                                    )
                                    .await
                                {
                                    Ok(sent) => state.record_recv_progress_sent(sent),
                                    Err(recovery_err)
                                        if reliable_path_error_is_migratable(&recovery_err) => {}
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
                        "stream_id={} active_underlay={:?} attached_paths={} reinjection_bytes={} recv_reorder_bytes={} sent_offset={} cause=product_stall_only",
                        stream_id.0,
                        path_snapshot.map(|snapshot| snapshot.underlay),
                        remotes.path_keys().len(),
                        send_stream.reinjection_bytes(),
                        recv_stream.reorder_bytes(),
                        send_stream.next_offset(),
                    ),
                );
                state.progress.last_response_stall_reinjection_at = Instant::now();
                state.progress.last_product_stall_attempt_at = Some(Instant::now());
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if remotes.path_keys().len() > 1
                && reliable_relay_recv_progress_resend_active(
                    &recv_stream,
                    state.endpoint.remote_open,
                    path_snapshot.map(|snapshot| snapshot.underlay),
                ) => {
                match sender.send_recv_progress(
                    &mut remotes,
                    context,
                    &mut recv_stream,
                    &mut state.progress.recv_progress,
                    RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                )
                .await
                {
                    Ok(sent) => {
                        if sent {
                            state.progress.last_stream_at = Instant::now();
                        }
                        state.progress.last_recv_progress_sent_at = Instant::now();
                    }
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        if remotes.is_empty() {
                            continue;
                        }
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &mut sender,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !state.endpoint.local_open,
                            ReliableRelayAttachMode::Any,
                            &mut state.recovery.excluded_paths,
                            &state.recovery.pending_additional_path_opens,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                state.progress.sender_retry_at = None;
                                send_stream.update_max_offset(remotes.max_offset());
                                state.progress.last_stream_at = Instant::now();
                                state.progress.last_recv_progress_sent_at = Instant::now();
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
                    Ok(_) => state.record_local_fin_sent(),
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        if remotes.is_empty() {
                            continue;
                        }
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &mut sender,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            true,
                            ReliableRelayAttachMode::Any,
                            &mut state.recovery.excluded_paths,
                            &state.recovery.pending_additional_path_opens,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                state.progress.sender_retry_at = None;
                                state.record_local_fin_sent();
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
                        state.record_terminal_fin_replayed();
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "terminal_fin_replay",
                            format_args!(
                                "stream_id={} final_offset={} ack_frontier={} reinjection_bytes={} role=client",
                                stream_id.0,
                                send_stream.next_offset(),
                                state.progress.last_send_ack_frontier,
                                send_stream.reinjection_bytes(),
                            ),
                        );
                    }
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        if remotes.is_empty() {
                            continue;
                        }
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &mut sender,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            true,
                            ReliableRelayAttachMode::Any,
                            &mut state.recovery.excluded_paths,
                            &state.recovery.pending_additional_path_opens,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                state.progress.sender_retry_at = None;
                                state.record_terminal_fin_replayed();
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
                    let dispatch = sender
                        .dispatch_client_queued_work(
                            context,
                            request_lane,
                            &mut remotes,
                            &mut send_stream,
                            &mut sender_queue,
                            reliable_relay_client_dispatch_payload_limit(
                                adaptive_chunk,
                                sender_dispatch_byte_budget
                                    .saturating_sub(dispatched_payload_bytes),
                            ),
                        )
                        .await;
                    match dispatch {
                        Ok(ClientQueuedDispatch::Data { payload_bytes }) => {
                            dispatched_items = dispatched_items.saturating_add(1);
                            dispatched_payload_bytes =
                                dispatched_payload_bytes.saturating_add(payload_bytes);
                            state.progress.last_stream_at = Instant::now();
                            state.delivery.total.record_payload_bytes(payload_bytes);
                        }
                        Ok(ClientQueuedDispatch::Reinjection { payload_bytes }) => {
                            let _ = payload_bytes;
                            dispatched_items = dispatched_items.saturating_add(1);
                            state.progress.last_stream_at = Instant::now();
                        }
                        Ok(ClientQueuedDispatch::ReinjectionDeferred) => {
                            dispatched_items = dispatched_items.saturating_add(1);
                        }
                        Ok(ClientQueuedDispatch::PersistentReinjectionCancelled) => {
                            state.progress.ack_gap_reinjection.release_reinjection_attempt();
                            state.progress.sender_retry_at = None;
                            dispatched_items = dispatched_items.saturating_add(1);
                        }
                        Ok(ClientQueuedDispatch::PathAttachmentRequired(err)) => {
                            if remotes.is_empty() {
                                state.progress.sender_retry_at = None;
                                break;
                            }
                            match attach_reliable_relay_paths_with_recovery_exclusions(
                                context,
                                &mut sender,
                                &spec,
                                relay_lane,
                                &mut remotes,
                                &send_stream,
                                !state.endpoint.local_open,
                                ReliableRelayAttachMode::Any,
                                &mut state.recovery.excluded_paths,
                                &state.recovery.pending_additional_path_opens,
                            )
                            .await
                            {
                                Ok(attached) if attached > 0 => {
                                    state.progress.sender_retry_at = None;
                                    continue;
                                }
                                Ok(_) => {
                                    dispatch_error = Some(err);
                                    break;
                                }
                                Err(attach_err) => {
                                    dispatch_error = Some(attach_err);
                                    break;
                                }
                            }
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
                    state.progress.sender_retry_at =
                        Some(tokio::time::Instant::now() + sender_service_retry_delay(path_snapshot));
                }
                if let Some(err) = dispatch_error {
                    break Err(err);
                }
                if dispatched_items > 0 && (state.endpoint.remote_open || send_stream.reinjection_bytes() > 0) {
                    tokio::task::yield_now().await;
                }
            }
            _ = tokio::time::sleep_until(queued_send_retry_deadline), if carrier_retry_blocked => {
                state.progress.sender_retry_at = None;
                continue;
            }
            _ = wait_for_carrier_capacity_notifies(carrier_capacity_notifies), if carrier_retry_blocked && has_carrier_capacity_notify => {
                state.progress.sender_retry_at = None;
                continue;
            }
            additional_path_open = additional_path_open_rx.recv(), if !state.recovery.pending_additional_path_opens.is_empty() => {
                let Some(additional_path_open) = additional_path_open else {
                    cancel_pending_additional_path_opens(stream_id, &mut state.recovery.pending_additional_path_opens);
                    continue;
                };
                if super::lifecycle::take_matching_additional_path_open(
                    &mut state.recovery.pending_additional_path_opens,
                    additional_path_open.key,
                    additional_path_open.generation,
                )
                .is_none()
                {
                    if let Ok(opened) = additional_path_open.result {
                        opened.close().await;
                    }
                    continue;
                }
                let attached_mode = handle_additional_path_open_result(
                    context,
                    &mut sender,
                    stream_id,
                    &mut remotes,
                    &mut send_stream,
                    !state.endpoint.local_open,
                    additional_path_open,
                    state.recovery.pending_additional_path_opens.len(),
                    &mut state.progress.last_stream_at,
                )
                .await;
                if attached_mode.is_some() {
                    state.progress.sender_retry_at = None;
                }
                if matches!(attached_mode, Some(ReliableRelayAttachMode::Recovery)) {
                    if sender.enqueue_tail_reinjection(
                        &mut sender_queue,
                        context,
                        &remotes,
                        &send_stream,
                        state.progress.last_send_ack.ranges(),
                        state.progress.last_send_ack.complete(),
                        state.progress.last_send_ack.horizon(),
                        state.progress.last_send_ack_frontier,
                        relay_lane,
                    ) {
                        state.progress.sender_retry_at = None;
                    }
                    match sender.send_recv_progress(
                        &mut remotes,
                        context,
                        &mut recv_stream,
                        &mut state.progress.recv_progress,
                        RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                    )
                    .await
                    {
                        Ok(sent) => state.record_recv_progress_sent(sent),
                        Err(err) if reliable_path_error_is_migratable(&err) => {
                            state.progress.sender_retry_at = None;
                        }
                        Err(err) => break Err(err),
                    }
                    let attempted_at = Instant::now();
                    state.progress.last_response_stall_reinjection_at = attempted_at;
                    state.progress.last_product_stall_attempt_at = Some(attempted_at);
                }
            }
            read = async {
                let read_budget = prospective_read_budget;
                let permit = context
                    .session_send_buffer
                    .reserve(&mut send_buffer_updates, read_budget)
                    .await;
                let reserved_read_budget = permit.bytes();
                #[cfg(feature = "lab-diagnostics")]
                let read_started = Instant::now();
                let result = read_reliable_relay_payload(
                    &mut local,
                    &mut buf,
                    reserved_read_budget,
                )
                .await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok((read, _)) = &result {
                    lab_perf_record("relay.local_read_wait", read_started.elapsed(), *read);
                }
                (result, permit)
            }, if can_read_local => {
                let (read, permit) = read;
                let (read, payload) = match read {
                    Ok(read) => read,
                    Err(err) => break Err(RuntimeError::Io(err)),
                };
                permit.retain(&mut send_buffer_reservation, read);
                if read == 0 {
                    sender.withdraw_tcp_service_observer(
                        context,
                        TcpServiceWithdrawalReason::DemandEnded,
                    );
                    state.record_local_eof();
                } else {
                    state.record_local_payload(relay_lane);
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
                    while state.endpoint.local_open {
                        let next_read_budget = reliable_relay_client_opportunistic_read_budget(
                            opportunistic_reads,
                            &send_stream,
                            &sender_queue,
                            ClientOpportunisticReadBounds {
                                sender_dispatch_byte_budget,
                                sender_dispatch_item_budget,
                                sender_queue_limit,
                                source_read_ceiling,
                                request_outstanding_limit,
                            },
                        );
                        if next_read_budget == 0 {
                            break;
                        }
                        let Some(read) = ready_at_entry(async {
                                let permit = context
                                    .session_send_buffer
                                    .reserve(&mut send_buffer_updates, next_read_budget)
                                    .await;
                                let result = read_reliable_relay_payload(
                                    &mut local,
                                    &mut buf,
                                    permit.bytes(),
                                )
                                .await;
                                (result, permit)
                            })
                            .await
                        else {
                            break;
                        };
                        let (read, permit) = read;
                        let (read, payload) = read.map_err(RuntimeError::Io)?;
                        permit.retain(&mut send_buffer_reservation, read);
                        if read == 0 {
                            sender.withdraw_tcp_service_observer(
                                context,
                                TcpServiceWithdrawalReason::DemandEnded,
                            );
                            state.record_local_eof();
                            break;
                        }
                        state.record_local_payload(relay_lane);
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
                let result = match deferred_remote_frame.take() {
                    Some(frame) => Ok(RequestRelayActorEvent::Frame(frame)),
                    None => remotes.recv_event().await,
                };
                #[cfg(feature = "lab-diagnostics")]
                if let Ok(RequestRelayActorEvent::Frame(ReliableRelayRemoteFrame {
                    frame: Ok(frame),
                    ..
                })) = &result
                {
                    lab_perf_record(
                        "relay.path_recv_frame_wait",
                        recv_started.elapsed(),
                        reliable_path_frame_pacing_bytes(frame),
                    );
                }
                result
            } => {
                let event = match frame {
                    Ok(event) => event,
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &mut sender,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !state.endpoint.local_open,
                            ReliableRelayAttachMode::Any,
                            &mut state.recovery.excluded_paths,
                            &state.recovery.pending_additional_path_opens,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                state.progress.sender_retry_at = None;
                                state.progress.last_stream_at = Instant::now();
                                continue;
                            }
                            Ok(_) => {
                                if state.is_finished(&send_stream, &recv_stream, &sender_queue) {
                                    break Ok(state.delivery.total);
                                }
                                break Err(err);
                            }
                            Err(_attach_err) => {
                                if state.is_finished(&send_stream, &recv_stream, &sender_queue) {
                                    break Ok(state.delivery.total);
                                }
                                break Err(err);
                            }
                        }
                    }
                    Err(err) => {
                        if state.is_finished(&send_stream, &recv_stream, &sender_queue) {
                            break Ok(state.delivery.total);
                        }
                        break Err(err);
                    }
                };
                let ReliableRelayRemoteFrame { instance, frame } = match event {
                    RequestRelayActorEvent::TcpService(control) => {
                        apply_request_tcp_service_control(
                            &state,
                            &mut sender,
                            &sender_queue,
                            context,
                            &remotes,
                            *control,
                        );
                        continue;
                    }
                    RequestRelayActorEvent::Frame(frame)
                        if state.endpoint.remote_open
                            || send_stream.reinjection_bytes() > 0 =>
                    {
                        frame
                    }
                    RequestRelayActorEvent::Frame(_) => continue,
                };
                let path_key = instance.key;
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "client_path_frame_error",
                            format_args!(
                                "stream_id={} path_underlay={:?} path_index={} path_instance_id={:?} attachment_id={} error={}",
                                stream_id.0,
                                instance.key.underlay,
                                instance.key.index,
                                instance.path_instance_id,
                                instance.attachment_id,
                                err,
                            ),
                        );
                        sender
                            .fail_client_path_instance(context, &mut remotes, instance)
                            .await;
                        state.recovery.excluded_paths.insert(path_key);
                        match recover_reliable_relay_after_path_failure(
                            &mut sender,
                            &mut sender_queue,
                            context,
                            &mut remotes,
                            &mut send_stream,
                            instance,
                        )
                        .await
                        {
                            Ok(Some(reinjection_queued)) => {
                                state.progress.last_stream_at = Instant::now();
                                state.progress.last_response_stall_reinjection_at = Instant::now();
                                if reinjection_queued {
                                    state.progress.sender_retry_at = None;
                                }
                            }
                            Ok(None) => {}
                            Err(err) => {
                                crate::observability::process_event!(
                                    Warn,
                                    "reliable_relay",
                                    "survivor_recovery_failed",
                                    "reliable path-error survivor recovery failed: {err}"
                                );
                            }
                        }
                        continue;
                    }
                    Err(err) => break Err(err),
                };
                state.progress.sender_retry_at = None;
                match frame {
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        payload,
                    } if received_stream_id == stream_id && state.endpoint.remote_open => {
                        let ready_items = if recv_stream.reorder_bytes() == 0 {
                            remotes.ready_frame_count()
                        } else {
                            0
                        };
                        let receive_limit = state
                            .endpoint
                            .pending_remote_fin_offset
                            .unwrap_or(u64::MAX)
                            .min(recv_stream.published_max_offset());
                        let payload_limit = reliable_relay_buffer_len(context.mux_limits)
                            .min(remotes.max_frame_payload_bytes(context.mux_limits))
                            .max(1);
                        let first = ReliableRelayRemoteFrame {
                            instance,
                            frame: Ok(Frame::StreamData {
                                stream_id: received_stream_id,
                                offset,
                                payload,
                            }),
                        };
                        let deferred = collect_ready_stream_data_batch(
                            &mut ready_remote_data,
                            first,
                            ReadyStreamDataBatchBounds {
                                stream_id,
                                receive_frontier: recv_stream.next_offset(),
                                receive_limit,
                                payload_limit,
                                ready_items,
                            },
                            || remotes.try_recv_frame(),
                            |item| match &item.frame {
                                Ok(Frame::StreamData {
                                    stream_id,
                                    offset,
                                    payload,
                                }) => Some((*stream_id, *offset, payload.len())),
                                _ => None,
                            },
                        );
                        debug_assert!(deferred_remote_frame.is_none());
                        deferred_remote_frame = deferred;
                        let mut data_effect = None;
                        let write = apply_and_write_ready_stream_data_batch(
                            &mut local,
                            &mut recv_stream,
                            &mut ready_remote_data,
                            ReadyStreamDataDirection::ClientDownload,
                            true,
                            |recv_stream, item| {
                                let ReliableRelayRemoteFrame { instance, frame } = item;
                                let frame = frame?;
                                let Frame::StreamData {
                                    stream_id: received_stream_id,
                                    offset,
                                    payload,
                                } = frame
                                else {
                                    unreachable!("ready data batch contains only STREAM_DATA");
                                };
                                debug_assert_eq!(received_stream_id, stream_id);
                                let (effect, outcome) = apply_client_stream_data_state(
                                    &mut state,
                                    context,
                                    recv_stream,
                                    stream_id,
                                    instance.key,
                                    offset,
                                    payload,
                                )?;
                                data_effect = Some(effect);
                                Ok(outcome)
                            },
                        )
                        .await;
                        if let Err(err) = write {
                            break Err(err);
                        }
                        let data_effect =
                            data_effect.expect("ready data batch contains its first frame");
                        match sender.send_recv_progress(
                            &mut remotes,
                            context,
                            &mut recv_stream,
                            &mut state.progress.recv_progress,
                            RelayRecvProgressSend::new(path_snapshot, relay_lane, false),
                        )
                        .await
                        {
                            Ok(sent) => state.record_recv_progress_sent(sent),
                            Err(err) if reliable_path_error_is_migratable(&err) => {
                                if remotes.is_empty() {
                                    continue;
                                }
                                match attach_reliable_relay_paths_with_recovery_exclusions(
                                    context,
                                    &mut sender,
                                    &spec,
                                    relay_lane,
                                    &mut remotes,
                                    &send_stream,
                                    !state.endpoint.local_open,
                                    ReliableRelayAttachMode::Any,
                                    &mut state.recovery.excluded_paths,
                                    &state.recovery.pending_additional_path_opens,
                                )
                                .await
                                {
                                    Ok(attached) if attached > 0 => {
                                        state.progress.sender_retry_at = None;
                                        state.progress.last_stream_at = Instant::now();
                                    }
                                    Ok(_) => break Err(err),
                                    Err(err) => break Err(err),
                                }
                            }
                            Err(err) => break Err(err),
                        }
                        if data_effect.fin_ready {
                            let feedback_published = match sender.send_recv_progress(
                                &mut remotes,
                                context,
                                &mut recv_stream,
                                &mut state.progress.recv_progress,
                                RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                            )
                            .await
                            {
                                Ok(sent) => {
                                    record_final_recv_progress_enqueue(
                                        &mut state,
                                        sent,
                                        path_snapshot,
                                    );
                                    sent
                                }
                                Err(err) if reliable_path_error_is_migratable(&err) => false,
                                Err(err) => break Err(err),
                            };
                            if let Err(err) = commit_pending_remote_fin(
                                &mut local,
                                &mut state,
                                &recv_stream,
                                feedback_published && !remotes.is_empty(),
                            )
                            .await
                            {
                                break Err(err);
                            }
                        }
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        complete,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        let ack = ClientStreamAckOperation {
                            state: &mut state,
                            sender_queue: &mut sender_queue,
                            context,
                            remotes: &remotes,
                            send_stream: &mut send_stream,
                            path_snapshot,
                            relay_lane,
                            send_buffer_reservation: &mut send_buffer_reservation,
                            stream_id,
                            complete,
                            ranges,
                        };
                        if let Err(err) = sender.with_tcp_service_ack_transaction(
                            ack,
                            apply_ordinary_client_stream_ack,
                            apply_observed_client_stream_ack,
                        ) {
                            break Err(err.into());
                        }
                        if reliable_relay_can_send_pending_fin(
                            state.endpoint.pending_local_fin,
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
                                Ok(_) => state.record_local_fin_sent(),
                                Err(err) if reliable_path_error_is_migratable(&err) => {
                                    if remotes.is_empty() {
                                        continue;
                                    }
                                    match attach_reliable_relay_paths_with_recovery_exclusions(
                                        context,
                                        &mut sender,
                                        &spec,
                                        relay_lane,
                                        &mut remotes,
                                        &send_stream,
                                        true,
                                        ReliableRelayAttachMode::Any,
                                        &mut state.recovery.excluded_paths,
                                        &state.recovery.pending_additional_path_opens,
                                    )
                                    .await
                                    {
                                        Ok(attached) if attached > 0 => {
                                            state.progress.sender_retry_at = None;
                                            state.record_local_fin_sent();
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
                        state.progress.last_stream_at = Instant::now();
                    }
                    Frame::StreamFin {
                        stream_id: fin_stream_id,
                        final_offset,
                    } if fin_stream_id == stream_id => {
                        state.progress.last_stream_at = Instant::now();
                        #[cfg(feature = "lab-diagnostics")]
                        let receive_frontier = recv_stream.next_offset();
                        let fin_ready = match receive_stream_fin(
                            &recv_stream,
                            &mut state.endpoint.pending_remote_fin_offset,
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
                                state.endpoint.pending_remote_fin_offset,
                                fin_ready,
                            ),
                        );
                        if fin_ready {
                            state.progress.last_delivery_at = Instant::now();
                            let feedback_published = match sender.send_recv_progress(
                                &mut remotes,
                                context,
                                &mut recv_stream,
                                &mut state.progress.recv_progress,
                                RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                            )
                            .await
                            {
                                Ok(sent) => {
                                    record_final_recv_progress_enqueue(
                                        &mut state,
                                        sent,
                                        path_snapshot,
                                    );
                                    sent
                                }
                                Err(err) if reliable_path_error_is_migratable(&err) => false,
                                Err(err) => break Err(err),
                            };
                            if let Err(err) = commit_pending_remote_fin(
                                &mut local,
                                &mut state,
                                &recv_stream,
                                feedback_published && !remotes.is_empty(),
                            )
                            .await
                            {
                                break Err(err);
                            }
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
                                &mut recv_stream,
                                &mut state.progress.recv_progress,
                                RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                            )
                        .await
                        {
                            Ok(sent) => state.record_recv_progress_sent(sent),
                            Err(err) if reliable_path_error_is_migratable(&err) => {}
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
            else => break Ok(state.delivery.total),
        }
    };

    sender.withdraw_tcp_service_observer(context, TcpServiceWithdrawalReason::DemandEnded);
    drop(tcp_service_writer_registration);
    let _ = drain_completed_additional_path_opens(
        context,
        &mut sender,
        stream_id,
        &mut remotes,
        &mut send_stream,
        !state.endpoint.local_open,
        &mut state.recovery.pending_additional_path_opens,
        &mut additional_path_open_rx,
        &mut state.progress.last_stream_at,
    )
    .await;
    cancel_pending_additional_path_opens(
        stream_id,
        &mut state.recovery.pending_additional_path_opens,
    );

    let remaining_paths = remotes
        .paths
        .iter()
        .map(ReliableRelayRemotePath::key)
        .collect::<Vec<_>>();
    if result.is_ok() {
        for (key, path_stats) in std::mem::take(&mut state.delivery.by_path) {
            context.mark_relay_path_delivery(key.underlay, key.index, path_stats);
        }
    }
    // Successful teardown stays behind ordered FIN work. A failed local
    // product socket is terminal, while carrier failures retain detach-only
    // semantics so the logical stream can survive path recovery.
    match &result {
        Ok(_) => remotes.close_all_ordered().await,
        Err(RuntimeError::Io(_)) => remotes.reset_all(ResetReason::RemoteClosed).await,
        Err(_) => remotes.close_all().await,
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "client_relay_result",
        format_args!(
            "stream_id={} ok={} local_open={} remote_open={} pending_local_fin={} pending_remote_fin_offset={:?} recv_next_offset={} recv_reorder_bytes={} sender_queue_bytes={} send_reinjection_bytes={} payload_bytes={}",
            stream_id.0,
            result.is_ok(),
            state.endpoint.local_open,
            state.endpoint.remote_open,
            state.endpoint.pending_local_fin,
            state.endpoint.pending_remote_fin_offset,
            recv_stream.next_offset(),
            recv_stream.reorder_bytes(),
            sender_queue.bytes(),
            send_stream.reinjection_bytes(),
            state.delivery.total.payload_bytes,
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
    for key in remaining_paths {
        if relay_error_is_tcp_path_failure(&result) {
            context.mark_relay_path_failure(key.underlay, key.index);
        }
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("multipath_stream_close");
    if result.is_ok() {
        telemetry_flow.complete();
    }
    result
}

#[cfg(test)]
#[path = "control_test.rs"]
mod tests;
