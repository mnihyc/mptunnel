use super::client::{
    ClientRelayDisconnectedState, ClientRelayPathOpenSuppressions, ClientRelayState,
    ClientStreamAckContext, apply_client_stream_ack, apply_client_stream_data_state,
    evaluate_client_data_ack_reinjection, update_request_path_staleness,
};
use super::diagnostics::log_unexpected_stream_relay_frame;
use super::flow::{
    ReliableRelayFlowDemandTracker, ReliableRelayFlowPathEvidence, ReliableRelayFlowSignals,
};
use super::io::{
    AuthoritativeStreamAckSnapshot, ReadyStreamDataBatchBounds, ReadyStreamDataDirection,
    accepted_copy_wake_is_due, apply_ready_stream_data_batch, collect_ready_stream_data_batch,
    pending_stream_fin_ready, read_reliable_relay_payload, receive_stream_fin,
    reconcile_accepted_copy_wake, resize_reliable_relay_buffer, retain_accepted_copy_wake,
    stream_ack_ranges_expose_authoritative_gap, stream_data_range_already_delivered,
    stream_terminal_fin_replay_required, write_applied_ready_stream_data_batch,
};
use super::lifecycle::{
    ClientReliableReturnPlan, RelayAdditionalPathOpenResult,
    begin_reliable_relay_attach_with_suppressions, cancel_pending_additional_path_opens,
    matching_additional_path_open_pending, reliable_relay_can_send_pending_fin,
    reliable_relay_disconnected_retry_delay, reliable_relay_lane_changed,
    reliable_relay_product_stall_deadline,
    reliable_relay_product_stall_preserves_attached_path_set,
    reliable_relay_product_stall_should_try_alternate_attach,
    reliable_relay_queued_send_blocked_for_retry, reliable_relay_receive_hole_reinjection_active,
    reliable_relay_receive_hole_reinjection_deadline, reliable_relay_response_stall_watch_active,
    reliable_relay_stall_progress_anchor, reliable_relay_stall_watch_active,
    settle_client_return_plan_open_result, spawn_reliable_relay_additional_path_opens,
    spawn_reliable_relay_disconnected_path_open, spawn_reliable_relay_recovery_path_open,
    spawn_reliable_relay_response_startup_path_opens, try_drain_completed_additional_path_opens,
    try_handle_additional_path_open_result,
};
use super::open::ReliableRelayOpenSpec;
use super::remote::{
    ReliableRelayAttachInput, ReliableRelayAttachMode, ReliableRelayAttachPlan,
    ReliableRelayPathLanes,
};
use super::service::{RelayServiceEvent, RelayServiceTurn};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_flush, lab_perf_record};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, adaptive_reliable_relay_chunk_bytes,
    adaptive_reliable_relay_chunk_bytes_with_frame_limit, reliable_relay_buffer_len,
    reliable_relay_sender_dispatch_budget, reliable_stream_initial_advertised_window_bytes,
};
use crate::model::multipath::{LiveOwnerRecoveryWake, live_owner_recovery_wake};
use crate::model::timing::{sender_service_retry_delay, transport_pto_from_snapshot};
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::performance::MppPerformanceConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{Frame, OffsetRange, PathUsage, ResetReason};
use crate::runtime::error::{RuntimeError, reliable_path_error_is_migratable};
use crate::runtime::path::commands::reliable_stream_frame_queue;
use crate::runtime::path::prepared::PreparedOriginalRegistration;
use crate::runtime::path::quic::client::ClientUdpErrorDisposition;
use crate::runtime::path::{ClientPathContext, PathDeliveryStats};
use crate::runtime::product_lifecycle::{ProductFlowActivity, ProductFlowActivityIo};
use crate::runtime::sender::{
    ClientQueuedDispatch, RelayRecvProgressSend, RelaySendCause, ReliableRelaySenderQueue,
    RequestPreparedSource, RequestProductState, RequestSenderService, SharedRequestProduct,
    reliable_relay_can_read_product_source, reliable_relay_sender_queue_limit,
    reliable_relay_sender_queue_read_budget,
};
use crate::runtime::stream::StreamFeedbackPublication;
use crate::runtime::stream::{
    OpenedRemoteStream, ReliablePathStreamOutput, ReliableRelayOpenedStartup,
    ReliableRelayRemoteFrame, ReliableRelayRemoteSet, ReliableRelayReturnPlan,
    RequalificationAttempt, arm_carrier_capacity_notifies, wait_for_carrier_capacity_notifies,
};
use crate::runtime::stream::{
    reliable_relay_recv_progress_resend_active, reliable_stream_recv_progress_interval,
};
use crate::runtime::telemetry::ObservedProductIo;
use crate::scheduler::TrafficClass;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

/// Apply a finite Input quantum without changing ACK transaction or credit order.
/// The caller retains Product ownership through its final fresh recovery and
/// queue-dependent postactions. The first item was selected as valid feedback;
/// all later non-feedback/error items are returned intact for normal handling.
fn apply_ready_client_feedback(
    first: Frame,
    stream_id: crate::protocol::StreamId,
    ready_items: usize,
    mut try_next: impl FnMut() -> Option<ReliableRelayRemoteFrame>,
    mut apply: impl FnMut(Frame) -> Result<(), RuntimeError>,
) -> Result<Option<ReliableRelayRemoteFrame>, RuntimeError> {
    let mut frame = first;
    let mut remaining = ready_items;
    loop {
        apply(frame)?;
        if remaining == 0 {
            return Ok(None);
        }
        remaining -= 1;
        let Some(item) = try_next() else {
            return Ok(None);
        };
        match item {
            ReliableRelayRemoteFrame {
                frame:
                    Ok(
                        next @ Frame::StreamAck {
                            stream_id: next_id, ..
                        },
                    ),
                ..
            }
            | ReliableRelayRemoteFrame {
                frame:
                    Ok(
                        next @ Frame::StreamMaxData {
                            stream_id: next_id, ..
                        },
                    ),
                ..
            } if next_id == stream_id => frame = next,
            barrier => return Ok(Some(barrier)),
        }
    }
}

async fn wait_for_optional_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Apply actual peer credit at its logical owner. Probe requirements never
/// enter this function: they may only compare with the resulting exact offset.
fn apply_client_peer_max_data(product: &mut RequestProductState, max_offset: u64) -> bool {
    let previous_credit = product.send_stream.send_credit_bytes();
    product.send_stream.update_max_offset(max_offset);
    product
        .remotes
        .observe_applied_peer_max_offset(product.send_stream.peer_max_offset());
    previous_credit != product.send_stream.send_credit_bytes()
}

async fn finish_request_attachment(
    context: &ClientPathContext,
    owner: &SharedRequestProduct,
    return_plan: &mut ClientReliableReturnPlan,
    mut attach: ReliableRelayAttachPlan,
) -> Result<usize, RuntimeError> {
    loop {
        let attempt = {
            let product = owner.lock();
            attach.next_attempt(context, &product.remotes, return_plan)?
        };
        let Some(attempt) = attempt else {
            return Ok(0);
        };
        let completion = attempt.open(context).await;
        let attached = {
            let mut product = owner.lock();
            attach.finish_attempt(&mut product.remotes, return_plan, completion)?
        };
        if attached {
            return Ok(1);
        }
    }
}

/// Publish weak, coalesced writer notices while the actor owns Product. This
/// reserves neither a target nor an offset: the native writer claims actual U.
pub(in crate::runtime) fn publish_prepared_request_work(
    product: &mut RequestProductState,
    owner: &SharedRequestProduct,
    context: &ClientPathContext,
    request_lane: TrafficClass,
    data_quantum_bytes: usize,
    work_changed: bool,
) {
    let prepared = &mut product.prepared;
    let mut changed = work_changed
        || prepared.request_lane != request_lane
        || prepared.data_quantum_bytes != data_quantum_bytes;
    prepared.request_lane = request_lane;
    prepared.data_quantum_bytes = data_quantum_bytes;
    let previous_len = prepared.registrations.len();
    prepared.registrations.retain(|registration| {
        prepared.claims_active
            && registration.lane() == request_lane
            && product.remotes.paths.iter().any(|path| {
                Some(path.instance()) == registration.request_instance()
                    && path.stream.product_admission_active()
            })
    });
    changed |= previous_len != prepared.registrations.len();
    if prepared.claims_active {
        for path in &product.remotes.paths {
            if !path.stream.product_admission_active()
                || prepared
                    .registrations
                    .iter()
                    .any(|registration| registration.request_instance() == Some(path.instance()))
            {
                continue;
            }
            let ReliablePathStreamOutput::Fixed(output) = &path.stream.output else {
                continue;
            };
            prepared
                .registrations
                .push(PreparedOriginalRegistration::new(
                    owner.downgrade(),
                    context.clone(),
                    product.remotes.stream_id(),
                    path.instance(),
                    output.commands().clone(),
                    request_lane,
                ));
            changed = true;
        }
    }
    if changed {
        prepared.work_changed.notify_waiters();
        if prepared.claims_active && product.sender_queue.data_bytes() > 0 {
            for registration in &prepared.registrations {
                registration.notify();
            }
        }
    }
}

/// Observe the shared source horizon using the actual successful claim clock,
/// never the time at which the actor happens to resume. A newer actor-owned
/// ACK/control progress event must survive observation of older claims.
pub(in crate::runtime) fn observe_prepared_request_claims(
    product: &RequestProductState,
    observed_claimed_offset: &mut u64,
    last_stream_at: &mut Instant,
) -> usize {
    let claimed_offset = product.send_stream.next_offset();
    if claimed_offset == *observed_claimed_offset {
        return 0;
    }
    let claimed_bytes = usize::try_from(claimed_offset.saturating_sub(*observed_claimed_offset))
        .unwrap_or(usize::MAX);
    *observed_claimed_offset = claimed_offset;
    let claimed_at = product
        .prepared
        .last_claimed_at
        .expect("advanced prepared source has a successful claim timestamp");
    *last_stream_at = (*last_stream_at).max(claimed_at);
    claimed_bytes
}

pub(in crate::runtime) fn request_retained_frontier_candidate(
    send_stream: &ReliableSendStream,
    remotes: &ReliableRelayRemoteSet,
) -> bool {
    send_stream.data_ack_frontier() < send_stream.next_offset()
        && send_stream.reinjection_bytes() > 0
        && remotes.path_keys().len() > 1
}

fn request_live_owner_tail_wake(
    retained_live_tail: bool,
    owner_fallback_deadline: Option<Instant>,
    epoch_deadline: Option<Instant>,
    observed_at: Instant,
) -> LiveOwnerRecoveryWake {
    // Preserve this actor's floor-only wake. Combining the owner and epoch
    // clocks prevents the generic cause branch from making a retained tail
    // independently actionable; broader cause wake is a separate policy.
    let floor_deadline = retained_live_tail
        .then_some(owner_fallback_deadline)
        .flatten()
        .map(|owner_deadline| {
            epoch_deadline.map_or(owner_deadline, |epoch_deadline| {
                owner_deadline.max(epoch_deadline)
            })
        });
    live_owner_recovery_wake(floor_deadline, None, observed_at)
}

fn reliable_relay_client_ack_gap_path_model_wait_active(
    authoritative_gap: bool,
    has_multipath_alternative: bool,
) -> bool {
    authoritative_gap && has_multipath_alternative
}

fn reliable_relay_client_ack_gap_capacity_wait_arm_active(
    authoritative_gap: bool,
    has_multipath_alternative: bool,
) -> bool {
    authoritative_gap && has_multipath_alternative
}

/// Arms one generation-backed reconciliation edge while the retained request
/// ledger contains an authoritative placement-staleness candidate. The
/// generation is captured before predicate observation, so a publication in
/// the read/arm interval makes the returned future immediately ready.
pub(super) fn arm_request_path_staleness_model_publication(
    context: &ClientPathContext,
    sender: &RequestSenderService,
    remotes: &ReliableRelayRemoteSet,
    gaps: &[OffsetRange],
    observed_generation: u64,
) -> Option<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>> {
    (!sender
        .unacked_original_paths_for_gaps(remotes, gaps)
        .is_empty())
    .then(|| context.arm_path_model_publication(observed_generation))
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

/// Completes the same remote-FIN transition after a retained receive ACK wins
/// carrier admission that the immediate FIN path completes on first publish.
fn commit_published_feedback_and_ready_fin<'a, S>(
    local: &'a mut S,
    state: &'a mut ClientRelayState,
    recv_stream: &'a mut ReliableRecvStream,
    remotes: &ReliableRelayRemoteSet,
    publication: StreamFeedbackPublication,
) -> impl Future<Output = Result<(), RuntimeError>> + use<'a, S>
where
    S: AsyncWrite + Unpin,
{
    if let Some(published_offset) = publication.max_data.published_offset {
        recv_stream.commit_max_data(published_offset);
    }
    state.record_recv_progress_sent(publication);
    let feedback_published = if publication.ack.published
        && pending_stream_fin_ready(recv_stream, state.endpoint.pending_remote_fin_offset)
    {
        state.progress.sender_retry_at = None;
        Some(remotes.has_receive_feedback_output())
    } else {
        None
    };
    // Publication is complete before this Product-free local-I/O future exists.
    async move {
        if let Some(feedback_published) = feedback_published {
            commit_pending_remote_fin(local, state, recv_stream, feedback_published).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
fn retry_stream_ack_and_commit_ready_fin<'a, S>(
    local: &'a mut S,
    state: &'a mut ClientRelayState,
    recv_stream: &'a mut ReliableRecvStream,
    remotes: &mut ReliableRelayRemoteSet,
) -> impl Future<Output = Result<(), RuntimeError>> + use<'a, S>
where
    S: AsyncWrite + Unpin,
{
    let publication = remotes.retry_pending_stream_ack();
    commit_published_feedback_and_ready_fin(local, state, recv_stream, remotes, publication)
}

fn client_relay_finished(
    state: &ClientRelayState,
    send_stream: &ReliableSendStream,
    recv_stream: &ReliableRecvStream,
    sender_queue: &ReliableRelaySenderQueue,
    remotes: &ReliableRelayRemoteSet,
) -> bool {
    state.is_finished(send_stream, recv_stream, sender_queue)
        && !remotes.has_pending_stream_ack_publication()
        && !remotes.has_pending_requalification_ack()
}

/// Arm before direct recovery observes target service. The actor retains this
/// exact future across a blocked Dispatch and its following select; rearming
/// there would lose notify_waiters edges between failed Apply and that select.
fn arm_request_recovery_service_wait(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
) -> impl Future<Output = ()> + Send + 'static {
    let generation = context.path_model_generation();
    let capacity = arm_carrier_capacity_notifies(
        remotes
            .paths
            .iter()
            .flat_map(|path| path.stream.capacity_notifies())
            .collect(),
    );
    let publication = context.arm_path_model_publication(generation);
    async move {
        tokio::select! {
            _ = async move {
                match capacity {
                    Some(wait) => wait.await,
                    None => std::future::pending().await,
                }
            } => {}
            _ = publication => {}
        }
    }
}

struct ClientRelayPathErrorSettlement {
    instance: crate::model::path::RelayPathInstance,
    retry_at: tokio::time::Instant,
}

fn begin_client_relay_path_error<'a>(
    context: &'a ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    instance: crate::model::path::RelayPathInstance,
    source: &'a RuntimeError,
) -> impl Future<Output = Option<ClientRelayPathErrorSettlement>> + use<'a> {
    let settlement = if matches!(source, RuntimeError::ReliablePathRetired) {
        remotes.retire_path_instance(instance);
        None
    } else {
        Some(ClientRelayPathErrorSettlement {
            instance,
            retry_at: tokio::time::Instant::now()
                + transport_pto_from_snapshot(
                    context.reliable_path_snapshot_for_instance(instance),
                ),
        })
    };
    // Only exact identity, its original retry time, and Native inputs cross
    // settlement I/O. Product withdrawal follows in a synchronous finish.
    async move {
        let settlement = settlement?;
        if instance.key.underlay == crate::protocol::UnderlayProtocol::Udp {
            // Operation-local evidence does not authorize carrier failure, but
            // settlement also observes an independently closed exact owner so a
            // concurrently dead connection cannot remain published.
            let disposition = context.udp_sessions[instance.key.index]
                .settle_established_error(instance.path_instance_id, source)
                .await;
            match disposition {
                ClientUdpErrorDisposition::Session => {
                    debug_assert!(
                        false,
                        "session-terminal QUIC error must bypass path-local recovery"
                    );
                    return None;
                }
                ClientUdpErrorDisposition::CarrierLifetime
                | ClientUdpErrorDisposition::Operation => {}
            }
        }
        Some(settlement)
    }
}

fn finish_client_relay_path_error(
    sender: &mut RequestSenderService,
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    path_open_suppressions: &mut ClientRelayPathOpenSuppressions,
    settlement: ClientRelayPathErrorSettlement,
) {
    let ClientRelayPathErrorSettlement { instance, retry_at } = settlement;
    let removed = if instance.key.underlay == crate::protocol::UnderlayProtocol::Udp {
        // This exact-instance PTO suppression belongs only to the affected
        // logical Product stream. It bounds immediate recovery retries without
        // fencing sibling streams; after the deadline, the same live carrier
        // may own a fresh attachment incarnation as RFC 8.1 permits.
        remotes.retire_path_instance(instance)
    } else {
        sender.fail_client_path_instance(context, remotes, instance)
    };
    if removed {
        path_open_suppressions.suppress(instance, retry_at);
    }
}

#[cfg(test)]
pub(in crate::runtime) async fn resolve_client_relay_path_error_for_test(
    sender: &mut RequestSenderService,
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    instance: crate::model::path::RelayPathInstance,
    source: &RuntimeError,
) -> bool {
    let mut path_open_suppressions = ClientRelayPathOpenSuppressions::default();
    let native_settlement = begin_client_relay_path_error(context, remotes, instance, source);
    if let Some(settlement) = native_settlement.await {
        finish_client_relay_path_error(
            sender,
            context,
            remotes,
            &mut path_open_suppressions,
            settlement,
        );
    }
    path_open_suppressions.blocks(context, instance.key, tokio::time::Instant::now())
}

fn record_final_recv_progress_enqueue(
    state: &mut ClientRelayState,
    publication: StreamFeedbackPublication,
    path: Option<crate::scheduler::PathSnapshot>,
) {
    state.record_recv_progress_sent(publication);
    if !publication.ack.published {
        // Admitting MAX or an older ACK tail does not publish this final ACK.
        // Keep FIN pending until an output accepts the complete latest ACK.
        state.progress.sender_retry_at =
            Some(tokio::time::Instant::now() + sender_service_retry_delay(path));
    }
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
    product_window_bytes: usize,
    mux_limits: crate::mux::MuxLimits,
) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let resource_ceiling = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(stream_window)
        .max(1);
    let configured_limit = if lane.is_bulk() {
        // Per-stream peer flow control remains path-independent. A separate
        // session owner bounds aggregate unique source bytes across streams.
        resource_ceiling
    } else {
        reliable_relay_buffer_len(mux_limits)
            .min(resource_ceiling)
            .max(payload_bytes.min(resource_ceiling))
            .max(1)
    };
    configured_limit.min(product_window_bytes)
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

fn reliable_relay_topology_lane(
    request_lane: TrafficClass,
    response_lane: TrafficClass,
) -> TrafficClass {
    if request_lane.is_bulk() || response_lane.is_bulk() {
        TrafficClass::Throughput
    } else {
        TrafficClass::Latency
    }
}

// Deferral owns the complete open result across an async control boundary;
// boxing would add allocation to every deferred additional-path open.
#[allow(clippy::large_enum_variant)]
enum PendingLocalWritePathOpen {
    Applied(Option<ReliableRelayAttachMode>),
    Deferred(RelayAdditionalPathOpenResult),
}

#[allow(clippy::too_many_arguments)]
fn drive_client_response_startup_control(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    return_plan: &mut ClientReliableReturnPlan,
    output_lane: TrafficClass,
    stream_id: crate::protocol::StreamId,
    response_frontier: u64,
    remotes: &mut ReliableRelayRemoteSet,
    state: &mut ClientRelayState,
    additional_path_open_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> Result<(), RuntimeError> {
    let response_startup_triggered = return_plan.observe_response_frontier(response_frontier);
    if return_plan.is_done() {
        remotes.clear_return_plan_final();
    }
    if response_startup_triggered
        && spawn_reliable_relay_response_startup_path_opens(
            context,
            spec,
            return_plan,
            output_lane,
            stream_id,
            &mut state.recovery.pending_additional_path_opens,
            additional_path_open_tx,
        )?
    {
        state.progress.last_stream_at = Instant::now();
    }
    if let Some(retained_ordinals) = return_plan.prepare_final(remotes).map(ToOwned::to_owned) {
        remotes.publish_return_plan_final(&retained_ordinals)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn settle_matching_client_additional_path_open(
    stream_id: crate::protocol::StreamId,
    state: &mut ClientRelayState,
    return_plan: &mut ClientReliableReturnPlan,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    output_lane: TrafficClass,
    additional_path_open: RelayAdditionalPathOpenResult,
    prepared_data_empty: bool,
) -> Result<Option<ReliableRelayAttachMode>, RuntimeError> {
    if let Some(error) = additional_path_open.terminal_error() {
        return Err(error);
    }
    if super::lifecycle::take_matching_additional_path_open(
        &mut state.recovery.pending_additional_path_opens,
        additional_path_open.key,
        additional_path_open.generation,
    )
    .is_none()
    {
        if let Ok(opened) = additional_path_open.result {
            opened.retire_uncommitted();
        }
        return Ok(None);
    }
    let additional_path_key = additional_path_open.key;
    let startup_ordinal = additional_path_open.startup_ordinal;
    let attached_mode = try_handle_additional_path_open_result(
        stream_id,
        remotes,
        send_stream,
        !state.endpoint.local_open && prepared_data_empty,
        output_lane,
        additional_path_open,
        state.recovery.pending_additional_path_opens.len(),
        &mut state.progress.last_stream_at,
    )?;
    settle_client_return_plan_open_result(
        return_plan,
        remotes,
        additional_path_key,
        startup_ordinal,
        attached_mode.is_some(),
    )?;
    if attached_mode.is_some() {
        state.progress.sender_retry_at = None;
    }
    Ok(attached_mode)
}

#[allow(clippy::too_many_arguments)]
fn apply_client_additional_path_open_postaction(
    attached_mode: Option<ReliableRelayAttachMode>,
    sender: &mut RequestSenderService,
    sender_queue: &mut ReliableRelaySenderQueue,
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    recv_stream: &mut ReliableRecvStream,
    state: &mut ClientRelayState,
    request_lane: TrafficClass,
    response_lane: TrafficClass,
) -> Result<(), RuntimeError> {
    if !matches!(attached_mode, Some(ReliableRelayAttachMode::Recovery)) {
        return Ok(());
    }
    if sender.enqueue_tail_reinjection(sender_queue, context, remotes, send_stream, request_lane) {
        state.progress.sender_retry_at = None;
    }
    let response_path_snapshot =
        remotes.lowest_eta_path_snapshot(context, response_lane, PATH_OPEN_SCORE_BYTES);
    match sender.send_recv_progress(
        remotes,
        context,
        recv_stream,
        &mut state.progress.recv_progress,
        RelayRecvProgressSend::new(response_path_snapshot, response_lane, true),
    ) {
        Ok(sent) => state.record_recv_progress_sent(sent),
        Err(err) if reliable_path_error_is_migratable(&err) => {
            state.progress.sender_retry_at = None;
        }
        Err(err) => return Err(err),
    }
    let attempted_at = Instant::now();
    state.progress.last_response_stall_reinjection_at = attempted_at;
    state.progress.last_product_stall_attempt_at = Some(attempted_at);
    Ok(())
}

pub(in crate::runtime) async fn relay_migrating_tcp_stream<S>(
    local: S,
    context: &ClientPathContext,
    performance: MppPerformanceConfig,
    spec: ReliableRelayOpenSpec,
    remote: OpenedRemoteStream,
    idle_timeout: Option<std::time::Duration>,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let retirement = context.session_retirement().wait();
    let active = relay_migrating_tcp_stream_active(
        local,
        context,
        performance,
        spec,
        remote,
        idle_timeout,
        #[cfg(test)]
        None,
    );
    tokio::pin!(retirement);
    tokio::pin!(active);
    tokio::select! {
        biased;
        reason = &mut retirement => Err(RuntimeError::RemoteClosed(reason)),
        result = &mut active => result,
    }
}

async fn relay_migrating_tcp_stream_active<S>(
    local: S,
    context: &ClientPathContext,
    performance: MppPerformanceConfig,
    spec: ReliableRelayOpenSpec,
    remote: OpenedRemoteStream,
    idle_timeout: Option<std::time::Duration>,
    #[cfg(test)] test_open_ingress: Option<super::lifecycle::BlockedWriteOpenTestIngress>,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _session_product_flow = context.reserve_session_product_flow()?;
    let opened_startup = match remote.startup().cloned() {
        Some(startup) => startup,
        None => {
            let key = crate::model::path::RelayPathKey {
                underlay: remote.stream().underlay,
                index: remote.path_index(),
            };
            ReliableRelayOpenedStartup {
                plan: Arc::new(ReliableRelayReturnPlan::new(
                    0,
                    PathUsage::Available,
                    vec![(key, Some(remote.path_instance_id()))],
                )?),
                opening_ordinal: 0,
                failed_ordinals: Vec::new(),
            }
        }
    };
    let spec = spec.with_startup_plan(opened_startup.plan.clone());
    let initial_lane = remote.stream().lane;
    let initial_recv_max_offset = reliable_stream_initial_advertised_window_bytes(
        remote.stream().underlay,
        initial_lane,
        context.mux_limits,
    );
    let (remotes, mut remote_input) =
        ReliableRelayRemoteSet::new(remote, reliable_stream_frame_queue(context.mux_limits));
    let opening_instance = remotes.paths[0].instance();
    let mut return_plan =
        ClientReliableReturnPlan::from_initial_open(opened_startup, opening_instance)?;
    let mut observed_request_membership_generation = remotes.membership_generation();
    let mut request_path_staleness_dirty = true;
    let mut request_recovery_dirty = true;
    let mut request_recovery_capacity_blocked = false;
    let mut request_recovery_service_wait: Option<
        std::pin::Pin<Box<dyn Future<Output = ()> + Send>>,
    > = None;
    let mut request_range_recovery_deadline = None::<Instant>;
    let mut request_requalification_capacity_wait = None;
    let mut accepted_copy_wake_at = None::<Instant>;
    let mut observed_stream_ack_generation = remotes.stream_ack_generation();
    let mut observed_feedback_max_offset = remotes.feedback_max_data_offset();
    let mut stream_ack_capacity_wait = None;
    let stream_id = remotes.stream_id();
    let telemetry_flow = context.telemetry.open_reliable_flow(
        Some(context.session_id),
        stream_id,
        spec.target.clone(),
    );
    let activity = ProductFlowActivity::new();
    let mut local = ObservedProductIo::new(
        ProductFlowActivityIo::new(local, activity.clone()),
        telemetry_flow.counter(),
    );
    let idle = activity.wait_until_idle(idle_timeout);
    tokio::pin!(idle);
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
    let sender = RequestSenderService::new_with_performance(stream_id, performance);
    let mut response_flow_demand = ReliableRelayFlowDemandTracker::with_initial_lane(initial_lane);
    let mut request_flow_demand = ReliableRelayFlowDemandTracker::with_initial_lane(initial_lane);
    let request_product = SharedRequestProduct::new(RequestProductState {
        sender_queue: ReliableRelaySenderQueue::default(),
        sender,
        send_stream,
        last_send_ack: AuthoritativeStreamAckSnapshot::default(),
        remotes,
        prepared: RequestPreparedSource::new(initial_lane, chunk_size),
    });
    let _request_product_lifetime = request_product.actor_lifetime();
    let mut observed_claimed_offset = 0u64;
    let mut prepared_work_changed = false;
    let mut deferred_remote_frame = None::<ReliableRelayRemoteFrame>;
    let mut service_turn = RelayServiceTurn::default();
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
    #[cfg(test)]
    if let Some(ingress) = test_open_ingress {
        ingress.install(
            &mut state.recovery.pending_additional_path_opens,
            additional_path_open_tx.clone(),
        );
    }
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_budget: Option<(TrafficClass, usize, usize)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_read_block: Option<(usize, usize, usize, usize, usize)> = None;
    let mut result = loop {
        let disconnected_wait = {
            let mut product_guard = request_product.lock();
            if let Some(error) = product_guard.prepared.pending_error.take() {
                break Err(error);
            }
            let claimed_bytes = observe_prepared_request_claims(
                &product_guard,
                &mut observed_claimed_offset,
                &mut state.progress.last_stream_at,
            );
            state.delivery.total.record_payload_bytes(claimed_bytes);
            let prepared_lane = product_guard.prepared.request_lane;
            let prepared_quantum = product_guard.prepared.data_quantum_bytes;
            publish_prepared_request_work(
                &mut product_guard,
                &request_product,
                context,
                prepared_lane,
                prepared_quantum,
                std::mem::take(&mut prepared_work_changed),
            );
            // Once true this is immutable: EOF forbids new U, and every remaining
            // byte has already been claimed into the exact final offset.
            let source_complete =
                !state.endpoint.local_open && product_guard.sender_queue.data_bytes() == 0;
            let (sender_queue, send_stream, remotes) = {
                let product = &*product_guard;
                (
                    &product.sender_queue,
                    &product.send_stream,
                    &product.remotes,
                )
            };
            if client_relay_finished(&state, send_stream, &recv_stream, sender_queue, remotes) {
                break Ok(state.delivery.total);
            }
            if remotes.is_empty() {
                let now = Instant::now();
                let now_async = tokio::time::Instant::now();
                let path_open_suppression_retry_at = state
                    .recovery
                    .path_open_suppressions
                    .next_retry_at(context, now_async);
                let disconnected = state
                    .recovery
                    .disconnected
                    .get_or_insert_with(|| ClientRelayDisconnectedState::new(now, now_async));
                if disconnected.expired(now, context.session_retention_timeout) {
                    break Err(RuntimeError::SessionRetentionTimeout);
                }
                let retention_deadline =
                    disconnected.retention_deadline(context.session_retention_timeout);
                let request_lane = request_flow_demand.current_lane();
                let response_lane = response_flow_demand.current_lane();
                let topology_lane = reliable_relay_topology_lane(request_lane, response_lane);
                if state.recovery.pending_additional_path_opens.is_empty()
                    && now_async >= disconnected.retry_at
                {
                    let spawned = spawn_reliable_relay_disconnected_path_open(
                        context,
                        &spec,
                        &mut return_plan,
                        ReliableRelayPathLanes::new(topology_lane, request_lane),
                        remotes,
                        send_stream,
                        &state.recovery.path_open_suppressions,
                        &mut disconnected.attempted_paths,
                        &mut state.recovery.pending_additional_path_opens,
                        &additional_path_open_tx,
                    );
                    if !spawned {
                        let ordinary_retry_at =
                            now_async + reliable_relay_disconnected_retry_delay();
                        disconnected.retry_at = path_open_suppression_retry_at
                            .map(|suppression_retry_at| suppression_retry_at.min(ordinary_retry_at))
                            .unwrap_or(ordinary_retry_at);
                    }
                }

                Some((
                    state.recovery.pending_additional_path_opens.is_empty(),
                    disconnected.retry_at,
                    retention_deadline,
                    request_lane,
                    response_lane,
                    source_complete,
                ))
            } else {
                state.recovery.disconnected = None;
                None
            }
        };
        if let Some((
            no_pending_opens,
            retry_at,
            retention_deadline,
            request_lane,
            response_lane,
            source_complete,
        )) = disconnected_wait
        {
            if no_pending_opens {
                tokio::select! {
                    _ = tokio::time::sleep_until(retry_at) => continue,
                    _ = wait_for_optional_deadline(retention_deadline) => {
                        break Err(RuntimeError::SessionRetentionTimeout);
                    }
                    () = &mut idle => break Err(RuntimeError::ProductIdleTimeout),
                }
            }

            tokio::select! {
                additional_path_open = additional_path_open_rx.recv() => {
                    let local_shutdown = {
                    let mut local_shutdown = None;
                    let mut product_guard = request_product.lock();
                    let (sender, send_stream, remotes) = {
                        let product = &mut *product_guard;
                        (&mut product.sender, &mut product.send_stream, &mut product.remotes)
                    };
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
                    let additional_path_key = additional_path_open.key;
                    let startup_ordinal = additional_path_open.startup_ordinal;
                    let attached = match try_handle_additional_path_open_result(
                        stream_id,
                        remotes,
                        send_stream,
                        source_complete,
                        request_lane,
                        additional_path_open,
                        state.recovery.pending_additional_path_opens.len(),
                        &mut state.progress.last_stream_at,
                    ) {
                        Ok(mode) => mode.is_some(),
                        Err(err) => break Err(err),
                    };
                    if let Err(err) = settle_client_return_plan_open_result(
                        &mut return_plan,
                        remotes,
                        additional_path_key,
                        startup_ordinal,
                        attached,
                    ) {
                        break Err(err);
                    }
                    if attached {
                        state.progress.sender_retry_at = None;
                        send_stream.update_max_offset(remotes.max_offset());
                        let path_snapshot = remotes.lowest_eta_path_snapshot(
                            context,
                            response_lane,
                            PATH_OPEN_SCORE_BYTES,
                        );
                        let recv_progress_send = if pending_stream_fin_ready(
                            &recv_stream,
                            state.endpoint.pending_remote_fin_offset,
                        ) {
                            RelayRecvProgressSend::final_ack(path_snapshot, response_lane)
                        } else {
                            RelayRecvProgressSend::new(path_snapshot, response_lane, true)
                        };
                        let progress_ready = match sender
                            .send_recv_progress(
                                remotes,
                                context,
                                &mut recv_stream,
                                &mut state.progress.recv_progress,
                                recv_progress_send,
                            )
                        {
                            Ok(sent) => {
                                record_final_recv_progress_enqueue(&mut state, sent, path_snapshot);
                                sent.ack.published
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
                            local_shutdown = Some(commit_pending_remote_fin(
                                &mut local,
                                &mut state,
                                &recv_stream,
                                progress_ready,
                            ));
                        }
                    } else if state.recovery.pending_additional_path_opens.is_empty()
                        && let Some(disconnected) = state.recovery.disconnected.as_mut()
                    {
                        disconnected.retry_after(reliable_relay_disconnected_retry_delay());
                    }
                    local_shutdown
                    };
                    if let Some(local_shutdown) = local_shutdown {
                        if let Err(error) = local_shutdown.await {
                            break Err(error);
                        }
                    }
                    continue;
                }
                _ = wait_for_optional_deadline(retention_deadline) => {
                    break Err(RuntimeError::SessionRetentionTimeout);
                }
                () = &mut idle => break Err(RuntimeError::ProductIdleTimeout),
            }
        }
        let (
            source_complete,
            path_model_generation_before_recovery_observation,
            response_lane,
            request_lane,
            topology_lane,
            path_snapshot,
            response_path_snapshot,
            accepted_copy_due,
            request_path_staleness_model_publication,
            request_path_staleness_model_wait_active,
            mut request_recovery_requested,
            request_requalification_capacity_blocked,
            request_path_recovery_deadline,
            pending_rebalance_attach,
            rebalance_requested,
        ) = {
            let mut product_guard = request_product.lock();
            let product = &mut *product_guard;
            let source_complete =
                !state.endpoint.local_open && product.sender_queue.data_bytes() == 0;
            let (sender_queue, sender, send_stream, last_send_ack, remotes) = (
                &mut product.sender_queue,
                &mut product.sender,
                &mut product.send_stream,
                &mut product.last_send_ack,
                &mut product.remotes,
            );
            let mut pending_rebalance_attach = None;
            let accepted_copy_due_before_topology =
                accepted_copy_wake_is_due(accepted_copy_wake_at, Instant::now());
            let completed_additional_path_attached = if !accepted_copy_due_before_topology
                && !state.recovery.pending_additional_path_opens.is_empty()
            {
                match try_drain_completed_additional_path_opens(
                    stream_id,
                    &mut return_plan,
                    remotes,
                    send_stream,
                    source_complete,
                    request_flow_demand.current_lane(),
                    &mut state.recovery.pending_additional_path_opens,
                    &mut additional_path_open_rx,
                    &mut state.progress.last_stream_at,
                ) {
                    Ok(attached) => attached,
                    Err(err) => break Err(err),
                }
            } else {
                false
            };
            if completed_additional_path_attached {
                // Membership changes precede the next immutable scheduling view.
                state.progress.sender_retry_at = None;
                send_stream.update_max_offset(remotes.max_offset());
            }
            // Capture before every path-model read used by ACK-gap recovery. The
            // generation-backed arm below then closes the publication/read/arm
            // race without making the relay poll shared path health.
            let path_model_generation_before_recovery_observation = context.path_model_generation();
            let timing_path_snapshot = remotes.lowest_eta_path_snapshot(
                context,
                TrafficClass::Latency,
                PATH_OPEN_SCORE_BYTES,
            );
            let response_demand_update = response_flow_demand.refresh(
                ReliableRelayFlowSignals::new(recv_stream.next_offset())
                    .with_product_work(0, recv_stream.reorder_bytes()),
                ReliableRelayFlowPathEvidence::timing_only(timing_path_snapshot),
                context.mux_limits,
            );
            let response_lane = response_demand_update.lane;
            let request_observed_bytes = send_stream
                .next_offset()
                .saturating_add(sender_queue.data_bytes() as u64);
            let request_demand_update = request_flow_demand.refresh(
                ReliableRelayFlowSignals::new(request_observed_bytes)
                    .with_product_work(sender_queue.data_bytes(), send_stream.reinjection_bytes()),
                ReliableRelayFlowPathEvidence::measured(timing_path_snapshot),
                context.mux_limits,
            );
            let request_lane = request_demand_update.lane;
            let request_lane_changed =
                reliable_relay_lane_changed(request_demand_update.previous_lane, request_lane);
            if request_lane_changed {
                request_path_staleness_dirty = true;
                request_recovery_dirty = true;
            }
            let topology_lane = reliable_relay_topology_lane(request_lane, response_lane);
            let path_snapshot =
                remotes.lowest_eta_path_snapshot(context, request_lane, PATH_OPEN_SCORE_BYTES);
            let response_path_snapshot =
                remotes.lowest_eta_path_snapshot(context, response_lane, PATH_OPEN_SCORE_BYTES);
            let request_membership_generation = remotes.membership_generation();
            let request_membership_changed =
                request_membership_generation != observed_request_membership_generation;
            if request_membership_changed {
                observed_request_membership_generation = request_membership_generation;
                request_path_staleness_dirty = true;
                request_recovery_dirty = true;
            }
            let stream_ack_generation = remotes.stream_ack_generation();
            let feedback_max_offset = remotes.feedback_max_data_offset();
            remotes.observe_applied_peer_max_offset(send_stream.peer_max_offset());
            let feedback_route_changed = remotes.prepare_feedback_route(context);
            if request_membership_changed
                || stream_ack_generation != observed_stream_ack_generation
                || feedback_max_offset != observed_feedback_max_offset
                || feedback_route_changed
            {
                observed_stream_ack_generation = stream_ack_generation;
                observed_feedback_max_offset = feedback_max_offset;
                // The old wait does not cover a replacement attachment or a
                // newly retained ACK/MAX state on a previously caught-up output.
                stream_ack_capacity_wait = None;
            }
            let accepted_copy_observation =
                sender.earliest_reinjection_suppression_deadline(remotes);
            let accepted_copy_due = reconcile_accepted_copy_wake(
                &mut accepted_copy_wake_at,
                accepted_copy_observation,
                Instant::now(),
            );
            if accepted_copy_due {
                request_recovery_dirty = true;
            }
            let request_path_staleness_due = state
                .progress
                .request_path_staleness
                .next_deadline()
                .is_some_and(|deadline| deadline <= Instant::now());
            if request_path_staleness_dirty || request_path_staleness_due {
                if update_request_path_staleness(
                    &mut state,
                    last_send_ack,
                    sender,
                    context,
                    remotes,
                    &[],
                    request_lane,
                    stream_id,
                ) {
                    request_recovery_dirty = true;
                    prepared_work_changed = true;
                }
                request_path_staleness_dirty = false;
            }
            let request_path_staleness_deadline = state
                .progress
                .request_path_staleness
                .next_deadline()
                .map(tokio::time::Instant::from_std);
            let request_path_staleness_model_publication =
                arm_request_path_staleness_model_publication(
                    context,
                    sender,
                    remotes,
                    last_send_ack.gaps(),
                    path_model_generation_before_recovery_observation,
                );
            let request_path_staleness_model_wait_active =
                request_path_staleness_model_publication.is_some();
            #[cfg(feature = "lab-diagnostics")]
            if request_lane_changed {
                lab_diagnostic(
                    "client_request_lane_changed",
                    format_args!(
                        "stream_id={} previous={:?} lane={:?} observed_bytes={} product_rate_mbps={:.3} byte_proven={} rate_proven={} buffered_data={} request_lane={:?}",
                        stream_id.0,
                        request_demand_update.previous_lane,
                        request_lane,
                        request_demand_update.observed_bytes,
                        request_demand_update.product_rate_bps / 1_000_000.0,
                        request_demand_update.byte_proven_bulk,
                        request_demand_update.rate_proven_sustained_bulk,
                        request_demand_update.buffered_bulk,
                        request_lane,
                    ),
                );
            }
            let request_range_recovery_due =
                request_range_recovery_deadline.is_some_and(|deadline| deadline <= Instant::now());
            let request_recovery_requested = request_recovery_dirty || request_range_recovery_due;
            if request_recovery_requested {
                state.progress.sender_retry_at = None;
            }
            if request_requalification_capacity_wait.is_none() {
                let request_requalification_attempt = match sender.try_send_requalification_probe(
                    context,
                    remotes,
                    send_stream,
                    request_lane,
                ) {
                    Ok(attempt) => attempt,
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        RequalificationAttempt::Idle
                    }
                    Err(err) => break Err(err),
                };
                request_requalification_capacity_wait =
                    request_requalification_attempt.into_capacity_wait();
            }
            let request_requalification_capacity_blocked =
                request_requalification_capacity_wait.is_some();
            let request_range_reinjection_deadline =
                request_range_recovery_deadline.map(tokio::time::Instant::from_std);
            let request_requalification_deadline = (!request_requalification_capacity_blocked)
                .then(|| sender.requalification_deadline())
                .flatten()
                .map(tokio::time::Instant::from_std);
            let request_path_recovery_deadline = request_path_staleness_deadline
                .into_iter()
                .chain(request_range_reinjection_deadline)
                .chain(request_requalification_deadline)
                .min();
            if reliable_relay_lane_changed(request_demand_update.previous_lane, request_lane) {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "client_request_lane_applied",
                    format_args!(
                        "stream_id={} previous={:?} lane={:?} sent_offset={} reinjection_bytes={} byte_proven={} rate_proven={} buffered_data={}",
                        stream_id.0,
                        request_demand_update.previous_lane,
                        request_lane,
                        send_stream.next_offset(),
                        send_stream.reinjection_bytes(),
                        request_demand_update.byte_proven_bulk,
                        request_demand_update.rate_proven_sustained_bulk,
                        request_demand_update.buffered_bulk,
                    ),
                );
                remotes.set_lane(request_lane);
            }
            #[cfg(feature = "lab-diagnostics")]
            if reliable_relay_lane_changed(response_demand_update.previous_lane, response_lane) {
                lab_diagnostic(
                    "client_response_lane_changed",
                    format_args!(
                        "stream_id={} previous={:?} lane={:?} received_offset={} reorder_bytes={} byte_proven={} rate_proven={}",
                        stream_id.0,
                        response_demand_update.previous_lane,
                        response_lane,
                        recv_stream.next_offset(),
                        recv_stream.reorder_bytes(),
                        response_demand_update.byte_proven_bulk,
                        response_demand_update.rate_proven_sustained_bulk,
                    ),
                );
            }
            if let Err(err) = drive_client_response_startup_control(
                context,
                &spec,
                &mut return_plan,
                request_lane,
                stream_id,
                recv_stream.next_offset(),
                remotes,
                &mut state,
                &additional_path_open_tx,
            ) {
                break Err(err);
            }
            let topology_preopen = request_demand_update.preopen_additional_paths
                || response_demand_update.preopen_additional_paths;
            if topology_preopen && !topology_lane.is_bulk() {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "client_stream_additional_path_open_due",
                    format_args!(
                        "stream_id={} request_observed_bytes={} response_observed_bytes={} attached_paths={}",
                        stream_id.0,
                        request_demand_update.observed_bytes,
                        response_demand_update.observed_bytes,
                        remotes.path_keys().len(),
                    ),
                );
                match spawn_reliable_relay_additional_path_opens(
                    context,
                    &spec,
                    &mut return_plan,
                    ReliableRelayPathLanes::new(TrafficClass::Throughput, request_lane),
                    remotes,
                    send_stream,
                    &state.recovery.path_open_suppressions,
                    &mut state.recovery.pending_additional_path_opens,
                    &additional_path_open_tx,
                ) {
                    Ok(true) => state.progress.last_stream_at = Instant::now(),
                    Ok(false) => {}
                    Err(err) => break Err(err),
                }
            }
            let request_rebalance_due = request_flow_demand.should_rebalance(request_demand_update);
            let response_rebalance_due =
                response_flow_demand.should_rebalance(response_demand_update);
            if request_rebalance_due || response_rebalance_due {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "client_stream_rebalance_due",
                    format_args!(
                        "stream_id={} lane={:?} promoted={} observed_bytes={} product_rate_mbps={:.3} interval_ms={:.3} attached_paths={}",
                        stream_id.0,
                        topology_lane,
                        request_demand_update.promoted_to_throughput
                            || response_demand_update.promoted_to_throughput,
                        request_demand_update
                            .observed_bytes
                            .max(response_demand_update.observed_bytes),
                        request_demand_update
                            .product_rate_bps
                            .max(response_demand_update.product_rate_bps)
                            / 1_000_000.0,
                        request_demand_update
                            .rebalance_interval
                            .max(response_demand_update.rebalance_interval)
                            .as_secs_f64()
                            * 1000.0,
                        remotes.path_keys().len(),
                    ),
                );
                if request_rebalance_due {
                    request_flow_demand.mark_rebalance_attempted();
                }
                if response_rebalance_due {
                    response_flow_demand.mark_rebalance_attempted();
                }
                if topology_lane.is_bulk() {
                    match spawn_reliable_relay_additional_path_opens(
                        context,
                        &spec,
                        &mut return_plan,
                        ReliableRelayPathLanes::new(topology_lane, request_lane),
                        remotes,
                        send_stream,
                        &state.recovery.path_open_suppressions,
                        &mut state.recovery.pending_additional_path_opens,
                        &additional_path_open_tx,
                    ) {
                        Ok(true) => state.progress.last_stream_at = Instant::now(),
                        Ok(false) => {}
                        Err(err) => break Err(err),
                    }
                } else if accepted_copy_due {
                    // Immutable accepted-copy expiry owns this turn. Do not let a
                    // topology path-open timeout delay its serialized recovery
                    // pass; rebalance remains eligible on the next turn.
                } else {
                    pending_rebalance_attach = Some(begin_reliable_relay_attach_with_suppressions(
                        context,
                        &spec,
                        ReliableRelayPathLanes::new(topology_lane, request_lane),
                        remotes,
                        ReliableRelayAttachInput::capture(
                            send_stream,
                            topology_lane,
                            context.mux_limits,
                            source_complete,
                            ReliableRelayAttachMode::BulkStriping,
                        ),
                        &state.recovery.path_open_suppressions,
                        &state.recovery.pending_additional_path_opens,
                    ));
                }
            }
            (
                source_complete,
                path_model_generation_before_recovery_observation,
                response_lane,
                request_lane,
                topology_lane,
                path_snapshot,
                response_path_snapshot,
                accepted_copy_due,
                request_path_staleness_model_publication,
                request_path_staleness_model_wait_active,
                request_recovery_requested,
                request_requalification_capacity_blocked,
                request_path_recovery_deadline,
                pending_rebalance_attach,
                request_rebalance_due || response_rebalance_due,
            )
        };
        if let Some(attach) = pending_rebalance_attach {
            if let Err(err) =
                finish_request_attachment(context, &request_product, &mut return_plan, attach).await
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
        }
        let (
            has_source_output,
            source_admission_path_model_wait_active,
            source_admission_path_model_publication,
            _adaptive_inflight,
            adaptive_chunk,
            request_outstanding_limit,
            sender_queue_limit,
            source_read_ceiling,
            stall_watch_active,
            stall_progress_anchor,
            receive_hole_reinjection_active,
            receive_hole_reinjection_deadline,
            retained_frontier_capacity_wait,
            has_retained_frontier_capacity_wait,
            retained_frontier_outcome,
            retained_frontier_path_model_publication,
            has_retained_frontier_path_model_publication,
            live_owner_tail_reinjection_at,
            stall_deadline,
            recv_progress_deadline,
            recv_progress_resend_active,
            recv_progress_ack_update_pending,
            data_ack_target_capacity_wait,
            has_data_ack_target_capacity_wait,
            data_ack_path_model_wait_active,
            data_ack_target_capacity_wait_active,
            data_ack_path_model_publication,
            data_ack_reinjection_at,
            retained_data_ack_recovery_due,
            pending_remote_fin_ready,
            sender_dispatch_byte_budget,
            sender_dispatch_item_budget,
        ) = {
            let (
                has_source_output,
                source_admission_path_model_wait_active,
                source_admission_path_model_publication,
                adaptive_inflight,
                adaptive_chunk,
                request_outstanding_limit,
                sender_queue_limit,
                source_read_ceiling,
                stall_watch_active,
                stall_progress_anchor,
                receive_hole_reinjection_active,
                receive_hole_reinjection_deadline,
                retained_frontier_capacity_wait,
                has_retained_frontier_capacity_wait,
                retained_frontier_outcome,
                retained_frontier_path_model_publication,
                has_retained_frontier_path_model_publication,
                live_owner_tail_reinjection_at,
                stall_deadline,
                recv_progress_deadline,
                recv_progress_resend_active,
                recv_progress_ack_update_pending,
                data_ack_target_capacity_wait,
                has_data_ack_target_capacity_wait,
                data_ack_path_model_wait_active,
                data_ack_target_capacity_wait_active,
                data_ack_path_model_publication,
                data_ack_reinjection_at,
                retained_data_ack_recovery_due,
                pending_remote_fin_ready,
                sender_dispatch_byte_budget,
                sender_dispatch_item_budget,
                pending_local_shutdown,
            ) = {
                let mut product_guard = request_product.lock();
                let product = &mut *product_guard;
                let (sender_queue, sender, send_stream, last_send_ack, remotes) = (
                    &mut product.sender_queue,
                    &mut product.sender,
                    &mut product.send_stream,
                    &mut product.last_send_ack,
                    &mut product.remotes,
                );
                if rebalance_requested {
                    send_stream.update_max_offset(remotes.max_offset());
                }
                let source_admission = sender.reliable_stream_source_admission(
                    context,
                    remotes,
                    request_lane,
                    PATH_OPEN_SCORE_BYTES,
                );
                let source_path_snapshot = source_admission.selected_path;
                let has_source_output = source_path_snapshot.is_some();
                // An authenticated QUIC attachment can briefly precede publication of
                // its exact Native scheduling shape. Zero source admission is correct
                // during that interval, but it must retain a generation-backed wake;
                // otherwise a quiet Product socket has no event that can re-evaluate
                // the newly published exact authority.
                let source_admission_path_model_wait_active =
                    state.endpoint.local_open && !remotes.is_empty() && !has_source_output;
                let source_admission_path_model_publication =
                    source_admission_path_model_wait_active.then(|| {
                        context.arm_path_model_publication(
                            path_model_generation_before_recovery_observation,
                        )
                    });
                let adaptive_inflight = source_admission.window_bytes;
                let adaptive_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
                    source_path_snapshot,
                    request_lane,
                    context.mux_limits,
                    remotes.max_frame_payload_bytes(context.mux_limits),
                );
                let request_outstanding_limit = reliable_relay_request_outstanding_limit_bytes(
                    request_lane,
                    adaptive_chunk,
                    adaptive_inflight,
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
                        request_lane,
                        adaptive_chunk,
                        adaptive_inflight,
                        sender_queue_limit,
                    );
                #[cfg(feature = "lab-diagnostics")]
                if last_reported_budget != Some((request_lane, adaptive_chunk, adaptive_inflight)) {
                    lab_diagnostic(
                        "client_relay_budget",
                        format_args!(
                            "stream_id={} lane={:?} chunk_bytes={} inflight_bytes={} request_outstanding_limit_bytes={} session_send_buffer_used_bytes={} session_send_buffer_limit_bytes={} attached_paths={} path_snapshot={}",
                            stream_id.0,
                            request_lane,
                            adaptive_chunk,
                            adaptive_inflight,
                            request_outstanding_limit,
                            context.session_send_buffer.used_bytes(),
                            context.session_send_buffer.limit_bytes(),
                            remotes.accepted_path_count(),
                            source_path_snapshot.is_some(),
                        ),
                    );
                    last_reported_budget = Some((request_lane, adaptive_chunk, adaptive_inflight));
                }
                let stall_watch_active = reliable_relay_stall_watch_active(
                    send_stream,
                    &recv_stream,
                    state.endpoint.remote_open,
                    response_lane,
                    state.progress.interactive_response_pending,
                    context.mux_limits,
                );
                let response_stall_watch_active = state.endpoint.remote_open
                    && (state.progress.interactive_response_pending
                        || reliable_relay_response_stall_watch_active(
                            &recv_stream,
                            state.endpoint.remote_open,
                            response_lane,
                            context.mux_limits,
                        ));
                let stall_progress_anchor = reliable_relay_stall_progress_anchor(
                    state.progress.last_stream_at,
                    state.progress.last_delivery_at,
                    state.progress.last_response_stall_reinjection_at,
                    &recv_stream,
                    state.endpoint.remote_open,
                    response_lane,
                    state.progress.interactive_response_pending,
                    context.mux_limits,
                );
                let receive_hole_reinjection_active =
                    reliable_relay_receive_hole_reinjection_active(
                        &recv_stream,
                        state.endpoint.remote_open,
                    );
                let receive_hole_reinjection_deadline =
                    reliable_relay_receive_hole_reinjection_deadline(
                        state.progress.last_delivery_at,
                        state.progress.last_receive_hole_reinjection_at,
                        response_path_snapshot,
                    );
                let stall_path_snapshot = if response_stall_watch_active {
                    response_path_snapshot
                } else {
                    path_snapshot
                };
                let retained_request_live_tail =
                    request_retained_frontier_candidate(send_stream, remotes);
                let persistent_product_stall = state
                    .progress
                    .last_product_stall_attempt_at
                    .is_some_and(|attempted_at| attempted_at >= stall_progress_anchor);
                // The sender proves exact committed OriginalData ownership and its
                // immutable age below. New source or suffix ACK activity cannot erase
                // that retained-prefix obligation; neither EOF nor negative ACK
                // completeness is evidence for this independently timed fallback.
                // Arm before exact-target selection: Notify edges are not retained if
                // the native writer releases capacity between Decide and select.
                let retained_frontier_capacity_wait = retained_request_live_tail
                    .then(|| {
                        arm_carrier_capacity_notifies(
                            remotes
                                .paths
                                .iter()
                                .flat_map(|path| path.stream.capacity_notifies())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .flatten();
                let has_retained_frontier_capacity_wait = retained_frontier_capacity_wait.is_some();
                let retained_frontier_outcome = if retained_request_live_tail {
                    sender.enqueue_retained_frontier_reinjection(
                        sender_queue,
                        context,
                        remotes,
                        send_stream,
                        request_lane,
                    )
                } else {
                    Default::default()
                };
                let retained_frontier_path_model_publication = retained_frontier_outcome
                    .waiting_for_path_model_publication
                    .then(|| {
                        context.arm_path_model_publication(
                            path_model_generation_before_recovery_observation,
                        )
                    });
                let has_retained_frontier_path_model_publication =
                    retained_frontier_path_model_publication.is_some();
                if retained_frontier_outcome.queued {
                    state.progress.sender_retry_at = None;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "request_retained_frontier_reinjection",
                        format_args!(
                            "stream_id={} ack_frontier={} sent_offset={} reinjection_bytes={} attached_paths={}",
                            stream_id.0,
                            state.progress.last_send_ack_frontier,
                            send_stream.next_offset(),
                            send_stream.reinjection_bytes(),
                            remotes.path_keys().len(),
                        ),
                    );
                }
                if persistent_product_stall
                    && sender.enqueue_tail_reinjection(
                        sender_queue,
                        context,
                        remotes,
                        send_stream,
                        request_lane,
                    )
                {
                    state.progress.sender_retry_at = None;
                }
                let live_owner_tail_wake = request_live_owner_tail_wake(
                    retained_request_live_tail,
                    sender.completion_tail_owner_fallback_deadline(),
                    sender.live_owner_frontier_floor_deadline(),
                    Instant::now(),
                );
                let live_owner_tail_reinjection_at = live_owner_tail_wake
                    .deadline
                    .map(tokio::time::Instant::from_std);
                let stall_deadline = reliable_relay_product_stall_deadline(
                    stall_progress_anchor,
                    state.progress.last_product_stall_attempt_at,
                    stall_path_snapshot,
                );
                let stall_deadline = accepted_copy_wake_at
                    .map(tokio::time::Instant::from_std)
                    .map_or(stall_deadline, |deadline| deadline.min(stall_deadline));
                let recv_progress_deadline = tokio::time::Instant::from_std(
                    state.progress.last_recv_progress_sent_at
                        + reliable_stream_recv_progress_interval(response_path_snapshot),
                );
                let recv_progress_resend_active = remotes.path_keys().len() > 1
                    && reliable_relay_recv_progress_resend_active(
                        &recv_stream,
                        state.endpoint.remote_open,
                        response_path_snapshot.map(|snapshot| snapshot.underlay),
                    );
                let recv_progress_ack_update_pending =
                    state.endpoint.remote_open && state.progress.recv_progress.ack_update_pending();
                if state
                    .progress
                    .sender_retry_at
                    .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
                {
                    state.progress.sender_retry_at = None;
                }
                let discarded_tail_reinjections = sender.discard_unusable_tail_reinjections(
                    sender_queue,
                    context,
                    remotes,
                    request_lane,
                );
                let discarded_bound_reinjections =
                    sender.discard_stale_bound_reinjections(sender_queue, remotes);
                if discarded_tail_reinjections > 0 || discarded_bound_reinjections > 0 {
                    state.progress.sender_retry_at = None;
                    request_recovery_dirty = true;
                    request_recovery_requested = true;
                    prepared_work_changed = true;
                }
                let data_ack_timer_due = state
                    .progress
                    .data_ack_reinjection_at
                    .is_some_and(|deadline| deadline <= tokio::time::Instant::now());
                if data_ack_timer_due {
                    state.progress.data_ack_reinjection_at = None;
                }
                let authoritative_data_ack_gap =
                    stream_ack_ranges_expose_authoritative_gap(last_send_ack.gaps());
                let data_ack_capacity_wait_arm_active =
                    reliable_relay_client_ack_gap_capacity_wait_arm_active(
                        authoritative_data_ack_gap,
                        remotes.path_keys().len() > 1,
                    );
                // Arm before target selection reads queue credit. A release between a
                // negative selection and the select poll is then retained by the
                // enabled waiter instead of being lost by `Notify::notify_waiters`.
                let data_ack_target_capacity_wait = data_ack_capacity_wait_arm_active
                    .then(|| {
                        arm_carrier_capacity_notifies(
                            remotes
                                .paths
                                .iter()
                                .flat_map(|path| path.stream.capacity_notifies())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .flatten();
                let has_data_ack_target_capacity_wait = data_ack_target_capacity_wait.is_some();
                // ACK receipt, timer expiry, path/model publication, and carrier
                // capacity all return through this stream-owner evaluation. A due gap
                // therefore remains recoverable when no target was eligible at the
                // exact timer event, without adding polling or another retry clock.
                let data_ack_reinjection = evaluate_client_data_ack_reinjection(
                    &mut state,
                    last_send_ack,
                    sender,
                    sender_queue,
                    context,
                    remotes,
                    send_stream,
                    path_snapshot,
                    request_lane,
                    stream_id,
                );
                #[cfg(feature = "lab-diagnostics")]
                if data_ack_timer_due {
                    lab_diagnostic(
                        "data_ack_loss_timer",
                        format_args!(
                            "stream_id={} reinjection_frames={} ack_gap_reinjection_ready={} multipath_reinjection_alternative={} next_deadline_armed={}",
                            stream_id.0,
                            data_ack_reinjection.frame_count,
                            data_ack_reinjection.persistent_ready,
                            data_ack_reinjection.has_multipath_alternative,
                            state.progress.data_ack_reinjection_at.is_some(),
                        ),
                    );
                }
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = data_ack_reinjection;
                if accepted_copy_due {
                    if sender.enqueue_tail_reinjection(
                        sender_queue,
                        context,
                        remotes,
                        send_stream,
                        request_lane,
                    ) {
                        state.progress.sender_retry_at = None;
                    }
                }
                let data_ack_path_model_wait_active =
                    reliable_relay_client_ack_gap_path_model_wait_active(
                        authoritative_data_ack_gap,
                        data_ack_reinjection.has_multipath_alternative,
                    );
                let data_ack_missing_target_wait_active =
                    data_ack_path_model_wait_active && !data_ack_reinjection.has_measured_target;
                let data_ack_target_capacity_wait_active = data_ack_missing_target_wait_active
                    || data_ack_reinjection.target_service_exhausted;
                let data_ack_path_model_publication = data_ack_path_model_wait_active.then(|| {
                    context.arm_path_model_publication(
                        path_model_generation_before_recovery_observation,
                    )
                });
                let data_ack_reinjection_at = state.progress.data_ack_reinjection_at;
                // Independent authoritative gaps carry their own cause clocks.
                // Any due range may need the existing native-capacity wake;
                // the first missing byte alone no longer represents this set.
                let retained_data_ack_recovery_due = data_ack_reinjection.due_recovery_work;
                let pending_remote_fin_ready = pending_stream_fin_ready(
                    &recv_stream,
                    state.endpoint.pending_remote_fin_offset,
                );
                let pending_local_shutdown = if !remotes.has_pending_feedback_publication() {
                    stream_ack_capacity_wait = None;
                    None
                } else if stream_ack_capacity_wait.is_none() {
                    let capacity_wait =
                        arm_carrier_capacity_notifies(remotes.pending_feedback_capacity_notifies());
                    let publication = remotes.retry_pending_feedback_with_context(context);
                    let local_shutdown = commit_published_feedback_and_ready_fin(
                        &mut local,
                        &mut state,
                        &mut recv_stream,
                        remotes,
                        publication,
                    );
                    Some((capacity_wait, local_shutdown))
                } else {
                    None
                };
                (
                    has_source_output,
                    source_admission_path_model_wait_active,
                    source_admission_path_model_publication,
                    adaptive_inflight,
                    adaptive_chunk,
                    request_outstanding_limit,
                    sender_queue_limit,
                    source_read_ceiling,
                    stall_watch_active,
                    stall_progress_anchor,
                    receive_hole_reinjection_active,
                    receive_hole_reinjection_deadline,
                    retained_frontier_capacity_wait,
                    has_retained_frontier_capacity_wait,
                    retained_frontier_outcome,
                    retained_frontier_path_model_publication,
                    has_retained_frontier_path_model_publication,
                    live_owner_tail_reinjection_at,
                    stall_deadline,
                    recv_progress_deadline,
                    recv_progress_resend_active,
                    recv_progress_ack_update_pending,
                    data_ack_target_capacity_wait,
                    has_data_ack_target_capacity_wait,
                    data_ack_path_model_wait_active,
                    data_ack_target_capacity_wait_active,
                    data_ack_path_model_publication,
                    data_ack_reinjection_at,
                    retained_data_ack_recovery_due,
                    pending_remote_fin_ready,
                    sender_dispatch_byte_budget,
                    sender_dispatch_item_budget,
                    pending_local_shutdown,
                )
            };
            if let Some((capacity_wait, local_shutdown)) = pending_local_shutdown {
                if let Err(err) = local_shutdown.await {
                    break Err(err);
                }
                let product = request_product.lock();
                if product.remotes.has_pending_feedback_publication() {
                    stream_ack_capacity_wait = capacity_wait;
                }
            }
            (
                has_source_output,
                source_admission_path_model_wait_active,
                source_admission_path_model_publication,
                adaptive_inflight,
                adaptive_chunk,
                request_outstanding_limit,
                sender_queue_limit,
                source_read_ceiling,
                stall_watch_active,
                stall_progress_anchor,
                receive_hole_reinjection_active,
                receive_hole_reinjection_deadline,
                retained_frontier_capacity_wait,
                has_retained_frontier_capacity_wait,
                retained_frontier_outcome,
                retained_frontier_path_model_publication,
                has_retained_frontier_path_model_publication,
                live_owner_tail_reinjection_at,
                stall_deadline,
                recv_progress_deadline,
                recv_progress_resend_active,
                recv_progress_ack_update_pending,
                data_ack_target_capacity_wait,
                has_data_ack_target_capacity_wait,
                data_ack_path_model_wait_active,
                data_ack_target_capacity_wait_active,
                data_ack_path_model_publication,
                data_ack_reinjection_at,
                retained_data_ack_recovery_due,
                pending_remote_fin_ready,
                sender_dispatch_byte_budget,
                sender_dispatch_item_budget,
            )
        };
        let (
            stream_ack_publication_blocked,
            has_stream_ack_capacity_wait,
            requalification_ack_capacity_wait,
            requalification_ack_blocked,
            has_requalification_ack_capacity_wait,
            return_plan_final_capacity_wait,
            return_plan_final_blocked,
            has_return_plan_final_capacity_wait,
            timed_carrier_retry_blocked,
            queued_send_ready,
            queued_send_retry_deadline,
            carrier_capacity_wait_needed,
            carrier_capacity_notifies,
            has_carrier_capacity_notify,
            prospective_read_budget,
            can_read_local,
            can_send_pending_fin,
            terminal_fin_replay_ready,
            path_open_suppression_retry_at,
            receive_feedback_output,
            service_active,
            mut prepared_work_wait,
        ) = {
            let mut product_guard = request_product.lock();
            let product = &mut *product_guard;
            let (sender_queue, send_stream, remotes) = (
                &mut product.sender_queue,
                &mut product.send_stream,
                &mut product.remotes,
            );
            let stream_ack_publication_blocked = remotes.has_pending_feedback_publication();
            let has_stream_ack_capacity_wait = stream_ack_capacity_wait.is_some();
            let requalification_ack_pending = remotes.has_pending_requalification_ack();
            let requalification_ack_capacity_wait = requalification_ack_pending
                .then(|| {
                    arm_carrier_capacity_notifies(
                        remotes.pending_requalification_ack_capacity_notifies(),
                    )
                })
                .flatten();
            if requalification_ack_pending {
                match remotes.retry_pending_requalification_ack() {
                    Ok(_) => {}
                    Err(error) if reliable_path_error_is_migratable(&error) => {}
                    Err(error) => break Err(error),
                }
            }
            let requalification_ack_blocked = remotes.has_pending_requalification_ack();
            let has_requalification_ack_capacity_wait = requalification_ack_capacity_wait.is_some();
            let return_plan_final_pending = remotes.has_pending_return_plan_final_publication();
            let return_plan_final_capacity_wait = return_plan_final_pending
                .then(|| {
                    arm_carrier_capacity_notifies(
                        remotes.pending_return_plan_final_capacity_notifies(),
                    )
                })
                .flatten();
            if return_plan_final_pending {
                remotes.retry_pending_return_plan_final();
            }
            let return_plan_final_blocked = remotes.has_pending_return_plan_final_publication();
            let has_return_plan_final_capacity_wait = return_plan_final_capacity_wait.is_some();
            let queued_send_blocked = reliable_relay_queued_send_blocked_for_retry(
                !sender_queue.has_reinjection() && !request_recovery_requested,
                state.progress.sender_retry_at,
            );
            let final_feedback_retry_blocked = pending_remote_fin_ready
                && remotes.has_receive_feedback_output()
                && state.progress.sender_retry_at.is_some();
            let pending_local_fin_ready = reliable_relay_can_send_pending_fin(
                state.endpoint.pending_local_fin,
                sender_queue.is_empty(),
            );
            let terminal_fin_replay_pending = stream_terminal_fin_replay_required(
                state.endpoint.local_fin_sent,
                state.endpoint.terminal_fin_replayed,
                sender_queue.is_empty(),
            );
            // STREAM_FIN uses the ordered carrier lane, so a live carrier can
            // temporarily reject it while previously accepted data drains. Keep
            // terminal state pending and reuse the ordinary capacity/retry wakeup.
            let terminal_control_retry_blocked = state.progress.sender_retry_at.is_some()
                && (pending_local_fin_ready || terminal_fin_replay_pending);
            let timed_carrier_retry_blocked = queued_send_blocked
                || final_feedback_retry_blocked
                || terminal_control_retry_blocked;
            let queued_send_ready = (sender_queue.has_reinjection() || request_recovery_requested)
                && !queued_send_blocked;
            let queued_send_retry_deadline = state
                .progress
                .sender_retry_at
                .unwrap_or_else(tokio::time::Instant::now);
            let carrier_capacity_wait_needed = final_feedback_retry_blocked
                || terminal_control_retry_blocked
                || retained_data_ack_recovery_due;
            let carrier_capacity_notifies = if carrier_capacity_wait_needed {
                remotes
                    .paths
                    .iter()
                    .flat_map(|path| path.stream.capacity_notifies())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let has_carrier_capacity_notify = !carrier_capacity_notifies.is_empty();
            let can_read_by_flow = has_source_output
                && reliable_relay_can_read_product_source(
                    state.endpoint.local_open,
                    queued_send_blocked,
                    send_stream,
                    sender_queue,
                    sender_queue_limit,
                );
            let request_outstanding_headroom = reliable_relay_request_outstanding_headroom_bytes(
                send_stream,
                sender_queue,
                request_outstanding_limit,
            );
            let can_read_by_flow = can_read_by_flow && request_outstanding_headroom > 0;
            let prospective_read_budget = if can_read_by_flow {
                reliable_relay_sender_queue_read_budget(
                    send_stream,
                    sender_queue,
                    sender_queue_limit,
                    source_read_ceiling,
                )
                .min(request_outstanding_headroom)
            } else {
                0
            };
            let can_read_local =
                !remotes.is_empty() && can_read_by_flow && prospective_read_budget > 0;
            let can_send_pending_fin = pending_local_fin_ready && !terminal_control_retry_blocked;
            let terminal_fin_replay_ready =
                terminal_fin_replay_pending && !terminal_control_retry_blocked;
            #[cfg(feature = "lab-diagnostics")]
            {
                if state.endpoint.local_open && !can_read_local {
                    let blocked_state = (
                        send_stream.reinjection_bytes(),
                        send_stream.send_credit_bytes(),
                        _adaptive_inflight,
                        request_outstanding_limit,
                        request_outstanding_headroom,
                    );
                    if last_reported_read_block != Some(blocked_state) {
                        lab_diagnostic(
                            "relay_local_read_blocked",
                            format_args!(
                                "stream_id={} lane={:?} reinjection_bytes={} send_credit_bytes={} inflight_limit={} request_outstanding_limit={} request_outstanding_headroom={} sent_offset={} received_offset={}",
                                stream_id.0,
                                request_lane,
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

            let path_open_suppression_retry_at = state
                .recovery
                .path_open_suppressions
                .next_retry_at(context, tokio::time::Instant::now());

            let receive_feedback_output = remotes.has_receive_feedback_output();
            let service_active = !state.is_finished(send_stream, &recv_stream, sender_queue);
            publish_prepared_request_work(
                &mut product_guard,
                &request_product,
                context,
                request_lane,
                adaptive_chunk,
                std::mem::take(&mut prepared_work_changed),
            );
            let mut prepared_work_wait =
                Box::pin(product_guard.prepared.work_changed.clone().notified_owned());
            prepared_work_wait.as_mut().enable();
            (
                stream_ack_publication_blocked,
                has_stream_ack_capacity_wait,
                requalification_ack_capacity_wait,
                requalification_ack_blocked,
                has_requalification_ack_capacity_wait,
                return_plan_final_capacity_wait,
                return_plan_final_blocked,
                has_return_plan_final_capacity_wait,
                timed_carrier_retry_blocked,
                queued_send_ready,
                queued_send_retry_deadline,
                carrier_capacity_wait_needed,
                carrier_capacity_notifies,
                has_carrier_capacity_notify,
                prospective_read_budget,
                can_read_local,
                can_send_pending_fin,
                terminal_fin_replay_ready,
                path_open_suppression_retry_at,
                receive_feedback_output,
                service_active,
                prepared_work_wait,
            )
        };
        let feedback_deadline = request_product
            .lock()
            .remotes
            .feedback_deadline()
            .map(tokio::time::Instant::from_std);
        tokio::select! {
            _ = wait_for_optional_deadline(feedback_deadline), if feedback_deadline.is_some() => {
                stream_ack_capacity_wait = None;
                continue;
            }
            () = &mut prepared_work_wait => {
                // A real writer commit or source/eligibility change requires
                // fresh Product state; it does not grant actor Data dispatch.
                continue;
            }
            _ = wait_for_optional_deadline(path_open_suppression_retry_at), if path_open_suppression_retry_at.is_some() => {
                // Re-enter serialized demand/recovery decisions exactly when
                // the failed attachment's path-derived retry bound expires.
                continue;
            }
            _ = async move {
                if let Some(publication) = source_admission_path_model_publication {
                    publication.await;
                }
            }, if source_admission_path_model_wait_active => {
                // Re-read the exact attachment and its Native authority. The
                // wake grants no capacity and preserves fail-closed admission
                // until the current instance publishes a usable shape.
                continue;
            }
            _ = async move {
                if let Some(publication) = request_path_staleness_model_publication {
                    publication.await;
                }
            }, if request_path_staleness_model_wait_active => {
                // Availability, usage, or policy can make the first distinct
                // alternate schedulable while no staleness clock exists. The
                // next serialized pass reconciles that predicate without
                // inventing an assignment or Data-ACK event.
                request_path_staleness_dirty = true;
                continue;
            }
            _ = async move {
                if let Some(publication) = data_ack_path_model_publication {
                    publication.await;
                }
            }, if data_ack_path_model_wait_active => {
                // Current owner or alternate evidence may introduce a target
                // or pull its absolute completion from fallback to the loss
                // boundary. Preserve the ACK-gap clocks and derive only the
                // current target on the next serialized pass.
                continue;
            }
            _ = async move {
                if let Some(wait) = data_ack_target_capacity_wait {
                    wait.await;
                }
            }, if data_ack_target_capacity_wait_active && has_data_ack_target_capacity_wait => {
                // Queue credit changed after a missing-target or exhausted
                // target observation; reselect without changing a recovery
                // epoch.
                continue;
            }
            _ = async move {
                if let Some(wait) = retained_frontier_capacity_wait {
                    wait.await;
                }
            }, if retained_frontier_outcome.blocked_for_carrier_capacity && has_retained_frontier_capacity_wait => {
                // A due retained frontier stays owned while its exact
                // alternate is full. Re-evaluate the same frontier when native
                // capacity is released; no polling clock is introduced.
                continue;
            }
            _ = async move {
                if let Some(publication) = retained_frontier_path_model_publication {
                    publication.await;
                }
            }, if retained_frontier_outcome.waiting_for_path_model_publication
                && has_retained_frontier_path_model_publication => {
                // Retained work can lie beyond the negative ACK horizon, so
                // the pre-horizon staleness scan cannot own this edge. Re-enter
                // the exact frontier decision when owner/alternate Product
                // evidence or path availability is published.
                continue;
            }
            _ = async {
                if let Some(wait) = request_recovery_service_wait.as_mut() {
                    wait.as_mut().await;
                }
            }, if request_recovery_capacity_blocked && request_recovery_service_wait.is_some() => {
                request_recovery_service_wait = None;
                request_recovery_dirty = true;
                state.progress.sender_retry_at = None;
                continue;
            }
            _ = async {
                if let Some(wait) = request_requalification_capacity_wait.as_mut() {
                    wait.as_mut().await;
                }
            }, if request_requalification_capacity_blocked => {
                // A stale target's maintenance queue is independent of
                // ordinary Product work on every sibling writer.
                request_requalification_capacity_wait = None;
                continue;
            }
            _ = wait_for_optional_deadline(request_path_recovery_deadline), if request_path_recovery_deadline.is_some() => {
                // Re-evaluate exact attachment and range recovery clocks
                // before assigning more OriginalData; native recovery continues.
                continue;
            }
            _ = std::future::ready(()), if pending_remote_fin_ready
                && receive_feedback_output
                && state.progress.sender_retry_at.is_none() => {
                let local_shutdown = {
                let mut product_guard = request_product.lock();
                let (sender, remotes) = {
                    let product = &mut *product_guard;
                    (
                        &mut product.sender,
                        &mut product.remotes,
                    )
                };
                let feedback_published = match sender
                    .send_recv_progress(
                        remotes,
                        context,
                        &mut recv_stream,
                        &mut state.progress.recv_progress,
                        RelayRecvProgressSend::final_ack(response_path_snapshot, response_lane),
                    )
                {
                    Ok(sent) => {
                        record_final_recv_progress_enqueue(
                            &mut state,
                            sent,
                            response_path_snapshot,
                        );
                        sent.ack.published
                    }
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        state.progress.sender_retry_at = None;
                        continue;
                    }
                    Err(err) => break Err(err),
                };
                commit_pending_remote_fin(
                    &mut local,
                    &mut state,
                    &recv_stream,
                    feedback_published && remotes.has_receive_feedback_output(),
                )
                };
                if let Err(err) = local_shutdown.await
                {
                    break Err(err);
                }
            }
            _ = tokio::time::sleep_until(receive_hole_reinjection_deadline), if receive_hole_reinjection_active => {
                let mut product_guard = request_product.lock();
                let (sender, send_stream, remotes) = {
                    let product = &mut *product_guard;
                    (
                        &mut product.sender,
                        &mut product.send_stream,
                        &mut product.remotes,
                    )
                };
                let recovery_path_open_spawned = spawn_reliable_relay_recovery_path_open(
                    context,
                    &spec,
                    &mut return_plan,
                    ReliableRelayPathLanes::new(response_lane, request_lane),
                    remotes,
                    send_stream,
                    &state.recovery.path_open_suppressions,
                    &mut state.recovery.pending_additional_path_opens,
                    &additional_path_open_tx,
                );
                state.progress.receive_hole_reinjection_attempts =
                    state.progress.receive_hole_reinjection_attempts.saturating_add(1);
                match sender
                    .send_recv_progress(
                        remotes,
                        context,
                        &mut recv_stream,
                        &mut state.progress.recv_progress,
                        RelayRecvProgressSend::new(
                            response_path_snapshot,
                            response_lane,
                            true,
                        ),
                    )
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
                        response_lane,
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
                // The next loop iteration owns evaluation and preserves the
                // due retained gap if no target is currently eligible.
                continue;
            }
            _ = wait_for_optional_deadline(live_owner_tail_reinjection_at), if live_owner_tail_reinjection_at.is_some() => {
                // The immutable shared epoch has elapsed. The next serialized
                // loop revalidates the retained tail and its exact owners;
                // mutable product-stall clocks cannot postpone this wake.
                continue;
            }
            _ = tokio::time::sleep_until(stall_deadline), if stall_watch_active => {
                let pending_attach = {
                let mut pending_attach = None;
                let mut product_guard = request_product.lock();
                let (sender_queue, sender, send_stream, remotes) = {
                    let product = &mut *product_guard;
                    (
                        &mut product.sender_queue,
                        &mut product.sender,
                        &mut product.send_stream,
                        &mut product.remotes,
                    )
                };
                if accepted_copy_wake_is_due(accepted_copy_wake_at, Instant::now()) {
                    // The stall timer is also the accepted-copy wake. Let the
                    // next loop's serialized one-shot consume D before this
                    // branch advances unrelated stall/topology clocks.
                    continue;
                }
                let persistent_product_stall = state
                    .progress
                    .last_product_stall_attempt_at
                    .is_some_and(|attempted_at| attempted_at >= stall_progress_anchor);
                let queued_existing_tail_reinjection = sender.enqueue_tail_reinjection(
                    sender_queue,
                    context,
                    remotes,
                    send_stream,
                    request_lane,
                );
                let recovery_open_spawned = persistent_product_stall
                    && spawn_reliable_relay_recovery_path_open(
                        context,
                        &spec,
                        &mut return_plan,
                        ReliableRelayPathLanes::new(topology_lane, request_lane),
                        remotes,
                        send_stream,
                        &state.recovery.path_open_suppressions,
                        &mut state.recovery.pending_additional_path_opens,
                        &additional_path_open_tx,
                    );
                if queued_existing_tail_reinjection
                    || recovery_open_spawned
                    || reliable_relay_product_stall_preserves_attached_path_set(remotes)
                {
                    if queued_existing_tail_reinjection {
                        state.progress.sender_retry_at = None;
                    }
                    match sender.send_recv_progress(
                        remotes,
                        context,
                        &mut recv_stream,
                        &mut state.progress.recv_progress,
                        RelayRecvProgressSend::new(
                            response_path_snapshot,
                            response_lane,
                            true,
                        ),
                    )
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
                            "stream_id={} active_underlay={:?} attached_paths={} pending_opens={} recovery_open_spawned={} reinjection_bytes={} recv_reorder_bytes={} sent_offset={} cause=product_stall_only",
                            stream_id.0,
                            response_path_snapshot.map(|snapshot| snapshot.underlay),
                            remotes.path_keys().len(),
                            state.recovery.pending_additional_path_opens.len(),
                            recovery_open_spawned,
                            send_stream.reinjection_bytes(),
                            recv_stream.reorder_bytes(),
                            send_stream.next_offset(),
                        ),
                    );
                    state.progress.last_response_stall_reinjection_at = Instant::now();
                    state.progress.last_product_stall_attempt_at = Some(Instant::now());
                    continue;
                }
                if reliable_relay_product_stall_should_try_alternate_attach(remotes) {
                    // Path establishment must not stop the live relay from
                    // consuming data or ACKs on its existing carrier.
                    let recovery_open_spawned = spawn_reliable_relay_recovery_path_open(
                        context,
                        &spec,
                        &mut return_plan,
                        ReliableRelayPathLanes::new(topology_lane, request_lane),
                        remotes,
                        send_stream,
                        &state.recovery.path_open_suppressions,
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
                            response_path_snapshot.map(|snapshot| snapshot.underlay),
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
                    remotes,
                    context,
                    &mut recv_stream,
                    &mut state.progress.recv_progress,
                    RelayRecvProgressSend::new(response_path_snapshot, response_lane, true),
                )
                {
                    Ok(sent) => state.record_recv_progress_sent(sent),
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        state.progress.sender_retry_at = None;
                        if remotes.is_empty() {
                            continue;
                        }
                            let attach = begin_reliable_relay_attach_with_suppressions(
                                context,
                                &spec,
                                ReliableRelayPathLanes::new(response_lane, request_lane),
                                remotes,
                                ReliableRelayAttachInput::capture(
                                    send_stream,
                                    response_lane,
                                    context.mux_limits,
                                    source_complete,
                                    ReliableRelayAttachMode::Any,
                                ),
                                &state.recovery.path_open_suppressions,
                                &state.recovery.pending_additional_path_opens,
                            );
                        pending_attach = Some((attach, err));
                    }
                    Err(err) => break Err(err),
                }
                pending_attach
                };
                if let Some((attach, original_error)) = pending_attach {
                    match finish_request_attachment(context, &request_product, &mut return_plan, attach).await {
                        Ok(attached) if attached > 0 => {
                            let mut product_guard = request_product.lock();
                            let product = &mut *product_guard;
                            let (sender, send_stream, remotes) = (
                                &mut product.sender, &mut product.send_stream, &mut product.remotes,
                            );

                                send_stream.update_max_offset(remotes.max_offset());
                                match sender
                                    .send_recv_progress(
                                        remotes,
                                        context,
                                        &mut recv_stream,
                                        &mut state.progress.recv_progress,
                                        RelayRecvProgressSend::new(
                                            response_path_snapshot,
                                            response_lane,
                                            true,
                                        ),
                                    )
                                {
                                    Ok(sent) => state.record_recv_progress_sent(sent),
                                    Err(recovery_err)
                                        if reliable_path_error_is_migratable(&recovery_err) => {}
                                    Err(recovery_err) => break Err(recovery_err),
                                }

                        }
                        Ok(_) => break Err(original_error),
                        Err(error) => break Err(error),
                    }
                }
                {
                #[cfg(feature = "lab-diagnostics")]
                let product_guard = request_product.lock();
                #[cfg(feature = "lab-diagnostics")]
                let (remotes, send_stream) = (&product_guard.remotes, &product_guard.send_stream);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "client_product_stall_keeps_carrier_membership",
                    format_args!(
                        "stream_id={} active_underlay={:?} attached_paths={} reinjection_bytes={} recv_reorder_bytes={} sent_offset={} cause=product_stall_only",
                        stream_id.0,
                        response_path_snapshot.map(|snapshot| snapshot.underlay),
                        remotes.path_keys().len(),
                        send_stream.reinjection_bytes(),
                        recv_stream.reorder_bytes(),
                        send_stream.next_offset(),
                    ),
                );
                state.progress.last_response_stall_reinjection_at = Instant::now();
                state.progress.last_product_stall_attempt_at = Some(Instant::now());
                }
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if recv_progress_resend_active
                || recv_progress_ack_update_pending => {
                let pending_attach = {
                let mut pending_attach = None;
                let mut product_guard = request_product.lock();
                let (sender, send_stream, remotes) = {
                    let product = &mut *product_guard;
                    (
                        &mut product.sender,
                        &mut product.send_stream,
                        &mut product.remotes,
                    )
                };
                let recv_progress_send = if recv_progress_resend_active {
                    RelayRecvProgressSend::new(response_path_snapshot, response_lane, true)
                } else {
                    RelayRecvProgressSend::ack_only(response_path_snapshot, response_lane)
                };
                match sender.send_recv_progress(
                    remotes,
                    context,
                    &mut recv_stream,
                    &mut state.progress.recv_progress,
                    recv_progress_send,
                )
                {
                    Ok(sent) => {
                        if sent.ack.published || sent.max_data.published_offset.is_some() {
                            state.progress.last_stream_at = Instant::now();
                        }
                        state.progress.last_recv_progress_sent_at = Instant::now();
                    }
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        if remotes.is_empty() {
                            continue;
                        }
                            let attach = begin_reliable_relay_attach_with_suppressions(
                                context,
                                &spec,
                                ReliableRelayPathLanes::new(response_lane, request_lane),
                                remotes,
                                ReliableRelayAttachInput::capture(
                                    send_stream,
                                    response_lane,
                                    context.mux_limits,
                                    source_complete,
                                    ReliableRelayAttachMode::Any,
                                ),
                                &state.recovery.path_open_suppressions,
                                &state.recovery.pending_additional_path_opens,
                            );
                        pending_attach = Some((attach, err));
                    }
                    Err(err) => break Err(err),
                }
                pending_attach
                };
                if let Some((attach, original_error)) = pending_attach {
                    match finish_request_attachment(context, &request_product, &mut return_plan, attach).await {
                        Ok(attached) if attached > 0 => {
                            let mut product_guard = request_product.lock();
                            let product = &mut *product_guard;
                            let (send_stream, remotes) = (&mut product.send_stream, &mut product.remotes);

                                state.progress.sender_retry_at = None;
                                send_stream.update_max_offset(remotes.max_offset());
                                state.progress.last_stream_at = Instant::now();
                                state.progress.last_recv_progress_sent_at = Instant::now();

                        }
                        Ok(_) => break Err(original_error),
                        Err(error) => break Err(error),
                    }
                }
            }
            _ = std::future::ready(()), if can_send_pending_fin => {
                let pending_attach = {
                let mut pending_attach = None;
                let mut product_guard = request_product.lock();
                let (sender_queue, sender, send_stream, remotes) = {
                    let product = &mut *product_guard;
                    (
                        &mut product.sender_queue,
                        &mut product.sender,
                        &mut product.send_stream,
                        &mut product.remotes,
                    )
                };
                if !reliable_relay_can_send_pending_fin(
                    state.endpoint.pending_local_fin,
                    sender_queue.is_empty(),
                ) {
                    continue;
                }
                match sender
                    .send_control_frame(
                        context,
                        remotes,
                        Frame::StreamFin {
                            stream_id,
                            final_offset: send_stream.next_offset(),
                        },
                        RelaySendCause::StreamFin,
                    )
                {
                    Ok(_) => state.record_local_fin_sent(),
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        if remotes.is_empty() {
                            continue;
                        }
                            let attach = begin_reliable_relay_attach_with_suppressions(
                                context,
                                &spec,
                                ReliableRelayPathLanes::new(request_lane, request_lane),
                                remotes,
                                ReliableRelayAttachInput::capture(
                                    send_stream,
                                    request_lane,
                                    context.mux_limits,
                                    true,
                                    ReliableRelayAttachMode::Any,
                                ),
                                &state.recovery.path_open_suppressions,
                                &state.recovery.pending_additional_path_opens,
                            );
                        pending_attach = Some((attach, err));
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {
                        state.progress.sender_retry_at = Some(
                            tokio::time::Instant::now()
                                + sender_service_retry_delay(path_snapshot),
                        );
                        continue;
                    }
                    Err(err) => break Err(err),
                }
                pending_attach
                };
                if let Some((attach, original_error)) = pending_attach {
                    match finish_request_attachment(context, &request_product, &mut return_plan, attach).await {
                        Ok(attached) if attached > 0 => {

                                state.progress.sender_retry_at = None;
                                state.record_local_fin_sent();

                        }
                        Ok(_) => break Err(original_error),
                        Err(error) => break Err(error),
                    }
                }
            }
            _ = std::future::ready(()), if terminal_fin_replay_ready => {
                let pending_attach = {
                let mut pending_attach = None;
                let mut product_guard = request_product.lock();
                let (sender_queue, sender, send_stream, remotes) = {
                    let product = &mut *product_guard;
                    (
                        &mut product.sender_queue,
                        &mut product.sender,
                        &mut product.send_stream,
                        &mut product.remotes,
                    )
                };
                if !stream_terminal_fin_replay_required(
                    state.endpoint.local_fin_sent,
                    state.endpoint.terminal_fin_replayed,
                    sender_queue.is_empty(),
                ) {
                    continue;
                }
                match sender
                    .send_control_frame(
                        context,
                        remotes,
                        Frame::StreamFin {
                            stream_id,
                            final_offset: send_stream.next_offset(),
                        },
                        RelaySendCause::StreamFin,
                    )
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
                            let attach = begin_reliable_relay_attach_with_suppressions(
                                context,
                                &spec,
                                ReliableRelayPathLanes::new(request_lane, request_lane),
                                remotes,
                                ReliableRelayAttachInput::capture(
                                    send_stream,
                                    request_lane,
                                    context.mux_limits,
                                    true,
                                    ReliableRelayAttachMode::Any,
                                ),
                                &state.recovery.path_open_suppressions,
                                &state.recovery.pending_additional_path_opens,
                            );
                        pending_attach = Some((attach, err));
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {
                        state.progress.sender_retry_at = Some(
                            tokio::time::Instant::now()
                                + sender_service_retry_delay(path_snapshot),
                        );
                        continue;
                    }
                    Err(err) => break Err(err),
                }
                pending_attach
                };
                if let Some((attach, original_error)) = pending_attach {
                    match finish_request_attachment(context, &request_product, &mut return_plan, attach).await {
                        Ok(attached) if attached > 0 => {

                                state.progress.sender_retry_at = None;
                                state.record_terminal_fin_replayed();

                        }
                        Ok(_) => break Err(original_error),
                        Err(error) => break Err(error),
                    }
                }
            }
            _ = tokio::time::sleep_until(queued_send_retry_deadline), if timed_carrier_retry_blocked => {
                state.progress.sender_retry_at = None;
                continue;
            }
            _ = wait_for_carrier_capacity_notifies(carrier_capacity_notifies), if carrier_capacity_wait_needed && has_carrier_capacity_notify => {
                state.progress.sender_retry_at = None;
                continue;
            }
            _ = async {
                if let Some(wait) = stream_ack_capacity_wait.as_mut() {
                    wait.as_mut().await;
                }
            }, if stream_ack_publication_blocked && has_stream_ack_capacity_wait => {
                stream_ack_capacity_wait = None;
                state.progress.sender_retry_at = None;
                continue;
            }
            _ = async move {
                if let Some(wait) = requalification_ack_capacity_wait {
                    wait.await;
                }
            }, if requalification_ack_blocked && has_requalification_ack_capacity_wait => {
                continue;
            }
            _ = async move {
                if let Some(wait) = return_plan_final_capacity_wait {
                    wait.await;
                }
            }, if return_plan_final_blocked && has_return_plan_final_capacity_wait => {
                continue;
            }
            additional_path_open = additional_path_open_rx.recv(), if !state.recovery.pending_additional_path_opens.is_empty() => {
                let mut product_guard = request_product.lock();
                let (sender_queue, sender, send_stream, remotes) = {
                    let product = &mut *product_guard;
                    (
                        &mut product.sender_queue,
                        &mut product.sender,
                        &mut product.send_stream,
                        &mut product.remotes,
                    )
                };
                let Some(additional_path_open) = additional_path_open else {
                    cancel_pending_additional_path_opens(stream_id, &mut state.recovery.pending_additional_path_opens);
                    continue;
                };
                let attached_mode = match settle_matching_client_additional_path_open(
                    stream_id,
                    &mut state,
                    &mut return_plan,
                    remotes,
                    send_stream,
                    request_lane,
                    additional_path_open,
                    sender_queue.data_bytes() == 0,
                ) {
                    Ok(mode) => mode,
                    Err(err) => break Err(err),
                };
                if let Err(err) = apply_client_additional_path_open_postaction(
                    attached_mode,
                    sender,
                    sender_queue,
                    context,
                    remotes,
                    send_stream,
                    &mut recv_stream,
                    &mut state,
                    request_lane,
                    response_lane,
                )
                {
                    break Err(err);
                }
            }
            service = service_turn.select(
                queued_send_ready,
                can_read_local,
                async {
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
                },
                service_active,
                async {
                    #[cfg(feature = "lab-diagnostics")]
                    let recv_started = Instant::now();
                    let result = match deferred_remote_frame.take() {
                        Some(frame) => Ok(frame),
                        None => remote_input.recv_frame().await,
                    };
                    #[cfg(feature = "lab-diagnostics")]
                    if let Ok(ReliableRelayRemoteFrame { frame: Ok(frame), .. }) = &result {
                        lab_perf_record(
                            "relay.path_recv_frame_wait",
                            recv_started.elapsed(),
                            reliable_path_frame_pacing_bytes(frame),
                        );
                    }
                    result
                },
            ), if service_active => {
                match service {
                    RelayServiceEvent::Dispatch => {
                        let mut recovery_batch = {
                            let product = request_product.lock();
                            let (sender, remotes, sender_queue) = (&product.sender, &product.remotes, &product.sender_queue);
                            if request_recovery_requested {
                            request_recovery_service_wait = Some(Box::pin(
                                arm_request_recovery_service_wait(context, remotes),
                            ));
                            Some(sender.collect_request_path_recovery(remotes, sender_queue))
                        } else {
                            None
                        }
                        };
                        let mut dispatched_items = 0usize;
                        #[cfg(feature = "lab-diagnostics")]
                        let dispatched_payload_bytes = 0usize;
                        let mut blocked_by_carrier = false;
                        let mut dispatch_error = None;
                        let mut queued_recovery_changed = false;
                        loop {
                        let pending_attach = {
                            let mut product_guard = request_product.lock();
                            let product = &mut *product_guard;
                            let (sender_queue, sender, send_stream, remotes) = (
                                &mut product.sender_queue, &mut product.sender, &mut product.send_stream, &mut product.remotes,
                            );
                            let mut pending_attach = None;
                        while (sender_queue.has_reinjection()
                            || recovery_batch.as_ref().is_some_and(|batch| batch.has_pending()))
                            && dispatched_items < sender_dispatch_item_budget
                        {
                            let direct_dispatch = if let Some(batch) = recovery_batch.as_mut()
                                && batch.has_pending()
                            {
                                match sender.dispatch_next_request_path_recovery(
                                    batch, context, remotes, send_stream, sender_queue,
                                ) {
                                    Ok(dispatch) => dispatch,
                                    Err(err) => {
                                        dispatch_error = Some(err);
                                        break;
                                    }
                                }
                            } else {
                                None
                            };
                            let is_direct_recovery = direct_dispatch.is_some();
                            let dispatch = if let Some(dispatch) = direct_dispatch {
                                Ok(dispatch)
                            } else {
                                match sender.dispatch_client_repair_work(
                                    context,
                                    request_lane,
                                    remotes,
                                    sender_queue,
                                ) {
                                    Ok(Some(dispatch)) => Ok(dispatch),
                                    Ok(None) => break,
                                    Err(error) => Err(error),
                                }
                            };
                            match dispatch {
                                Ok(ClientQueuedDispatch::Reinjection {
                                    payload_bytes,
                                    accepted_copy_deadline,
                                }) => {
                                    let _ = payload_bytes;
                                    retain_accepted_copy_wake(
                                        &mut accepted_copy_wake_at,
                                        accepted_copy_deadline,
                                    );
                                    dispatched_items = dispatched_items.saturating_add(1);
                                    state.progress.last_stream_at = Instant::now();
                                    queued_recovery_changed |= !is_direct_recovery;
                                }
                                Ok(ClientQueuedDispatch::ReinjectionDeferred) => {
                                    dispatched_items = dispatched_items.saturating_add(1);
                                    queued_recovery_changed = true;
                                }
                                Ok(ClientQueuedDispatch::PersistentReinjectionCancelled) => {
                                    state.progress.sender_retry_at = None;
                                    dispatched_items = dispatched_items.saturating_add(1);
                                    queued_recovery_changed = true;
                                }
                                Ok(ClientQueuedDispatch::PathAttachmentRequired(err)) => {
                                    if remotes.is_empty() {
                                        state.progress.sender_retry_at = None;
                                        break;
                                    }
                                        let attach = begin_reliable_relay_attach_with_suppressions(
                                            context,
                                            &spec,
                                            ReliableRelayPathLanes::new(request_lane, request_lane),
                                            remotes,
                                            ReliableRelayAttachInput::capture(
                                                send_stream,
                                                request_lane,
                                                context.mux_limits,
                                                source_complete,
                                                ReliableRelayAttachMode::Any,
                                            ),
                                            &state.recovery.path_open_suppressions,
                                            &state.recovery.pending_additional_path_opens,
                                        );
                                    pending_attach = Some((attach, err));
                                    break;
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
                            pending_attach
                        };
                        let Some((attach, original_error)) = pending_attach else {
                            break;
                        };
                        match finish_request_attachment(context, &request_product, &mut return_plan, attach).await {
                            Ok(attached) if attached > 0 => {
                                state.progress.sender_retry_at = None;
                            }
                            Ok(_) => {
                                dispatch_error = Some(original_error);
                                break;
                            }
                            Err(error) => {
                                dispatch_error = Some(error);
                                break;
                            }
                        }
                        }
                        if let Some(batch) = recovery_batch {
                            request_range_recovery_deadline = batch.retry_deadline;
                            request_recovery_capacity_blocked = batch.blocked_for_carrier_capacity;
                            request_recovery_dirty = batch.has_pending() || queued_recovery_changed;
                            if !request_recovery_capacity_blocked {
                                request_recovery_service_wait = None;
                            }
                        } else {
                            request_recovery_dirty |= queued_recovery_changed;
                        }
                        prepared_work_changed |= queued_recovery_changed;
                        let should_yield = {
                        let product_guard = request_product.lock();
                        let send_stream = &product_guard.send_stream;
                        #[cfg(feature = "lab-diagnostics")]
                        let sender_queue = &product_guard.sender_queue;
                        #[cfg(feature = "lab-diagnostics")]
                        if dispatched_items > 0 {
                            lab_diagnostic(
                                "client_sender_drain",
                                format_args!(
                                    "stream_id={} lane={:?} dispatches={} payload_bytes={} byte_budget={} item_budget={} queue_bytes_after={} blocked_by_carrier={}",
                                    stream_id.0,
                                    request_lane,
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
                        dispatched_items > 0 && (state.endpoint.remote_open || send_stream.reinjection_bytes() > 0)
                        };
                        if should_yield {
                            tokio::task::yield_now().await;
                        }
                    }
                    RelayServiceEvent::Read(read) => {
                        let (read, permit) = read;
                        let (read, payload) = match read {
                            Ok(read) => read,
                            Err(err) => break Err(RuntimeError::Io(err)),
                        };
                        permit.retain(&mut send_buffer_reservation, read);
                        if read == 0 {
                            state.record_local_eof();
                            prepared_work_changed = true;
                        } else {
                            state.record_local_payload(request_lane);
                            {
                                let mut product = request_product.lock();
                                let payload = payload.expect("positive read returns payload");
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "client_sender_enqueue",
                                    format_args!(
                                        "stream_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} opportunistic=false",
                                        stream_id.0, request_lane, read,
                                        product.sender_queue.bytes().saturating_add(read), sender_queue_limit,
                                    ),
                                );
                                product.sender_queue.push_data(payload);
                                publish_prepared_request_work(
                                    &mut product, &request_product, context, request_lane, adaptive_chunk, true,
                                );
                            }
                            let mut opportunistic_reads = 1usize;
                            while state.endpoint.local_open {
                                let next_read_budget = {
                                    let product = request_product.lock();
                                    reliable_relay_client_opportunistic_read_budget(
                                        opportunistic_reads,
                                        &product.send_stream,
                                        &product.sender_queue,
                                        ClientOpportunisticReadBounds {
                                            sender_dispatch_byte_budget, sender_dispatch_item_budget,
                                            sender_queue_limit, source_read_ceiling, request_outstanding_limit,
                                        },
                                    )
                                };
                                if next_read_budget == 0 {
                                    break;
                                }
                                let read = ready_at_entry(async {
                                    let permit = context.session_send_buffer
                                        .reserve(&mut send_buffer_updates, next_read_budget).await;
                                    let result = read_reliable_relay_payload(
                                        &mut local, &mut buf, permit.bytes(),
                                    ).await;
                                    (result, permit)
                                }).await;
                                let Some((read, permit)) = read else {
                                    break;
                                };
                                let (read, payload) = read.map_err(RuntimeError::Io)?;
                                permit.retain(&mut send_buffer_reservation, read);
                                if read == 0 {
                                    state.record_local_eof();
                                    prepared_work_changed = true;
                                    break;
                                }
                                state.record_local_payload(request_lane);
                                {
                                    let mut product = request_product.lock();
                                    let payload = payload.expect("positive read returns payload");
                                    #[cfg(feature = "lab-diagnostics")]
                                    lab_diagnostic(
                                        "client_sender_enqueue",
                                        format_args!(
                                            "stream_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} opportunistic=true",
                                            stream_id.0, request_lane, read,
                                            product.sender_queue.bytes().saturating_add(read), sender_queue_limit,
                                        ),
                                    );
                                    product.sender_queue.push_data(payload);
                                    publish_prepared_request_work(
                                        &mut product, &request_product, context, request_lane, adaptive_chunk, true,
                                    );
                                }
                                opportunistic_reads = opportunistic_reads.saturating_add(1);
                            }
                        }
                    }
                    RelayServiceEvent::Input(frame) => {
                        let ReliableRelayRemoteFrame { instance, frame } = match frame {
                            Ok(frame) => frame,
                            Err(err) if reliable_path_error_is_migratable(&err) => {
                                let attach = {
                                    let product = request_product.lock();
                                    begin_reliable_relay_attach_with_suppressions(
                                        context, &spec,
                                        ReliableRelayPathLanes::new(topology_lane, request_lane),
                                        &product.remotes,
                                        ReliableRelayAttachInput::capture(
                                            &product.send_stream, topology_lane, context.mux_limits,
                                            source_complete, ReliableRelayAttachMode::Any,
                                        ),
                                        &state.recovery.path_open_suppressions,
                                        &state.recovery.pending_additional_path_opens,
                                    )
                                };
                                let attached = finish_request_attachment(
                                    context, &request_product, &mut return_plan, attach,
                                ).await;
                                if matches!(attached, Ok(count) if count > 0) {
                                    state.progress.sender_retry_at = None;
                                    state.progress.last_stream_at = Instant::now();
                                    continue;
                                }
                                let finished = {
                                    let product = request_product.lock();
                                    state.is_finished(&product.send_stream, &recv_stream, &product.sender_queue)
                                };
                                if finished {
                                    break Ok(state.delivery.total);
                                }
                                break Err(err);
                            }
                            Err(err) => {
                                let finished = {
                                    let product = request_product.lock();
                                    state.is_finished(&product.send_stream, &recv_stream, &product.sender_queue)
                                };
                                if finished {
                                    break Ok(state.delivery.total);
                                }
                                break Err(err);
                            }
                        };
                        let frame = match frame {
                            Ok(frame) => frame,
                            Err(err) if reliable_path_error_is_migratable(&err) => {
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "client_path_frame_error",
                                    format_args!(
                                        "stream_id={} path_underlay={:?} path_index={} path_instance_id={:?} attachment_id={} error={}",
                                        stream_id.0, instance.key.underlay, instance.key.index,
                                        instance.path_instance_id, instance.attachment_id, err,
                                    ),
                                );
                                let native_settlement = {
                                    let mut product = request_product.lock();
                                    begin_client_relay_path_error(context, &mut product.remotes, instance, &err)
                                };
                                let settlement = native_settlement.await;
                                {
                                    let mut product_guard = request_product.lock();
                                    let product = &mut *product_guard;
                                    let (sender, send_stream, remotes) =
                                        (&mut product.sender, &mut product.send_stream, &mut product.remotes);
                                    if let Some(settlement) = settlement {
                                        finish_client_relay_path_error(
                                            sender, context, remotes,
                                            &mut state.recovery.path_open_suppressions, settlement,
                                        );
                                    }
                                    if !remotes.is_empty() {
                                        send_stream.update_max_offset(remotes.max_offset());
                                        state.progress.last_stream_at = Instant::now();
                                        state.progress.last_response_stall_reinjection_at = Instant::now();
                                    }
                                }
                                request_recovery_dirty = true;
                                state.progress.sender_retry_at = None;
                                continue;
                            }
                            Err(err) => break Err(err),
                        };
                        #[cfg(test)]
                        super::client::gate_ready_feedback_test_input(stream_id, &frame, &mut remote_input).await;
                        state.progress.sender_retry_at = None;
                        match frame {
                            Frame::StreamFeedbackProbe {
                                stream_id: probe_stream_id, token, max_offset,
                            } if probe_stream_id == stream_id => {
                                let mut product = request_product.lock();
                                // MAX ingress is deliberately coalesced outside FIFO.
                                // Consume only that real preceding credit, not the
                                // marker's claimed offset, before confirming service.
                                if let Some(ReliableRelayRemoteFrame {
                                    frame: Ok(Frame::StreamMaxData { max_offset, .. }), ..
                                }) = remote_input.take_pending_credit() {
                                    prepared_work_changed |= apply_client_peer_max_data(&mut product, max_offset);
                                    state.progress.last_stream_at = Instant::now();
                                }
                                let applied_max = product.send_stream.peer_max_offset();
                                #[cfg(feature = "lab-diagnostics")]
                                crate::lab_diagnostics::lab_diagnostic("feedback_return", format_args!(
                                    "session_id={} stream_id={} kind=owner_received output={:?} token={} required_max_offset={} applied_peer_max_offset={}",
                                    context.session_id.0, stream_id.0, instance, token, max_offset, applied_max,
                                ));
                                product.remotes.observe_applied_peer_max_offset(applied_max);
                                if let Err(error) = product.remotes.receive_feedback_probe(instance, token, max_offset) {
                                    break Err(error);
                                }
                                stream_ack_capacity_wait = None;
                            }
                            Frame::StreamFeedbackReceipt {
                                stream_id: receipt_stream_id, token,
                            } if receipt_stream_id == stream_id => {
                                // The token owns the probed local incarnation. This
                                // receipt's reverse carrier is not proof authority.
                                let mut product = request_product.lock();
                                product.remotes.receive_feedback_receipt(context, token);
                                stream_ack_capacity_wait = None;
                            }
                            Frame::StreamRequalifyData {
                                stream_id: received_stream_id,
                                probe_id,
                                offset,
                                payload,
                            } if received_stream_id == stream_id && state.endpoint.remote_open => {
                                let mut product_guard = request_product.lock();
                                let remotes = &mut product_guard.remotes;
                                // This duplicate deliberately owns no DSN range and is
                                // never delivered. Its exact probe tuple identifies
                                // the forward attachment; any authenticated sibling in
                                // this session may provide reverse ACK service.
                                let payload_bytes = u32::try_from(payload.len())
                                    .map_err(|_| RuntimeError::Protocol("requalification payload overflow"))?;
                                match remotes.publish_requalification_ack(
                                    instance,
                                    Frame::StreamRequalifyAck {
                                        stream_id,
                                        probe_id,
                                        offset,
                                        payload_bytes,
                                    },
                                ) {
                                    Ok(_) => {}
                                    Err(err) if reliable_path_error_is_migratable(&err) => {}
                                    Err(err) => break Err(err),
                                }
                            }
                            Frame::StreamRequalifyAck {
                                stream_id: ack_stream_id,
                                probe_id,
                                offset,
                                payload_bytes,
                            } if ack_stream_id == stream_id => {
                                let mut product_guard = request_product.lock();
                                let sender = &mut product_guard.sender;
                                if sender.acknowledge_requalification_probe(
                                    instance,
                                    probe_id,
                                    offset,
                                    payload_bytes,
                                ) {
                                    state.progress.sender_retry_at = None;
                                    request_recovery_dirty = true;
                                    prepared_work_changed = true;
                                }
                            }
                            Frame::StreamData {
                                stream_id: received_stream_id,
                                offset,
                                payload,
                            } if received_stream_id == stream_id && state.endpoint.remote_open => {
                                let ready_items = if recv_stream.reorder_bytes() == 0 {
                                    remote_input.ready_frame_count()
                                } else {
                                    0
                                };
                                let receive_limit = state
                                    .endpoint
                                    .pending_remote_fin_offset
                                    .unwrap_or(u64::MAX)
                                    .min(recv_stream.published_max_offset());
                                let payload_limit = reliable_relay_buffer_len(context.mux_limits)
                                    .min({
                                        let product = request_product.lock();
                                        product.remotes.max_frame_payload_bytes(context.mux_limits)
                                    })
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
                                    || remote_input.try_recv_frame(),
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
                                let applied = apply_ready_stream_data_batch(
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
                                            recv_stream,
                                            stream_id,
                                            instance,
                                            offset,
                                            payload,
                                        )?;
                                        data_effect = Some(effect);
                                        Ok(outcome)
                                    },
                                );
                                if applied.has_apply_error() {
                                    // Client ready-batch collection admits only the exact
                                    // contiguous stream, payload, final-offset, and
                                    // published-credit geometry consumed by the callback.
                                    // Its first fallible operation therefore precedes all
                                    // receive-state mutation; a later-item error cannot
                                    // follow an accepted prefix in this synchronous branch.
                                    debug_assert!(data_effect.is_none());
                                    if let Err(err) = write_applied_ready_stream_data_batch(
                                        &mut local,
                                        &mut ready_remote_data,
                                        applied,
                                    )
                                    .await
                                    {
                                        break Err(err);
                                    }
                                    unreachable!("a deferred apply error must surface after its valid prefix write");
                                }

                                {
                                let mut product_guard = request_product.lock();
                                let product = &mut *product_guard;
                                let (sender, remotes) = (&mut product.sender, &mut product.remotes);
                                let prewrite_response_path_snapshot = remotes.lowest_eta_path_snapshot(
                                    context,
                                    response_lane,
                                    PATH_OPEN_SCORE_BYTES,
                                );
                                match sender
                                    .send_recv_progress(
                                        remotes,
                                        context,
                                        &mut recv_stream,
                                        &mut state.progress.recv_progress,
                                        RelayRecvProgressSend::ack_only(
                                            prewrite_response_path_snapshot,
                                            response_lane,
                                        ),
                                    )
                                {
                                    Ok(sent) => state.record_recv_progress_sent(sent),
                                    Err(err) if reliable_path_error_is_migratable(&err) => {
                                        state.progress.sender_retry_at = None;
                                    }
                                    Err(err) => break Err(err),
                                }
                                if let Err(err) = drive_client_response_startup_control(
                                    context,
                                    &spec,
                                    &mut return_plan,
                                    request_lane,
                                    stream_id,
                                    recv_stream.next_offset(),
                                    remotes,
                                    &mut state,
                                    &additional_path_open_tx,
                                ) {
                                    break Err(err);
                                }

                                }
                                let mut pending_write_path_opens = VecDeque::new();
                                let write = {
                                    let write = write_applied_ready_stream_data_batch(
                                        &mut local,
                                        &mut ready_remote_data,
                                        applied,
                                    );
                                    tokio::pin!(write);
                                    loop {
                                        let (stream_ack_capacity_wait, stream_ack_blocked, has_stream_ack_capacity_wait, feedback_deadline, return_plan_final_capacity_wait, return_plan_final_blocked, has_return_plan_final_capacity_wait, mut prepared_work_wait) = {
                                        let mut product_guard = request_product.lock();
                                        if let Some(error) = product_guard.prepared.pending_error.take() {
                                            break Err(error);
                                        }
                                        publish_prepared_request_work(
                                            &mut product_guard,
                                            &request_product,
                                            context,
                                            request_lane,
                                            adaptive_chunk,
                                            false,
                                        );
                                        let applied_max = product_guard.send_stream.peer_max_offset();
                                        let remotes = &mut product_guard.remotes;
                                        remotes.observe_applied_peer_max_offset(applied_max);
                                        remotes.prepare_feedback_route(context);
                                        if let Err(err) = drive_client_response_startup_control(
                                            context,
                                            &spec,
                                            &mut return_plan,
                                            request_lane,
                                            stream_id,
                                            recv_stream.next_offset(),
                                            remotes,
                                            &mut state,
                                            &additional_path_open_tx,
                                        ) {
                                            break Err(err);
                                        }

                                        let stream_ack_pending =
                                            remotes.has_pending_feedback_publication();
                                        let stream_ack_capacity_wait = stream_ack_pending
                                            .then(|| {
                                                arm_carrier_capacity_notifies(
                                                    remotes.pending_feedback_capacity_notifies(),
                                                )
                                            })
                                            .flatten();
                                        if stream_ack_pending {
                                            let publication = remotes.retry_pending_feedback_with_context(context);
                                            if let Some(published_offset) = publication.max_data.published_offset {
                                                recv_stream.commit_max_data(published_offset);
                                            }
                                            state.record_recv_progress_sent(publication);
                                        }
                                        let stream_ack_blocked =
                                            remotes.has_pending_feedback_publication();
                                        let has_stream_ack_capacity_wait =
                                            stream_ack_capacity_wait.is_some();
                                        let feedback_deadline = remotes.feedback_deadline()
                                            .map(tokio::time::Instant::from_std);

                                        let return_plan_final_pending =
                                            remotes.has_pending_return_plan_final_publication();
                                        let return_plan_final_capacity_wait = return_plan_final_pending
                                            .then(|| {
                                                arm_carrier_capacity_notifies(
                                                    remotes
                                                        .pending_return_plan_final_capacity_notifies(),
                                                )
                                            })
                                            .flatten();
                                        if return_plan_final_pending {
                                            remotes.retry_pending_return_plan_final();
                                        }
                                        let return_plan_final_blocked =
                                            remotes.has_pending_return_plan_final_publication();
                                        let has_return_plan_final_capacity_wait =
                                            return_plan_final_capacity_wait.is_some();

                                        let mut prepared_work_wait = Box::pin(
                                            product_guard.prepared.work_changed.clone().notified_owned(),
                                        );
                                        prepared_work_wait.as_mut().enable();
                                        (stream_ack_capacity_wait, stream_ack_blocked, has_stream_ack_capacity_wait, feedback_deadline, return_plan_final_capacity_wait, return_plan_final_blocked, has_return_plan_final_capacity_wait, prepared_work_wait)
                                        };
                                        tokio::select! {
                                            biased;
                                            result = &mut write => break result,
                                            _ = wait_for_optional_deadline(feedback_deadline), if feedback_deadline.is_some() => continue,
                                            () = &mut prepared_work_wait => continue,
                                            additional_path_open = additional_path_open_rx.recv(), if !state.recovery.pending_additional_path_opens.is_empty() => {
                                                let mut product_guard = request_product.lock();
                                                let (sender, send_stream, remotes) = {
                                                    let product = &mut *product_guard;
                                                    (&mut product.sender, &mut product.send_stream, &mut product.remotes)
                                                };
                                                let Some(additional_path_open) = additional_path_open else {
                                                    cancel_pending_additional_path_opens(
                                                        stream_id,
                                                        &mut state.recovery.pending_additional_path_opens,
                                                    );
                                                    continue;
                                                };
                                                // Stream-terminal authority is independent of local
                                                // delivery and of the attachment attempt's generation.
                                                if let Some(error) = additional_path_open.terminal_error() {
                                                    break Err(error);
                                                }
                                                if additional_path_open.startup_ordinal.is_none() {
                                                    if matching_additional_path_open_pending(
                                                        &state.recovery.pending_additional_path_opens,
                                                        additional_path_open.key,
                                                        additional_path_open.generation,
                                                    ) {
                                                        pending_write_path_opens.push_back(
                                                            PendingLocalWritePathOpen::Deferred(
                                                                additional_path_open,
                                                            ),
                                                        );
                                                    } else if let Ok(opened) = additional_path_open.result {
                                                        opened.retire_uncommitted();
                                                    }
                                                    continue;
                                                }
                                                let attached_mode = match settle_matching_client_additional_path_open(
                                                    stream_id,
                                                    &mut state,
                                                    &mut return_plan,
                                                    remotes,
                                                    send_stream,
                                                    request_lane,
                                                    additional_path_open,
                                                    source_complete,
                                                ) {
                                                    Ok(mode) => mode,
                                                    Err(err) => break Err(err),
                                                };
                                                let attached = attached_mode.is_some();
                                                pending_write_path_opens.push_back(
                                                    PendingLocalWritePathOpen::Applied(attached_mode),
                                                );
                                                if attached {
                                                    let current_response_path_snapshot = remotes
                                                        .lowest_eta_path_snapshot(
                                                            context,
                                                            response_lane,
                                                            PATH_OPEN_SCORE_BYTES,
                                                        );
                                                    match sender
                                                        .send_recv_progress(
                                                            remotes,
                                                            context,
                                                            &mut recv_stream,
                                                            &mut state.progress.recv_progress,
                                                            RelayRecvProgressSend::ack_only(
                                                                current_response_path_snapshot,
                                                                response_lane,
                                                            ),
                                                        )
                                                    {
                                                        Ok(sent) => state.record_recv_progress_sent(sent),
                                                        Err(err) if reliable_path_error_is_migratable(&err) => {
                                                            state.progress.sender_retry_at = None;
                                                        }
                                                        Err(err) => break Err(err),
                                                    }
                                                }
                                            }
                                            _ = async move {
                                                if let Some(wait) = stream_ack_capacity_wait {
                                                    wait.await;
                                                }
                                            }, if stream_ack_blocked && has_stream_ack_capacity_wait => {
                                                continue;
                                            }
                                            _ = async move {
                                                if let Some(wait) = return_plan_final_capacity_wait {
                                                    wait.await;
                                                }
                                            }, if return_plan_final_blocked && has_return_plan_final_capacity_wait => {
                                                continue;
                                            }
                                        }
                                    }
                                };
                                if let Err(err) = write {
                                    break Err(err);
                                }
                                let data_effect = data_effect.expect("ready data batch contains its first frame");
                                let pending_attach = {
                                let mut product_guard = request_product.lock();
                                let (sender_queue, sender, send_stream, remotes) = {
                                    let product = &mut *product_guard;
                                    (&mut product.sender_queue, &mut product.sender, &mut product.send_stream, &mut product.remotes)
                                };

                                let postactions = 'postactions: {
                                    while let Some(open) = pending_write_path_opens.pop_front() {
                                        let attached_mode = match open {
                                            PendingLocalWritePathOpen::Applied(mode) => mode,
                                            PendingLocalWritePathOpen::Deferred(additional_path_open) => {
                                                match settle_matching_client_additional_path_open(
                                                    stream_id,
                                                    &mut state,
                                                    &mut return_plan,
                                                    remotes,
                                                    send_stream,
                                                    request_lane,
                                                    additional_path_open,
                                                    sender_queue.data_bytes() == 0,
                                                ) {
                                                    Ok(mode) => mode,
                                                    Err(err) => break 'postactions Err(err),
                                                }
                                            }
                                        };
                                        if let Err(err) = apply_client_additional_path_open_postaction(
                                            attached_mode,
                                            sender,
                                            sender_queue,
                                            context,
                                            remotes,
                                            send_stream,
                                            &mut recv_stream,
                                            &mut state,
                                            request_lane,
                                            response_lane,
                                        )
                                        {
                                            break 'postactions Err(err);
                                        }
                                    }
                                    Ok(())
                                };
                                if let Err(err) = postactions {
                                    break Err(err);
                                }

                                let mut pending_attach = None;
                                let current_response_path_snapshot = remotes.lowest_eta_path_snapshot(
                                    context,
                                    response_lane,
                                    PATH_OPEN_SCORE_BYTES,
                                );
                                match sender.send_recv_progress(
                                    remotes,
                                    context,
                                    &mut recv_stream,
                                    &mut state.progress.recv_progress,
                                    RelayRecvProgressSend::new(
                                        current_response_path_snapshot,
                                        response_lane,
                                        false,
                                    ),
                                )
                                {
                                    Ok(sent) => state.record_recv_progress_sent(sent),
                                    Err(err) if reliable_path_error_is_migratable(&err) => {
                                        if remotes.is_empty() {
                                            continue;
                                        }
                                            let attach = begin_reliable_relay_attach_with_suppressions(
                                                context,
                                                &spec,
                                                ReliableRelayPathLanes::new(response_lane, request_lane),
                                                remotes,
                                                ReliableRelayAttachInput::capture(
                                                    send_stream,
                                                    response_lane,
                                                    context.mux_limits,
                                                    source_complete,
                                                    ReliableRelayAttachMode::Any,
                                                ),
                                                &state.recovery.path_open_suppressions,
                                                &state.recovery.pending_additional_path_opens,
                                            );
                                        pending_attach = Some((attach, err));
                                    }
                                    Err(err) => break Err(err),
                                }
                                pending_attach
                                };
                                if let Some((attach, original_error)) = pending_attach {
                                    match finish_request_attachment(context, &request_product, &mut return_plan, attach).await {
                                        Ok(attached) if attached > 0 => {
                                            state.progress.sender_retry_at = None;
                                            state.progress.last_stream_at = Instant::now();
                                        }
                                        Ok(_) => break Err(original_error),
                                        Err(error) => break Err(error),
                                    }
                                }
                                let local_shutdown = {
                                let mut product_guard = request_product.lock();
                                let product = &mut *product_guard;
                                let (sender, remotes) = (&mut product.sender, &mut product.remotes);
                                let current_response_path_snapshot = remotes.lowest_eta_path_snapshot(
                                    context,
                                    response_lane,
                                    PATH_OPEN_SCORE_BYTES,
                                );
                                if data_effect.fin_ready {
                                    let feedback_published = match sender.send_recv_progress(
                                        remotes,
                                        context,
                                        &mut recv_stream,
                                        &mut state.progress.recv_progress,
                                        RelayRecvProgressSend::final_ack(
                                            current_response_path_snapshot,
                                            response_lane,
                                        ),
                                    )
                                    {
                                        Ok(sent) => {
                                            record_final_recv_progress_enqueue(
                                                &mut state,
                                                sent,
                                                current_response_path_snapshot,
                                            );
                                            sent.ack.published
                                        }
                                        Err(err) if reliable_path_error_is_migratable(&err) => false,
                                        Err(err) => break Err(err),
                                    };
                                    Some(commit_pending_remote_fin(
                                        &mut local,
                                        &mut state,
                                        &recv_stream,
                                        feedback_published && remotes.has_receive_feedback_output(),
                                    ))
                                } else {
                                    None
                                }
                                };
                                if let Some(local_shutdown) = local_shutdown {
                                    if let Err(err) = local_shutdown.await {
                                        break Err(err);
                                    }
                                }
                            }
                            feedback @ Frame::StreamAck { stream_id: feedback_stream_id, .. }
                            | feedback @ Frame::StreamMaxData { stream_id: feedback_stream_id, .. }
                                if feedback_stream_id == stream_id => {
                                // Freeze only the attempt count. Actual coalesced credit can
                                // advance concurrently; try_recv_frame retains its own order.
                                let ready_items = remote_input.ready_frame_count();
                                let pending_attach = {
                                let mut product_guard = request_product.lock();
                                let previous_queue_bytes = product_guard.sender_queue.bytes();
                                let mut pending_attach = None;
                                let mut received_ack = false;
                                let mut ack_facts_changed = false;
                                let mut claim_inputs_changed = false;
                                let deferred = match apply_ready_client_feedback(
                                    feedback,
                                    stream_id,
                                    ready_items,
                                    || remote_input.try_recv_frame(),
                                    |feedback| {
                                        state.progress.sender_retry_at = None;
                                        match feedback {
                                            Frame::StreamAck { scope_start, ranges, .. } => {
                                                received_ack = true;
                                                let product = &mut *product_guard;
                                                let previous_had_ack_gaps = product.last_send_ack.has_gaps();
                                                let previous_ack_frontier = product.send_stream.data_ack_frontier();
                                                let previous_queue_bytes = product.sender_queue.bytes();
                                                let ack_outcome = apply_client_stream_ack(
                                                    ClientStreamAckContext {
                                                        state: &mut state,
                                                        sender: &mut product.sender,
                                                        sender_queue: &mut product.sender_queue,
                                                        context,
                                                        remotes: &mut product.remotes,
                                                        send_stream: &mut product.send_stream,
                                                        last_send_ack: &mut product.last_send_ack,
                                                        relay_lane: request_lane,
                                                    },
                                                    stream_id,
                                                    scope_start,
                                                    ranges,
                                                )?;
                                                send_buffer_reservation.release(ack_outcome.released_bytes);
                                                request_recovery_dirty |= ack_outcome.has_new_facts;
                                                ack_facts_changed |= ack_outcome.has_new_facts;
                                                claim_inputs_changed |= ack_outcome.released_bytes > 0
                                                    || previous_had_ack_gaps != product.last_send_ack.has_gaps()
                                                    || previous_ack_frontier != product.send_stream.data_ack_frontier()
                                                    || previous_queue_bytes != product.sender_queue.bytes();
                                            }
                                            Frame::StreamMaxData { max_offset, .. } => {
                                                claim_inputs_changed |= apply_client_peer_max_data(
                                                    &mut product_guard, max_offset,
                                                );
                                                stream_ack_capacity_wait = None;
                                                state.progress.last_stream_at = Instant::now();
                                            }
                                            _ => unreachable!("the Input quantum contains only this stream's ACK/MAX"),
                                        }
                                        Ok(())
                                    },
                                ) {
                                    Ok(deferred) => deferred,
                                    Err(err) => {
                                        // Earlier valid feedback is committed, but the
                                        // stream now terminates without recovery discovery.
                                        // Revoke old writer notices before this unlock.
                                        product_guard.prepared.claims_active = false;
                                        break Err(err);
                                    }
                                };
                                debug_assert!(deferred_remote_frame.is_none());
                                deferred_remote_frame = deferred;
                                // Existing prepared registrations can claim upon unlock.
                                // Refresh recovery first, not at a later actor iteration.
                                if ack_facts_changed {
                                    let product = &mut *product_guard;
                                    evaluate_client_data_ack_reinjection(
                                        &mut state,
                                        &product.last_send_ack,
                                        &mut product.sender,
                                        &mut product.sender_queue,
                                        context,
                                        &mut product.remotes,
                                        &mut product.send_stream,
                                        path_snapshot,
                                        request_lane,
                                        stream_id,
                                    );
                                }
                                claim_inputs_changed |=
                                    previous_queue_bytes != product_guard.sender_queue.bytes();
                                publish_prepared_request_work(
                                    &mut product_guard,
                                    &request_product,
                                    context,
                                    request_lane,
                                    adaptive_chunk,
                                    claim_inputs_changed,
                                );
                                let (sender_queue, sender, send_stream, remotes) = {
                                    let product = &mut *product_guard;
                                    (&mut product.sender_queue, &mut product.sender,
                                     &mut product.send_stream, &mut product.remotes)
                                };
                                if received_ack && reliable_relay_can_send_pending_fin(
                                    state.endpoint.pending_local_fin,
                                    sender_queue.is_empty(),
                                ) {
                                    match sender
                                        .send_control_frame(
                                            context,
                                            remotes,
                                            Frame::StreamFin {
                                                stream_id,
                                                final_offset: send_stream.next_offset(),
                                            },
                                            RelaySendCause::StreamFin,
                                        )
                                    {
                                        Ok(_) => state.record_local_fin_sent(),
                                        Err(err) if reliable_path_error_is_migratable(&err) => {
                                            if remotes.is_empty() {
                                                continue;
                                            }
                                                let attach = begin_reliable_relay_attach_with_suppressions(
                                                    context,
                                                    &spec,
                                                    ReliableRelayPathLanes::new(request_lane, request_lane),
                                                    remotes,
                                                    ReliableRelayAttachInput::capture(
                                                        send_stream,
                                                        request_lane,
                                                        context.mux_limits,
                                                        true,
                                                        ReliableRelayAttachMode::Any,
                                                    ),
                                                    &state.recovery.path_open_suppressions,
                                                    &state.recovery.pending_additional_path_opens,
                                                );
                                            pending_attach = Some((attach, err));
                                        }
                                        Err(RuntimeError::SenderServiceBlocked) => {
                                            state.progress.sender_retry_at = Some(
                                                tokio::time::Instant::now()
                                                    + sender_service_retry_delay(path_snapshot),
                                            );
                                        }
                                        Err(err) => break Err(err),
                                    }
                                }
                                pending_attach
                                };
                                if let Some((attach, original_error)) = pending_attach {
                                    match finish_request_attachment(context, &request_product, &mut return_plan, attach).await {
                                        Ok(attached) if attached > 0 => {
                                            state.progress.sender_retry_at = None;
                                            state.record_local_fin_sent();
                                        }
                                        Ok(_) => break Err(original_error),
                                        Err(error) => break Err(error),
                                    }
                                }
                            }
                            Frame::StreamFin {
                                stream_id: fin_stream_id,
                                final_offset,
                            } if fin_stream_id == stream_id => {
                                let local_shutdown = {
                                let mut product_guard = request_product.lock();
                                let product = &mut *product_guard;
                                let (sender, remotes) = (&mut product.sender, &mut product.remotes);
                                remotes.finish_feedback_route();
                                stream_ack_capacity_wait = None;
                                state.progress.last_stream_at = Instant::now();
                                return_plan.observe_response_terminal(
                                    final_offset,
                                    recv_stream.next_offset(),
                                );
                                if return_plan.is_done() {
                                    remotes.clear_return_plan_final();
                                }
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
                                        remotes,
                                        context,
                                        &mut recv_stream,
                                        &mut state.progress.recv_progress,
                                        RelayRecvProgressSend::final_ack(
                                            response_path_snapshot,
                                            response_lane,
                                        ),
                                    )
                                    {
                                        Ok(sent) => {
                                            record_final_recv_progress_enqueue(
                                                &mut state,
                                                sent,
                                                response_path_snapshot,
                                            );
                                            sent.ack.published
                                        }
                                        Err(err) if reliable_path_error_is_migratable(&err) => false,
                                        Err(err) => break Err(err),
                                    };
                                    Some(commit_pending_remote_fin(
                                        &mut local,
                                        &mut state,
                                        &recv_stream,
                                        feedback_published && remotes.has_receive_feedback_output(),
                                    ))
                                } else {
                                    None
                                }
                                };
                                if let Some(local_shutdown) = local_shutdown {
                                    if let Err(err) = local_shutdown.await {
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
                                let mut product_guard = request_product.lock();
                                let product = &mut *product_guard;
                                let (sender, remotes) = (&mut product.sender, &mut product.remotes);
                                match sender
                                    .send_recv_progress(
                                        remotes,
                                        context,
                                        &mut recv_stream,
                                        &mut state.progress.recv_progress,
                                        RelayRecvProgressSend::new(
                                            response_path_snapshot,
                                            response_lane,
                                            true,
                                        ),
                                    )
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
                }
            }
            () = &mut idle => break Err(RuntimeError::ProductIdleTimeout),
            else => break Ok(state.delivery.total),
        }
    };

    let cleanup: std::pin::Pin<Box<dyn Future<Output = ()> + Send>> = {
        let mut product_guard = request_product.lock();
        let product = &mut *product_guard;
        product.prepared.claims_active = false;
        product.prepared.registrations.clear();
        product.prepared.work_changed.notify_waiters();
        let claimed_offset = product.send_stream.next_offset();
        if claimed_offset != observed_claimed_offset {
            state.delivery.total.record_payload_bytes(
                usize::try_from(claimed_offset.saturating_sub(observed_claimed_offset))
                    .unwrap_or(usize::MAX),
            );
            if result.is_ok() {
                result = Ok(state.delivery.total);
            }
        }
        let source_complete = !state.endpoint.local_open && product.sender_queue.data_bytes() == 0;
        let (remotes, send_stream) = (&mut product.remotes, &mut product.send_stream);
        let _ = try_drain_completed_additional_path_opens(
            stream_id,
            &mut return_plan,
            remotes,
            send_stream,
            source_complete,
            request_flow_demand.current_lane(),
            &mut state.recovery.pending_additional_path_opens,
            &mut additional_path_open_rx,
            &mut state.progress.last_stream_at,
        );
        cancel_pending_additional_path_opens(
            stream_id,
            &mut state.recovery.pending_additional_path_opens,
        );

        // Successful teardown stays behind ordered FIN work. A failed local
        // product socket is terminal, while carrier failures retain detach-only
        // semantics so the logical stream can survive path recovery.
        match &result {
            Ok(_) => Box::pin(remotes.close_all_ordered()),
            Err(RuntimeError::Io(_)) => Box::pin(remotes.reset_all(ResetReason::RemoteClosed)),
            Err(RuntimeError::ProductIdleTimeout) => {
                remotes.retire_all_with_reset(ResetReason::TimedOut);
                Box::pin(std::future::ready(()))
            }
            Err(_) => Box::pin(remotes.close_all()),
        }
    };
    cleanup.await;
    if matches!(result, Err(RuntimeError::ProductIdleTimeout)) {
        result = Ok(state.delivery.total);
    }
    let mut product_guard = request_product.lock();
    let product = &mut *product_guard;
    let sender = &mut product.sender;
    #[cfg(feature = "lab-diagnostics")]
    let (sender_queue, send_stream) = (&product.sender_queue, &product.send_stream);
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
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("multipath_stream_close");
    if result.is_ok() {
        telemetry_flow.complete();
    }
    result
}

#[cfg(test)]
#[path = "tests_control.rs"]
mod tests;

#[cfg(test)]
#[path = "tests_c3_common.rs"]
mod tests_c3_common;
