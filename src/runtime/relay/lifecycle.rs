//! Client relay progress, completion, validation, and recovery lifecycle.
//!
//! The bidirectional actor remains in `control`; this owner decides when
//! progress is authoritative, when completion is safe, and how path loss or
//! stalls open and attach replacement carriers.

use super::open::{
    OpenedRemoteStream, ReliableRelayOpenSpec, open_remote_stream_on_preselected_tcp_path,
    open_remote_stream_on_preselected_udp_path, relay_path_open_error_is_retryable,
    relay_path_open_with_deadline, reliable_relay_attach_open_timeouts,
};
use super::remote::{
    ReliableRelayAttachMode, ReliableRelayAttachOutcome, ReliableRelayRemoteSet,
    attach_reliable_relay_paths, attach_reliable_relay_paths_with_claims_and_recovery_exclusions,
    reliable_relay_bulk_validation_payload_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{QUIC_PERSISTENT_CONGESTION_THRESHOLD, reliable_relay_buffer_len};
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::protocol::{StreamId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ClientTcpOpenDeadlines;
use crate::runtime::path::{ClientPathContext, PathDeliveryStats, UdpStreamOpenOptions};
use crate::runtime::sender::{ReliableRelaySenderQueue, RequestSenderService};
use crate::scheduler::{FlowLane, PathSnapshot};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio::sync::mpsc;

pub(super) fn reliable_relay_lane_changed(previous: FlowLane, current: FlowLane) -> bool {
    previous != current
}

#[allow(clippy::too_many_arguments)]
pub(in crate::runtime) async fn recover_reliable_relay_after_path_failure(
    sender: &mut RequestSenderService,
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

pub(super) async fn switch_reliable_relay_to_best_path(
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

pub(super) fn maybe_mark_live_relay_path_delivery(
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

pub(super) fn record_client_response_delivery_accounting(
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

pub(super) fn reliable_relay_can_finish_after_path_loss(
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

pub(super) fn reliable_relay_can_send_pending_fin(
    pending_local_fin: bool,
    sender_queue_empty: bool,
) -> bool {
    pending_local_fin && sender_queue_empty
}

pub(super) fn reliable_relay_queued_send_blocked_for_retry(
    sender_queue_empty: bool,
    sender_retry_at: Option<tokio::time::Instant>,
    _front_has_carrier_credit: bool,
) -> bool {
    !sender_queue_empty && sender_retry_at.is_some()
}

pub(super) fn reliable_relay_should_wait_for_pending_path_recovery(
    remote_open: bool,
    pending_validation_opens: &HashMap<RelayPathKey, RelayValidationOpenTask>,
) -> bool {
    remote_open && !pending_validation_opens.is_empty()
}

pub(super) async fn handle_validation_open_result(
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
            #[cfg(feature = "lab-diagnostics")]
            let lane = opened.stream().lane;
            match remotes.attach_for_validation(opened) {
                ReliableRelayAttachOutcome::Attached => {
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
                }
                ReliableRelayAttachOutcome::RejectedDuplicate => {
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

pub(super) async fn drain_completed_validation_opens(
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

pub(super) fn cancel_pending_validation_opens(
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

pub(super) struct RelayValidationOpenResult {
    pub(super) key: RelayPathKey,
    pub(super) result: Result<OpenedRemoteStream, RuntimeError>,
}

pub(super) struct RelayValidationOpenTask {
    #[cfg(feature = "lab-diagnostics")]
    lane: FlowLane,
    handle: tokio::task::JoinHandle<()>,
}

const MAX_RELIABLE_RELAY_VALIDATION_OPEN_ATTEMPTS_PER_PATH: u8 = 2;

pub(super) fn spawn_reliable_relay_validation_opens(
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
            let open_timeouts = reliable_relay_attach_open_timeouts(&context, key);
            let open_started_at = tokio::time::Instant::now();
            let result = match key.underlay {
                UnderlayProtocol::Tcp => {
                    let open_deadlines = ClientTcpOpenDeadlines::from_timeouts(
                        open_started_at,
                        open_timeouts.live,
                        open_timeouts.setup,
                    );
                    let result = relay_path_open_with_deadline(
                        open_deadlines.setup,
                        open_remote_stream_on_preselected_tcp_path(
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
                    let open_deadline = open_started_at + open_timeouts.setup;
                    relay_path_open_with_deadline(
                        open_deadline,
                        open_remote_stream_on_preselected_udp_path(
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
                            open_deadline,
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
                    let lane = opened.stream().lane;
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

pub(super) async fn attach_reliable_relay_paths_with_recovery_exclusions(
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
) -> tokio::time::Instant {
    let anchor = if last_delivery_progress_at > last_receive_hole_repair_at {
        last_delivery_progress_at
    } else {
        last_receive_hole_repair_at
    };
    tokio::time::Instant::from_std(anchor + transport_pto_from_snapshot(path))
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

pub(in crate::runtime) fn reliable_relay_response_stall_watch_bytes(mux_limits: MuxLimits) -> u64 {
    (reliable_relay_buffer_len(mux_limits) as u64).min(mux_limits.max_stream_window_bytes)
}

pub(in crate::runtime) fn reliable_relay_stall_deadline(
    last_progress_at: Instant,
    path: Option<PathSnapshot>,
) -> tokio::time::Instant {
    tokio::time::Instant::from_std(last_progress_at + transport_pto_from_snapshot(path))
}

pub(in crate::runtime) fn reliable_relay_product_stall_deadline(
    last_progress_at: Instant,
    last_attempt_at: Option<Instant>,
    path: Option<PathSnapshot>,
) -> tokio::time::Instant {
    let stall_timeout = transport_pto_from_snapshot(path);
    match last_attempt_at.filter(|attempt| *attempt >= last_progress_at) {
        Some(last_attempt_at) => tokio::time::Instant::from_std(
            last_attempt_at + stall_timeout.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
        ),
        None => reliable_relay_stall_deadline(last_progress_at, path),
    }
}

#[cfg(test)]
#[path = "lifecycle_test.rs"]
mod tests;
