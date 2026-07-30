//! Client relay progress, additional-path establishment, and recovery lifecycle.
//!
//! The bidirectional actor remains in `control`; this owner decides when
//! progress is authoritative, when completion is safe, and how path loss or
//! stalls open and attach replacement carriers.

use super::open::{
    ReliableRelayOpenSpec, open_remote_stream_for_relay_path, relay_path_open_error_is_retryable,
};
use super::remote::{
    ReliableRelayAttachMode, attach_reliable_relay_paths,
    attach_reliable_relay_paths_with_claims_and_recovery_exclusions,
    reliable_relay_additional_path_open_payload_bytes, reliable_relay_attach_payload_bytes,
    reliable_relay_reinjection_path_candidates,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{QUIC_PERSISTENT_CONGESTION_THRESHOLD, reliable_relay_buffer_len};
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::protocol::{Frame, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::{ClientPathContext, PathDeliveryStats};
use crate::runtime::sender::{ReliableRelaySenderQueue, RequestSenderService};
use crate::runtime::stream::{
    OpenedRemoteStream, ReliableRelayAttachOutcome, ReliableRelayRemoteSet,
};
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;

static NEXT_RELAY_ADDITIONAL_PATH_OPEN_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RelayAdditionalPathOpenGeneration(u64);

fn next_relay_additional_path_open_generation() -> RelayAdditionalPathOpenGeneration {
    let mut generation = NEXT_RELAY_ADDITIONAL_PATH_OPEN_GENERATION.fetch_add(1, Ordering::Relaxed);
    if generation == 0 {
        generation = NEXT_RELAY_ADDITIONAL_PATH_OPEN_GENERATION.fetch_add(1, Ordering::Relaxed);
    }
    RelayAdditionalPathOpenGeneration(generation)
}

pub(super) fn reliable_relay_lane_changed(previous: TrafficClass, current: TrafficClass) -> bool {
    previous != current
}

#[allow(clippy::too_many_arguments)]
pub(in crate::runtime) async fn recover_reliable_relay_after_path_failure(
    sender: &mut RequestSenderService,
    sender_queue: &mut ReliableRelaySenderQueue,
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    failed_instance: RelayPathInstance,
) -> Result<Option<bool>, RuntimeError> {
    if remotes.is_empty() {
        return Ok(None);
    }

    send_stream.update_max_offset(remotes.max_offset());
    let reinjection_queued = sender.enqueue_failed_path_reinjections(
        sender_queue,
        context,
        remotes,
        send_stream,
        failed_instance,
    );
    Ok(Some(reinjection_queued))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn switch_reliable_relay_to_best_path(
    context: &ClientPathContext,
    sender: &mut RequestSenderService,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    pending_additional_path_opens: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) -> Result<bool, RuntimeError> {
    let inflight_path_claims = pending_additional_path_opens
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    let attached = attach_reliable_relay_paths(
        context,
        sender,
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

pub(super) fn reliable_relay_can_send_pending_fin(
    pending_local_fin: bool,
    sender_queue_empty: bool,
) -> bool {
    pending_local_fin && sender_queue_empty
}

pub(super) fn reliable_relay_queued_send_blocked_for_retry(
    sender_queue_empty: bool,
    sender_retry_at: Option<tokio::time::Instant>,
) -> bool {
    !sender_queue_empty && sender_retry_at.is_some()
}

// Keep the relay and stream owners visible across the asynchronous attach boundary.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_additional_path_open_result(
    context: &ClientPathContext,
    sender: &mut RequestSenderService,
    stream_id: StreamId,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    resend_fin: bool,
    additional_path_open: RelayAdditionalPathOpenResult,
    pending_count: usize,
    last_stream_progress_at: &mut Instant,
) -> Option<ReliableRelayAttachMode> {
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (stream_id, pending_count);
    let mode = additional_path_open.mode;
    match additional_path_open.result {
        Ok(opened) => {
            #[cfg(feature = "lab-diagnostics")]
            let lane = opened.stream().lane;
            if matches!(mode, ReliableRelayAttachMode::Recovery)
                && remotes.accepted_path_count() > 1
            {
                opened.close().await;
                return None;
            }
            if resend_fin
                && let Err(err) =
                    opened
                        .stream()
                        .try_enqueue_request_control_frame(Frame::StreamFin {
                            stream_id,
                            final_offset: send_stream.next_offset(),
                        })
            {
                opened.close().await;
                context.mark_relay_path_failure(
                    additional_path_open.key.underlay,
                    additional_path_open.key.index,
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = err;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "relay_additional_path_open_attach_control_failed",
                    format_args!(
                        "stream_id={} path_underlay={:?} path_index={} mode={:?} error={}",
                        stream_id.0,
                        additional_path_open.key.underlay,
                        additional_path_open.key.index,
                        mode,
                        err,
                    ),
                );
                return None;
            }
            match remotes.attach_candidate_before_commit(opened, |_| {
                sender.invalidate_tcp_service_observer();
            }) {
                ReliableRelayAttachOutcome::Attached => {
                    send_stream.update_max_offset(remotes.max_offset());
                    *last_stream_progress_at = Instant::now();
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "relay_additional_path_open_attached",
                        format_args!(
                            "stream_id={} path_underlay={:?} path_index={} pending={}",
                            stream_id.0,
                            additional_path_open.key.underlay,
                            additional_path_open.key.index,
                            pending_count,
                        ),
                    );
                    Some(mode)
                }
                ReliableRelayAttachOutcome::RejectedDuplicate => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "relay_additional_path_open_duplicate_closed",
                        format_args!(
                            "stream_id={} path_underlay={:?} path_index={} lane={:?} pending={}",
                            stream_id.0,
                            additional_path_open.key.underlay,
                            additional_path_open.key.index,
                            lane,
                            pending_count,
                        ),
                    );
                    None
                }
                ReliableRelayAttachOutcome::RejectedResourceLimit => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "relay_additional_path_open_resource_limit",
                        format_args!(
                            "stream_id={} path_underlay={:?} path_index={} pending={}",
                            stream_id.0,
                            additional_path_open.key.underlay,
                            additional_path_open.key.index,
                            pending_count,
                        ),
                    );
                    None
                }
            }
        }
        Err(err) if relay_path_open_error_is_retryable(additional_path_open.key.underlay, &err) => {
            // Preserve global health fencing. A later open becomes eligible
            // only after independent path evidence reactivates the carrier.
            context.mark_relay_path_failure(
                additional_path_open.key.underlay,
                additional_path_open.key.index,
            );
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "relay_additional_path_open_failed",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} retryable=true error={}",
                    stream_id.0,
                    additional_path_open.key.underlay,
                    additional_path_open.key.index,
                    err,
                ),
            );
            None
        }
        Err(err) => {
            context.mark_relay_path_failure(
                additional_path_open.key.underlay,
                additional_path_open.key.index,
            );
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = &err;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "relay_additional_path_open_failed",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} retryable=false error={}",
                    stream_id.0,
                    additional_path_open.key.underlay,
                    additional_path_open.key.index,
                    err,
                ),
            );
            None
        }
    }
}

// Keep the relay and stream owners visible across the asynchronous attach boundary.
#[allow(clippy::too_many_arguments)]
pub(super) async fn drain_completed_additional_path_opens(
    context: &ClientPathContext,
    sender: &mut RequestSenderService,
    stream_id: StreamId,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    resend_fin: bool,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    additional_path_open_rx: &mut mpsc::Receiver<RelayAdditionalPathOpenResult>,
    last_stream_progress_at: &mut Instant,
) -> bool {
    let mut attached = false;
    while let Ok(additional_path_open) = additional_path_open_rx.try_recv() {
        if take_matching_additional_path_open(
            pending,
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
        attached |= handle_additional_path_open_result(
            context,
            sender,
            stream_id,
            remotes,
            send_stream,
            resend_fin,
            additional_path_open,
            pending.len(),
            last_stream_progress_at,
        )
        .await
        .is_some();
    }
    attached
}

pub(super) fn cancel_pending_additional_path_opens(
    stream_id: StreamId,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) {
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    for (key, task) in pending.drain() {
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = key;
        task.handle.abort();
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "relay_additional_path_open_cancelled",
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

pub(super) struct RelayAdditionalPathOpenResult {
    pub(super) key: RelayPathKey,
    pub(super) generation: RelayAdditionalPathOpenGeneration,
    pub(super) mode: ReliableRelayAttachMode,
    pub(super) result: Result<OpenedRemoteStream, RuntimeError>,
}

pub(super) struct RelayAdditionalPathOpenTask {
    generation: RelayAdditionalPathOpenGeneration,
    #[cfg(feature = "lab-diagnostics")]
    lane: TrafficClass,
    handle: tokio::task::JoinHandle<()>,
}

pub(super) fn take_matching_additional_path_open(
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    key: RelayPathKey,
    generation: RelayAdditionalPathOpenGeneration,
) -> Option<RelayAdditionalPathOpenTask> {
    if !pending
        .get(&key)
        .is_some_and(|task| task.generation == generation)
    {
        return None;
    }
    pending.remove(&key)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_reliable_relay_additional_path_opens(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> bool {
    if !lane.is_bulk() {
        return false;
    }
    if !pending.is_empty() {
        return false;
    }
    let stream_id = remotes.stream_id();
    let payload_bytes =
        reliable_relay_additional_path_open_payload_bytes(send_stream, context.mux_limits);
    let candidates =
        reliable_relay_additional_path_open_candidates(context, remotes, lane, payload_bytes);
    let candidates = reliable_relay_available_path_open_candidates(candidates, pending);
    if candidates.is_empty() {
        return false;
    }
    spawn_reliable_relay_path_opens(
        context,
        spec,
        lane,
        ReliableRelayAttachMode::BulkStriping,
        stream_id,
        candidates,
        pending,
        result_tx,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_reliable_relay_recovery_path_open(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    recovery_excluded_paths: &HashSet<RelayPathKey>,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> bool {
    if !reliable_relay_should_open_recovery_path(remotes) || !pending.is_empty() {
        return false;
    }
    let payload_bytes = reliable_relay_attach_payload_bytes(send_stream, lane, context.mux_limits);
    let candidates =
        reliable_relay_reinjection_path_candidates(context, remotes, lane, payload_bytes);
    let candidates =
        reliable_relay_recovery_path_open_candidates(candidates, recovery_excluded_paths, pending);
    if candidates.is_empty() {
        return false;
    }
    spawn_reliable_relay_path_opens(
        context,
        spec,
        lane,
        ReliableRelayAttachMode::Recovery,
        remotes.stream_id(),
        candidates,
        pending,
        result_tx,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_reliable_relay_disconnected_path_open(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    attempted_paths: &mut HashSet<RelayPathKey>,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> bool {
    if !remotes.is_empty() || !pending.is_empty() {
        return false;
    }
    let payload_bytes = reliable_relay_attach_payload_bytes(send_stream, lane, context.mux_limits);
    let candidates =
        reliable_relay_reinjection_path_candidates(context, remotes, lane, payload_bytes);
    let Some(key) = candidates
        .iter()
        .copied()
        .find(|candidate| !attempted_paths.contains(candidate))
    else {
        // A complete ranked round gets one PTO of rest before ranking afresh.
        // Health cooldowns and the shared probe service may change that order.
        if !candidates.is_empty() {
            attempted_paths.clear();
        }
        return false;
    };
    attempted_paths.insert(key);
    spawn_reliable_relay_path_opens(
        context,
        spec,
        lane,
        ReliableRelayAttachMode::Recovery,
        remotes.stream_id(),
        vec![key],
        pending,
        result_tx,
    )
}

pub(super) fn reliable_relay_disconnected_retry_delay() -> std::time::Duration {
    transport_pto_from_snapshot(None)
}

#[allow(clippy::too_many_arguments)]
fn spawn_reliable_relay_path_opens(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    mode: ReliableRelayAttachMode,
    stream_id: StreamId,
    candidates: Vec<RelayPathKey>,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> bool {
    let mut spawned = false;
    for key in candidates {
        match key.underlay {
            UnderlayProtocol::Tcp if context.tcp_paths.get(key.index).is_some() => {}
            UnderlayProtocol::Udp if context.udp_paths.get(key.index).is_some() => {}
            _ => continue,
        }
        let context = context.clone();
        let target = spec.target.clone();
        let result_tx = result_tx.clone();
        let generation = next_relay_additional_path_open_generation();
        let handle = tokio::spawn(async move {
            let result =
                open_remote_stream_for_relay_path(&context, stream_id, target, lane, key).await;
            let message = RelayAdditionalPathOpenResult {
                key,
                generation,
                mode,
                result,
            };
            if let Err(err) = result_tx.send(message).await {
                let RelayAdditionalPathOpenResult { key, result, .. } = err.0;
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = key;
                if let Ok(opened) = result {
                    #[cfg(feature = "lab-diagnostics")]
                    let lane = opened.stream().lane;
                    opened.close().await;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "relay_additional_path_open_orphan_closed",
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
            RelayAdditionalPathOpenTask {
                generation,
                #[cfg(feature = "lab-diagnostics")]
                lane,
                handle,
            },
        );
        spawned = true;
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "relay_additional_path_open_spawned",
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

fn reliable_relay_additional_path_open_candidates(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    lane: TrafficClass,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    // Admission gives unproven OriginalData only to an idle carrier. Offer the
    // same candidates first here; otherwise the one-shot opener can attach an
    // occupied measured path that startup policy must immediately reject.
    let mut candidates = context
        .ordered_reliable_unproven_bulk_path_keys(payload_bytes)
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
    prefer_current_underlay_additional_path_open_candidate(
        candidates,
        remotes.preferred_path_underlay(context, lane, payload_bytes),
    )
}

fn prefer_current_underlay_additional_path_open_candidate(
    mut candidates: Vec<RelayPathKey>,
    preferred_underlay: Option<UnderlayProtocol>,
) -> Vec<RelayPathKey> {
    let Some(preferred_underlay) = preferred_underlay else {
        return candidates;
    };
    if let Some(position) = candidates
        .iter()
        .position(|candidate| candidate.underlay == preferred_underlay)
    {
        let candidate = candidates.remove(position);
        candidates.insert(0, candidate);
    }
    candidates
}

fn reliable_relay_available_path_open_candidates(
    candidates: Vec<RelayPathKey>,
    pending: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) -> Vec<RelayPathKey> {
    let mut selected = Vec::new();
    for candidate in candidates {
        if pending.contains_key(&candidate) || selected.contains(&candidate) {
            continue;
        }
        // TCP and QUIC carrier actors own independent stream-open handshakes.
        // Once bulk demand is established, starting both transports together
        // avoids an artificial cross-transport round trip.
        selected.push(candidate);
    }
    selected
}

fn reliable_relay_recovery_path_open_candidates(
    candidates: Vec<RelayPathKey>,
    recovery_excluded_paths: &HashSet<RelayPathKey>,
    pending: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) -> Vec<RelayPathKey> {
    let candidates = candidates
        .into_iter()
        .filter(|key| !recovery_excluded_paths.contains(key))
        .collect::<Vec<_>>();
    let mut candidates = reliable_relay_available_path_open_candidates(candidates, pending);
    candidates.truncate(1);
    candidates
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn attach_reliable_relay_paths_with_recovery_exclusions(
    context: &ClientPathContext,
    sender: &mut RequestSenderService,
    spec: &ReliableRelayOpenSpec,
    lane: TrafficClass,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    recovery_excluded_paths: &mut HashSet<RelayPathKey>,
    pending_additional_path_opens: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) -> Result<usize, RuntimeError> {
    // A pending open owns logical (stream, path) membership. Synchronous
    // recovery must not race that claim through either carrier.
    let inflight_path_claims = pending_additional_path_opens
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    attach_reliable_relay_paths_with_claims_and_recovery_exclusions(
        context,
        sender,
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
    lane: TrafficClass,
    interactive_response_pending: bool,
    mux_limits: MuxLimits,
) -> bool {
    send_stream.reinjection_bytes() > 0
        || (remote_open && interactive_response_pending)
        || reliable_relay_response_stall_watch_active(recv_stream, remote_open, lane, mux_limits)
}

pub(in crate::runtime) fn reliable_relay_response_stall_watch_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> bool {
    remote_open
        && recv_stream.next_offset() > 0
        && (matches!(lane, TrafficClass::Throughput | TrafficClass::Background)
            || recv_stream.next_offset() >= reliable_relay_response_stall_watch_bytes(mux_limits))
}

#[allow(clippy::too_many_arguments)]
pub(in crate::runtime) fn reliable_relay_stall_progress_anchor(
    last_stream_progress_at: Instant,
    last_delivery_progress_at: Instant,
    last_response_stall_reinjection_at: Instant,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: TrafficClass,
    interactive_response_pending: bool,
    mux_limits: MuxLimits,
) -> Instant {
    if (remote_open && interactive_response_pending)
        || reliable_relay_response_stall_watch_active(recv_stream, remote_open, lane, mux_limits)
    {
        last_delivery_progress_at.max(last_response_stall_reinjection_at)
    } else {
        last_stream_progress_at
    }
}

pub(in crate::runtime) fn reliable_relay_receive_hole_reinjection_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
) -> bool {
    remote_open && recv_stream.next_offset() > 0 && recv_stream.reorder_bytes() > 0
}

pub(in crate::runtime) fn reliable_relay_receive_hole_reinjection_deadline(
    last_delivery_progress_at: Instant,
    last_receive_hole_reinjection_at: Instant,
    path: Option<PathSnapshot>,
) -> tokio::time::Instant {
    let anchor = if last_delivery_progress_at > last_receive_hole_reinjection_at {
        last_delivery_progress_at
    } else {
        last_receive_hole_reinjection_at
    };
    tokio::time::Instant::from_std(anchor + transport_pto_from_snapshot(path))
}

pub(in crate::runtime) fn reliable_relay_product_stall_preserves_attached_path_set(
    remotes: &ReliableRelayRemoteSet,
) -> bool {
    remotes.accepted_path_count() > 1
}

pub(in crate::runtime) fn reliable_relay_product_stall_should_try_alternate_attach(
    remotes: &ReliableRelayRemoteSet,
) -> bool {
    reliable_relay_should_open_recovery_path(remotes)
}

pub(in crate::runtime) fn reliable_relay_should_open_recovery_path(
    remotes: &ReliableRelayRemoteSet,
) -> bool {
    remotes.accepted_path_count() <= 1 && !remotes.is_empty()
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
