//! Client-side relay session state and peer-frame application.
//!
//! `control` owns the serialized select loop. This module owns durable client
//! state so peer frames cannot update FIN, progress, delivery, and recovery
//! evidence as unrelated local variables.

use super::io::{
    AuthoritativeStreamAckSnapshot, ReliableAckGapReinjectionProgress,
    ReliablePathStalenessObservation, ReliableRequestPathStaleness, begin_reliable_stream_ack,
    stream_ack_gap_reinjection_frames_normalized, stream_ack_ranges_expose_authoritative_gap,
    update_reinjection_authoritative_ack_snapshot,
};
use super::lifecycle::{RelayAdditionalPathOpenTask, maybe_mark_live_relay_path_delivery};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::adaptive_reliable_relay_reinjection_bytes;
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::timing::{
    reliable_data_ack_gap_reinjection_deadline, reliable_data_retransmission_interval,
};
use crate::model::work::reliable_reinjection_service_limit_bytes;
use crate::mux::stream::{ReceiveOutcome, ReliableRecvStream, ReliableSendStream, StreamError};
use crate::protocol::{OffsetRange, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::{ClientPathContext, PathDeliveryStats};
use crate::runtime::sender::{RelaySendCause, ReliableRelaySenderQueue, RequestSenderService};
use crate::runtime::stream::{ReliableRecvProgress, ReliableRelayRemoteSet};
use crate::scheduler::{PathSnapshot, TrafficClass};
use bytes::Bytes;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

pub(super) struct ClientRelayEndpointState {
    pub(super) local_open: bool,
    pub(super) remote_open: bool,
    pub(super) pending_local_fin: bool,
    pub(super) local_fin_sent: bool,
    pub(super) terminal_fin_replayed: bool,
    pub(super) pending_remote_fin_offset: Option<u64>,
}

pub(super) struct ClientRelayProgressState {
    pub(super) last_stream_at: Instant,
    pub(super) last_delivery_at: Instant,
    pub(super) last_response_stall_reinjection_at: Instant,
    pub(super) last_product_stall_attempt_at: Option<Instant>,
    pub(super) last_receive_hole_reinjection_at: Instant,
    pub(super) receive_hole_reinjection_attempts: u32,
    pub(super) interactive_response_pending: bool,
    pub(super) recv_progress: ReliableRecvProgress,
    pub(super) ack_gap_reinjection: ReliableAckGapReinjectionProgress,
    pub(super) request_path_staleness: ReliableRequestPathStaleness,
    pub(super) last_recv_progress_sent_at: Instant,
    pub(super) last_send_ack_frontier: u64,
    pub(super) last_send_ack: AuthoritativeStreamAckSnapshot,
    pub(super) data_ack_reinjection_at: Option<tokio::time::Instant>,
    pub(super) sender_retry_at: Option<tokio::time::Instant>,
    #[cfg(feature = "lab-diagnostics")]
    last_reported_receive_hole: Option<(u64, usize, usize, u64)>,
}

pub(super) struct ClientRelayRecoveryState {
    pub(super) pending_additional_path_opens: HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    pub(super) excluded_paths: HashSet<RelayPathKey>,
    pub(super) disconnected: Option<ClientRelayDisconnectedState>,
}

/// Break-before-make state for one already-established logical stream.
pub(super) struct ClientRelayDisconnectedState {
    pub(super) since: Instant,
    pub(super) retry_at: tokio::time::Instant,
    pub(super) attempted_paths: HashSet<RelayPathKey>,
}

impl ClientRelayDisconnectedState {
    pub(super) fn new(since: Instant, retry_at: tokio::time::Instant) -> Self {
        Self {
            since,
            retry_at,
            attempted_paths: HashSet::new(),
        }
    }

    pub(super) fn expired(&self, now: Instant, retention_timeout: std::time::Duration) -> bool {
        now.saturating_duration_since(self.since) >= retention_timeout
    }

    pub(super) fn retention_deadline(
        &self,
        retention_timeout: std::time::Duration,
    ) -> Option<tokio::time::Instant> {
        self.since
            .checked_add(retention_timeout)
            .map(tokio::time::Instant::from_std)
    }

    pub(super) fn retry_after(&mut self, delay: std::time::Duration) {
        self.retry_at = tokio::time::Instant::now() + delay;
    }
}

#[derive(Default)]
pub(super) struct ClientRelayDeliveryState {
    pub(super) total: PathDeliveryStats,
    pub(super) by_path: HashMap<RelayPathInstance, PathDeliveryStats>,
    next_live_sample_bytes: HashMap<RelayPathInstance, u64>,
}

impl ClientRelayDeliveryState {
    fn record_response(
        &mut self,
        instance: RelayPathInstance,
        delivered: &[Bytes],
        path_scoped_received_bytes: usize,
    ) -> usize {
        let mut delivered_payload_bytes = 0usize;
        for chunk in delivered {
            self.total.record_payload_bytes(chunk.len());
            delivered_payload_bytes = delivered_payload_bytes.saturating_add(chunk.len());
        }
        let path_scoped_delivered_bytes = path_scoped_received_bytes.min(delivered_payload_bytes);
        if path_scoped_delivered_bytes > 0 {
            self.by_path
                .entry(instance)
                .or_default()
                .record_payload_bytes(path_scoped_delivered_bytes);
        }
        delivered_payload_bytes
    }
}

/// Durable state for one logical client stream. Carrier tasks remain separate;
/// only the serialized relay actor mutates this value.
pub(super) struct ClientRelayState {
    pub(super) endpoint: ClientRelayEndpointState,
    pub(super) progress: ClientRelayProgressState,
    pub(super) recovery: ClientRelayRecoveryState,
    pub(super) delivery: ClientRelayDeliveryState,
}

impl ClientRelayState {
    pub(super) fn new() -> Self {
        let now = Instant::now();
        Self {
            endpoint: ClientRelayEndpointState {
                local_open: true,
                remote_open: true,
                pending_local_fin: false,
                local_fin_sent: false,
                terminal_fin_replayed: false,
                pending_remote_fin_offset: None,
            },
            progress: ClientRelayProgressState {
                last_stream_at: now,
                last_delivery_at: now,
                last_response_stall_reinjection_at: now,
                last_product_stall_attempt_at: None,
                last_receive_hole_reinjection_at: now,
                receive_hole_reinjection_attempts: 0,
                interactive_response_pending: false,
                recv_progress: ReliableRecvProgress::default(),
                ack_gap_reinjection: ReliableAckGapReinjectionProgress::default(),
                request_path_staleness: ReliableRequestPathStaleness::default(),
                last_recv_progress_sent_at: now,
                last_send_ack_frontier: 0,
                last_send_ack: AuthoritativeStreamAckSnapshot::default(),
                data_ack_reinjection_at: None,
                sender_retry_at: None,
                #[cfg(feature = "lab-diagnostics")]
                last_reported_receive_hole: None,
            },
            recovery: ClientRelayRecoveryState {
                pending_additional_path_opens: HashMap::new(),
                excluded_paths: HashSet::new(),
                disconnected: None,
            },
            delivery: ClientRelayDeliveryState::default(),
        }
    }

    /// Product-stream completion is stricter than carrier queue acceptance.
    /// Retire only after both FIN directions and all Data Sequence state drain.
    pub(super) fn is_finished(
        &self,
        send_stream: &ReliableSendStream,
        recv_stream: &ReliableRecvStream,
        sender_queue: &ReliableRelaySenderQueue,
    ) -> bool {
        !self.endpoint.local_open
            && !self.endpoint.remote_open
            && !self.endpoint.pending_local_fin
            && self.endpoint.local_fin_sent
            && self.endpoint.terminal_fin_replayed
            && self.endpoint.pending_remote_fin_offset.is_none()
            && sender_queue.is_empty()
            && send_stream.reinjection_bytes() == 0
            && recv_stream.reorder_bytes() == 0
    }

    pub(super) fn record_local_eof(&mut self) {
        self.endpoint.local_open = false;
        self.endpoint.pending_local_fin = true;
    }

    pub(super) fn record_local_payload(&mut self, lane: TrafficClass) {
        if lane.is_latency_sensitive() && self.endpoint.remote_open {
            self.progress.interactive_response_pending = true;
            self.progress.last_response_stall_reinjection_at = Instant::now();
        }
    }

    pub(super) fn record_local_fin_sent(&mut self) {
        self.endpoint.pending_local_fin = false;
        self.endpoint.local_fin_sent = true;
        self.endpoint.terminal_fin_replayed = false;
        self.progress.last_stream_at = Instant::now();
    }

    pub(super) fn record_terminal_fin_replayed(&mut self) {
        self.endpoint.local_fin_sent = true;
        self.endpoint.terminal_fin_replayed = true;
        self.progress.last_stream_at = Instant::now();
    }

    pub(super) fn record_remote_finished(&mut self) {
        self.endpoint.remote_open = false;
        self.endpoint.pending_remote_fin_offset = None;
        self.progress.interactive_response_pending = false;
        self.progress.last_delivery_at = Instant::now();
    }

    pub(super) fn record_recv_progress_sent(&mut self, sent: bool) {
        if sent {
            self.progress.last_recv_progress_sent_at = Instant::now();
        }
    }

    fn record_delivery(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
        delivered: &[Bytes],
        path_scoped_received_bytes: usize,
    ) -> usize {
        let delivered_payload_bytes =
            self.delivery
                .record_response(instance, delivered, path_scoped_received_bytes);
        if let Some(path_stats) = self.delivery.by_path.get(&instance).copied() {
            maybe_mark_live_relay_path_delivery(
                context,
                instance,
                path_stats,
                &mut self.delivery.next_live_sample_bytes,
            );
        }
        delivered_payload_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClientStreamDataEffect {
    pub(super) delivered_payload_bytes: usize,
    pub(super) delivered_progress: bool,
    pub(super) fin_ready: bool,
}

/// Applies one original frame to mux and client delivery state without taking
/// local-socket ownership. The relay I/O layer may therefore preserve
/// per-path attribution for every frame and write several ready outcomes with
/// one vectored transaction.
#[allow(clippy::too_many_arguments)]
pub(super) fn apply_client_stream_data_state(
    state: &mut ClientRelayState,
    context: &ClientPathContext,
    recv_stream: &mut ReliableRecvStream,
    stream_id: StreamId,
    instance: RelayPathInstance,
    offset: u64,
    payload: Bytes,
) -> Result<(ClientStreamDataEffect, ReceiveOutcome), RuntimeError> {
    let path_key = instance.key;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (stream_id, path_key);
    let previous_remote_offset = recv_stream.next_offset();
    let payload_len = payload.len();
    super::io::validate_stream_data_final_offset(
        state.endpoint.pending_remote_fin_offset,
        offset,
        payload_len,
    )?;
    #[cfg(feature = "lab-diagnostics")]
    let mux_started = Instant::now();
    let outcome = recv_stream
        .receive_data(offset, payload)
        .map_err(RuntimeError::Stream)?;
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
            if state.progress.last_reported_receive_hole != Some(hole_state) {
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
                state.progress.last_reported_receive_hole = Some(hole_state);
            }
        } else {
            state.progress.last_reported_receive_hole = None;
        }
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record("mux.receive_data", mux_started.elapsed(), payload_len);

    state.progress.last_stream_at = Instant::now();
    let delivered_progress = recv_stream.next_offset() > previous_remote_offset;
    if delivered_progress {
        state.progress.last_delivery_at = Instant::now();
        state.progress.receive_hole_reinjection_attempts = 0;
        state.progress.interactive_response_pending = false;
    }
    let delivered = &outcome.delivered;
    let delivered_payload_bytes = state.record_delivery(
        context,
        instance,
        delivered.as_slice(),
        if delivered_progress { payload_len } else { 0 },
    );

    Ok((
        ClientStreamDataEffect {
            delivered_payload_bytes,
            delivered_progress,
            fin_ready: super::io::pending_stream_fin_ready(
                recv_stream,
                state.endpoint.pending_remote_fin_offset,
            ),
        },
        outcome,
    ))
}

pub(super) struct ClientStreamAckContext<'a> {
    pub(super) state: &'a mut ClientRelayState,
    pub(super) sender: &'a mut RequestSenderService,
    pub(super) sender_queue: &'a mut ReliableRelaySenderQueue,
    pub(super) context: &'a ClientPathContext,
    pub(super) remotes: &'a ReliableRelayRemoteSet,
    pub(super) send_stream: &'a mut ReliableSendStream,
    pub(super) path_snapshot: Option<PathSnapshot>,
    pub(super) relay_lane: TrafficClass,
}

/// Runs exact-attachment stale-path decisions on both Data ACK events and
/// relay timer ticks. A path that stops every ACK must still become stale;
/// TCP and QUIC continue to own recovery of their already-emitted flights.
#[allow(clippy::too_many_arguments)]
pub(super) fn update_request_path_staleness(
    state: &mut ClientRelayState,
    sender: &mut RequestSenderService,
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    data_ack_progress_paths: &[RelayPathInstance],
    stream_id: StreamId,
) -> bool {
    let authoritative_horizon = state.progress.last_send_ack.horizon().unwrap_or(0);
    let candidates = sender.unacked_original_paths_before(remotes, authoritative_horizon);
    let observations = candidates
        .iter()
        .copied()
        .map(|candidate| {
            ReliablePathStalenessObservation::new(
                candidate,
                sender.request_path_has_reinjection_path(remotes, candidate),
                Some(candidate.key.underlay),
                context.reliable_path_snapshot_for_instance(candidate),
            )
        })
        .collect::<SmallVec<[_; 4]>>();
    let stale_paths = state
        .progress
        .request_path_staleness
        .stale_paths(&observations, data_ack_progress_paths);
    let mut marked_stale = false;
    for stale_path in stale_paths {
        if !sender.mark_request_path_stale(remotes, stale_path) {
            continue;
        }
        marked_stale = true;
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "request_path_stale",
            format_args!(
                "stream_id={} path_underlay={:?} path_index={} path_instance_id={} attachment_id={}",
                stream_id.0,
                stale_path.key.underlay,
                stale_path.key.index,
                stale_path.path_instance_id.as_u64(),
                stale_path.attachment_id,
            ),
        );
    }
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    marked_stale
}

#[derive(Debug, Default)]
#[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
pub(super) struct ClientDataAckReinjectionOutcome {
    pub(super) frame_count: usize,
    pub(super) persistent_ready: bool,
    pub(super) has_multipath_alternative: bool,
}

/// Evaluates retained authoritative Data ACK evidence against the exact
/// original-flight assignment and one measured alternative.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
pub(super) fn evaluate_client_data_ack_reinjection(
    state: &mut ClientRelayState,
    sender: &mut RequestSenderService,
    sender_queue: &mut ReliableRelaySenderQueue,
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    path_snapshot: Option<PathSnapshot>,
    relay_lane: TrafficClass,
    stream_id: StreamId,
) -> ClientDataAckReinjectionOutcome {
    let has_multipath_reinjection_alternative = remotes.path_keys().len() > 1;
    let authoritative_ack_complete = state.progress.last_send_ack.complete();
    let authoritative_ack_ranges = state.progress.last_send_ack.ranges();
    if !stream_ack_ranges_expose_authoritative_gap(
        authoritative_ack_complete,
        authoritative_ack_ranges,
    ) || !has_multipath_reinjection_alternative
    {
        state.progress.ack_gap_reinjection.arm_recovery_deadline(
            authoritative_ack_complete,
            authoritative_ack_ranges,
            has_multipath_reinjection_alternative,
            None,
        );
        state.progress.data_ack_reinjection_at = None;
        return ClientDataAckReinjectionOutcome {
            has_multipath_alternative: has_multipath_reinjection_alternative,
            ..ClientDataAckReinjectionOutcome::default()
        };
    }
    let base_reinjection_limit =
        adaptive_reliable_relay_reinjection_bytes(path_snapshot, relay_lane, context.mux_limits);
    let reinjection_event_budget =
        sender.reinjection_extra_event_budget_remaining(context.mux_limits);
    let reinjection = sender.data_ack_gap_reinjection_model(
        context,
        remotes,
        send_stream,
        authoritative_ack_ranges,
        base_reinjection_limit,
        relay_lane,
    );
    let has_live_original_path = reinjection.has_live_original_path;
    let original_path_timing = reinjection.original_path_timing;
    let reinjection_target = reinjection.reinjection_target;
    let ack_gap_original_underlay = original_path_timing
        .map(|snapshot| snapshot.underlay)
        .or(reinjection.original_underlay)
        .or(path_snapshot.map(|snapshot| snapshot.underlay));
    let observed_at = Instant::now();
    let candidate_gap_deadline = has_live_original_path
        .then(|| {
            reliable_data_ack_gap_reinjection_deadline(
                reinjection.original_assignment_at,
                ack_gap_original_underlay,
                original_path_timing,
                reinjection.reinjection_completion,
            )
        })
        .flatten();
    let recovery_deadline = state.progress.ack_gap_reinjection.arm_recovery_deadline(
        authoritative_ack_complete,
        authoritative_ack_ranges,
        has_multipath_reinjection_alternative,
        candidate_gap_deadline,
    );
    let measured_reinjection_ready = candidate_gap_deadline.is_some()
        && recovery_deadline.is_some_and(|deadline| observed_at >= deadline);
    let reinjection_retry_after = reinjection_target.map_or_else(
        || reliable_data_retransmission_interval(ack_gap_original_underlay, original_path_timing),
        |(_, snapshot)| {
            reliable_data_retransmission_interval(Some(snapshot.underlay), Some(snapshot))
        },
    );
    let ack_gap_reinjection_ready = state.progress.ack_gap_reinjection.reinjection_ready(
        authoritative_ack_complete,
        authoritative_ack_ranges,
        has_multipath_reinjection_alternative,
        measured_reinjection_ready,
    );
    let persistent_ack_gap_reinjection_ready =
        ack_gap_reinjection_ready && reinjection_target.is_some();
    // Persistent authoritative evidence may fill one measured target service
    // window despite optional duplicate-budget exhaustion. Missing, failed, or
    // declared-stale owners retain their separate exact-range recovery.
    let reinjection_limit = if persistent_ack_gap_reinjection_ready {
        reliable_reinjection_service_limit_bytes(
            reinjection_target.map(|(_, snapshot)| snapshot),
            sender_queue.bytes(),
            send_stream.reinjection_bytes(),
            context.mux_limits,
        )
    } else {
        base_reinjection_limit.min(reinjection_event_budget)
    };
    let ack_gap_reinjection_cause = if persistent_ack_gap_reinjection_ready {
        let (target, snapshot) =
            reinjection_target.expect("persistent reinjection requires a measured path");
        RelaySendCause::persistent_client_ack_gap_reinjection(target, snapshot)
    } else {
        RelaySendCause::AckGapReinjection
    };
    let reinjection_frames = stream_ack_gap_reinjection_frames_normalized(
        send_stream,
        authoritative_ack_ranges,
        reinjection_limit,
        authoritative_ack_complete,
        has_multipath_reinjection_alternative,
        persistent_ack_gap_reinjection_ready,
    );
    let frame_count = reinjection_frames.len();
    let persistent_ack_gap_reinjection =
        persistent_ack_gap_reinjection_ready && !reinjection_frames.is_empty();
    let mut queued_persistent_ack_gap_reinjection = false;
    for frame in reinjection_frames {
        let queued = if sender_queue.has_queued_reinjection_overlap(&frame) {
            false
        } else if persistent_ack_gap_reinjection {
            sender.enqueue_critical_reinjection_frame(
                sender_queue,
                frame,
                ack_gap_reinjection_cause,
            );
            true
        } else {
            sender.enqueue_reinjection_frame_with_priority(
                sender_queue,
                frame,
                RelaySendCause::AckGapReinjection,
                context.mux_limits,
                true,
            )
        };
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "reinjection",
            format_args!(
                "stream_id={} cause={} queued={}",
                stream_id.0, "ack_gap", queued,
            ),
        );
        if queued {
            queued_persistent_ack_gap_reinjection |= persistent_ack_gap_reinjection_ready;
            state.progress.sender_retry_at = None;
        }
    }
    if queued_persistent_ack_gap_reinjection {
        state
            .progress
            .ack_gap_reinjection
            .record_reinjection_queued(reinjection_retry_after);
    }
    let timer_active = stream_ack_ranges_expose_authoritative_gap(
        authoritative_ack_complete,
        authoritative_ack_ranges,
    ) && has_multipath_reinjection_alternative
        && recovery_deadline.is_some();
    let next_deadline = state
        .progress
        .ack_gap_reinjection
        .next_reinjection_deadline();
    state.progress.data_ack_reinjection_at = timer_active
        .then_some(next_deadline)
        .flatten()
        .filter(|deadline| *deadline > observed_at)
        .map(tokio::time::Instant::from_std);

    ClientDataAckReinjectionOutcome {
        frame_count,
        persistent_ready: persistent_ack_gap_reinjection_ready,
        has_multipath_alternative: has_multipath_reinjection_alternative,
    }
}

/// Commits one peer ACK and derives reinjection work in the same ownership step, so
/// ACK evidence and queued recovery cannot diverge across select iterations.
pub(super) fn apply_client_stream_ack(
    ack_context: ClientStreamAckContext<'_>,
    stream_id: StreamId,
    complete: bool,
    ranges: Vec<OffsetRange>,
) -> Result<usize, StreamError> {
    // Capture one immutable send-assignment extent before touching any ACK-owned
    // cache, flight, queue, reservation, or recovery evidence.
    let validated_ack = begin_reliable_stream_ack(ack_context.send_stream, complete, ranges)?;
    if ack_context
        .state
        .progress
        .last_send_ack
        .subsumes(&validated_ack)
    {
        ack_context.state.progress.last_stream_at = Instant::now();
        return Ok(0);
    }
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    let ClientStreamAckContext {
        state,
        sender,
        sender_queue,
        context,
        remotes,
        send_stream,
        path_snapshot,
        relay_lane,
    } = ack_context;
    let normalized_ranges = validated_ack.ranges();
    #[cfg(feature = "lab-diagnostics")]
    let previous_reinjection_bytes = send_stream.reinjection_bytes();
    let ack_outcome =
        sender.apply_request_product_ack(context, remotes, send_stream, &validated_ack)?;
    update_reinjection_authoritative_ack_snapshot(
        &mut state.progress.last_send_ack,
        &validated_ack,
    );
    state.progress.last_send_ack_frontier = send_stream.data_ack_frontier();
    let data_ack_progress_paths = ack_outcome.data_ack_progress_paths;
    let ack = ack_outcome.mux;
    sender_queue.release_normalized_acked_reinjections(normalized_ranges);
    update_request_path_staleness(
        state,
        sender,
        context,
        remotes,
        &data_ack_progress_paths,
        stream_id,
    );
    let reinjection = evaluate_client_data_ack_reinjection(
        state,
        sender,
        sender_queue,
        context,
        remotes,
        send_stream,
        path_snapshot,
        relay_lane,
        stream_id,
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = reinjection;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "stream_ack_received",
        format_args!(
            "stream_id={} complete={} ranges={} largest_end={} released_bytes={} reinjection_bytes_before={} reinjection_bytes_after={} reinjection_frames={} reinjection_kind={} active_underlay={:?} multipath_reinjection_alternative={} ack_gap_reinjection_ready={}",
            stream_id.0,
            complete,
            normalized_ranges.len(),
            normalized_ranges
                .iter()
                .map(|range| range.end)
                .max()
                .unwrap_or(0),
            ack.released_bytes,
            previous_reinjection_bytes,
            ack.remaining_reinjection_bytes,
            reinjection.frame_count,
            "ack_gap",
            path_snapshot.map(|snapshot| snapshot.underlay),
            reinjection.has_multipath_alternative,
            reinjection.persistent_ready,
        ),
    );
    state.progress.last_stream_at = Instant::now();
    Ok(ack.released_bytes)
}

#[cfg(test)]
#[path = "tests_client.rs"]
mod tests;
