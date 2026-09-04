//! Client-side relay session state and peer-frame application.
//!
//! `control` owns the serialized select loop. This module owns durable client
//! state so peer frames cannot update FIN, progress, delivery, and recovery
//! evidence as unrelated local variables.

use super::io::{
    AuthoritativeStreamAckSnapshot, ReliableAckGapReinjectionProgress,
    ReliablePathStalenessObservation, ReliableRequestPathStaleness, begin_reliable_stream_ack,
    exact_contiguous_retransmission_frames, normalized_stream_ack_first_gap,
    preserve_reinjection_frontier_quantum, stream_ack_ranges_expose_authoritative_gap,
    update_reinjection_authoritative_ack_snapshot,
};
use super::lifecycle::RelayAdditionalPathOpenTask;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::adaptive_reliable_relay_reinjection_bytes;
use crate::model::multipath::live_owner_gap_recovery_wake;
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::model::timing::reliable_data_retransmission_interval;
use crate::model::work::{
    ReliableReinjectionTargetWork, flight_interval_bytes,
    reliable_live_frontier_reinjection_limit_bytes, reliable_live_gap_reinjection_authority,
    reliable_reinjection_service_limit_bytes,
};
use crate::mux::MuxLimits;
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
    pub(super) path_open_suppressions: ClientRelayPathOpenSuppressions,
    pub(super) disconnected: Option<ClientRelayDisconnectedState>,
}

#[derive(Debug, Clone, Copy)]
struct ClientRelayPathOpenSuppression {
    path_instance_id: CarrierPathInstanceId,
    retry_at: tokio::time::Instant,
}

/// Per-logical-stream retry bounds for failed carrier attachments.
///
/// A Product operation cannot fence shared carrier health. It can only defer
/// another attachment to the same physical owner until one path-derived PTO
/// has elapsed. A replacement carrier instance bypasses the old entry.
#[derive(Default)]
pub(super) struct ClientRelayPathOpenSuppressions {
    entries: HashMap<RelayPathKey, ClientRelayPathOpenSuppression>,
}

impl ClientRelayPathOpenSuppressions {
    pub(super) fn suppress(&mut self, instance: RelayPathInstance, retry_at: tokio::time::Instant) {
        self.entries.insert(
            instance.key,
            ClientRelayPathOpenSuppression {
                path_instance_id: instance.path_instance_id,
                retry_at,
            },
        );
    }

    pub(super) fn blocks(
        &self,
        context: &ClientPathContext,
        key: RelayPathKey,
        now: tokio::time::Instant,
    ) -> bool {
        let Some(suppression) = self.entries.get(&key) else {
            return false;
        };
        if now >= suppression.retry_at {
            return false;
        }
        context
            .health()
            .lock()
            .expect("client path health lock")
            .path_record(key)
            .and_then(|record| record.path_instance_id())
            == Some(suppression.path_instance_id)
    }

    pub(super) fn next_retry_at(
        &self,
        context: &ClientPathContext,
        now: tokio::time::Instant,
    ) -> Option<tokio::time::Instant> {
        let health = context.health().lock().expect("client path health lock");
        self.entries
            .iter()
            .filter_map(|(key, suppression)| {
                (suppression.retry_at > now
                    && health
                        .path_record(*key)
                        .and_then(|record| record.path_instance_id())
                        == Some(suppression.path_instance_id))
                .then_some(suppression.retry_at)
            })
            .min()
    }
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
    // Logical response progress is useful for stream lifecycle/accounting, but
    // it is receiver-owned. It must never become client-to-server path-rate
    // evidence; only the directional sender owns that attribution.
    pub(super) total: PathDeliveryStats,
}

impl ClientRelayDeliveryState {
    fn record_response(&mut self, delivered: &[Bytes]) -> usize {
        let mut delivered_payload_bytes = 0usize;
        for chunk in delivered {
            self.total.record_payload_bytes(chunk.len());
            delivered_payload_bytes = delivered_payload_bytes.saturating_add(chunk.len());
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
                path_open_suppressions: ClientRelayPathOpenSuppressions::default(),
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

    fn record_delivery(&mut self, delivered: &[Bytes]) -> usize {
        self.delivery.record_response(delivered)
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
    let frontier_before = recv_stream.next_offset();
    #[cfg(feature = "lab-diagnostics")]
    let reorder_before = recv_stream.reorder_bytes();
    #[cfg(feature = "lab-diagnostics")]
    let mux_started = Instant::now();
    let outcome = recv_stream
        .receive_data(offset, payload)
        .map_err(RuntimeError::Stream)?;
    #[cfg(feature = "lab-diagnostics")]
    {
        let frontier_after = recv_stream.next_offset();
        let reorder_bytes = recv_stream.reorder_bytes();
        if reorder_before > 0 && frontier_after > frontier_before {
            let released_bytes = outcome
                .delivered
                .iter()
                .map(bytes::Bytes::len)
                .sum::<usize>();
            lab_diagnostic(
                "receive_hole_release",
                format_args!(
                    "stream_id={} source_underlay={:?} source_path_index={} frame_start={} frame_end={} frontier_before={} frontier_after={} reorder_before={} reorder_after={} released_bytes={}",
                    stream_id.0,
                    path_key.underlay,
                    path_key.index,
                    offset,
                    offset.saturating_add(payload_len as u64),
                    frontier_before,
                    frontier_after,
                    reorder_before,
                    reorder_bytes,
                    released_bytes,
                ),
            );
        }
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
    let delivered_payload_bytes = state.record_delivery(delivered.as_slice());

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
    pub(super) remotes: &'a mut ReliableRelayRemoteSet,
    pub(super) send_stream: &'a mut ReliableSendStream,
    pub(super) path_snapshot: Option<PathSnapshot>,
    pub(super) relay_lane: TrafficClass,
}

/// Runs exact-attachment stale-path decisions for OriginalData omitted below a
/// complete authoritative Data ACK horizon. TCP and QUIC continue to own
/// recovery of their already-emitted flights.
#[allow(clippy::too_many_arguments)]
pub(super) fn update_request_path_staleness(
    state: &mut ClientRelayState,
    sender: &mut RequestSenderService,
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    data_ack_progress_paths: &[RelayPathInstance],
    lane: TrafficClass,
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
                sender.request_path_has_reinjection_path(context, remotes, candidate, lane),
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
        if !sender.mark_request_path_stale(context, remotes, stale_path, lane) {
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
    pub(super) has_measured_target: bool,
    pub(super) target_service_exhausted: bool,
}

fn request_target_reinjection_service_limit(
    target: RelayPathInstance,
    snapshot: PathSnapshot,
    sender_queue: &ReliableRelaySenderQueue,
    accepted_reinjection_bytes: usize,
    reinjection_debt_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let queued = sender_queue.request_target_queued_reinjection_bytes(target, false);
    reliable_reinjection_service_limit_bytes(
        ReliableReinjectionTargetWork::new(Some(snapshot), queued, accepted_reinjection_bytes),
        reinjection_debt_bytes,
        mux_limits,
    )
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
    let has_multipath_reinjection_alternative =
        sender.has_multipath_reinjection_alternative(context, remotes, relay_lane);
    let authoritative_ack_complete = state.progress.last_send_ack.complete();
    let authoritative_ack_ranges = state.progress.last_send_ack.ranges();
    if !stream_ack_ranges_expose_authoritative_gap(
        authoritative_ack_complete,
        authoritative_ack_ranges,
    ) || !has_multipath_reinjection_alternative
    {
        state.progress.ack_gap_reinjection.observe_recovery_timing(
            authoritative_ack_complete,
            authoritative_ack_ranges,
            has_multipath_reinjection_alternative,
            None,
            None,
            None,
            Instant::now(),
        );
        state.progress.data_ack_reinjection_at = None;
        return ClientDataAckReinjectionOutcome {
            has_multipath_alternative: has_multipath_reinjection_alternative,
            ..ClientDataAckReinjectionOutcome::default()
        };
    }
    let base_reinjection_limit =
        adaptive_reliable_relay_reinjection_bytes(path_snapshot, relay_lane, context.mux_limits);
    if base_reinjection_limit == 0
        || context.mux_limits.max_repair_bytes == 0
        || context.mux_limits.max_path_flight_bytes == 0
    {
        return ClientDataAckReinjectionOutcome {
            has_multipath_alternative: has_multipath_reinjection_alternative,
            ..ClientDataAckReinjectionOutcome::default()
        };
    }
    let reinjection = sender.data_ack_gap_reinjection_model(
        context,
        remotes,
        send_stream,
        sender_queue,
        authoritative_ack_ranges,
        base_reinjection_limit,
        relay_lane,
    );
    let has_live_original_path = reinjection.has_live_original_path;
    let original_path_timing = reinjection.original_path_timing;
    let reinjection_target = reinjection.reinjection_target;
    let has_measured_reinjection_target = reinjection_target.is_some();
    let target_reinjection_quantum =
        reinjection_target.map_or(base_reinjection_limit, |(_, snapshot)| {
            adaptive_reliable_relay_reinjection_bytes(
                Some(snapshot),
                relay_lane,
                context.mux_limits,
            )
        });
    let frontier_extent = normalized_stream_ack_first_gap(authoritative_ack_ranges)
        .map_or(0, |(start, end)| flight_interval_bytes(start, end))
        .min(reinjection.uniform_frontier_extent_bytes);
    let frontier_limit = reliable_live_frontier_reinjection_limit_bytes(
        target_reinjection_quantum,
        base_reinjection_limit,
        frontier_extent,
        send_stream.reinjection_bytes(),
        context.mux_limits,
    );
    if reinjection_target.is_some() && frontier_limit == 0 {
        return ClientDataAckReinjectionOutcome {
            has_multipath_alternative: has_multipath_reinjection_alternative,
            has_measured_target: true,
            target_service_exhausted: reinjection.target_service_exhausted,
            ..ClientDataAckReinjectionOutcome::default()
        };
    }
    let ack_gap_original_underlay = original_path_timing
        .map(|snapshot| snapshot.underlay)
        .or(reinjection.original_underlay);
    let observed_at = Instant::now();
    let observed_gap_timing = has_live_original_path
        .then_some(reinjection.owner_recovery_timing)
        .flatten();
    let candidate_gap_deadline = state.progress.ack_gap_reinjection.observe_recovery_timing(
        authoritative_ack_complete,
        authoritative_ack_ranges,
        has_multipath_reinjection_alternative,
        observed_gap_timing,
        reinjection.reinjection_completion,
        reinjection.owner_completion,
        observed_at,
    );
    let measured_reinjection_ready =
        candidate_gap_deadline.is_some_and(|deadline| observed_at >= deadline);
    let ack_gap_reinjection_ready = state.progress.ack_gap_reinjection.reinjection_ready(
        authoritative_ack_complete,
        authoritative_ack_ranges,
        has_multipath_reinjection_alternative,
        measured_reinjection_ready,
    );
    let persistent_ack_gap_reinjection_ready =
        ack_gap_reinjection_ready && reinjection_target.is_some();
    // A complete persistent gap proves missing Product order. Its immutable
    // cause clock and exact current ownership decide recovery readiness; the
    // selected target's Product headroom bounds the admitted extent.
    let owner_recovery_deadline = state
        .progress
        .ack_gap_reinjection
        .original_owner_recovery_deadline();
    let target_service_limit = if reinjection.target_service_exhausted {
        0
    } else if persistent_ack_gap_reinjection_ready {
        let (target, snapshot) =
            reinjection_target.expect("persistent reinjection requires a measured path");
        request_target_reinjection_service_limit(
            target.instance(),
            snapshot,
            sender_queue,
            reinjection.reinjection_target_flight_bytes,
            send_stream.reinjection_bytes(),
            context.mux_limits,
        )
    } else {
        base_reinjection_limit
    };
    let reinjection_limit =
        reliable_live_gap_reinjection_authority(target_service_limit, ack_gap_reinjection_ready);
    let reinjection_retry_after = reinjection_target.map_or_else(
        || reliable_data_retransmission_interval(ack_gap_original_underlay, original_path_timing),
        |(_, snapshot)| {
            reliable_data_retransmission_interval(Some(snapshot.underlay), Some(snapshot))
        },
    );
    let ack_gap_reinjection_cause = if persistent_ack_gap_reinjection_ready {
        let (target, snapshot) =
            reinjection_target.expect("persistent reinjection requires a measured path");
        RelaySendCause::persistent_client_ack_gap_reinjection(target, snapshot)
    } else {
        RelaySendCause::AckGapReinjection
    };
    let reinjection_frames = normalized_stream_ack_first_gap(authoritative_ack_ranges)
        .and_then(|(frontier, _)| {
            let applied_extent = reinjection_limit.min(reinjection.uniform_frontier_extent_bytes);
            exact_contiguous_retransmission_frames(
                send_stream,
                OffsetRange {
                    start: frontier,
                    end: frontier.saturating_add(applied_extent as u64),
                },
            )
        })
        .map(|frames| preserve_reinjection_frontier_quantum(frames, frontier_limit))
        .unwrap_or_default();
    let frame_count = reinjection_frames.len();
    let persistent_ack_gap_reinjection =
        persistent_ack_gap_reinjection_ready && !reinjection_frames.is_empty();
    let mut accepted_copy_deadline = None::<Instant>;
    let mut accepted_live_owner_reinjection = false;
    for frame in reinjection_frames {
        let live_copy_deadline = sender.reinjection_suppression_deadline_for_frame(&frame, remotes);
        accepted_copy_deadline = match (accepted_copy_deadline, live_copy_deadline) {
            (Some(current), Some(deadline)) => Some(current.min(deadline)),
            (None, deadline) => deadline,
            (current, None) => current,
        };
        let queued = if live_copy_deadline.is_some()
            || sender_queue.has_queued_reinjection_overlap(&frame)
        {
            false
        } else {
            let cause = if persistent_ack_gap_reinjection {
                ack_gap_reinjection_cause
            } else {
                RelaySendCause::AckGapReinjection
            };
            sender.enqueue_reinjection_frame_with_priority(sender_queue, frame, cause, true);
            true
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
            accepted_live_owner_reinjection = true;
            state.progress.sender_retry_at = None;
        } else {
            break;
        }
    }
    if accepted_live_owner_reinjection {
        // L/D/target/F were fixed by the observation above; acceptance is the
        // linearization point for the retained epoch observation. A due cause
        // remains independently actionable; this clock does not gate it.
        let accepted_at = Instant::now();
        if owner_recovery_deadline.is_some_and(|deadline| accepted_at >= deadline)
            && sender.live_owner_frontier_floor_ready(accepted_at)
        {
            sender.record_live_owner_frontier_floor_attempt(accepted_at, reinjection_retry_after);
        }
    }
    let timer_active = stream_ack_ranges_expose_authoritative_gap(
        authoritative_ack_complete,
        authoritative_ack_ranges,
    ) && has_multipath_reinjection_alternative
        && state
            .progress
            .ack_gap_reinjection
            .next_reinjection_deadline()
            .is_some();
    let live_owner_wake = if timer_active {
        live_owner_gap_recovery_wake(
            state
                .progress
                .ack_gap_reinjection
                .next_reinjection_deadline(),
            owner_recovery_deadline,
            sender.live_owner_frontier_floor_deadline(),
            observed_at,
        )
    } else {
        Default::default()
    };
    let candidate_wake = live_owner_wake.deadline.map(tokio::time::Instant::from_std);
    let accepted_copy_wake = accepted_copy_deadline.map(tokio::time::Instant::from_std);
    state.progress.data_ack_reinjection_at =
        candidate_wake.into_iter().chain(accepted_copy_wake).min();

    ClientDataAckReinjectionOutcome {
        frame_count,
        persistent_ready: persistent_ack_gap_reinjection_ready,
        has_multipath_alternative: has_multipath_reinjection_alternative,
        has_measured_target: has_measured_reinjection_target,
        target_service_exhausted: ack_gap_reinjection_ready
            && reinjection.target_service_exhausted
            && send_stream.reinjection_bytes() > 0,
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
    for instance in ack_outcome.idle_original_data_instances.iter().copied() {
        remotes.depublish_path_instance_load(instance);
    }
    let previous_ack_frontier = state.progress.last_send_ack_frontier;
    update_reinjection_authoritative_ack_snapshot(
        &mut state.progress.last_send_ack,
        &validated_ack,
    );
    state.progress.last_send_ack_frontier = send_stream.data_ack_frontier();
    if state.progress.last_send_ack_frontier > previous_ack_frontier {
        sender.record_live_owner_data_ack_frontier_progress(Instant::now());
    }
    let data_ack_progress_paths = ack_outcome.data_ack_progress_paths;
    let ack = ack_outcome.mux;
    sender_queue.release_normalized_acked_reinjections(normalized_ranges);
    update_request_path_staleness(
        state,
        sender,
        context,
        remotes,
        &data_ack_progress_paths,
        relay_lane,
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
