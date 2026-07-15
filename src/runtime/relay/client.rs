//! Client-side relay session state and peer-frame application.
//!
//! `control` owns the serialized select loop. This module owns durable client
//! state so peer frames cannot update FIN, progress, delivery, and recovery
//! evidence as unrelated local variables.

use super::io::{
    ReliableAckGapRepairProgress, ReliableRecvProgress,
    reliable_critical_tail_repair_is_over_budget, reliable_critical_tail_repair_limit_bytes,
    reliable_persistent_ack_gap_repair_limit_bytes, stream_ack_gap_repair_frames_normalized,
    stream_final_offset_tail_repair_frames, update_repair_authoritative_ack_snapshot,
    write_delivered_payloads,
};
use super::lifecycle::{RelayValidationOpenTask, maybe_mark_live_relay_path_delivery};
use super::remote::ReliableRelayRemoteSet;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::adaptive_reliable_relay_repair_bytes;
use crate::model::path::RelayPathKey;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::protocol::frame::normalized_offset_ranges;
use crate::protocol::{OffsetRange, StreamFlags, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::{ClientPathContext, PathDeliveryStats};
use crate::runtime::sender::{RelaySendCause, ReliableRelaySenderQueue, RequestSenderService};
use crate::runtime::stream::request::RequestOutstandingWindow;
use crate::scheduler::{FlowLane, PathSnapshot};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio::io::{AsyncWrite, AsyncWriteExt};

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
    pub(super) last_response_stall_repair_at: Instant,
    pub(super) last_product_stall_attempt_at: Option<Instant>,
    pub(super) last_receive_hole_repair_at: Instant,
    pub(super) receive_hole_repair_attempts: u32,
    pub(super) interactive_response_pending: bool,
    pub(super) recv_progress: ReliableRecvProgress,
    pub(super) ack_gap_repair: ReliableAckGapRepairProgress,
    pub(super) last_recv_progress_sent_at: Instant,
    pub(super) last_send_ack_frontier: u64,
    pub(super) last_send_ack_ranges: Vec<OffsetRange>,
    pub(super) last_send_ack_complete: bool,
    pub(super) sender_retry_at: Option<tokio::time::Instant>,
    #[cfg(feature = "lab-diagnostics")]
    last_reported_receive_hole: Option<(u64, usize, usize, u64)>,
}

pub(super) struct ClientRelayRecoveryState {
    pub(super) pending_validation_opens: HashMap<RelayPathKey, RelayValidationOpenTask>,
    pub(super) validation_open_attempts: HashMap<RelayPathKey, u8>,
    pub(super) excluded_paths: HashSet<RelayPathKey>,
}

#[derive(Default)]
pub(super) struct ClientRelayDeliveryState {
    pub(super) total: PathDeliveryStats,
    pub(super) by_path: HashMap<RelayPathKey, PathDeliveryStats>,
    next_live_sample_bytes: HashMap<RelayPathKey, u64>,
}

impl ClientRelayDeliveryState {
    fn record_response(
        &mut self,
        path_key: RelayPathKey,
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
                .entry(path_key)
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
                last_response_stall_repair_at: now,
                last_product_stall_attempt_at: None,
                last_receive_hole_repair_at: now,
                receive_hole_repair_attempts: 0,
                interactive_response_pending: false,
                recv_progress: ReliableRecvProgress::default(),
                ack_gap_repair: ReliableAckGapRepairProgress::default(),
                last_recv_progress_sent_at: now,
                last_send_ack_frontier: 0,
                last_send_ack_ranges: Vec::new(),
                last_send_ack_complete: false,
                sender_retry_at: None,
                #[cfg(feature = "lab-diagnostics")]
                last_reported_receive_hole: None,
            },
            recovery: ClientRelayRecoveryState {
                pending_validation_opens: HashMap::new(),
                validation_open_attempts: HashMap::new(),
                excluded_paths: HashSet::new(),
            },
            delivery: ClientRelayDeliveryState::default(),
        }
    }

    pub(super) fn is_finished(&self, sender_queue_empty: bool) -> bool {
        !self.endpoint.local_open && !self.endpoint.remote_open && sender_queue_empty
    }

    pub(super) fn record_local_eof(&mut self) {
        self.endpoint.local_open = false;
        self.endpoint.pending_local_fin = true;
    }

    pub(super) fn record_local_payload(&mut self, lane: FlowLane) {
        if lane.is_latency_sensitive() && self.endpoint.remote_open {
            self.progress.interactive_response_pending = true;
            self.progress.last_response_stall_repair_at = Instant::now();
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

    pub(super) fn record_stream_progress_sent(&mut self, sent: bool) {
        if sent {
            let now = Instant::now();
            self.progress.last_recv_progress_sent_at = now;
            self.progress.last_stream_at = now;
        }
    }

    fn record_delivery(
        &mut self,
        context: &ClientPathContext,
        path_key: RelayPathKey,
        delivered: &[Bytes],
        path_scoped_received_bytes: usize,
    ) -> usize {
        let delivered_payload_bytes =
            self.delivery
                .record_response(path_key, delivered, path_scoped_received_bytes);
        if let Some(path_stats) = self.delivery.by_path.get(&path_key).copied() {
            maybe_mark_live_relay_path_delivery(
                context,
                path_key,
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

/// Applies mux ordering and product writes as one event. The typed effect lets
/// `control` make path-policy decisions only after delivery state is committed.
#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_client_stream_data<S>(
    state: &mut ClientRelayState,
    context: &ClientPathContext,
    local: &mut S,
    recv_stream: &mut ReliableRecvStream,
    stream_id: StreamId,
    path_key: RelayPathKey,
    offset: u64,
    flags: StreamFlags,
    payload: Bytes,
) -> Result<ClientStreamDataEffect, RuntimeError>
where
    S: AsyncWrite + Unpin,
{
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    let previous_remote_offset = recv_stream.next_offset();
    let payload_len = payload.len();
    #[cfg(feature = "lab-diagnostics")]
    let mux_started = Instant::now();
    let outcome = recv_stream
        .receive_data(offset, payload, flags)
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
        state.progress.receive_hole_repair_attempts = 0;
        state.progress.interactive_response_pending = false;
    }
    let delivered = outcome.delivered;
    let delivered_payload_bytes = state.record_delivery(
        context,
        path_key,
        delivered.as_slice(),
        if delivered_progress { payload_len } else { 0 },
    );
    #[cfg(feature = "lab-diagnostics")]
    let write_started = Instant::now();
    write_delivered_payloads(local, delivered.as_slice())
        .await
        .map_err(RuntimeError::Io)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "relay.local_write_wait",
        write_started.elapsed(),
        delivered_payload_bytes,
    );
    #[cfg(feature = "lab-diagnostics")]
    let flush_started = Instant::now();
    local.flush().await.map_err(RuntimeError::Io)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record("relay.local_flush_wait", flush_started.elapsed(), 0);

    Ok(ClientStreamDataEffect {
        delivered_payload_bytes,
        delivered_progress,
        fin_ready: outcome.fin
            || super::io::pending_stream_fin_ready(
                recv_stream,
                state.endpoint.pending_remote_fin_offset,
            ),
    })
}

pub(super) struct ClientStreamAckContext<'a> {
    pub(super) state: &'a mut ClientRelayState,
    pub(super) sender: &'a mut RequestSenderService,
    pub(super) sender_queue: &'a mut ReliableRelaySenderQueue,
    pub(super) context: &'a ClientPathContext,
    pub(super) remotes: &'a ReliableRelayRemoteSet,
    pub(super) send_stream: &'a mut ReliableSendStream,
    pub(super) outstanding_window: &'a mut RequestOutstandingWindow,
    pub(super) path_snapshot: Option<PathSnapshot>,
    pub(super) relay_lane: FlowLane,
}

/// Commits one peer ACK and derives repair work in the same ownership step, so
/// ACK evidence and queued recovery cannot diverge across select iterations.
pub(super) fn apply_client_stream_ack(
    ack_context: ClientStreamAckContext<'_>,
    stream_id: StreamId,
    complete: bool,
    ranges: &[OffsetRange],
) {
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    let ClientStreamAckContext {
        state,
        sender,
        sender_queue,
        context,
        remotes,
        send_stream,
        outstanding_window,
        path_snapshot,
        relay_lane,
    } = ack_context;
    let normalized_ranges = normalized_offset_ranges(ranges);
    update_repair_authoritative_ack_snapshot(
        &mut state.progress.last_send_ack_frontier,
        &mut state.progress.last_send_ack_ranges,
        &mut state.progress.last_send_ack_complete,
        complete,
        &normalized_ranges,
    );
    #[cfg(feature = "lab-diagnostics")]
    let previous_repair_bytes = send_stream.repair_bytes();
    let ack_outcome =
        sender.apply_request_product_ack(context, remotes, send_stream, &normalized_ranges);
    let ack = ack_outcome.mux;
    outstanding_window.apply_growth_evidence(ack_outcome.window, relay_lane, context.mux_limits);
    sender_queue.release_normalized_acked_repairs(&normalized_ranges);
    let base_repair_limit =
        adaptive_reliable_relay_repair_bytes(path_snapshot, relay_lane, context.mux_limits);
    let repair_event_budget = sender.repair_extra_event_budget_remaining(context.mux_limits);
    let has_multipath_repair_alternative = remotes.path_keys().len() > 1;
    let (owner_underlay, owner_timing_path, repair_target) = sender.ack_gap_repair_path_model(
        context,
        remotes,
        send_stream,
        &normalized_ranges,
        base_repair_limit,
        relay_lane,
    );
    let ack_gap_repair_ready = state.progress.ack_gap_repair.repair_ready(
        complete,
        &normalized_ranges,
        owner_timing_path
            .map(|snapshot| snapshot.underlay)
            .or(owner_underlay)
            .or(remotes.active_path_underlay()),
        has_multipath_repair_alternative,
        owner_timing_path,
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
    let amplified_ack_gap_repair = ack_gap_repair_ready && repair_limit > base_repair_limit;
    let ack_gap_repair_cause = if amplified_ack_gap_repair {
        let (target, snapshot) = repair_target.expect("amplified repair requires a modeled output");
        RelaySendCause::persistent_client_ack_gap_repair(target, snapshot)
    } else {
        RelaySendCause::AckGapRepair
    };
    let mut repair_frames = stream_ack_gap_repair_frames_normalized(
        send_stream,
        &normalized_ranges,
        repair_limit,
        complete,
        has_multipath_repair_alternative,
        ack_gap_repair_ready,
    );
    let mut critical_tail_repair = ack_gap_repair_ready && !repair_frames.is_empty();
    let repair_kind = if repair_frames.is_empty() {
        let fin_tail_limit = if !state.endpoint.local_open {
            let limit = reliable_critical_tail_repair_limit_bytes(
                base_repair_limit,
                send_stream.repair_bytes(),
                context.mux_limits,
            );
            critical_tail_repair =
                reliable_critical_tail_repair_is_over_budget(repair_event_budget, limit);
            limit
        } else {
            repair_limit
        };
        let fin_tail_frames = stream_final_offset_tail_repair_frames(
            send_stream,
            ranges,
            fin_tail_limit,
            !state.endpoint.local_open,
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
                sender.enqueue_critical_tail_repair_frame(sender_queue, frame)
            } else {
                sender.enqueue_critical_repair_frame(sender_queue, frame, ack_gap_repair_cause);
                true
            }
        } else {
            sender.enqueue_repair_frame_with_priority(
                sender_queue,
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
            queued_persistent_ack_gap_repair |= ack_gap_repair_ready && repair_kind == "ack_gap";
            state.progress.sender_retry_at = None;
        }
    }
    if queued_persistent_ack_gap_repair {
        state.progress.ack_gap_repair.record_repair_queued();
    }
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = ack;
    state.progress.last_stream_at = Instant::now();
}

#[cfg(test)]
#[path = "client_test.rs"]
mod tests;
