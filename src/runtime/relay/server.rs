//! Server-side target relay ownership for reliable product streams.
//!
//! Carrier paths admit streams through the registry; this service owns the
//! carrier-neutral lifetime from target connection through ordered close.

use super::diagnostics::log_unexpected_stream_relay_frame;
use super::flow::{
    ReliableRelayFlowDemandTracker, ReliableRelayFlowSignals,
    reliable_latency_startup_credit_remaining_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use super::io::normalized_stream_ack_first_gap;
use super::io::{
    AuthoritativeStreamAckSnapshot, ReadyStreamDataBatchBounds, ReadyStreamDataDirection,
    ReliableAckGapReinjectionProgress, apply_and_write_ready_stream_data_batch,
    begin_reliable_stream_ack, collect_ready_stream_data_batch, pending_stream_fin_ready,
    read_reliable_relay_payload, receive_stream_fin, resize_reliable_relay_buffer,
    stream_ack_gap_reinjection_frames_normalized, stream_ack_ranges_expose_authoritative_gap,
    stream_data_range_already_delivered, stream_final_offset_tail_reinjection_frames_normalized,
    stream_terminal_fin_replay_required, update_reinjection_authoritative_ack_snapshot,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{
    lab_assert_server_sender_service_balanced, lab_diagnostic, lab_perf_flush, lab_perf_record,
};
use crate::model::admission::reliable_relay_source_staging_headroom;
use crate::model::capacity::{
    adaptive_reliable_relay_chunk_bytes_with_frame_limit, adaptive_reliable_relay_inflight_bytes,
    adaptive_reliable_relay_reinjection_bytes, relay_lane_startup_chunk_bytes,
    reliable_bulk_carrier_feed_quantum_bytes, reliable_relay_buffer_len,
    reliable_relay_sender_dispatch_budget, reliable_stream_advertised_window_bytes,
    reliable_stream_initial_advertised_window_bytes,
};
use crate::model::timing::{
    reliable_data_ack_gap_reinjection_ready, reliable_data_ack_recovery_deadline,
    reliable_data_retransmission_interval, reliable_relay_tail_reinjection_delay,
    sender_service_retry_delay,
};
use crate::model::work::ReliableWorkClass;
use crate::model::work::{
    reliable_critical_tail_reinjection_is_over_budget,
    reliable_critical_tail_reinjection_limit_bytes,
    reliable_failed_original_reinjection_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::outbound::{OutboundTcpStream, ServerDestinationPolicy};
use crate::performance::MppPerformanceConfig;
use crate::product::DnsPlanId;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::stream_ack_contiguous_frontier;
use crate::protocol::frame::{
    normalize_offset_ranges, offset_ranges_not_covered, reliable_stream_frame_extent,
};
use crate::protocol::{Frame, OffsetRange, ResetReason, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::outbound_registry::{
    EgressSelection, OpenedTcpOutbound, RuntimeOutboundRegistry, finish_gateway_flow,
};
use crate::runtime::path::PathDeliveryStats;
use crate::runtime::sender::{
    RelaySendCause, ServerResponseSenderService, emit_response_control_frame,
    reliable_relay_sender_queue_limit,
};
use crate::runtime::stream::response::ResponseDataAckRecoveryCandidate;
use crate::runtime::stream::{
    AcceptedServerReliableStream, AcceptedServerReliableStreamRetirement, ReliablePathStream,
    ReliableRecvProgress, ServerReliableStreamRegistry, reliable_relay_recv_progress_resend_active,
    reliable_stream_recv_progress_interval, wait_for_carrier_capacity_notifies,
};
use crate::runtime::telemetry::{ObservedProductIo, RuntimeTelemetry};
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::{Id, JoinError, JoinSet};

pub(in crate::runtime) struct ServerReliableRelayContext {
    pub(in crate::runtime) outbound_registry: RuntimeOutboundRegistry,
    pub(in crate::runtime) egress_selection: EgressSelection,
    pub(in crate::runtime) dns_plan: Option<DnsPlanId>,
    pub(in crate::runtime) destination_policy: Arc<ServerDestinationPolicy>,
    pub(in crate::runtime) performance: MppPerformanceConfig,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) max_paths_per_session: usize,
    pub(in crate::runtime) session_retention_timeout: Duration,
    pub(in crate::runtime) telemetry: RuntimeTelemetry,
}

/// Runs target relays independently of TCP and QUIC carrier actors.
///
/// The admission channel does not need another byte or item limit: the stream
/// registry bounds queued plus running admissions by its active-stream limit.
pub(in crate::runtime) struct ServerReliableRelayService {
    context: Arc<ServerReliableRelayContext>,
    accepted: mpsc::UnboundedReceiver<AcceptedServerReliableStream>,
}

impl ServerReliableRelayService {
    pub(in crate::runtime) fn new(
        context: ServerReliableRelayContext,
    ) -> (Arc<ServerReliableStreamRegistry>, Self) {
        let (registry, accepted) = ServerReliableStreamRegistry::new_accepting_with_limits(
            context.mux_limits,
            context.max_paths_per_session,
        );
        let context = Arc::new(context);
        (registry, Self { context, accepted })
    }

    pub(in crate::runtime) async fn run(mut self) -> Result<(), RuntimeError> {
        let mut retirements = HashMap::new();
        let mut cleanups = JoinSet::new();
        let mut relays = JoinSet::new();
        loop {
            tokio::select! {
                accepted = self.accepted.recv() => {
                    let Some(mut accepted) = accepted else {
                        relays.abort_all();
                        while let Some(result) = relays.join_next_with_id().await {
                            finish_relay_task(result, &mut retirements, &mut cleanups);
                        }
                        while let Some(result) = cleanups.join_next().await {
                            report_relay_cleanup_task(result);
                        }
                        debug_assert!(retirements.is_empty());
                        return Err(RuntimeError::Protocol(
                            "server reliable stream accept service closed",
                        ));
                    };
                    let retirement = accepted.supervise();
                    let context = self.context.clone();
                    let task = relays.spawn(async move {
                        relay_accepted_stream(context, accepted).await
                    });
                    let replaced = retirements.insert(task.id(), retirement);
                    debug_assert!(replaced.is_none());
                }
                result = relays.join_next_with_id(), if !relays.is_empty() => {
                    if let Some(result) = result {
                        finish_relay_task(result, &mut retirements, &mut cleanups);
                    }
                }
                result = cleanups.join_next(), if !cleanups.is_empty() => {
                    if let Some(result) = result {
                        report_relay_cleanup_task(result);
                    }
                }
            }
        }
    }
}

fn finish_relay_task(
    result: Result<(Id, Result<(), RuntimeError>), JoinError>,
    retirements: &mut HashMap<Id, AcceptedServerReliableStreamRetirement>,
    cleanups: &mut JoinSet<()>,
) {
    let task_id = match &result {
        Ok((task_id, _)) => *task_id,
        Err(err) => err.id(),
    };
    let retirement = retirements
        .remove(&task_id)
        .expect("accepted relay retirement");
    cleanups.spawn(async move {
        retirement.retire().await;
    });
    match result {
        Ok((_, Ok(()))) => {}
        Ok((_, Err(err))) => crate::observability::process_event!(
            Warn,
            "reliable_relay",
            "server_stream_failed",
            "server reliable stream failed: {err}"
        ),
        Err(err) => crate::observability::process_event!(
            Warn,
            "reliable_relay",
            "server_stream_task_failed",
            "server reliable stream task failed: {err}"
        ),
    }
}

fn report_relay_cleanup_task(result: Result<(), JoinError>) {
    if let Err(err) = result {
        crate::observability::process_event!(
            Warn,
            "reliable_relay",
            "server_cleanup_task_failed",
            "server reliable stream cleanup task failed: {err}"
        );
    }
}

async fn relay_accepted_stream(
    context: Arc<ServerReliableRelayContext>,
    mut accepted: AcceptedServerReliableStream,
) -> Result<(), RuntimeError> {
    let session_id = accepted.session_id();
    let stream_id = accepted.stream().stream_id;
    let target = accepted.target().clone();
    let principal_destination_policy = context
        .destination_policy
        .for_principal(accepted.principal_permit().principal().clone());
    let outbound_stream = match context
        .outbound_registry
        .open_tcp(
            &context.egress_selection,
            &target,
            context.dns_plan.as_ref(),
            TrafficClass::Latency,
            &principal_destination_policy,
        )
        .await
    {
        Ok(stream) => stream,
        Err(err) => {
            let lane = accepted.stream().current_lane();
            accepted.reject(ResetReason::Refused, lane).await;
            return Err(err);
        }
    };
    let product_scope = match &outbound_stream {
        OpenedTcpOutbound::Mpp { _product_flow, .. }
        | OpenedTcpOutbound::Local { _product_flow, .. } => _product_flow.scope().clone(),
    };
    let telemetry_flow = context.telemetry.scoped(product_scope).open_reliable_flow(
        Some(session_id),
        stream_id,
        target.clone(),
    );
    if accepted.accept_opening_path().await.is_err()
        && let Err(err) = emit_response_control_frame(
            accepted.stream(),
            Frame::StreamMaxData {
                stream_id,
                max_offset: reliable_stream_initial_advertised_window_bytes(
                    accepted.stream().underlay,
                    accepted.stream().lane,
                    context.mux_limits,
                ),
            },
        )
    {
        accepted.close().await;
        return Err(err);
    }

    let session_send_buffer = accepted.session_send_buffer();
    let stream = accepted.take_stream();
    // Resolve the Product connector variant once, before entering the relay
    // loop. Direct and plain-proxy traffic therefore retains a concrete
    // TcpStream on the steady data path with no per-read/write enum dispatch.
    let counter = telemetry_flow.counter();
    let result = match outbound_stream {
        OpenedTcpOutbound::Local {
            stream: OutboundTcpStream::Plain(outbound_stream),
            mut _gateway_lease,
            _product_flow,
        } => {
            let result = relay_reliable_stream(
                ObservedProductIo::new(outbound_stream, counter),
                stream,
                context.as_ref(),
                session_id,
                session_send_buffer,
            )
            .await
            .map(|_| ());
            finish_gateway_flow(&mut _gateway_lease, &result);
            result
        }
        OpenedTcpOutbound::Local {
            stream: OutboundTcpStream::Tls(outbound_stream),
            mut _gateway_lease,
            _product_flow,
        } => {
            let result = relay_reliable_stream(
                ObservedProductIo::new(*outbound_stream, counter),
                stream,
                context.as_ref(),
                session_id,
                session_send_buffer,
            )
            .await
            .map(|_| ());
            finish_gateway_flow(&mut _gateway_lease, &result);
            result
        }
        OpenedTcpOutbound::Mpp { .. } => Err(RuntimeError::Protocol(
            "MPP inbound cannot route TCP to an MPP outbound",
        )),
    };
    // The relay function closes carrier output on every ordinary return. The
    // accepted guard can now retire registry membership without a second close.
    accepted.mark_closed().await;
    if result.is_ok() {
        telemetry_flow.complete();
    }
    result
}

// Response relay policy stays with the server lifecycle because it selects
// response carriers and translates request progress into server-owned work.
// Response tail-reinjection evidence
fn stream_tail_timer_reinjection_allowed(
    tail_reinjection_candidate: bool,
    has_failed_original_reinjection_output: bool,
) -> bool {
    tail_reinjection_candidate || has_failed_original_reinjection_output
}

fn reliable_relay_tail_reinjection_timer_active(
    reinjection_bytes: usize,
    tail_reinjection_candidate: bool,
    failed_original_tail_reinjection_ready: bool,
) -> bool {
    reinjection_bytes > 0
        && stream_tail_timer_reinjection_allowed(
            tail_reinjection_candidate,
            failed_original_tail_reinjection_ready,
        )
}

fn stream_ack_is_authoritative_contiguous_prefix(
    complete: bool,
    ranges: &[OffsetRange],
    frontier: u64,
) -> bool {
    complete
        && frontier > 0
        && matches!(ranges, [range] if range.start == 0 && range.end == frontier)
}

// Response ordered-owner debt
fn reliable_relay_data_ack_outstanding_bytes(
    lane: TrafficClass,
    ack_frontier: u64,
    next_offset: u64,
) -> usize {
    if !lane.is_bulk() || ack_frontier >= next_offset {
        return 0;
    }
    // This is a tail guard, not reinjection debt. It blocks alternate OriginalData and
    // missing-owner failover while lower original-data bytes are unresolved, but it
    // must not make the live leading path itself inadmissible.
    usize::try_from(next_offset.saturating_sub(ack_frontier)).unwrap_or(usize::MAX)
}

// Response reinjection deadlines
fn reliable_relay_current_data_ack_outstanding_bytes(
    lane: TrafficClass,
    send_stream: &ReliableSendStream,
    ack_frontier: u64,
) -> usize {
    reliable_relay_data_ack_outstanding_bytes(lane, ack_frontier, send_stream.next_offset())
}

fn reliable_relay_tail_reinjection_deadline(
    data_ack_progress_at: Instant,
    last_attempt_at: Option<Instant>,
    path: Option<PathSnapshot>,
) -> tokio::time::Instant {
    let stall_timeout = reliable_relay_tail_reinjection_delay(path);
    let recovery_anchor = last_attempt_at.map_or(data_ack_progress_at, |attempted_at| {
        attempted_at.max(data_ack_progress_at)
    });
    tokio::time::Instant::from_std(recovery_anchor + stall_timeout)
}

fn reliable_relay_effective_tail_reinjection_deadline(
    data_ack_progress_at: Instant,
    last_attempt_at: Option<Instant>,
    path: Option<PathSnapshot>,
    failed_original_tail_reinjection_ready: bool,
) -> tokio::time::Instant {
    if failed_original_tail_reinjection_ready {
        return last_attempt_at.map_or_else(
            || tokio::time::Instant::from_std(data_ack_progress_at),
            |attempted_at| {
                tokio::time::Instant::from_std(
                    attempted_at + reliable_relay_tail_reinjection_delay(None),
                )
            },
        );
    }
    reliable_relay_tail_reinjection_deadline(data_ack_progress_at, last_attempt_at, path)
}

/// One retransmission deadline for the current lowest unacknowledged range.
///
/// Carrier metric refreshes may refine the next recovery interval, but they
/// cannot postpone a timer that was already armed for the same Data ACK gap.
#[derive(Debug, Default)]
struct ReliableRelayTailReinjectionTimer {
    candidate: Option<ReliableRelayTailRecoveryCandidate>,
    deadline: Option<tokio::time::Instant>,
    last_attempt_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReliableRelayTailRecoveryCandidate {
    Tracked(ResponseDataAckRecoveryCandidate),
    Untracked {
        start: u64,
        end: u64,
        sent_at: Instant,
    },
}

impl ReliableRelayTailRecoveryCandidate {
    fn sent_at(self) -> Instant {
        match self {
            Self::Tracked(candidate) => candidate.sent_at,
            Self::Untracked { sent_at, .. } => sent_at,
        }
    }
}

impl ReliableRelayTailReinjectionTimer {
    fn arm_recovery_deadline(
        &mut self,
        candidate: ReliableRelayTailRecoveryCandidate,
        deadline: Instant,
    ) {
        let deadline = tokio::time::Instant::from_std(deadline);
        if self.candidate != Some(candidate) {
            self.candidate = Some(candidate);
            self.deadline = Some(deadline);
            return;
        }
        self.deadline = Some(
            self.deadline
                .map_or(deadline, |current| current.min(deadline)),
        );
    }

    fn observe(
        &mut self,
        candidate: Option<ReliableRelayTailRecoveryCandidate>,
        data_ack_progress_at: Instant,
        path: Option<PathSnapshot>,
        failed_original_ready: bool,
    ) -> tokio::time::Instant {
        let Some(candidate) = candidate else {
            self.candidate = None;
            self.deadline = None;
            self.last_attempt_at = None;
            return tokio::time::Instant::now() + reliable_relay_tail_reinjection_delay(path);
        };
        let candidate_changed = self.candidate != Some(candidate);
        if candidate_changed {
            self.candidate = Some(candidate);
            self.deadline = None;
        }
        // A range cannot be stalled before it is assigned to a carrier. Data
        // ACK progress after that assignment starts a fresh recovery interval.
        // Native TCP or QUIC recovery retains ownership below this clock.
        let recovery_progress_at = data_ack_progress_at.max(candidate.sent_at());
        // Retain the last attempt across candidates to prevent a repair burst.
        let candidate = reliable_relay_effective_tail_reinjection_deadline(
            recovery_progress_at,
            self.last_attempt_at,
            path,
            failed_original_ready,
        );
        if failed_original_ready {
            // Confirmed failure may shorten an already armed live-path timer.
            let deadline = self
                .deadline
                .map_or(candidate, |current| current.min(candidate));
            self.deadline = Some(deadline);
            return deadline;
        }
        *self.deadline.get_or_insert(candidate)
    }

    fn record_scan(&mut self) {
        // Carrier capacity has a separate wake channel from attachment/model
        // updates. Pace an empty eligibility scan by one recovery interval so
        // a transiently busy alternate cannot permanently disarm this gap.
        self.record_attempt_at(Instant::now());
    }

    fn record_attempt_at(&mut self, attempted_at: Instant) {
        self.deadline = None;
        self.last_attempt_at = Some(attempted_at);
    }
}

// Server receive-hole diagnostics
#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Default)]
struct ServerReceiveHoleDiagnostics {
    opened_at: Option<Instant>,
    last_delivery_at: Option<Instant>,
}

#[cfg(feature = "lab-diagnostics")]
impl ServerReceiveHoleDiagnostics {
    fn observe(
        &mut self,
        stream_id: StreamId,
        recv_stream: &ReliableRecvStream,
        delivered_bytes: usize,
    ) {
        let now = Instant::now();
        let reorder_bytes = recv_stream.reorder_bytes();
        let ranges = recv_stream.ack_ranges();
        let first_gap = normalized_stream_ack_first_gap(&ranges);
        if reorder_bytes > 0 && self.opened_at.is_none() {
            self.opened_at = Some(now);
            lab_diagnostic(
                "server_receive_hole",
                format_args!(
                    "stream_id={} state=open next_offset={} reorder_bytes={} range_count={} first_gap_start={} first_gap_end={}",
                    stream_id.0,
                    recv_stream.next_offset(),
                    reorder_bytes,
                    ranges.len(),
                    first_gap.map_or(0, |gap| gap.0),
                    first_gap.map_or(0, |gap| gap.1),
                ),
            );
        } else if reorder_bytes == 0
            && let Some(opened_at) = self.opened_at.take()
        {
            lab_diagnostic(
                "server_receive_hole",
                format_args!(
                    "stream_id={} state=clear duration_us={} next_offset={} delivered_bytes={}",
                    stream_id.0,
                    now.saturating_duration_since(opened_at).as_micros(),
                    recv_stream.next_offset(),
                    delivered_bytes,
                ),
            );
        }
        if delivered_bytes > 0 {
            if let Some(last_delivery_at) = self.last_delivery_at {
                let delivery_gap = now.saturating_duration_since(last_delivery_at);
                // Keep the causal trace bounded to WAN-scale stalls; ordinary
                // per-frame delivery remains visible in the perf counters.
                if delivery_gap >= Duration::from_millis(100) {
                    lab_diagnostic(
                        "server_receive_delivery_stall",
                        format_args!(
                            "stream_id={} duration_us={} delivered_bytes={} next_offset={} reorder_bytes={} range_count={} first_gap_start={} first_gap_end={} hole_open={}",
                            stream_id.0,
                            delivery_gap.as_micros(),
                            delivered_bytes,
                            recv_stream.next_offset(),
                            reorder_bytes,
                            ranges.len(),
                            first_gap.map_or(0, |gap| gap.0),
                            first_gap.map_or(0, |gap| gap.1),
                            self.opened_at.is_some(),
                        ),
                    );
                }
            }
            self.last_delivery_at = Some(now);
        }
    }
}

// Server sparse ACK state
// Sparse history belongs only to server-side request feedback. Keeping it out
// of shared receive progress leaves the cloned response hot path cumulative.
#[derive(Debug, Default)]
struct RequestTcpSparseAckProgress {
    acknowledged_ranges: Vec<OffsetRange>,
}

// Server receive-progress emission
impl RequestTcpSparseAckProgress {
    pub(in crate::runtime) fn ack_frames(
        &mut self,
        recv_stream: &ReliableRecvStream,
        sparse_delta: bool,
    ) -> Vec<Frame> {
        let current_ranges = recv_stream.ack_ranges();
        if !sparse_delta {
            self.acknowledged_ranges = current_ranges;
            return recv_stream.ack_frames();
        }
        let delta = offset_ranges_not_covered(&current_ranges, &self.acknowledged_ranges);
        if delta.is_empty() {
            return Vec::new();
        }
        let mut acknowledged = std::mem::take(&mut self.acknowledged_ranges);
        acknowledged.extend(delta.iter().copied());
        self.acknowledged_ranges = normalize_offset_ranges(acknowledged);
        recv_stream.ack_delta_frames(&delta)
    }
}

#[allow(clippy::too_many_arguments)]
fn enqueue_tcp_recv_progress(
    response_sender: &mut ServerResponseSenderService,
    recv_stream: &mut ReliableRecvStream,
    progress: &mut ReliableRecvProgress,
    sparse_ack_progress: &mut RequestTcpSparseAckProgress,
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
    force_max_data: bool,
) -> bool {
    let mut sent_any = false;
    let sparse_delta = !force_max_data
        && progress.has_sent_ack()
        && lane.is_bulk()
        && path.is_some_and(|snapshot| snapshot.underlay == UnderlayProtocol::Tcp)
        && recv_stream.reorder_bytes() > 0;
    if progress.should_send_ack(recv_stream, path, lane, mux_limits, force_max_data) {
        #[cfg(feature = "lab-diagnostics")]
        let ack_started = Instant::now();
        let ack_frames = sparse_ack_progress.ack_frames(recv_stream, sparse_delta);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
        // Multipath receive ranges can exceed one ACK frame under normal
        // reordering. Send every incomplete ACK chunk instead of truncating a
        // single `complete=true` ACK; otherwise the peer treats omitted ranges
        // as loss and starts product reinjection that cannot improve TCP/QUIC
        // carrier delivery.
        for ack_frame in ack_frames {
            response_sender.enqueue_control_frame(ack_frame);
        }
        sent_any = true;
    }
    if progress.should_send_max_data(recv_stream, path, lane, mux_limits, force_max_data) {
        let advertised_window = reliable_stream_advertised_window_bytes(path, lane, mux_limits);
        let max_offset = recv_stream.max_data_offset_with_window(advertised_window);
        response_sender
            .enqueue_control_frame(recv_stream.max_data_frame_with_window(advertised_window));
        recv_stream.commit_max_data(max_offset);
        sent_any = true;
    }
    sent_any
}

// Server feedback timer
fn reliable_relay_recv_progress_timer_enabled(
    initial_underlay: UnderlayProtocol,
    has_multipath_reinjection_alternative: bool,
) -> bool {
    initial_underlay == UnderlayProtocol::Udp || has_multipath_reinjection_alternative
}

// Response sender wait state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseSenderWaitState {
    blocked: bool,
    ready: bool,
    subscribe_capacity: bool,
    retry_at: Option<tokio::time::Instant>,
}

fn response_sender_wait_state(
    queue_nonempty: bool,
    queue_ready: bool,
    front_has_carrier_credit: bool,
    retry_at: Option<tokio::time::Instant>,
    now: tokio::time::Instant,
    retry_delay: Duration,
) -> ResponseSenderWaitState {
    if !queue_nonempty {
        return ResponseSenderWaitState {
            blocked: false,
            ready: false,
            subscribe_capacity: false,
            retry_at: None,
        };
    }
    if let Some(retry_at) = retry_at.filter(|deadline| *deadline > now) {
        return ResponseSenderWaitState {
            blocked: true,
            ready: false,
            subscribe_capacity: true,
            retry_at: Some(retry_at),
        };
    }
    if front_has_carrier_credit {
        return ResponseSenderWaitState {
            blocked: false,
            ready: queue_ready,
            subscribe_capacity: false,
            retry_at: None,
        };
    }
    let retry_at = now + retry_delay;
    ResponseSenderWaitState {
        blocked: true,
        ready: false,
        subscribe_capacity: true,
        retry_at: Some(retry_at),
    }
}

// Response reinjection output selection
fn reliable_failed_original_tail_reinjection_ready(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
) -> bool {
    send_stream.reinjection_bytes() > 0
        && !path_stream.uncovered_failed_original_ranges().is_empty()
}

fn reliable_final_tail_reinjection_ready(
    final_offset_known: bool,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    last_send_ack_frontier: u64,
    tail_reinjection_deadline: tokio::time::Instant,
    now: tokio::time::Instant,
) -> bool {
    if !final_offset_known
        || send_stream.reinjection_bytes() == 0
        || last_send_ack_frontier >= send_stream.next_offset()
        || now < tail_reinjection_deadline
    {
        return false;
    }
    !last_send_ack_ranges.is_empty()
        || (last_send_ack_frontier == 0 && send_stream.next_offset() > 0)
}

#[cfg(test)]
fn prefix_reinjection_frames_with_available_output(
    path_stream: &ReliablePathStream,
    reinjection_frames: Vec<Frame>,
    allow_same_output_frontier_retransmit: bool,
) -> (Vec<Frame>, Option<u64>) {
    let (frames, blocked, _) = prefix_reinjection_frames_with_available_output_classified(
        path_stream,
        reinjection_frames,
        allow_same_output_frontier_retransmit,
    );
    (frames, blocked)
}

fn prefix_final_tail_reinjection_frames_with_available_output(
    path_stream: &ReliablePathStream,
    reinjection_frames: Vec<Frame>,
) -> (Vec<Frame>, Option<u64>, bool) {
    prefix_reinjection_frames_with_available_output_classified(
        path_stream,
        reinjection_frames,
        true,
    )
}

fn prefix_reinjection_frames_with_available_output_classified(
    path_stream: &ReliablePathStream,
    reinjection_frames: Vec<Frame>,
    allow_same_output_frontier_retransmit: bool,
) -> (Vec<Frame>, Option<u64>, bool) {
    let mut accepted = Vec::with_capacity(reinjection_frames.len());
    for frame in reinjection_frames {
        if !path_stream.has_reinjection_path_for_frame(&frame) {
            if allow_same_output_frontier_retransmit && accepted.is_empty() {
                accepted.push(frame);
                return (accepted, None, true);
            }
            return (
                accepted,
                reliable_stream_frame_extent(&frame).map(|(offset, _, _)| offset),
                false,
            );
        }
        accepted.push(frame);
    }
    (accepted, None, false)
}

fn prefix_live_reinjection_frames_with_carrier_credit(
    response_sender: &ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    reinjection_frames: Vec<Frame>,
    cause: RelaySendCause,
) -> (Vec<Frame>, Option<u64>) {
    let mut accepted = Vec::with_capacity(reinjection_frames.len());
    for frame in reinjection_frames {
        // Match Linux MPTCP recovery: select an eligible idle carrier before
        // publishing reinjection work, rather than blocking new data behind it.
        if response_sender
            .reinjection_path_snapshot_for_frame(path_stream, &frame, cause)
            .is_none()
        {
            return (
                accepted,
                reliable_stream_frame_extent(&frame).map(|(offset, _, _)| offset),
            );
        }
        accepted.push(frame);
    }
    (accepted, None)
}

fn prefix_reinjection_frames_with_unknown_owner_output(
    path_stream: &ReliablePathStream,
    reinjection_frames: Vec<Frame>,
) -> (Vec<Frame>, Option<u64>) {
    let mut accepted = Vec::with_capacity(reinjection_frames.len());
    for frame in reinjection_frames {
        if !path_stream.has_untracked_data_reinjection_path_for_frame(&frame) {
            return (
                accepted,
                reliable_stream_frame_extent(&frame).map(|(offset, _, _)| offset),
            );
        }
        accepted.push(frame);
    }
    (accepted, None)
}

// Response reinjection queue and dispatch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailReinjectionEnqueueOutcome {
    queued: usize,
    pending: bool,
}

#[cfg(test)]
impl TailReinjectionEnqueueOutcome {
    fn has_reinjection_attempt(self) -> bool {
        self.queued > 0 || self.pending
    }
}

#[allow(clippy::too_many_arguments)]
fn enqueue_reliable_tail_reinjection_with_ack_horizon(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] stream_id: StreamId,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    last_send_ack_complete: bool,
    last_send_ack_horizon: Option<u64>,
    tail_reinjection_path_snapshot: Option<PathSnapshot>,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    performance: MppPerformanceConfig,
    max_frame_payload_bytes: usize,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    last_send_ack_frontier: u64,
) -> TailReinjectionEnqueueOutcome {
    let base_reinjection_limit = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        tail_reinjection_path_snapshot,
        TrafficClass::Throughput,
        mux_limits,
        max_frame_payload_bytes,
    )
    .max(adaptive_reliable_relay_reinjection_bytes(
        tail_reinjection_path_snapshot,
        relay_lane,
        mux_limits,
    ));
    let mut reinjection_limit = 0usize;
    let mut critical_tail_reinjection = false;
    let mut reinjection_kind = "none";
    let mut reinjection_cause = RelaySendCause::AckGapReinjection;
    let mut reinjection_frames = Vec::new();
    let mut blocked_frontier_offset = None;
    let failed_original_ranges = path_stream.uncovered_failed_original_ranges();
    if !failed_original_ranges.is_empty() {
        let reinjection_path = send_stream
            .retransmission_frames_for_ranges(&failed_original_ranges, 1)
            .into_iter()
            .next()
            .and_then(|preview| {
                response_sender.reinjection_path_snapshot_for_frame(
                    path_stream,
                    &preview,
                    RelaySendCause::PathFailureReinjection,
                )
            })
            .map(|(_, snapshot)| snapshot);
        let failed_original_limit = reliable_failed_original_reinjection_limit_bytes(
            reinjection_path,
            send_stream.reinjection_bytes(),
            mux_limits,
        );
        reinjection_frames = send_stream
            .retransmission_frames_for_ranges(&failed_original_ranges, failed_original_limit);
        if !reinjection_frames.is_empty() {
            critical_tail_reinjection = true;
            reinjection_limit = failed_original_limit;
            reinjection_kind = "failed_original_tail_reinjection";
            reinjection_cause = RelaySendCause::PathFailureReinjection;
        }
    }
    let no_ack_frontier_failed_original_tail = last_send_ack_ranges.is_empty()
        && last_send_ack_frontier == 0
        && send_stream.next_offset() > 0;
    if reinjection_frames.is_empty()
        && (last_send_ack_complete || no_ack_frontier_failed_original_tail)
    {
        if stream_ack_ranges_expose_authoritative_gap(last_send_ack_complete, last_send_ack_ranges)
            && path_stream.has_multipath_reinjection_alternative()
        {
            // A generic timeout proves only that the lowest product gap is
            // blocking delivery. Repair one adaptive quantum here; persistent
            // evidence may bypass optional traffic budget but does not enlarge it.
            let gap_limit = reliable_critical_tail_reinjection_limit_bytes(
                base_reinjection_limit,
                send_stream.reinjection_bytes(),
                mux_limits,
            );
            let gap_source_frames = stream_ack_gap_reinjection_frames_normalized(
                send_stream,
                last_send_ack_ranges,
                gap_limit,
                true,
                true,
                true,
            );
            let (gap_frames, gap_blocked_offset) =
                prefix_live_reinjection_frames_with_carrier_credit(
                    response_sender,
                    path_stream,
                    gap_source_frames,
                    RelaySendCause::AckGapReinjection,
                );
            if !gap_frames.is_empty() {
                critical_tail_reinjection = true;
                reinjection_limit = gap_limit;
                reinjection_frames = gap_frames;
                blocked_frontier_offset = gap_blocked_offset;
                reinjection_kind = "ack_gap_retransmission";
                reinjection_cause = RelaySendCause::AckGapReinjection;
            } else if blocked_frontier_offset.is_none() {
                blocked_frontier_offset = gap_blocked_offset;
            }
        }
        if reinjection_frames.is_empty()
            && last_send_ack_complete
            && last_send_ack_horizon.is_some_and(|horizon| last_send_ack_frontier < horizon)
        {
            let last_send_ack_horizon =
                last_send_ack_horizon.expect("complete ACK tail requires a snapshot horizon");
            let tail_limit = reliable_critical_tail_reinjection_limit_bytes(
                base_reinjection_limit,
                send_stream.reinjection_bytes(),
                mux_limits,
            );
            let tail_source_frames = send_stream.retransmission_frames_for_ranges(
                &[OffsetRange {
                    start: last_send_ack_frontier,
                    end: last_send_ack_horizon,
                }],
                tail_limit,
            );
            let (unknown_owner_frames, unknown_owner_blocked_offset) =
                prefix_reinjection_frames_with_unknown_owner_output(
                    path_stream,
                    tail_source_frames,
                );
            if !unknown_owner_frames.is_empty() {
                critical_tail_reinjection = true;
                reinjection_limit = tail_limit;
                reinjection_frames = unknown_owner_frames;
                blocked_frontier_offset = unknown_owner_blocked_offset;
                reinjection_kind = "tail_unknown_owner";
                reinjection_cause = RelaySendCause::PathFailureReinjection;
            } else if blocked_frontier_offset.is_none() {
                blocked_frontier_offset = unknown_owner_blocked_offset;
            }
        }
        if reinjection_frames.is_empty()
            && stream_ack_is_authoritative_contiguous_prefix(
                last_send_ack_complete,
                last_send_ack_ranges,
                last_send_ack_frontier,
            )
            && last_send_ack_horizon.is_some_and(|horizon| last_send_ack_frontier < horizon)
            && path_stream.has_multipath_reinjection_alternative()
        {
            // A live carrier still owns native recovery. One MPP quantum is
            // enough to race the blocking frontier without creating a second
            // congestion window above TCP or QUIC.
            let tail_limit = reliable_critical_tail_reinjection_limit_bytes(
                base_reinjection_limit,
                send_stream.reinjection_bytes(),
                mux_limits,
            );
            let tail_source_frames = send_stream.retransmission_frames_for_ranges(
                &[OffsetRange {
                    start: last_send_ack_frontier,
                    end: last_send_ack_horizon
                        .expect("authoritative tail requires a snapshot horizon"),
                }],
                tail_limit,
            );
            let (tail_reinjection_frames, tail_reinjection_blocked_offset) =
                prefix_live_reinjection_frames_with_carrier_credit(
                    response_sender,
                    path_stream,
                    tail_source_frames,
                    RelaySendCause::TailReinjection,
                );
            if !tail_reinjection_frames.is_empty() {
                critical_tail_reinjection = true;
                reinjection_limit = tail_limit;
                reinjection_frames = tail_reinjection_frames;
                blocked_frontier_offset = tail_reinjection_blocked_offset;
                reinjection_kind = "tail_reinjection";
                // A live carrier still owns recovery for its original flight.
                // Product tail reinjection may race it only on a distinct output.
                reinjection_cause = RelaySendCause::TailReinjection;
            } else if blocked_frontier_offset.is_none() {
                blocked_frontier_offset = tail_reinjection_blocked_offset;
            }
        }
    }
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = base_reinjection_limit;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = reinjection_kind;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = blocked_frontier_offset;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = reinjection_limit;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "tail_stall_reinjection",
        format_args!(
            "stream_id={} lane={:?} ack_frontier={} sent_offset={} reinjection_bytes={} reinjection_frames={} blocked_frontier_offset={:?} base_reinjection_limit={} reinjection_limit={} extra_traffic_hint_percent={} reinjection_kind={}",
            stream_id.0,
            relay_lane,
            last_send_ack_frontier,
            send_stream.next_offset(),
            send_stream.reinjection_bytes(),
            reinjection_frames.len(),
            blocked_frontier_offset,
            base_reinjection_limit,
            reinjection_limit,
            performance.extra_traffic_hint_percent,
            reinjection_kind,
        ),
    );
    let mut reinjection_count = 0usize;
    let mut reinjection_pending = false;
    let live_reinjection_retry_after =
        reliable_relay_tail_reinjection_delay(tail_reinjection_path_snapshot);
    for frame in reinjection_frames {
        if response_sender.has_queued_reinjection_overlap(&frame)
            || path_stream.has_recent_reinjection_overlap(&frame, live_reinjection_retry_after)
        {
            reinjection_pending = true;
            continue;
        }
        let queued = if critical_tail_reinjection {
            Some(
                response_sender
                    .enqueue_critical_reinjection_frame_with_cause(frame, reinjection_cause),
            )
        } else {
            response_sender.enqueue_reinjection_frame_with_priority(frame, mux_limits, true)
        };
        if queued.is_some() {
            reinjection_count = reinjection_count.saturating_add(1);
        }
    }
    TailReinjectionEnqueueOutcome {
        queued: reinjection_count,
        pending: reinjection_pending,
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn enqueue_reliable_tail_reinjection(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    stream_id: StreamId,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    last_send_ack_complete: bool,
    tail_reinjection_path_snapshot: Option<PathSnapshot>,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
    max_frame_payload_bytes: usize,
    last_send_ack_frontier: u64,
) -> TailReinjectionEnqueueOutcome {
    enqueue_reliable_tail_reinjection_with_ack_horizon(
        response_sender,
        path_stream,
        stream_id,
        send_stream,
        last_send_ack_ranges,
        last_send_ack_complete,
        last_send_ack_complete.then_some(send_stream.next_offset()),
        tail_reinjection_path_snapshot,
        relay_lane,
        mux_limits,
        performance,
        max_frame_payload_bytes,
        last_send_ack_frontier,
    )
}

#[allow(clippy::too_many_arguments)]
async fn drain_server_response_sender_ready(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    mut data_ack_outstanding_bytes: usize,
    send_stream: &mut ReliableSendStream,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    sender_dispatch_byte_budget: usize,
    sender_dispatch_item_budget: usize,
    stats: &mut PathDeliveryStats,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] session_id: SessionId,
) -> Result<bool, RuntimeError> {
    let mut dispatched_items = 0usize;
    let mut dispatched_payload_bytes = 0usize;
    let mut blocked_by_carrier = false;

    while response_sender.queued_send_ready()
        && dispatched_items < sender_dispatch_item_budget
        && (dispatched_payload_bytes < sender_dispatch_byte_budget || dispatched_items == 0)
    {
        let dispatch = match response_sender.dispatch_next_with_data_ack_outstanding(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            data_ack_outstanding_bytes,
        ) {
            Ok(dispatch) => dispatch,
            Err(RuntimeError::SenderServiceBlocked) => {
                blocked_by_carrier = true;
                break;
            }
            Err(err) => return Err(err),
        };
        dispatched_items = dispatched_items.saturating_add(1);
        if dispatch.lane == ReliableWorkClass::Reinjection {
            #[cfg(feature = "lab-diagnostics")]
            {
                let (selected_underlay, selected_path_id) = dispatch
                    .selected_path
                    .map(|path| (format!("{:?}", path.underlay), path.path_id.0.to_string()))
                    .unwrap_or_else(|| ("none".to_string(), "none".to_string()));
                lab_diagnostic(
                    "reinjection_frame_dispatched",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={} path_id={} payload_bytes={}",
                        session_id.0,
                        path_stream.stream_id.0,
                        selected_underlay,
                        selected_path_id,
                        dispatch.payload_bytes,
                    ),
                );
            }
        } else {
            dispatched_payload_bytes =
                dispatched_payload_bytes.saturating_add(dispatch.payload_bytes);
            stats.record_payload_bytes(dispatch.payload_bytes);
            if dispatch.lane == ReliableWorkClass::Data {
                data_ack_outstanding_bytes =
                    data_ack_outstanding_bytes.saturating_add(dispatch.payload_bytes);
            }
        }
    }

    #[cfg(feature = "lab-diagnostics")]
    if dispatched_items > 0 {
        lab_diagnostic(
            "server_sender_drain",
            format_args!(
                "session_id={} stream_id={} lane={:?} dispatches={} payload_bytes={} byte_budget={} item_budget={} queue_bytes_after={} blocked_by_carrier={}",
                session_id.0,
                path_stream.stream_id.0,
                relay_lane,
                dispatched_items,
                dispatched_payload_bytes,
                sender_dispatch_byte_budget,
                sender_dispatch_item_budget,
                response_sender.bytes(),
                blocked_by_carrier,
            ),
        );
    }

    if dispatched_payload_bytes > 0 {
        tokio::task::yield_now().await;
    }

    Ok(blocked_by_carrier)
}

// Reliable server response relay
async fn relay_reliable_stream<S>(
    local: S,
    mut path_stream: ReliablePathStream,
    context: &ServerReliableRelayContext,
    session_id: SessionId,
    session_send_buffer: crate::runtime::stream::SessionSendBuffer,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    #[cfg(feature = "lab-diagnostics")]
    let stream_id = path_stream.stream_id;
    let mut close = ServerRelayClose {
        sent: false,
        lane: path_stream.current_lane(),
    };
    let result = relay_reliable_stream_body(
        local,
        &mut path_stream,
        context,
        session_id,
        session_send_buffer,
        &mut close,
    )
    .await;
    // This wrapper is the single ordinary-return close path. The admission
    // supervisor handles cancellation and panic outside this future.
    match &result {
        // Target socket failure is terminal for the product stream. Publish the
        // reset on every attached output before registry retirement; ordinary
        // target EOF remains the half-close/STREAM_FIN path in the relay body.
        Err(RuntimeError::Io(_)) => {
            path_stream
                .reset_and_close_ordered(ResetReason::RemoteClosed, close.lane)
                .await;
        }
        _ if close.sent => path_stream.close_ordered(close.lane).await,
        _ => path_stream.close().await,
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("stream_close");
    #[cfg(feature = "lab-diagnostics")]
    lab_assert_server_sender_service_balanced(session_id.0, stream_id.0);
    result
}

#[derive(Debug, Clone, Copy)]
struct ServerRelayClose {
    sent: bool,
    lane: TrafficClass,
}

async fn relay_reliable_stream_body<S>(
    mut local: S,
    path_stream: &mut ReliablePathStream,
    context: &ServerReliableRelayContext,
    session_id: SessionId,
    session_send_buffer: crate::runtime::stream::SessionSendBuffer,
    close: &mut ServerRelayClose,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mux_limits = context.mux_limits;
    let performance = context.performance;
    let session_retention_timeout = context.session_retention_timeout;
    let stream_id = path_stream.stream_id;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = session_id;
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, 0);
    send_stream.update_max_offset(path_stream.max_offset);
    let initial_recv_max_offset = reliable_stream_initial_advertised_window_bytes(
        path_stream.underlay,
        path_stream.lane,
        mux_limits,
    );
    let mut recv_stream = ReliableRecvStream::new_with_initial_max_offset(
        stream_id,
        mux_limits,
        initial_recv_max_offset,
    );
    let chunk_size = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        None,
        TrafficClass::Latency,
        mux_limits,
        path_stream.max_frame_payload_bytes,
    );
    let mut buf = bytes::BytesMut::with_capacity(chunk_size);
    let mut local_open = true;
    let mut remote_open = true;
    let mut stats = PathDeliveryStats::default();
    let mut terminal_fin_replayed = false;
    let mut pending_local_fin = false;
    let mut pending_remote_fin_offset = None;
    let mut recv_progress = ReliableRecvProgress::default();
    let mut request_sparse_ack_progress = RequestTcpSparseAckProgress::default();
    let mut ack_gap_reinjection = ReliableAckGapReinjectionProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();
    let mut last_send_ack_progress_at = Instant::now();
    let mut last_send_ack_frontier = 0_u64;
    let mut last_send_ack = AuthoritativeStreamAckSnapshot::default();
    let mut tail_reinjection_timer = ReliableRelayTailReinjectionTimer::default();
    let mut flow_demand =
        ReliableRelayFlowDemandTracker::with_initial_lane(path_stream.current_lane());
    let mut output_updates = path_stream.subscribe_output_updates();
    let mut multipath_reinjection_alternative_available =
        path_stream.has_multipath_reinjection_alternative();
    let mut response_sender =
        ServerResponseSenderService::new_with_performance(session_id, stream_id, performance);
    let mut deferred_path_frame = None::<Result<Frame, RuntimeError>>;
    let mut ready_path_data = super::io::ReadyStreamDataBatch::new();
    let mut send_buffer_reservation = session_send_buffer.stream_reservation();
    let mut send_buffer_updates = session_send_buffer.subscribe();
    let mut response_sender_retry_at: Option<tokio::time::Instant> = None;
    let mut no_output_since: Option<Instant> = None;
    let mut last_sender_dispatch_byte_budget =
        relay_lane_startup_chunk_bytes(close.lane, mux_limits)
            .min(path_stream.max_frame_payload_bytes)
            .max(1);
    let mut last_sender_dispatch_item_budget = 1usize;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_budget: Option<(TrafficClass, usize, usize)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut receive_hole_diagnostics = ServerReceiveHoleDiagnostics::default();

    let mut result = loop {
        let now = Instant::now();
        let has_live_output = path_stream.has_live_output();
        if has_live_output {
            no_output_since = None;
        } else {
            let disconnected_at = *no_output_since.get_or_insert(now);
            if now.saturating_duration_since(disconnected_at) >= session_retention_timeout {
                break Err(RuntimeError::SessionRetentionTimeout);
            }
        }
        let session_retention_deadline = no_output_since
            .and_then(|since| since.checked_add(session_retention_timeout))
            .map(tokio::time::Instant::from_std);
        if stream_terminal_fin_replay_required(
            close.sent,
            terminal_fin_replayed,
            response_sender.is_empty(),
        ) {
            response_sender.enqueue_final_control_frame(Frame::StreamFin {
                stream_id,
                final_offset: send_stream.next_offset(),
            });
            response_sender_retry_at = None;
            terminal_fin_replayed = true;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "terminal_fin_replay",
                format_args!(
                    "stream_id={} final_offset={} ack_frontier={} reinjection_bytes={} role=server",
                    stream_id.0,
                    send_stream.next_offset(),
                    last_send_ack_frontier,
                    send_stream.reinjection_bytes(),
                ),
            );
        }
        if !local_open
            && !remote_open
            && send_stream.reinjection_bytes() == 0
            && response_sender.is_empty()
            && (!pending_local_fin || close.sent)
        {
            break Ok(stats);
        }
        let previous_lane = path_stream.current_lane();
        let classifier_payload_hint = relay_lane_startup_chunk_bytes(previous_lane, mux_limits)
            .min(path_stream.max_frame_payload_bytes);
        let classifier_path =
            path_stream.send_path_snapshot(previous_lane, classifier_payload_hint);
        let demand_update = flow_demand.refresh(
            ReliableRelayFlowSignals::new(
                send_stream
                    .next_offset()
                    .saturating_add(response_sender.data_bytes() as u64),
                recv_stream.next_offset(),
            )
            .with_product_work(
                response_sender.data_bytes(),
                send_stream
                    .reinjection_bytes()
                    .saturating_add(recv_stream.reorder_bytes()),
            ),
            classifier_path,
            mux_limits,
        );
        let relay_lane = demand_update.lane;
        if relay_lane != previous_lane {
            path_stream.set_lane(relay_lane);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_lane_changed",
                format_args!(
                    "stream_id={} previous={:?} lane={:?} sent_offset={} received_offset={} reinjection_bytes={} reorder_bytes={} byte_proven={} rate_proven={} buffered_data={}",
                    stream_id.0,
                    previous_lane,
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
        }
        response_sender.publish_queue_bytes(path_stream);
        let payload_hint = relay_lane_startup_chunk_bytes(relay_lane, mux_limits)
            .min(path_stream.max_frame_payload_bytes);
        let send_path_snapshot = path_stream.send_path_snapshot(relay_lane, payload_hint);
        let tail_reinjection_path_snapshot = path_stream.tail_reinjection_snapshot(
            last_send_ack_frontier,
            relay_lane,
            relay_lane_startup_chunk_bytes(relay_lane, mux_limits)
                .min(path_stream.max_frame_payload_bytes),
        );
        let data_ack_recovery_candidate = path_stream
            .data_ack_recovery_candidate(last_send_ack_frontier)
            .map(ReliableRelayTailRecoveryCandidate::Tracked);
        let request_feedback_path_snapshot = path_stream.request_feedback_path_snapshot(relay_lane);
        let request_feedback_underlay = request_feedback_path_snapshot
            .map(|snapshot| snapshot.underlay)
            .or_else(|| path_stream.request_feedback_underlay())
            .unwrap_or(path_stream.underlay);
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(request_feedback_path_snapshot),
        );
        let has_tail_reinjection_alternative = path_stream.has_multipath_reinjection_alternative();
        let failed_original_tail_reinjection_ready =
            reliable_failed_original_tail_reinjection_ready(path_stream, &send_stream);
        let tail_reinjection_candidate = has_tail_reinjection_alternative
            && last_send_ack.has_unacknowledged_extent(last_send_ack_frontier)
            && (stream_ack_is_authoritative_contiguous_prefix(
                last_send_ack.complete(),
                last_send_ack.ranges(),
                last_send_ack_frontier,
            ) || stream_ack_ranges_expose_authoritative_gap(
                last_send_ack.complete(),
                last_send_ack.ranges(),
            ));
        let tail_reinjection_active = reliable_relay_tail_reinjection_timer_active(
            send_stream.reinjection_bytes(),
            tail_reinjection_candidate,
            failed_original_tail_reinjection_ready,
        );
        let data_ack_recovery_candidate = tail_reinjection_active.then(|| {
            data_ack_recovery_candidate.unwrap_or(ReliableRelayTailRecoveryCandidate::Untracked {
                start: last_send_ack_frontier,
                end: send_stream.next_offset(),
                sent_at: last_send_ack_progress_at,
            })
        });
        let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
            relay_lane,
            &send_stream,
            last_send_ack_frontier,
        );
        let tail_reinjection_deadline = tail_reinjection_timer.observe(
            data_ack_recovery_candidate,
            last_send_ack_progress_at,
            tail_reinjection_path_snapshot,
            failed_original_tail_reinjection_ready,
        );
        let adaptive_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
            send_path_snapshot,
            relay_lane,
            mux_limits,
            path_stream.max_frame_payload_bytes,
        );
        let inflight_limit =
            adaptive_reliable_relay_inflight_bytes(send_path_snapshot, relay_lane, mux_limits);
        let sender_queue_limit = reliable_relay_sender_queue_limit(mux_limits, inflight_limit);
        let latency_startup_credit = reliable_latency_startup_credit_remaining_bytes(
            relay_lane,
            classifier_path,
            send_stream.next_offset(),
            response_sender.data_bytes(),
            mux_limits,
        );
        let source_staging_headroom = reliable_relay_source_staging_headroom(
            relay_lane,
            data_ack_outstanding_bytes,
            response_sender.data_bytes(),
            reliable_bulk_carrier_feed_quantum_bytes(mux_limits),
            mux_limits,
        );
        // Source bytes do not receive a data sequence or path assignment until
        // dispatch; the shared Data-ACK/reorder envelope bounds this staging.
        let source_read_ceiling = reliable_relay_buffer_len(mux_limits)
            .min(path_stream.max_frame_payload_bytes)
            .min(sender_queue_limit)
            .min(latency_startup_credit)
            .min(source_staging_headroom);
        if source_read_ceiling > 0 {
            resize_reliable_relay_buffer(&mut buf, source_read_ceiling);
        }
        let (sender_dispatch_byte_budget, sender_dispatch_item_budget) =
            reliable_relay_sender_dispatch_budget(
                mux_limits,
                relay_lane,
                adaptive_chunk,
                inflight_limit,
                sender_queue_limit,
            );
        close.lane = relay_lane;
        last_sender_dispatch_byte_budget = sender_dispatch_byte_budget;
        last_sender_dispatch_item_budget = sender_dispatch_item_budget;
        #[cfg(feature = "lab-diagnostics")]
        if last_reported_budget != Some((relay_lane, adaptive_chunk, inflight_limit)) {
            let snapshot = send_path_snapshot;
            lab_diagnostic(
                "server_relay_budget",
                format_args!(
                    "stream_id={} underlay={:?} lane={:?} chunk_bytes={} inflight_bytes={} max_frame_payload_bytes={} snapshot={} rate_mbps={:.3} pacing_mbps={:.3} product_progress_mbps={:.3} queue_bytes={} data_level_queue_bytes={} carrier_flight_bytes={} product_flight_bytes={} confidence_ppm={}",
                    stream_id.0,
                    path_stream.underlay,
                    relay_lane,
                    adaptive_chunk,
                    inflight_limit,
                    path_stream.max_frame_payload_bytes,
                    snapshot.is_some(),
                    snapshot.map_or(0.0, |path| path.delivery_rate_bps / 1_000_000.0),
                    snapshot.map_or(0.0, |path| path.pacing_rate_bps / 1_000_000.0),
                    snapshot
                        .and_then(|path| path.product_progress_rate_bps)
                        .unwrap_or(0.0)
                        / 1_000_000.0,
                    snapshot.map_or(0, |path| path.queue_bytes),
                    snapshot.map_or(0, |path| path.data_level_queue_bytes),
                    snapshot.map_or(0, |path| path.bytes_in_flight),
                    snapshot.map_or(0, |path| path.data_level_bytes_in_flight),
                    snapshot.map_or(0, |path| (path.confidence.clamp(0.0, 1.0) * 1_000_000.0)
                        .round() as u32),
                ),
            );
            last_reported_budget = Some((relay_lane, adaptive_chunk, inflight_limit));
        }
        let now = tokio::time::Instant::now();
        response_sender.discard_unusable_tail_reinjections(path_stream);
        if response_sender.discard_stale_persistent_ack_gap_reinjections(path_stream) > 0 {
            ack_gap_reinjection.release_reinjection_attempt();
            response_sender_retry_at = None;
        }
        if response_sender_retry_at.is_some_and(|deadline| deadline <= now) {
            response_sender_retry_at = None;
        }
        let queued_front_has_carrier_credit = response_sender
            .front_has_carrier_credit_with_data_ack_outstanding(
                path_stream,
                &send_stream,
                relay_lane,
                mux_limits,
                data_ack_outstanding_bytes,
            );
        let sender_wait = response_sender_wait_state(
            !response_sender.is_empty(),
            response_sender.queued_send_ready(),
            queued_front_has_carrier_credit,
            response_sender_retry_at,
            now,
            sender_service_retry_delay(send_path_snapshot),
        );
        response_sender_retry_at = sender_wait.retry_at;
        let queued_send_blocked = sender_wait.blocked;
        let queued_send_ready = sender_wait.ready;
        let queued_send_retry_deadline = sender_wait.retry_at.unwrap_or(now);
        let carrier_capacity_notifies = if sender_wait.subscribe_capacity {
            path_stream.capacity_notifies()
        } else {
            Vec::new()
        };
        let has_carrier_capacity_notify = !carrier_capacity_notifies.is_empty();
        let queued_send_blocks_source_read = queued_send_blocked;
        let can_read_by_flow = source_read_ceiling > 0
            && source_staging_headroom > 0
            && response_sender.can_read_product_source(
                local_open,
                queued_send_blocks_source_read,
                &send_stream,
                sender_queue_limit,
            );
        let read_budget = if can_read_by_flow {
            response_sender.read_budget(&send_stream, sender_queue_limit, source_read_ceiling)
        } else {
            0
        };
        // A target socket can stay established while every MPP carrier is down.
        // Stop reading so ordinary socket backpressure bounds retained response data.
        let can_read_local = has_live_output && can_read_by_flow && read_budget > 0;
        let can_send_pending_fin = pending_local_fin && response_sender.is_empty() && !close.sent;

        // Carrier input and target responses can both remain continuously
        // ready during an upload. Fair polling keeps response progress from
        // being hidden behind an unbounded run of incoming STREAM_DATA.
        tokio::select! {
        _ = async {
            match session_retention_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        }, if no_output_since.is_some() => {
            // Attachment and expiry can become ready in the same scheduler turn.
            // A newly authenticated carrier wins over retiring the logical stream.
            if path_stream.has_live_output() {
                no_output_since = None;
                continue;
            }
            break Err(RuntimeError::SessionRetentionTimeout);
        }
        _ = tokio::time::sleep_until(tail_reinjection_deadline), if tail_reinjection_active => {
            let _ = enqueue_reliable_tail_reinjection_with_ack_horizon(
                &mut response_sender,
                path_stream,
                    stream_id,
                    &send_stream,
                    last_send_ack.ranges(),
                    last_send_ack.complete(),
                    last_send_ack.horizon(),
                    tail_reinjection_path_snapshot,
                relay_lane,
                mux_limits,
                performance,
                path_stream.max_frame_payload_bytes,
                last_send_ack_frontier,
            );
            tail_reinjection_timer.record_scan();
            let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                relay_lane,
                &send_stream,
                last_send_ack_frontier,
            );
            if drain_server_response_sender_ready(
                &mut response_sender,
                path_stream,
                data_ack_outstanding_bytes,
                &mut send_stream,
                relay_lane,
                mux_limits,
                sender_dispatch_byte_budget,
                sender_dispatch_item_budget,
                &mut stats,
                session_id,
            )
            .await?
            {
                response_sender_retry_at =
                    Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
            }
            continue;
        }
        frame = async {
            #[cfg(feature = "lab-diagnostics")]
            let recv_started = Instant::now();
            let result = match deferred_path_frame.take() {
                Some(frame) => frame,
                None => path_stream.recv_frame().await,
            };
            #[cfg(feature = "lab-diagnostics")]
            if let Ok(frame) = &result {
                lab_perf_record(
                    "relay.path_recv_frame_wait",
                    recv_started.elapsed(),
                    reliable_path_frame_pacing_bytes(frame),
                );
            }
            result
        }, if remote_open || send_stream.reinjection_bytes() > 0 => {
            let frame = frame?;
            response_sender_retry_at = None;
            match frame {
                Frame::StreamData {
                    stream_id: received_stream_id,
                    offset,
                    payload,
                } if received_stream_id == stream_id && remote_open => {
                    let ready_items = if recv_stream.reorder_bytes() == 0 {
                        path_stream.ready_frame_count()
                    } else {
                        0
                    };
                    let receive_limit = pending_remote_fin_offset
                        .unwrap_or(u64::MAX)
                        .min(recv_stream.published_max_offset());
                    let payload_limit = reliable_relay_buffer_len(mux_limits)
                        .min(path_stream.max_frame_payload_bytes)
                        .max(1);
                    let first = Ok(Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        payload,
                    });
                    let deferred = collect_ready_stream_data_batch(
                        &mut ready_path_data,
                        first,
                        ReadyStreamDataBatchBounds {
                            stream_id,
                            receive_frontier: recv_stream.next_offset(),
                            receive_limit,
                            payload_limit,
                            ready_items,
                        },
                        || path_stream.try_recv_frame(),
                        |item| match item {
                            Ok(Frame::StreamData {
                                stream_id,
                                offset,
                                payload,
                            }) => Some((*stream_id, *offset, payload.len())),
                            _ => None,
                        },
                    );
                    debug_assert!(deferred_path_frame.is_none());
                    deferred_path_frame = deferred;
                    apply_and_write_ready_stream_data_batch(
                        &mut local,
                        &mut recv_stream,
                        &mut ready_path_data,
                        ReadyStreamDataDirection::ServerUpload,
                        false,
                        |recv_stream, item| {
                            let frame = item?;
                            let Frame::StreamData {
                                stream_id: received_stream_id,
                                offset,
                                payload,
                            } = frame
                            else {
                                unreachable!("ready data batch contains only STREAM_DATA");
                            };
                            debug_assert_eq!(received_stream_id, stream_id);
                            let payload_len = payload.len();
                            super::io::validate_stream_data_final_offset(
                                pending_remote_fin_offset,
                                offset,
                                payload_len,
                            )?;
                            #[cfg(feature = "lab-diagnostics")]
                            let mux_started = Instant::now();
                            let outcome = recv_stream.receive_data(offset, payload)?;
                            #[cfg(feature = "lab-diagnostics")]
                            lab_perf_record(
                                "mux.receive_data",
                                mux_started.elapsed(),
                                payload_len,
                            );
                            #[cfg(feature = "lab-diagnostics")]
                            receive_hole_diagnostics.observe(
                                stream_id,
                                recv_stream,
                                outcome
                                    .delivered
                                    .iter()
                                    .map(|chunk| chunk.len())
                                    .sum(),
                            );
                            for chunk in outcome.delivered.iter() {
                                stats.record_payload_bytes(chunk.len());
                            }
                            Ok(outcome)
                        },
                    )
                    .await?;
                    if enqueue_tcp_recv_progress(
                        &mut response_sender,
                        &mut recv_stream,
                        &mut recv_progress,
                        &mut request_sparse_ack_progress,
                        request_feedback_path_snapshot,
                        relay_lane,
                        mux_limits,
                        false,
                    )
                    {
                        response_sender_retry_at = None;
                        last_recv_progress_sent_at = Instant::now();
                    }
                    if pending_stream_fin_ready(&recv_stream, pending_remote_fin_offset) {
                        if enqueue_tcp_recv_progress(
                            &mut response_sender,
                            &mut recv_stream,
                            &mut recv_progress,
                            &mut request_sparse_ack_progress,
                            request_feedback_path_snapshot,
                            relay_lane,
                            mux_limits,
                            true,
                        ) {
                            response_sender_retry_at = None;
                            last_recv_progress_sent_at = Instant::now();
                        }
                        local.shutdown().await?;
                        remote_open = false;
                        pending_remote_fin_offset = None;
                    }
                }
                Frame::StreamAck {
                    stream_id: ack_stream_id,
                    complete,
                    ranges,
                } if ack_stream_id == stream_id => {
                    let tcp_service = path_stream.tcp_service_coordinator();
                    let mut tcp_service_transaction =
                        tcp_service.as_ref().map(|coordinator| coordinator.lock());
                    // Freeze the assigned DSN horizon and validate every
                    // original range before any cache, flight, queue,
                    // reservation, or recovery-evidence mutation.
                    let validated_ack =
                        match begin_reliable_stream_ack(&send_stream, complete, ranges) {
                            Ok(ack) => ack,
                            Err(err) => break Err(err.into()),
                        };
                    let normalized_ranges = validated_ack.ranges();
                    #[cfg(feature = "lab-diagnostics")]
                    let mux_started = Instant::now();
                    let ack = match send_stream.apply_validated_ack(&validated_ack) {
                        Ok(ack) => ack,
                        Err(err) => break Err(err.into()),
                    };
                    if ack.released_bytes > 0 {
                        response_sender.record_delivered_data(ack.released_bytes);
                        send_buffer_reservation.release(ack.released_bytes);
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("mux.apply_ack", mux_started.elapsed(), ack.released_bytes);
                    if let Some(transaction) = tcp_service_transaction.as_mut() {
                        path_stream.release_normalized_acked_ranges_for_tcp_service(
                            normalized_ranges,
                            validated_ack.assigned_end(),
                            transaction.lifecycle(),
                        );
                        response_sender.release_normalized_acked_reinjections(normalized_ranges);
                        path_stream.finish_tcp_service_ack(transaction);
                    } else {
                        path_stream.release_normalized_acked_ranges(normalized_ranges);
                        response_sender.release_normalized_acked_reinjections(normalized_ranges);
                    }
                    // Recovery planning below is outside the indivisible Data
                    // ACK release/boundary transaction and must not stall
                    // frozen writers in either direction.
                    drop(tcp_service_transaction);
                    #[cfg(feature = "lab-diagnostics")]
                    let largest_ack_end = normalized_ranges.last().map_or(0, |range| range.end);
                    #[cfg(feature = "lab-diagnostics")]
                    let incoming_ack_frontier =
                        stream_ack_contiguous_frontier(normalized_ranges);
                    let previous_ack_frontier = last_send_ack_frontier;
                    update_reinjection_authoritative_ack_snapshot(
                        &mut last_send_ack,
                        &validated_ack,
                    );
                    // Positive ACK chunks release bytes even when their range
                    // list is incomplete. Only the complete snapshot above may
                    // authorize gap inference; flow control follows the exact
                    // lowest outstanding Data Sequence offset.
                    last_send_ack_frontier = send_stream.data_ack_frontier();
                    let authoritative_ack_complete = last_send_ack.complete();
                    let authoritative_ack_ranges = last_send_ack.ranges();
                    let ack_made_progress = last_send_ack_frontier > previous_ack_frontier;
                    if ack_made_progress {
                        last_send_ack_progress_at = Instant::now();
                    }
                    let base_reinjection_limit = adaptive_reliable_relay_reinjection_bytes(
                        send_path_snapshot,
                        relay_lane,
                        mux_limits,
                    );
                    let reinjection_event_budget =
                        response_sender.reinjection_extra_event_budget_remaining(mux_limits);
                    let has_multipath_reinjection_alternative =
                        path_stream.has_multipath_reinjection_alternative();
                    let ack_gap_original_flight =
                        path_stream.data_ack_recovery_candidate(last_send_ack_frontier);
                    let reinjection_original_underlay = ack_gap_original_flight
                        .map(|candidate| candidate.key.underlay)
                        .or_else(|| {
                            path_stream
                                .tail_reinjection_original_underlay(last_send_ack_frontier)
                        });
                    let reinjection_target =
                        has_multipath_reinjection_alternative.then(|| {
                            response_sender.ack_gap_reinjection_path_snapshot(
                                path_stream,
                                &send_stream,
                                authoritative_ack_ranges,
                                base_reinjection_limit,
                            )
                        });
                    let reinjection_target = reinjection_target.flatten();
                    // Later ACKs use the time-threshold loss check below. A
                    // retained timer represents silence, so it waits RTO/PTO.
                    let data_ack_recovery_deadline =
                        stream_ack_ranges_expose_authoritative_gap(
                            authoritative_ack_complete,
                            authoritative_ack_ranges,
                        )
                        .then(|| {
                            reliable_data_ack_recovery_deadline(
                                ack_gap_original_flight.map(|candidate| candidate.sent_at),
                                reinjection_original_underlay,
                                tail_reinjection_path_snapshot,
                                reinjection_target.map(|target| target.completion),
                            )
                        })
                        .flatten();
                    if let (Some(candidate), Some(deadline)) =
                        (ack_gap_original_flight, data_ack_recovery_deadline)
                    {
                        tail_reinjection_timer.arm_recovery_deadline(
                            ReliableRelayTailRecoveryCandidate::Tracked(candidate),
                            deadline,
                        );
                    }
                    let reinjection_observed_at = Instant::now();
                    let measured_reinjection_ready =
                        reliable_data_ack_gap_reinjection_ready(
                            ack_gap_original_flight.map(|candidate| candidate.sent_at),
                            reinjection_original_underlay,
                            tail_reinjection_path_snapshot,
                            reinjection_target.map(|target| target.completion),
                            reinjection_observed_at,
                        );
                    let reinjection_retry_after = reliable_data_retransmission_interval(
                        reinjection_original_underlay,
                        tail_reinjection_path_snapshot,
                    );
                    let ack_gap_reinjection_ready = ack_gap_reinjection.reinjection_ready(
                        authoritative_ack_complete,
                        authoritative_ack_ranges,
                        has_multipath_reinjection_alternative,
                        measured_reinjection_ready,
                        reinjection_retry_after,
                    );
                    let persistent_ack_gap_reinjection_ready =
                        ack_gap_reinjection_ready && reinjection_target.is_some();
                    let reinjection_limit = if persistent_ack_gap_reinjection_ready {
                        reliable_critical_tail_reinjection_limit_bytes(
                            base_reinjection_limit,
                            send_stream.reinjection_bytes(),
                            mux_limits,
                        )
                    } else {
                        base_reinjection_limit.min(reinjection_event_budget)
                    };
                    let ack_gap_reinjection_cause = if persistent_ack_gap_reinjection_ready {
                        let target = reinjection_target
                            .expect("persistent reinjection requires a measured output");
                        RelaySendCause::persistent_server_ack_gap_reinjection(
                            target.identity,
                            target.snapshot,
                        )
                    } else {
                        RelaySendCause::AckGapReinjection
                    };
                    let mut reinjection_frames = stream_ack_gap_reinjection_frames_normalized(
                        &send_stream,
                        authoritative_ack_ranges,
                        reinjection_limit,
                        authoritative_ack_complete,
                        has_multipath_reinjection_alternative,
                        persistent_ack_gap_reinjection_ready,
                    );
                    let mut critical_tail_reinjection =
                        persistent_ack_gap_reinjection_ready && !reinjection_frames.is_empty();
                    let reinjection_kind = if reinjection_frames.is_empty() {
                        let fin_tail_stall_ready =
                            tokio::time::Instant::now() >= tail_reinjection_deadline
                                && !ack_made_progress;
                        let fin_tail_ready = close.sent || pending_local_fin;
                        let fin_tail_limit = if fin_tail_ready {
                            let limit = reliable_critical_tail_reinjection_limit_bytes(
                                base_reinjection_limit,
                                send_stream.reinjection_bytes(),
                                mux_limits,
                            );
                            critical_tail_reinjection = reliable_critical_tail_reinjection_is_over_budget(
                                reinjection_event_budget,
                                limit,
                            );
                            limit
                        } else {
                            reinjection_limit
                        };
                        let (
                            fin_tail_frames,
                            blocked_frontier_offset,
                            _same_output_frontier_retransmit,
                        ) = prefix_final_tail_reinjection_frames_with_available_output(
                            path_stream,
                            stream_final_offset_tail_reinjection_frames_normalized(
                                &send_stream,
                                authoritative_ack_ranges,
                                fin_tail_limit,
                                fin_tail_ready,
                                fin_tail_stall_ready,
                            ),
                        );
                        #[cfg(feature = "lab-diagnostics")]
                        if blocked_frontier_offset.is_some() {
                            lab_diagnostic(
                                "tail_stall_reinjection_blocked_frontier",
                                format_args!(
                                    "stream_id={} blocked_frontier_offset={:?} reinjection_kind=fin_tail",
                                    stream_id.0, blocked_frontier_offset,
                                ),
                            );
                        }
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = blocked_frontier_offset;
                        if fin_tail_frames.is_empty() {
                            "ack_gap"
                        } else {
                            reinjection_frames = fin_tail_frames;
                            "fin_tail"
                        }
                    } else {
                        "ack_gap"
                    };
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = reinjection_kind;
                    let live_reinjection_retry_after =
                        reliable_relay_tail_reinjection_delay(tail_reinjection_path_snapshot);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "stream_ack_received",
                        format_args!(
                            "stream_id={} complete={} ranges={} incoming_frontier={} stored_frontier={} largest_end={} released_bytes={} sent_offset={} sender_queue_bytes={} reinjection_bytes_after={} reinjection_frames={} reinjection_kind={} active_underlay={:?} multipath_reinjection_alternative={} ack_gap_reinjection_ready={} base_reinjection_limit={} reinjection_limit={} extra_traffic_hint_percent={}",
                            stream_id.0,
                            complete,
                            normalized_ranges.len(),
                            incoming_ack_frontier,
                            last_send_ack_frontier,
                            largest_ack_end,
                            ack.released_bytes,
                            send_stream.next_offset(),
                            response_sender.bytes(),
                            ack.remaining_reinjection_bytes,
                            reinjection_frames.len(),
                            reinjection_kind,
                            Some(path_stream.underlay),
                            has_multipath_reinjection_alternative,
                            persistent_ack_gap_reinjection_ready,
                            base_reinjection_limit,
                            reinjection_limit,
                            performance.extra_traffic_hint_percent,
                        ),
                    );
                    let mut queued_persistent_ack_gap_reinjection = false;
                    for frame in reinjection_frames {
                        let queued = if path_stream.has_recent_reinjection_overlap(
                            &frame,
                            live_reinjection_retry_after,
                        ) || response_sender.has_queued_reinjection_overlap(&frame)
                        {
                            false
                        } else if critical_tail_reinjection {
                            if reinjection_kind == "fin_tail" {
                                response_sender
                                    .enqueue_critical_tail_reinjection_frame(frame)
                                    .is_some()
                            } else {
                                response_sender.enqueue_critical_reinjection_frame_with_cause(
                                    frame,
                                    ack_gap_reinjection_cause,
                                );
                                true
                            }
                        } else {
                            response_sender
                                .enqueue_reinjection_frame_with_priority(frame, mux_limits, true)
                                .is_some()
                        };
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = queued;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "reinjection",
                            format_args!(
                                "stream_id={} cause={} queued={}",
                                stream_id.0, reinjection_kind, queued,
                            ),
                        );
                        if queued {
                            queued_persistent_ack_gap_reinjection |=
                                persistent_ack_gap_reinjection_ready
                                    && reinjection_kind == "ack_gap";
                        }
                    }
                    if queued_persistent_ack_gap_reinjection {
                        ack_gap_reinjection.record_reinjection_queued();
                    }
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = ack;
                    if pending_local_fin
                        && response_sender.is_empty()
                        && send_stream.reinjection_bytes() == 0
                    {
                        let frame = Frame::StreamFin {
                            stream_id,
                            final_offset: send_stream.next_offset(),
                        };
                        response_sender.enqueue_final_control_frame(frame);
                        response_sender_retry_at = None;
                        close.sent = true;
                        pending_local_fin = false;
                    }
                }
                Frame::StreamMaxData {
                    stream_id: max_stream_id,
                    max_offset,
                } if max_stream_id == stream_id => {
                    send_stream.update_max_offset(max_offset);
                }
                Frame::StreamFin {
                    stream_id: fin_stream_id,
                    final_offset,
                } if fin_stream_id == stream_id => {
                    if receive_stream_fin(
                        &recv_stream,
                        &mut pending_remote_fin_offset,
                        final_offset,
                    )? {
                        if enqueue_tcp_recv_progress(
                            &mut response_sender,
                            &mut recv_stream,
                            &mut recv_progress,
                            &mut request_sparse_ack_progress,
                            request_feedback_path_snapshot,
                            relay_lane,
                            mux_limits,
                            true,
                        ) {
                            response_sender_retry_at = None;
                            last_recv_progress_sent_at = Instant::now();
                        }
                        local.shutdown().await?;
                        remote_open = false;
                        pending_remote_fin_offset = None;
                    }
                }
                Frame::StreamReset {
                    stream_id: reset_stream_id,
                    reason,
                } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
                Frame::StreamData {
                    stream_id: received_stream_id,
                    offset,
                    payload,
                    ..
                } if received_stream_id == stream_id
                    && stream_data_range_already_delivered(&recv_stream, offset, payload.len()) =>
                {
                    if enqueue_tcp_recv_progress(
                        &mut response_sender,
                        &mut recv_stream,
                        &mut recv_progress,
                        &mut request_sparse_ack_progress,
                        request_feedback_path_snapshot,
                        relay_lane,
                        mux_limits,
                        true,
                    ) {
                        response_sender_retry_at = None;
                        last_recv_progress_sent_at = Instant::now();
                    }
                }
                unexpected => {
                    log_unexpected_stream_relay_frame("single", stream_id, &unexpected);
                    return Err(RuntimeError::Protocol("unexpected stream relay frame"));
                }
            }
            if response_sender.queued_send_ready() {
                let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_frontier,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    path_stream,
                    data_ack_outstanding_bytes,
                    &mut send_stream,
                    relay_lane,
                    mux_limits,
                    sender_dispatch_byte_budget,
                    sender_dispatch_item_budget,
                    &mut stats,
                    session_id,
                )
                .await?
                {
                    response_sender_retry_at =
                        Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
                }
            }
        }
        changed = async {
            match output_updates.as_mut() {
                Some(updates) => updates
                    .changed()
                    .await
                    .map_err(|_| RuntimeError::ReliablePathSessionClosed),
                None => std::future::pending::<Result<(), RuntimeError>>().await,
            }
        }, if output_updates.is_some() => {
            changed?;
            let now_has_reinjection_alternative = path_stream.has_multipath_reinjection_alternative();
            let gained_reinjection_alternative =
                now_has_reinjection_alternative && !multipath_reinjection_alternative_available;
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = gained_reinjection_alternative;
            multipath_reinjection_alternative_available = now_has_reinjection_alternative;
            response_sender_retry_at = None;
            let final_tail_reinjection_ready = reliable_final_tail_reinjection_ready(
                close.sent || pending_local_fin,
                &send_stream,
                last_send_ack.ranges(),
                last_send_ack_frontier,
                tail_reinjection_deadline,
                tokio::time::Instant::now(),
            );
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_output_update",
                format_args!(
                    "stream_id={} now_has_reinjection_alternative={} gained_reinjection_alternative={} final_tail_reinjection_ready={} close_sent={} pending_local_fin={} reinjection_bytes={} ack_ranges={} ack_frontier={} sent_offset={} queue_bytes={}",
                    stream_id.0,
                    now_has_reinjection_alternative,
                    gained_reinjection_alternative,
                    final_tail_reinjection_ready,
                    close.sent,
                    pending_local_fin,
                    send_stream.reinjection_bytes(),
                    last_send_ack.ranges().len(),
                    last_send_ack_frontier,
                    send_stream.next_offset(),
                    response_sender.bytes(),
                ),
            );
            if final_tail_reinjection_ready {
                let reinjection_limit = reliable_critical_tail_reinjection_limit_bytes(
                    adaptive_reliable_relay_reinjection_bytes(
                        tail_reinjection_path_snapshot,
                        relay_lane,
                        mux_limits,
                    ),
                    send_stream.reinjection_bytes(),
                    mux_limits,
                );
                let (
                    reinjection_frames,
                    blocked_frontier_offset,
                    same_output_frontier_retransmit,
                ) = prefix_final_tail_reinjection_frames_with_available_output(
                    path_stream,
                    stream_final_offset_tail_reinjection_frames_normalized(
                        &send_stream,
                        last_send_ack.ranges(),
                        reinjection_limit,
                        true,
                        true,
                    ),
                );
                #[cfg(feature = "lab-diagnostics")]
                if blocked_frontier_offset.is_some() {
                    lab_diagnostic(
                        "tail_stall_reinjection_blocked_frontier",
                        format_args!(
                            "stream_id={} blocked_frontier_offset={:?} reinjection_kind=fin_tail",
                            stream_id.0, blocked_frontier_offset,
                        ),
                    );
                }
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = blocked_frontier_offset;
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = same_output_frontier_retransmit;
                let live_reinjection_retry_after =
                    reliable_relay_tail_reinjection_delay(tail_reinjection_path_snapshot);
                let mut reinjection_count = 0usize;
                for frame in reinjection_frames {
                    let queued = if path_stream.has_recent_reinjection_overlap(
                        &frame,
                        live_reinjection_retry_after,
                    ) {
                        false
                    } else {
                        response_sender
                            .enqueue_critical_tail_reinjection_frame(frame)
                            .is_some()
                    };
                    if queued {
                        reinjection_count = reinjection_count.saturating_add(1);
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "reinjection",
                        format_args!(
                            "stream_id={} cause=fin_tail queued={}",
                            stream_id.0, queued
                        ),
                    );
                }
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "tail_stall_reinjection",
                    format_args!(
                        "stream_id={} lane={:?} ack_frontier={} sent_offset={} reinjection_bytes={} reinjection_frames={} blocked_frontier_offset={:?} same_output_frontier_retransmit={} base_reinjection_limit={} reinjection_limit={} extra_traffic_hint_percent={} reinjection_kind=fin_tail",
                        stream_id.0,
                        relay_lane,
                        last_send_ack_frontier,
                        send_stream.next_offset(),
                        send_stream.reinjection_bytes(),
                        reinjection_count,
                        blocked_frontier_offset,
                        same_output_frontier_retransmit,
                        reinjection_limit,
                        reinjection_limit,
                        performance.extra_traffic_hint_percent,
                    ),
                );
                if reinjection_count > 0 {
                    tail_reinjection_timer.record_attempt_at(Instant::now());
                }
            }
            if response_sender.queued_send_ready() {
                let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_frontier,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    path_stream,
                    data_ack_outstanding_bytes,
                    &mut send_stream,
                    relay_lane,
                    mux_limits,
                    sender_dispatch_byte_budget,
                    sender_dispatch_item_budget,
                    &mut stats,
                    session_id,
                )
                .await?
                {
                    response_sender_retry_at =
                        Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
                }
            }
            continue;
        }
        _ = wait_for_carrier_capacity_notifies(carrier_capacity_notifies), if queued_send_blocked && has_carrier_capacity_notify => {
            response_sender_retry_at = None;
            continue;
        }
        _ = tokio::time::sleep_until(queued_send_retry_deadline), if queued_send_blocked => {
            response_sender_retry_at = None;
            continue;
        }
        _ = tokio::time::sleep_until(recv_progress_deadline), if reliable_relay_recv_progress_timer_enabled(
                request_feedback_underlay,
                multipath_reinjection_alternative_available,
            )
            && reliable_relay_recv_progress_resend_active(
                &recv_stream,
                remote_open,
                Some(request_feedback_underlay),
            ) => {
            if enqueue_tcp_recv_progress(
                &mut response_sender,
                &mut recv_stream,
                &mut recv_progress,
                &mut request_sparse_ack_progress,
                request_feedback_path_snapshot,
                relay_lane,
                mux_limits,
                true,
            ) {
                response_sender_retry_at = None;
                last_recv_progress_sent_at = Instant::now();
            }
            if response_sender.queued_send_ready() {
                let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_frontier,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    path_stream,
                    data_ack_outstanding_bytes,
                    &mut send_stream,
                    relay_lane,
                    mux_limits,
                    sender_dispatch_byte_budget,
                    sender_dispatch_item_budget,
                    &mut stats,
                    session_id,
                )
                .await?
                {
                    response_sender_retry_at =
                        Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
                }
            }
        }
        _ = std::future::ready(()), if can_send_pending_fin => {
            let frame = Frame::StreamFin {
                stream_id,
                final_offset: send_stream.next_offset(),
            };
            response_sender.enqueue_final_control_frame(frame);
            response_sender_retry_at = None;
            close.sent = true;
            pending_local_fin = false;
            let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                relay_lane,
                &send_stream,
                last_send_ack_frontier,
            );
            if drain_server_response_sender_ready(
                &mut response_sender,
                path_stream,
                data_ack_outstanding_bytes,
                &mut send_stream,
                relay_lane,
                mux_limits,
                sender_dispatch_byte_budget,
                sender_dispatch_item_budget,
                &mut stats,
                session_id,
            )
            .await?
            {
                response_sender_retry_at =
                    Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
            }
        }
        _ = std::future::ready(()), if queued_send_ready => {
            let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                relay_lane,
                &send_stream,
                last_send_ack_frontier,
            );
            if drain_server_response_sender_ready(
                &mut response_sender,
                path_stream,
                data_ack_outstanding_bytes,
                &mut send_stream,
                relay_lane,
                mux_limits,
                sender_dispatch_byte_budget,
                sender_dispatch_item_budget,
                &mut stats,
                session_id,
            )
            .await?
            {
                response_sender_retry_at =
                    Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
            }
            tokio::task::yield_now().await;
        }
        read = async {
            let permit = session_send_buffer
                .reserve(&mut send_buffer_updates, read_budget)
                .await;
            let reserved_read_budget = permit.bytes();
            #[cfg(feature = "lab-diagnostics")]
            let read_started = Instant::now();
            let result =
                read_reliable_relay_payload(&mut local, &mut buf, reserved_read_budget).await;
            #[cfg(feature = "lab-diagnostics")]
            if let Ok((read, _)) = &result {
                lab_perf_record("relay.local_read_wait", read_started.elapsed(), *read);
            }
            (result, permit)
        }, if can_read_local => {
            let (read, permit) = read;
            let (read, payload) = read?;
            permit.retain(&mut send_buffer_reservation, read);
            if read == 0 {
                pending_local_fin = true;
                local_open = false;
            } else {
                let payload = payload.expect("positive read returns payload");
                #[cfg(feature = "lab-diagnostics")]
                let enqueue_id = response_sender.enqueue_data_for_lane(payload, relay_lane);
                #[cfg(not(feature = "lab-diagnostics"))]
                response_sender.enqueue_data_for_lane(payload, relay_lane);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_sender_enqueue",
                    format_args!(
                        "session_id={} stream_id={} enqueue_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} send_credit_bytes={} reinjection_bytes={}",
                        session_id.0,
                        stream_id.0,
                        enqueue_id,
                        relay_lane,
                        read,
                        response_sender.bytes(),
                        sender_queue_limit,
                        send_stream.send_credit_bytes(),
                        send_stream.reinjection_bytes(),
                    ),
                );
                let mut opportunistic_reads = 1usize;
                while local_open
                    && opportunistic_reads < sender_dispatch_item_budget
                    && response_sender.can_read_product_source(
                        local_open,
                        false,
                        &send_stream,
                        sender_queue_limit,
                    )
                    && response_sender.data_bytes() < sender_dispatch_byte_budget
                {
                    let source_staging_headroom = reliable_relay_source_staging_headroom(
                            relay_lane,
                            data_ack_outstanding_bytes,
                            response_sender.data_bytes(),
                            reliable_bulk_carrier_feed_quantum_bytes(mux_limits),
                            mux_limits,
                        );
                    if source_staging_headroom == 0 {
                        break;
                    }
                    let next_read_budget = response_sender
                        .read_budget(&send_stream, sender_queue_limit, buf.len())
                        .min(source_staging_headroom);
                    if next_read_budget == 0 {
                        break;
                    }
                    let read = tokio::select! {
                        biased;
                        read = async {
                            let permit = session_send_buffer
                                .reserve(&mut send_buffer_updates, next_read_budget)
                                .await;
                            let result = read_reliable_relay_payload(
                                &mut local,
                                &mut buf,
                                permit.bytes(),
                            )
                            .await;
                            (result, permit)
                        } => read,
                        _ = std::future::ready(()) => break,
                    };
                    let (read, permit) = read;
                    let (read, payload) = read?;
                    permit.retain(&mut send_buffer_reservation, read);
                    if read == 0 {
                        pending_local_fin = true;
                        local_open = false;
                        break;
                    }
                    let payload = payload.expect("positive read returns payload");
                    #[cfg(feature = "lab-diagnostics")]
                    let enqueue_id = response_sender.enqueue_data_for_lane(payload, relay_lane);
                    #[cfg(not(feature = "lab-diagnostics"))]
                    response_sender.enqueue_data_for_lane(payload, relay_lane);
                    opportunistic_reads = opportunistic_reads.saturating_add(1);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "server_sender_enqueue",
                        format_args!(
                            "session_id={} stream_id={} enqueue_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} send_credit_bytes={} reinjection_bytes={} opportunistic=true",
                            session_id.0,
                            stream_id.0,
                            enqueue_id,
                            relay_lane,
                            read,
                            response_sender.bytes(),
                            sender_queue_limit,
                            send_stream.send_credit_bytes(),
                            send_stream.reinjection_bytes(),
                        ),
                    );
                }
                if response_sender.queued_send_ready() {
                    let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                        relay_lane,
                        &send_stream,
                        last_send_ack_frontier,
                    );
                    if drain_server_response_sender_ready(
                        &mut response_sender,
                        path_stream,
                        data_ack_outstanding_bytes,
                        &mut send_stream,
                        relay_lane,
                        mux_limits,
                        sender_dispatch_byte_budget,
                        sender_dispatch_item_budget,
                        &mut stats,
                        session_id,
                    )
                    .await?
                    {
                        response_sender_retry_at =
                            Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
                    }
                }
            }
        }
        else => break Ok(stats),
        }
    };
    if result.is_ok() && pending_local_fin && !close.sent {
        while result.is_ok() {
            if response_sender.discard_stale_persistent_ack_gap_reinjections(path_stream) > 0 {
                ack_gap_reinjection.release_reinjection_attempt();
            }
            if response_sender.is_empty() {
                break;
            }
            let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                close.lane,
                &send_stream,
                last_send_ack_frontier,
            );
            match drain_server_response_sender_ready(
                &mut response_sender,
                path_stream,
                data_ack_outstanding_bytes,
                &mut send_stream,
                close.lane,
                mux_limits,
                last_sender_dispatch_byte_budget,
                last_sender_dispatch_item_budget,
                &mut stats,
                session_id,
            )
            .await
            {
                Ok(true) => {
                    let capacity_notifies = path_stream.capacity_notifies();
                    let has_capacity_notify = !capacity_notifies.is_empty();
                    let retry_at = tokio::time::Instant::now()
                        + sender_service_retry_delay(path_stream.send_path_snapshot(close.lane, 0));
                    let wake_at = response_sender
                        .persistent_ack_gap_reinjection_deadline()
                        .map(tokio::time::Instant::from_std)
                        .map_or(retry_at, |deadline| deadline.min(retry_at));
                    tokio::select! {
                        _ = wait_for_carrier_capacity_notifies(capacity_notifies), if has_capacity_notify => {}
                        changed = async {
                            match output_updates.as_mut() {
                                Some(updates) => updates.changed().await,
                                None => std::future::pending().await,
                            }
                        }, if output_updates.is_some() => {
                            if changed.is_err() {
                                result = Err(RuntimeError::ReliablePathSessionClosed);
                            }
                        }
                        _ = tokio::time::sleep_until(wake_at) => {}
                    }
                }
                Ok(false) if response_sender.queued_send_ready() => {}
                Ok(false) => break,
                Err(err) => result = Err(err),
            }
        }
        if result.is_ok() && response_sender.is_empty() {
            let frame = Frame::StreamFin {
                stream_id,
                final_offset: send_stream.next_offset(),
            };
            response_sender.enqueue_final_control_frame(frame);
            match response_sender.dispatch_next(
                path_stream,
                &mut send_stream,
                close.lane,
                mux_limits,
            ) {
                Ok(dispatch) if dispatch.lane == ReliableWorkClass::Control => {
                    close.sent = true;
                }
                Ok(_) => {
                    result = Err(RuntimeError::Protocol(
                        "server response sender dispatched non-control final close",
                    ));
                }
                Err(err) => {
                    result = Err(err);
                }
            }
        }
    }
    result
}

#[cfg(test)]
#[path = "server_test.rs"]
mod tests;
