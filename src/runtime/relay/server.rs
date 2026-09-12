//! Server-side target relay ownership for reliable product streams.
//!
//! Carrier paths admit streams through the registry; this service owns the
//! carrier-neutral lifetime from target connection through ordered close.

use super::diagnostics::log_unexpected_stream_relay_frame;
use super::flow::{
    ReliableRelayFlowDecision, ReliableRelayFlowDemandTracker, ReliableRelayFlowPathEvidence,
    ReliableRelayFlowSignals,
};
use super::io::first_proven_ack_gap;
use super::io::{
    AuthoritativeStreamAckSnapshot, ReadyStreamDataBatchBounds, ReadyStreamDataDirection,
    ReliableAckGapReinjectionProgress, ReliablePathStalenessObservation,
    ReliableResponsePathStaleness, apply_ready_stream_data_batch, begin_reliable_stream_ack,
    collect_ready_stream_data_batch, exact_contiguous_retransmission_frames,
    pending_stream_fin_ready, preserve_reinjection_frontier_quantum, read_reliable_relay_payload,
    receive_stream_fin, reconcile_accepted_copy_wake, retain_accepted_copy_wake,
    stream_ack_gap_frontier_reinjection_frames_normalized,
    stream_ack_ranges_expose_authoritative_gap, stream_data_range_already_delivered,
    stream_terminal_fin_replay_required, update_reinjection_authoritative_ack_snapshot,
    write_applied_ready_stream_data_batch,
};
#[cfg(test)]
use super::io::{
    stream_ack_gap_reinjection_frames_normalized,
    stream_final_offset_tail_reinjection_frames_normalized,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{
    lab_assert_server_sender_service_balanced, lab_diagnostic, lab_perf_flush, lab_perf_record,
};
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::capacity::{
    adaptive_reliable_relay_chunk_bytes_with_frame_limit,
    adaptive_reliable_relay_reinjection_bytes, relay_lane_startup_chunk_bytes,
    reliable_relay_buffer_len, reliable_relay_sender_dispatch_budget,
    reliable_stream_advertised_window_bytes, reliable_stream_initial_advertised_window_bytes,
};
use crate::model::multipath::{
    LiveOwnerRecoveryWake, include_live_owner_recovery_interval, live_owner_gap_recovery_wake,
    live_owner_recovery_wake,
};
#[cfg(test)]
use crate::model::timing::reliable_data_ack_recovery_deadline;
use crate::model::timing::{
    reliable_data_ack_gap_timing, reliable_data_retransmission_interval,
    reliable_relay_tail_reinjection_delay, sender_service_retry_delay,
};
use crate::model::work::{
    RangeRecoveryState, ReliableWorkClass, flight_interval_bytes,
    reliable_critical_tail_reinjection_limit_bytes, reliable_live_frontier_reinjection_limit_bytes,
    reliable_live_gap_reinjection_authority,
};
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::outbound::OutboundTcpStream;
use crate::performance::MppPerformanceConfig;
use crate::product::InboundId;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::frame::reliable_stream_frame_extent;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::stream_ack_contiguous_frontier;
use crate::protocol::{Frame, OffsetRange, ResetReason, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::error::reliable_path_error_is_migratable;
use crate::runtime::outbound_registry::{OpenedTcpOutbound, finish_gateway_flow};
use crate::runtime::path::PathDeliveryStats;
use crate::runtime::product_lifecycle::{ProductFlowActivity, ProductFlowActivityIo};
use crate::runtime::product_policy::{ClientIngressRouter, ClientPolicyDisposition, ClientRoute};
use crate::runtime::sender::{
    PreparedResponseSource, RelaySendCause, ResponseProductState, ServerReinjectionOutputIdentity,
    ServerResponseSenderService, SharedResponseProduct, publish_prepared_response_work,
    reliable_relay_sender_queue_limit,
};
use crate::runtime::stream::response::ResponseDataAckRecoveryCandidate;
use crate::runtime::stream::{
    AcceptedServerReliableStream, AcceptedServerReliableStreamRetirement, ReliablePathStream,
    ReliablePathStreamOutput, ReliableRecvProgress, RequalificationAttempt,
    ServerReliableStreamRegistry, StreamFeedbackPublication, arm_carrier_capacity_notifies,
    reliable_relay_recv_progress_resend_active, reliable_stream_recv_progress_interval,
    wait_for_carrier_capacity_notifies,
};
use crate::runtime::telemetry::{ObservedProductIo, RuntimeTelemetry};
use crate::scheduler::{PathSnapshot, TrafficClass};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::{Id, JoinError, JoinSet};

pub(in crate::runtime) struct ServerReliableRelayContext {
    pub(in crate::runtime) router: ClientIngressRouter,
    pub(in crate::runtime) inbound: InboundId,
    pub(in crate::runtime) performance: MppPerformanceConfig,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) max_paths_per_session: usize,
    pub(in crate::runtime) session_retention_timeout: Duration,
    pub(in crate::runtime) flow_idle_timeout: Option<Duration>,
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
        let (registry, accepted) =
            ServerReliableStreamRegistry::new_accepting_with_limits_and_retention(
                context.mux_limits,
                context.max_paths_per_session,
                context.session_retention_timeout,
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
                    let session_retirement = match accepted.session_retirement() {
                        Ok(retirement) => retirement,
                        Err(_) => {
                            accepted.close().await;
                            continue;
                        }
                    };
                    let execution_domain = match accepted.session_execution_domain() {
                        Ok(domain) => domain,
                        Err(_) => {
                            accepted.close().await;
                            continue;
                        }
                    };
                    let retirement = accepted.supervise();
                    let context = self.context.clone();
                    // ACK application and actual cancellation/drop participate
                    // in the same session domain as the carrier source claim.
                    let task = relays.spawn(execution_domain.wrap(async move {
                        tokio::select! {
                            biased;
                            _ = session_retirement.wait() => Ok(()),
                            result = relay_accepted_stream(context, accepted) => result,
                        }
                    }));
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
    let ingress = accepted.ingress().ok_or(RuntimeError::Protocol(
        "accepted MPP stream is missing its authenticated opening carrier peer",
    ))?;
    // Zero-credit admission already committed in the carrier actor. Queue the
    // optional proof here, where ordinary async carrier backpressure cannot
    // self-deadlock that actor. A carrier-local failure does not revoke this
    // exactly-once target owner; a surviving or later attachment remains able
    // to establish the same logical stream.
    let _ = accepted.publish_opening_path_validation().await;
    let outbound_stream = match context.router.route_mpp_tcp_with_ingress(
        &target,
        accepted.principal_permit().principal().clone(),
        context.inbound.clone(),
        ingress,
    ) {
        Ok(ClientRoute::Open(plan)) => plan.open_tcp(&target).await,
        Ok(ClientRoute::Deny(ClientPolicyDisposition::Reject)) => Err(RuntimeError::RouteRejected),
        Ok(ClientRoute::Deny(ClientPolicyDisposition::Drop)) => Err(RuntimeError::RouteDropped),
        Err(error) => Err(error),
    };
    let outbound_stream = match outbound_stream {
        Ok(stream) => stream,
        Err(RuntimeError::RouteDropped) => {
            // Post-resolution policy can refine an initially admissible
            // domain route. Retire only this logical stream and publish no
            // refusal frame; sibling flows and their carriers remain live.
            accepted.close().await;
            return Ok(());
        }
        Err(RuntimeError::RouteRejected) => {
            let lane = accepted.stream().current_lane();
            accepted.reject(ResetReason::Refused, lane).await;
            return Ok(());
        }
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
    // Carrier admission was acknowledged before this target task was
    // submitted. Establish the logical OPEN by retaining and publishing one
    // nonzero receive grant across every currently live attachment. A later
    // attachment inherits the same cumulative grant.
    accepted
        .stream()
        .publish_max_data(reliable_stream_initial_advertised_window_bytes(
            accepted.stream().underlay,
            accepted.stream().lane,
            context.mux_limits,
        ));

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

fn response_retained_frontier_candidate(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
) -> bool {
    let frontier = send_stream.data_ack_frontier();
    if frontier >= send_stream.next_offset() || send_stream.reinjection_bytes() == 0 {
        return false;
    }
    // This is structural eligibility only. Queue fullness must not prevent
    // the actor from arming capacity before the exact helper observes it.
    send_stream
        .retransmission_frames_for_ranges(
            &[OffsetRange {
                start: frontier,
                end: send_stream.next_offset(),
            }],
            1,
        )
        .first()
        .is_some_and(|frame| path_stream.has_reinjection_path_for_frame(frame))
}

// Response reinjection deadlines
fn reliable_relay_current_data_ack_outstanding_bytes(
    _lane: TrafficClass,
    send_stream: &ReliableSendStream,
    _ack_frontier: u64,
) -> usize {
    // The retained send cache is the exact unique Product debt after all
    // scoped and positive-only DataACK releases. It applies in every lane.
    send_stream.reinjection_bytes()
}

fn reliable_relay_response_source_staging_headroom(
    _lane: TrafficClass,
    product_window_bytes: usize,
    retained_product_bytes: usize,
    queued_original_data_bytes: usize,
) -> usize {
    // `product_window_bytes` is already the chosen exact output tier's sum P,
    // bounded by the stream/reorder/repair resource envelope. Source bytes do
    // not acquire another authority before Data Sequence assignment.
    product_window_bytes
        .saturating_sub(retained_product_bytes.saturating_add(queued_original_data_bytes))
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
    fn recovery_anchor(
        self,
        data_ack_progress_at: Instant,
        tracked_first_live_attempt: bool,
    ) -> Instant {
        match self {
            // A tracked OriginalData flight has an exact immutable assignment
            // epoch. Advancing the connection frontier can expose another old
            // flight, but it does not make that flight newly sent. Once a
            // repair has been attempted, however, subsequent live-owner
            // retries require a full quiet recovery interval after the latest
            // Data ACK progress.
            Self::Tracked(candidate) if tracked_first_live_attempt => candidate.sent_at,
            Self::Tracked(candidate) => data_ack_progress_at.max(candidate.sent_at),
            // The fallback has no exact flight ledger. Keep the conservative
            // connection-progress bound so newly published untracked work is
            // never declared stalled before it could have been assigned.
            Self::Untracked { sent_at, .. } => data_ack_progress_at.max(sent_at),
        }
    }
}

impl ReliableRelayTailReinjectionTimer {
    fn arm_recovery_deadline(
        &mut self,
        candidate: ReliableRelayTailRecoveryCandidate,
        deadline: Instant,
    ) {
        if self.last_attempt_at.is_some() {
            // This deadline is derived from the immutable OriginalData
            // assignment. It is valid for the first repair only. After an
            // attempt or scan, leave the deadline unarmed so observe() can
            // enforce the current retry clock: latest Data ACK progress for a
            // live owner, or the existing attempt-paced failed-owner retry.
            self.candidate = Some(candidate);
            self.deadline = None;
            return;
        }
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
        // The first live-owner repair uses exact tracked assignment age. After
        // any repair attempt or empty eligibility scan, a repeat waits for a
        // full quiet recovery interval after the latest Data ACK progress.
        // Confirmed owner failure retains the pre-existing failover clock.
        let tracked_first_live_attempt = self.last_attempt_at.is_none() && !failed_original_ready;
        let recovery_progress_at =
            candidate.recovery_anchor(data_ack_progress_at, tracked_first_live_attempt);
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
        let first_gap = ranges
            .first()
            .filter(|range| range.start > 0)
            .map(|range| (0, range.start))
            .or_else(|| {
                ranges.windows(2).find_map(|pair| {
                    (pair[0].end < pair[1].start).then_some((pair[0].end, pair[1].start))
                })
            });
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

#[derive(Debug, Default)]
struct ServerAckPublicationState {
    generation: u64,
    published_generation: u64,
    pending: bool,
}

impl ServerAckPublicationState {
    fn record_status(&mut self, generation: u64, published: bool, pending: bool) {
        self.generation = generation;
        self.pending = pending;
        self.published_generation = if published { generation } else { 0 };
    }

    fn record_feedback(
        &mut self,
        publication: StreamFeedbackPublication,
        recv_stream: &mut ReliableRecvStream,
    ) {
        self.record_status(
            publication.ack_generation,
            publication.ack.published,
            publication.ack.pending,
        );
        if let Some(offset) = publication.max_data.published_offset {
            recv_stream.commit_max_data(offset);
        }
    }

    fn current_generation_is_fully_published(&self) -> bool {
        !self.pending && (self.generation == 0 || self.published_generation == self.generation)
    }
}

struct ServerPendingReceiveAck<'a> {
    progress: &'a mut ReliableRecvProgress,
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    limits: MuxLimits,
}

/// Feedback liveness does not belong to the target socket. Keep the same
/// partially completed write/flush/shutdown future while servicing its exact
/// route deadline and retained publication work. A DATA write that actually
/// parks first offers its admitted receipt, independently of target consumption.
/// Immediately completed writes retain their normal postwrite publication.
/// This consumes no more input and grants no credit before local delivery.
async fn service_server_feedback_while_pending<F, T>(
    operation: F,
    path_stream: &ReliablePathStream,
    recv_stream: &mut ReliableRecvStream,
    publication: &mut ServerAckPublicationState,
    peer_max_offset: u64,
    mut pending_receipt: Option<ServerPendingReceiveAck<'_>>,
) -> Result<T, RuntimeError>
where
    F: std::future::Future<Output = Result<T, RuntimeError>>,
{
    tokio::pin!(operation);
    // A separate watch cursor preserves the outer actor's own recovery wake.
    let mut updates = path_stream.subscribe_output_updates();
    loop {
        publication.record_feedback(
            path_stream.service_feedback_route(peer_max_offset),
            recv_stream,
        );
        publication.record_feedback(path_stream.feedback_status(), recv_stream);
        let mut notifies = path_stream.pending_ack_capacity_notifies(publication.generation);
        for notify in path_stream
            .pending_max_data_capacity_notifies()
            .into_iter()
            .chain(path_stream.feedback_route_capacity_notifies())
        {
            if !notifies.iter().any(|current| Arc::ptr_eq(current, &notify)) {
                notifies.push(notify);
            }
        }
        let mut capacity = arm_carrier_capacity_notifies(notifies);
        // Arm before the final retry, including recipients exposed by expiry.
        publication.record_feedback(
            path_stream.service_feedback_route(peer_max_offset),
            recv_stream,
        );
        let deadline = path_stream.feedback_route_deadline();
        let observe_first_pending = pending_receipt.is_some();
        tokio::select! {
            result = poll_fn(|cx| match operation.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(Some(result)),
                Poll::Pending if observe_first_pending => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }) => {
                if let Some(result) = result {
                    return result;
                }
                let receipt = pending_receipt.take().expect("first pending DATA write");
                enqueue_tcp_recv_progress(
                    path_stream, recv_stream, receipt.progress, publication,
                    receipt.path, receipt.lane, receipt.limits, true, false, false,
                );
                // The new generation can introduce blocked recipients. Re-arm
                // their exact capacity waits before parking, rather than using
                // the wait set collected before this receipt was materialized.
                continue;
            }
            _ = async { if let Some(wait) = capacity.as_mut() { wait.as_mut().await; } }, if capacity.is_some() => {}
            _ = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
                    None => std::future::pending().await,
                }
            } => {}
            changed = async {
                match updates.as_mut() {
                    Some(updates) => updates.changed().await,
                    None => std::future::pending().await,
                }
            }, if updates.is_some() => {
                changed.map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn enqueue_tcp_recv_progress(
    path_stream: &ReliablePathStream,
    recv_stream: &mut ReliableRecvStream,
    progress: &mut ReliableRecvProgress,
    ack_publication: &mut ServerAckPublicationState,
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
    force_ack: bool,
    publish_max_data: bool,
    force_max_data: bool,
) -> bool {
    let mut sent_any = false;
    let previous_ack_generation = progress.ack_generation();
    if progress.should_send_ack(recv_stream, path, lane, mux_limits, force_ack) {
        let generation = progress.ack_generation();
        if generation == previous_ack_generation && generation == ack_publication.generation {
            let publication = path_stream.retry_pending_ack();
            sent_any |= publication.ack.published;
            ack_publication.record_feedback(publication, recv_stream);
        } else {
            #[cfg(feature = "lab-diagnostics")]
            let ack_started = Instant::now();
            let cumulative_ack_frames = recv_stream.ack_frames();
            let ack_frames = recv_stream.take_ack_update();
            #[cfg(feature = "lab-diagnostics")]
            let cumulative_ack_frame_count = cumulative_ack_frames.len();
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record(
                "mux.ack_frames",
                ack_started.elapsed(),
                cumulative_ack_frame_count,
            );
            let publication =
                path_stream.publish_ack(generation, &ack_frames, cumulative_ack_frames);
            sent_any |= publication.ack.published;
            ack_publication.record_feedback(publication, recv_stream);
        }
    }
    if publish_max_data
        && progress.should_send_max_data(recv_stream, path, lane, mux_limits, force_max_data)
    {
        let advertised_window = reliable_stream_advertised_window_bytes(path, lane, mux_limits);
        let max_offset = recv_stream.max_data_offset_with_window(advertised_window);
        let publication = path_stream.publish_max_data(max_offset);
        if publication.max_data.published_offset.is_some() {
            sent_any = true;
        }
        ack_publication.record_feedback(publication, recv_stream);
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

/// Arms the carrier release edge before response credit is revalidated.
///
/// `Notify::notify_waiters` is not retained for a waiter created afterward.
/// The response actor therefore has to establish this observation before the
/// synchronous credit check that can race with a writer release.
fn arm_response_sender_capacity_wait(
    notifies: Vec<Arc<tokio::sync::Notify>>,
) -> Option<impl std::future::Future<Output = ()> + Send> {
    arm_carrier_capacity_notifies(notifies)
}

// Response reinjection output selection
fn reliable_failed_original_tail_reinjection_ready(
    recovery: &RangeRecoveryState,
    send_stream: &ReliableSendStream,
) -> bool {
    send_stream.reinjection_bytes() > 0 && !recovery.uncovered_ranges.is_empty()
}

/// Whether the exact lowest retained Product range has a structurally eligible
/// output other than every output already carrying that range.
///
/// This is owner-relative by construction. A draining owner is not counted as
/// a target, but it also cannot erase the one healthy output that can recover
/// its bytes. Queue, flight, rate, and pacing capacity remain apply-time gates.
fn has_distinct_response_reinjection_alternative(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    gaps: &[OffsetRange],
    ack_frontier: u64,
) -> bool {
    let preview = if !gaps.is_empty() {
        send_stream
            .retransmission_frames_for_ranges(gaps, 1)
            .into_iter()
            .next()
    } else if ack_frontier < send_stream.next_offset() {
        send_stream
            .retransmission_frames_for_ranges(
                &[OffsetRange {
                    start: ack_frontier,
                    end: send_stream.next_offset(),
                }],
                1,
            )
            .into_iter()
            .next()
    } else {
        None
    };
    preview.is_some_and(|frame| path_stream.has_reinjection_path_for_frame(&frame))
}

fn response_recovery_output_identity(
    candidate: ResponseDataAckRecoveryCandidate,
) -> ServerReinjectionOutputIdentity {
    ServerReinjectionOutputIdentity {
        key: candidate.key,
        incarnation: candidate.output_incarnation,
    }
}

#[allow(clippy::too_many_arguments)]
fn mark_response_path_staleness(
    staleness: &mut ReliableResponsePathStaleness,
    path_stream: &ReliablePathStream,
    candidates: &[ResponseDataAckRecoveryCandidate],
    data_ack_progress_outputs: &[ServerReinjectionOutputIdentity],
    lane: TrafficClass,
) -> bool {
    let observations = candidates
        .iter()
        .map(|candidate| {
            let identity = response_recovery_output_identity(*candidate);
            ReliablePathStalenessObservation::new(
                identity,
                path_stream.has_nonstale_reinjection_alternative(identity, lane),
                Some(candidate.key.underlay),
                path_stream.response_output_snapshot(identity, lane),
            )
        })
        .collect::<SmallVec<[_; 4]>>();
    let mut marked_stale = false;
    for stale in staleness.stale_paths(&observations, data_ack_progress_outputs) {
        if path_stream.mark_response_output_stale(stale, lane) {
            marked_stale = true;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "response_path_stale",
                format_args!(
                    "stream_id={} path_underlay={:?} path_id={} output_incarnation={}",
                    path_stream.stream_id.0,
                    stale.key.underlay,
                    stale.key.path_id.0,
                    stale.incarnation,
                ),
            );
        }
    }
    marked_stale
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

#[cfg(test)]
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

#[cfg(test)]
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
    send_stream: &ReliableSendStream,
    reinjection_frames: Vec<Frame>,
    cause: RelaySendCause,
) -> (Vec<Frame>, Option<u64>) {
    let mut accepted = Vec::with_capacity(reinjection_frames.len());
    for frame in reinjection_frames {
        // Select an eligible alternate before publishing reinjection work.
        // Live-tail recovery prefers a drained carrier but retains bounded
        // liveness when every alternate carries unrelated shared work.
        if response_sender
            .reinjection_path_snapshot_for_frame(path_stream, send_stream, &frame, cause)
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

#[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
#[derive(Debug, Default)]
struct LiveResponseRetainedFrontierEnqueueOutcome {
    queued: usize,
    pending: bool,
    frontier_limit: usize,
    service_limit: usize,
    blocked_frontier_offset: Option<u64>,
    blocked_for_carrier_capacity: bool,
    owner_fallback_deadline: Option<Instant>,
}

/// Preserves the existing operation's sizing, not its recovery authority.
/// Active sending used the frame-capped bulk quantum; final drain used the
/// smaller adaptive repair quantum. Both now share the same retained owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseRetainedFrontierPhase {
    Active,
    FinalDrain,
}

fn response_live_owner_recovery_interval_for_frame(
    path_stream: &ReliablePathStream,
    frame: &Frame,
    fallback_owner_snapshot: Option<PathSnapshot>,
) -> Duration {
    let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
        return reliable_relay_tail_reinjection_delay(fallback_owner_snapshot);
    };
    binding
        .original_flight_outputs_overlapping_frame(frame)
        .into_iter()
        .map(|(key, incarnation)| {
            reliable_data_retransmission_interval(
                Some(key.underlay),
                path_stream.response_output_snapshot(
                    ServerReinjectionOutputIdentity { key, incarnation },
                    path_stream.current_lane(),
                ),
            )
        })
        .max()
        .unwrap_or_else(|| reliable_relay_tail_reinjection_delay(fallback_owner_snapshot))
}

/// Enqueues the exact retained frontier while its original carrier is live.
///
/// Positive mux/cache ownership supplies this obligation during active sending
/// and final drain; neither source EOF nor a historical negative ACK horizon
/// supplies its authority. It shares the existing live-owner cause and successor
/// observations with ACK-gap recovery and requires a distinct response output.
/// Exact failed-owner recovery remains in the separate `failed_original_ranges`
/// path.
#[allow(clippy::too_many_arguments)]
fn enqueue_live_response_retained_frontier_reinjection(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    base_reinjection_limit: usize,
    phase: ResponseRetainedFrontierPhase,
    mux_limits: MuxLimits,
    observed_at: Instant,
) -> LiveResponseRetainedFrontierEnqueueOutcome {
    if base_reinjection_limit == 0
        || mux_limits.max_repair_bytes == 0
        || mux_limits.max_path_flight_bytes == 0
        || !response_retained_frontier_candidate(path_stream, send_stream)
    {
        return LiveResponseRetainedFrontierEnqueueOutcome::default();
    }
    let frontier = send_stream.data_ack_frontier();
    let frontier_end = send_stream.next_offset();
    let base_reinjection_limit = match phase {
        ResponseRetainedFrontierPhase::Active => {
            let lane = path_stream.current_lane();
            let owner_snapshot = path_stream.tail_reinjection_snapshot(
                frontier,
                lane,
                relay_lane_startup_chunk_bytes(lane, mux_limits)
                    .min(path_stream.max_frame_payload_bytes),
            );
            adaptive_reliable_relay_chunk_bytes_with_frame_limit(
                owner_snapshot,
                TrafficClass::Throughput,
                mux_limits,
                path_stream.max_frame_payload_bytes,
            )
            .max(adaptive_reliable_relay_reinjection_bytes(
                owner_snapshot,
                lane,
                mux_limits,
            ))
        }
        ResponseRetainedFrontierPhase::FinalDrain => base_reinjection_limit,
    };
    let selection_limit = reliable_critical_tail_reinjection_limit_bytes(
        base_reinjection_limit,
        send_stream.reinjection_bytes(),
        mux_limits,
    );
    let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
        return LiveResponseRetainedFrontierEnqueueOutcome {
            frontier_limit: selection_limit,
            blocked_frontier_offset: Some(frontier),
            ..LiveResponseRetainedFrontierEnqueueOutcome::default()
        };
    };
    let Some(owner_observation) = binding.observe_live_owner_retained_frontier(
        OffsetRange {
            start: frontier,
            end: frontier_end.min(frontier.saturating_add(selection_limit as u64)),
        },
        path_stream.current_lane(),
        observed_at,
    ) else {
        return LiveResponseRetainedFrontierEnqueueOutcome {
            frontier_limit: selection_limit,
            blocked_frontier_offset: Some(frontier),
            ..LiveResponseRetainedFrontierEnqueueOutcome::default()
        };
    };
    let owner_recovery_deadline = owner_observation.owner_fallback_deadline;
    // Wake ownership comes from the exact current head, not a cached aggregate
    // whose latest assignment could change when the source appends a suffix.
    let owner_outcome = LiveResponseRetainedFrontierEnqueueOutcome {
        owner_fallback_deadline: Some(owner_recovery_deadline),
        ..LiveResponseRetainedFrontierEnqueueOutcome::default()
    };
    let Some(scoring_frontier) = owner_observation.mature_frontier else {
        return LiveResponseRetainedFrontierEnqueueOutcome {
            frontier_limit: selection_limit,
            blocked_frontier_offset: Some(frontier),
            ..owner_outcome
        };
    };
    let scoring_range = scoring_frontier.range;
    let scoring_extent = flight_interval_bytes(scoring_range.start, scoring_range.end);
    let Some(scoring_frames) = exact_contiguous_retransmission_frames(send_stream, scoring_range)
    else {
        return LiveResponseRetainedFrontierEnqueueOutcome {
            frontier_limit: selection_limit,
            blocked_frontier_offset: Some(frontier),
            ..LiveResponseRetainedFrontierEnqueueOutcome::default()
        };
    };
    let (source_frames, cause, frontier_limit, service_limit, target_recovery_interval) =
        match phase {
            ResponseRetainedFrontierPhase::Active => {
                // Preserve active sending's existing per-cache-frame selection and
                // full-credit preflight. Unbound dispatch re-ranks and revalidates
                // each whole frame; a common target cannot replace that contract.
                (
                    scoring_frames,
                    RelaySendCause::TailReinjection,
                    scoring_extent,
                    scoring_extent,
                    None,
                )
            }
            ResponseRetainedFrontierPhase::FinalDrain => {
                let preview = scoring_frames
                    .first()
                    .expect("non-empty exact cache prefix");
                let Some((target, target_snapshot)) = response_sender
                    .reinjection_frontier_preview_target_for_extent(
                        path_stream,
                        send_stream,
                        preview,
                        RelaySendCause::TailReinjection,
                        scoring_extent,
                    )
                else {
                    return LiveResponseRetainedFrontierEnqueueOutcome {
                        frontier_limit: selection_limit,
                        blocked_frontier_offset: reliable_stream_frame_extent(preview)
                            .map(|(offset, _, _)| offset),
                        blocked_for_carrier_capacity: true,
                        ..owner_outcome
                    };
                };
                let target_recovery_interval = reliable_data_retransmission_interval(
                    Some(target_snapshot.underlay),
                    Some(target_snapshot),
                );
                if scoring_frontier.avoid.contains(&target) {
                    return LiveResponseRetainedFrontierEnqueueOutcome {
                        frontier_limit: selection_limit,
                        blocked_frontier_offset: Some(frontier),
                        ..owner_outcome
                    };
                }
                let target_quantum = adaptive_reliable_relay_reinjection_bytes(
                    Some(target_snapshot),
                    path_stream.current_lane(),
                    mux_limits,
                );
                let frontier_limit = reliable_live_frontier_reinjection_limit_bytes(
                    target_quantum,
                    base_reinjection_limit,
                    scoring_extent,
                    send_stream.reinjection_bytes(),
                    mux_limits,
                );
                let target_service_limit = response_sender.reinjection_service_limit_for_target(
                    path_stream,
                    send_stream,
                    target,
                    target_snapshot,
                    false,
                    mux_limits,
                );
                let service_limit = reliable_live_gap_reinjection_authority(
                    target_service_limit,
                    frontier_limit,
                    true,
                );
                if service_limit == 0 {
                    return LiveResponseRetainedFrontierEnqueueOutcome {
                        frontier_limit,
                        service_limit,
                        blocked_frontier_offset: Some(frontier),
                        blocked_for_carrier_capacity: target_service_limit == 0,
                        ..owner_outcome
                    };
                }
                let applied_extent = service_limit.min(scoring_extent);
                let apply_range = OffsetRange {
                    start: frontier,
                    end: frontier.saturating_add(applied_extent as u64),
                };
                let Some(source_frames) =
                    exact_contiguous_retransmission_frames(send_stream, apply_range)
                else {
                    return LiveResponseRetainedFrontierEnqueueOutcome {
                        frontier_limit,
                        service_limit,
                        blocked_frontier_offset: Some(frontier),
                        ..owner_outcome
                    };
                };
                let source_frames =
                    preserve_reinjection_frontier_quantum(source_frames, frontier_limit);
                let cause =
                    RelaySendCause::response_completion_tail_reinjection(target, target_snapshot);
                (
                    source_frames,
                    cause,
                    frontier_limit,
                    service_limit,
                    Some(target_recovery_interval),
                )
            }
        };
    let (reinjection_frames, blocked_frontier_offset) =
        prefix_live_reinjection_frames_with_carrier_credit(
            response_sender,
            path_stream,
            send_stream,
            source_frames,
            cause,
        );
    let mut queued = 0usize;
    let mut pending = false;
    let mut accepted_recovery_interval = None::<Duration>;
    for frame in reinjection_frames {
        if response_sender.has_queued_reinjection_overlap(&frame)
            || path_stream.has_recent_reinjection_overlap(&frame)
        {
            pending = true;
            break;
        }
        let recovery_interval = target_recovery_interval.unwrap_or_else(|| {
            response_live_owner_recovery_interval_for_frame(path_stream, &frame, None)
        });
        response_sender.enqueue_reinjection_frame_with_cause_and_priority(frame, cause, true);
        queued = queued.saturating_add(1);
        accepted_recovery_interval = Some(include_live_owner_recovery_interval(
            accepted_recovery_interval,
            recovery_interval,
        ));
    }
    if let Some(recovery_interval) = accepted_recovery_interval {
        let accepted_at = Instant::now();
        if accepted_at >= owner_recovery_deadline
            && response_sender.live_owner_frontier_floor_ready(accepted_at)
        {
            response_sender
                .record_live_owner_frontier_floor_attempt(accepted_at, recovery_interval);
        }
    }
    LiveResponseRetainedFrontierEnqueueOutcome {
        queued,
        pending,
        frontier_limit,
        service_limit,
        owner_fallback_deadline: Some(owner_recovery_deadline),
        blocked_frontier_offset,
        blocked_for_carrier_capacity: queued == 0 && blocked_frontier_offset.is_some(),
    }
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

#[derive(Debug)]
#[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
struct ServerDataAckReinjectionOutcome {
    /// Epoch sampled after the current target snapshot and completion score.
    /// The caller reuses it when deciding whether the resulting timer is due.
    observed_at: Instant,
    frame_count: usize,
    queued: usize,
    persistent_ready: bool,
    has_multipath_alternative: bool,
    has_measured_target: bool,
    target_service_exhausted: bool,
    base_limit: usize,
    service_limit: usize,
    tail_recovery_candidate: Option<ResponseDataAckRecoveryCandidate>,
    tail_recovery_deadline: Option<Instant>,
}

fn server_ack_gap_capacity_wait_arm_active(
    authoritative_gap: bool,
    has_multipath_alternative: bool,
) -> bool {
    authoritative_gap && has_multipath_alternative
}

fn server_ack_gap_missing_target_wait_active(
    authoritative_gap: bool,
    has_multipath_alternative: bool,
    has_measured_target: bool,
) -> bool {
    authoritative_gap && has_multipath_alternative && !has_measured_target
}

#[cfg(test)]
fn server_ack_gap_timer_deadline(
    deadline: Option<Instant>,
    observed_at: Instant,
) -> Option<tokio::time::Instant> {
    // Evaluation owns this epoch. Sampling the clock again here can cross a
    // not-yet-due deadline after evaluation and discard its only wakeup.
    deadline
        .filter(|deadline| *deadline > observed_at)
        .map(tokio::time::Instant::from_std)
}

fn server_live_owner_recovery_wake(
    cause_deadline: Option<tokio::time::Instant>,
    epoch_deadline: Option<Instant>,
    observed_at: Instant,
) -> LiveOwnerRecoveryWake {
    live_owner_recovery_wake(
        cause_deadline.map(tokio::time::Instant::into_std),
        epoch_deadline,
        observed_at,
    )
}

#[cfg(feature = "lab-diagnostics")]
fn lab_server_response_recovery_wake(
    stream_id: StreamId,
    wake: &'static str,
    ack_frontier: u64,
    sent_offset: u64,
    consumed_deadline: Option<Instant>,
    successor_deadline: Option<Instant>,
    observed_at: Instant,
) {
    let deadline_late_us = consumed_deadline
        .map(|deadline| {
            observed_at
                .saturating_duration_since(deadline)
                .as_micros()
                .to_string()
        })
        .unwrap_or_else(|| "none".to_string());
    let successor_deadline_in_us = successor_deadline
        .map(|deadline| {
            deadline
                .saturating_duration_since(observed_at)
                .as_micros()
                .to_string()
        })
        .unwrap_or_else(|| "none".to_string());
    lab_diagnostic(
        "server_response_recovery_wake",
        format_args!(
            "stream_id={} wake={} ack_frontier={} sent_offset={} deadline_late_us={} successor_deadline_in_us={}",
            stream_id.0,
            wake,
            ack_frontier,
            sent_offset,
            deadline_late_us,
            successor_deadline_in_us,
        ),
    );
}

/// Evaluates the retained response Data ACK state as one recovery lifecycle.
///
/// ACK receipt, timer expiry, carrier-capacity release, and output-model
/// updates all return through the relay loop and call this function. Therefore
/// a measured alternate that becomes eligible after the ACK event cannot lose
/// the already authoritative gap, while mutable metrics cannot postpone an
/// armed deadline for that same lowest missing frontier.
#[allow(clippy::too_many_arguments)]
fn evaluate_server_data_ack_reinjection(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    progress: &mut ReliableAckGapReinjectionProgress,
    authoritative_ack: &AuthoritativeStreamAckSnapshot,
    ack_frontier: u64,
    send_path_snapshot: Option<PathSnapshot>,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] stream_id: StreamId,
) -> ServerDataAckReinjectionOutcome {
    let ranges = authoritative_ack.gaps();
    let base_limit =
        adaptive_reliable_relay_reinjection_bytes(send_path_snapshot, relay_lane, mux_limits);
    let exposes_gap = stream_ack_ranges_expose_authoritative_gap(ranges);
    let has_multipath_alternative = exposes_gap
        && has_distinct_response_reinjection_alternative(
            path_stream,
            send_stream,
            ranges,
            ack_frontier,
        );
    if base_limit == 0 || mux_limits.max_repair_bytes == 0 || mux_limits.max_path_flight_bytes == 0
    {
        return ServerDataAckReinjectionOutcome {
            observed_at: Instant::now(),
            frame_count: 0,
            queued: 0,
            persistent_ready: false,
            has_multipath_alternative,
            has_measured_target: false,
            target_service_exhausted: false,
            base_limit,
            service_limit: 0,
            tail_recovery_candidate: None,
            tail_recovery_deadline: None,
        };
    }
    let original_flight = authoritative_ack
        .first_gap()
        .and_then(|gap| path_stream.data_ack_recovery_candidate(gap.start));
    let observation = (exposes_gap && has_multipath_alternative)
        .then(|| {
            response_sender.ack_gap_reinjection_path_snapshot(
                path_stream,
                send_stream,
                ranges,
                base_limit,
            )
        })
        .flatten();
    // The evaluation epoch follows the complete immutable target batch. It
    // must not precede the evidence whose completion race it serializes.
    let observed_at = Instant::now();
    let target = observation.and_then(|observation| observation.target);
    let target_reinjection_quantum = target.map_or(base_limit, |target| {
        adaptive_reliable_relay_reinjection_bytes(Some(target.snapshot), relay_lane, mux_limits)
    });
    let frontier_extent = first_proven_ack_gap(ranges)
        .map_or(0, |(start, end)| flight_interval_bytes(start, end))
        .min(
            observation
                .map(|observation| observation.uniform_frontier_extent_bytes)
                .unwrap_or(0),
        );
    let frontier_limit = reliable_live_frontier_reinjection_limit_bytes(
        target_reinjection_quantum,
        base_limit,
        frontier_extent,
        send_stream.reinjection_bytes(),
        mux_limits,
    );
    if target.is_some() && frontier_limit == 0 {
        return ServerDataAckReinjectionOutcome {
            observed_at,
            frame_count: 0,
            queued: 0,
            persistent_ready: false,
            has_multipath_alternative,
            has_measured_target: true,
            target_service_exhausted: false,
            base_limit,
            service_limit: 0,
            tail_recovery_candidate: original_flight,
            tail_recovery_deadline: None,
        };
    }
    // Completion starts from the current target observation, not from a
    // caller epoch sampled before target selection and lock acquisition.
    let has_measured_target = target.is_some();

    // Silence and a later ACK are distinct RFC authorities. The tail timer
    // retains the original owner's RTO/PTO fallback, while this lifecycle uses
    // the response-direction Data-ACK time threshold.
    let observed_gap_timing = observation
        .map(|observation| observation.owner_recovery_timing)
        .or_else(|| {
            let original = original_flight?;
            reliable_data_ack_gap_timing(
                Some(original.sent_at),
                Some(original.key.underlay),
                path_stream.response_output_snapshot(
                    ServerReinjectionOutputIdentity {
                        key: original.key,
                        incarnation: original.output_incarnation,
                    },
                    relay_lane,
                ),
            )
        });
    let recovery_deadline =
        observation.map(|observation| observation.owner_recovery_timing.fallback_at);
    let candidate_gap_deadline = progress.observe_recovery_timing(
        ranges,
        has_multipath_alternative,
        observed_gap_timing,
        target.map(|target| target.completion),
        observation.and_then(|observation| observation.owner_completion),
        observed_at,
    );
    let measured_ready = candidate_gap_deadline.is_some_and(|deadline| observed_at >= deadline);
    let persistent_ready =
        progress.reinjection_ready(ranges, has_multipath_alternative, measured_ready)
            && target.is_some();
    let Some(target) = target.filter(|_| persistent_ready) else {
        return ServerDataAckReinjectionOutcome {
            observed_at,
            frame_count: 0,
            queued: 0,
            persistent_ready,
            has_multipath_alternative,
            has_measured_target,
            target_service_exhausted: false,
            base_limit,
            service_limit: 0,
            tail_recovery_candidate: original_flight,
            tail_recovery_deadline: recovery_deadline,
        };
    };

    // An explicitly proven persistent gap establishes missing Product order, not failure of
    // the live native-reliable owner. The selected target's Product service
    // window is only a capacity ceiling; the ranked frontier below is the
    // publication authority. Stable-slot, retained-range, queue, and native
    // bounds remain hard. Exact terminal failure uses its separate
    // cause-bounded critical path below.
    let target_service_limit = response_sender.reinjection_service_limit_for_target(
        path_stream,
        send_stream,
        target.identity,
        target.snapshot,
        false,
        mux_limits,
    );
    let owner_recovery_deadline = progress
        .original_owner_recovery_deadline()
        .expect("persistent target retains its exact owner fallback");
    let service_limit = reliable_live_gap_reinjection_authority(
        target_service_limit,
        frontier_limit,
        persistent_ready,
    );
    let frames = first_proven_ack_gap(ranges)
        .and_then(|(frontier, _)| {
            let applied_extent = service_limit.min(
                observation
                    .expect("persistent target has a live-owner observation")
                    .uniform_frontier_extent_bytes,
            );
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
    #[cfg(feature = "lab-diagnostics")]
    let recovery_extent = frames
        .first()
        .and_then(reliable_stream_frame_extent)
        .map(|(start, end, _)| (start, end));
    let frame_count = frames.len();
    let cause =
        RelaySendCause::persistent_server_ack_gap_reinjection(target.identity, target.snapshot);
    let retry_after = reliable_data_retransmission_interval(
        Some(target.snapshot.underlay),
        Some(target.snapshot),
    );
    let mut queued = 0usize;
    for frame in frames {
        let accepted = if response_sender.has_queued_reinjection_overlap(&frame)
            || path_stream.has_recent_reinjection_overlap(&frame)
        {
            false
        } else {
            response_sender.enqueue_reinjection_frame_with_cause_and_priority(frame, cause, true);
            true
        };
        if accepted {
            queued = queued.saturating_add(1);
        }
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "reinjection",
            format_args!(
                "stream_id={} cause=persistent_ack_gap queued={}",
                stream_id.0, accepted,
            ),
        );
        if !accepted {
            break;
        }
    }
    if queued > 0 {
        let accepted_at = Instant::now();
        if accepted_at >= owner_recovery_deadline
            && response_sender.live_owner_frontier_floor_ready(accepted_at)
        {
            response_sender.record_live_owner_frontier_floor_attempt(accepted_at, retry_after);
        }
    }
    #[cfg(feature = "lab-diagnostics")]
    if queued > 0 {
        let (gap_start, gap_end) = recovery_extent.unwrap_or((ack_frontier, ack_frontier));
        let (owner_underlay, owner_path_id, owner_incarnation, owner_age_us) = original_flight
            .map(|owner| {
                (
                    format!("{:?}", owner.key.underlay),
                    owner.key.path_id.0.to_string(),
                    owner.output_incarnation.to_string(),
                    observed_at
                        .saturating_duration_since(owner.sent_at)
                        .as_micros()
                        .to_string(),
                )
            })
            .unwrap_or_else(|| {
                (
                    "none".to_string(),
                    "none".to_string(),
                    "none".to_string(),
                    "none".to_string(),
                )
            });
        let deadline_late_us = candidate_gap_deadline
            .map(|deadline| observed_at.saturating_duration_since(deadline).as_micros())
            .unwrap_or(0);
        lab_diagnostic(
            "server_data_ack_recovery",
            format_args!(
                "stream_id={} ack_frontier={} ranges={} gap_start={} gap_end={} owner_underlay={} owner_path_id={} owner_incarnation={} owner_age_us={} target_underlay={:?} target_path_id={} target_incarnation={} target_eta_us={} deadline_late_us={} frame_count={} queued={} base_limit={} service_limit={}",
                stream_id.0,
                ack_frontier,
                ranges.len(),
                gap_start,
                gap_end,
                owner_underlay,
                owner_path_id,
                owner_incarnation,
                owner_age_us,
                target.snapshot.underlay,
                target.identity.key.path_id.0,
                target.identity.incarnation,
                target.completion.as_micros(),
                deadline_late_us,
                frame_count,
                queued,
                base_limit,
                service_limit,
            ),
        );
    }
    ServerDataAckReinjectionOutcome {
        observed_at,
        frame_count,
        queued,
        persistent_ready,
        has_multipath_alternative,
        has_measured_target,
        target_service_exhausted: persistent_ready
            && target_service_limit == 0
            && send_stream.reinjection_bytes() > 0,
        base_limit,
        service_limit,
        tail_recovery_candidate: original_flight,
        tail_recovery_deadline: recovery_deadline,
    }
}

// Response reinjection queue and dispatch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailReinjectionEnqueueOutcome {
    queued: usize,
    pending: bool,
    blocked_for_carrier_capacity: bool,
}

#[cfg(test)]
impl TailReinjectionEnqueueOutcome {
    fn has_reinjection_attempt(self) -> bool {
        self.queued > 0 || self.pending
    }
}

#[allow(clippy::too_many_arguments)]
fn enqueue_reliable_tail_reinjection_with_ack_gaps(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    failed_original_ranges: &[OffsetRange],
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] stream_id: StreamId,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    tail_reinjection_path_snapshot: Option<PathSnapshot>,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    performance: MppPerformanceConfig,
    max_frame_payload_bytes: usize,
    live_ack_gap_owner_recovery_deadline: Option<Instant>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    last_send_ack_frontier: u64,
) -> TailReinjectionEnqueueOutcome {
    let observed_at = Instant::now();
    let live_ack_gap_owner_recovery_ready =
        live_ack_gap_owner_recovery_deadline.is_some_and(|deadline| observed_at >= deadline);
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
    if !failed_original_ranges.is_empty() {
        let reinjection_path = send_stream
            .retransmission_frames_for_ranges(failed_original_ranges, 1)
            .into_iter()
            .next()
            .and_then(|preview| {
                response_sender.reinjection_path_snapshot_for_frame(
                    path_stream,
                    send_stream,
                    &preview,
                    RelaySendCause::PathFailureReinjection,
                )
            });
        let failed_original_limit = reinjection_path.map_or(0, |(identity, snapshot)| {
            response_sender.reinjection_service_limit_for_target(
                path_stream,
                send_stream,
                identity,
                snapshot,
                false,
                mux_limits,
            )
        });
        reinjection_frames = send_stream
            .retransmission_frames_for_ranges(failed_original_ranges, failed_original_limit);
        if !reinjection_frames.is_empty() {
            critical_tail_reinjection = true;
            reinjection_limit = failed_original_limit;
            reinjection_kind = "failed_original_tail_reinjection";
            reinjection_cause = RelaySendCause::PathFailureReinjection;
        }
    }
    if reinjection_frames.is_empty() && !last_send_ack_ranges.is_empty() {
        if live_ack_gap_owner_recovery_ready
            && stream_ack_ranges_expose_authoritative_gap(last_send_ack_ranges)
            && has_distinct_response_reinjection_alternative(
                path_stream,
                send_stream,
                last_send_ack_ranges,
                last_send_ack_frontier,
            )
        {
            // This tail clock authorizes only the exact live-owner frontier.
            // Persistent gap service is driven separately from retained ACK
            // evidence and a measured target, so this generic clock cannot
            // inherit that target's larger service window. Both observations
            // contribute to the sender's shared successor observation below.
            let frontier_extent = first_proven_ack_gap(last_send_ack_ranges)
                .map_or(0, |(start, end)| flight_interval_bytes(start, end));
            let frontier_limit = reliable_live_frontier_reinjection_limit_bytes(
                base_reinjection_limit,
                base_reinjection_limit,
                frontier_extent,
                send_stream.reinjection_bytes(),
                mux_limits,
            );
            let gap_limit = reliable_live_gap_reinjection_authority(
                frontier_limit,
                frontier_limit,
                live_ack_gap_owner_recovery_ready,
            );
            let gap_source_frames = stream_ack_gap_frontier_reinjection_frames_normalized(
                send_stream,
                last_send_ack_ranges,
                gap_limit,
                true,
                true,
            );
            let (gap_frames, gap_blocked_offset) =
                prefix_live_reinjection_frames_with_carrier_credit(
                    response_sender,
                    path_stream,
                    send_stream,
                    gap_source_frames,
                    RelaySendCause::AckGapReinjection,
                );
            if !gap_frames.is_empty() {
                reinjection_limit = gap_limit;
                reinjection_frames = gap_frames;
                blocked_frontier_offset = gap_blocked_offset;
                reinjection_kind = "ack_gap_retransmission";
            } else if blocked_frontier_offset.is_none() {
                blocked_frontier_offset = gap_blocked_offset;
            }
        }
        if reinjection_frames.is_empty() {
            let tail_limit = reliable_critical_tail_reinjection_limit_bytes(
                base_reinjection_limit,
                send_stream.reinjection_bytes(),
                mux_limits,
            );
            let tail_source_frames =
                send_stream.retransmission_frames_for_ranges(last_send_ack_ranges, tail_limit);
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
            "stream_id={} lane={:?} ack_frontier={} sent_offset={} reinjection_bytes={} reinjection_frames={} blocked_frontier_offset={:?} base_reinjection_limit={} reinjection_limit={} optional_reinjection_budget_percent={} reinjection_kind={}",
            stream_id.0,
            relay_lane,
            last_send_ack_frontier,
            send_stream.next_offset(),
            send_stream.reinjection_bytes(),
            reinjection_frames.len(),
            blocked_frontier_offset,
            base_reinjection_limit,
            reinjection_limit,
            performance.optional_reinjection_budget_percent,
            reinjection_kind,
        ),
    );
    let mut reinjection_count = 0usize;
    let mut reinjection_pending = false;
    let mut accepted_live_owner_recovery_interval = None::<Duration>;
    for frame in reinjection_frames {
        if response_sender.has_queued_reinjection_overlap(&frame)
            || path_stream.has_recent_reinjection_overlap(&frame)
        {
            reinjection_pending = true;
            if matches!(reinjection_cause, RelaySendCause::PathFailureReinjection) {
                // Failed-owner authority may describe disjoint terminal
                // ranges. An overlap in one range does not resolve later
                // ranges, so retain the pre-existing exhaustive scan here.
                continue;
            }
            // Live-owner work is one exact lowest-prefix transaction. It may
            // not skip an overlapped byte and publish a later suffix.
            break;
        }
        let recovery_interval = matches!(reinjection_cause, RelaySendCause::AckGapReinjection)
            .then(|| {
                response_live_owner_recovery_interval_for_frame(
                    path_stream,
                    &frame,
                    tail_reinjection_path_snapshot,
                )
            });
        if critical_tail_reinjection {
            response_sender.enqueue_critical_reinjection_frame_with_cause(frame, reinjection_cause);
        } else {
            response_sender.enqueue_reinjection_frame_with_cause_and_priority(
                frame,
                reinjection_cause,
                true,
            );
        }
        reinjection_count = reinjection_count.saturating_add(1);
        if let Some(recovery_interval) = recovery_interval {
            accepted_live_owner_recovery_interval = Some(include_live_owner_recovery_interval(
                accepted_live_owner_recovery_interval,
                recovery_interval,
            ));
        }
    }
    if reinjection_count > 0 && matches!(reinjection_cause, RelaySendCause::AckGapReinjection) {
        let accepted_at = Instant::now();
        if live_ack_gap_owner_recovery_deadline.is_some_and(|deadline| accepted_at >= deadline)
            && response_sender.live_owner_frontier_floor_ready(accepted_at)
        {
            response_sender.record_live_owner_frontier_floor_attempt(
                accepted_at,
                accepted_live_owner_recovery_interval.unwrap_or_else(|| {
                    reliable_relay_tail_reinjection_delay(tail_reinjection_path_snapshot)
                }),
            );
        }
    }
    TailReinjectionEnqueueOutcome {
        queued: reinjection_count,
        pending: reinjection_pending,
        blocked_for_carrier_capacity: reinjection_count == 0
            && !reinjection_pending
            && blocked_frontier_offset.is_some(),
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
    tail_reinjection_path_snapshot: Option<PathSnapshot>,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
    max_frame_payload_bytes: usize,
    last_send_ack_frontier: u64,
) -> TailReinjectionEnqueueOutcome {
    let failed_original_recovery = path_stream.failed_original_recovery_state();
    enqueue_reliable_tail_reinjection_with_ack_gaps(
        response_sender,
        path_stream,
        &failed_original_recovery.uncovered_ranges,
        stream_id,
        send_stream,
        last_send_ack_ranges,
        tail_reinjection_path_snapshot,
        relay_lane,
        mux_limits,
        performance,
        max_frame_payload_bytes,
        Some(Instant::now()),
        last_send_ack_frontier,
    )
}

fn server_data_ack_frontier_state(
    last_send_ack: &AuthoritativeStreamAckSnapshot,
    send_stream: &ReliableSendStream,
) -> ReliableDataAckFrontierState {
    ReliableDataAckFrontierState::from_authoritative_gap(
        last_send_ack
            .gap_at(send_stream.data_ack_frontier())
            .is_some(),
    )
}

#[allow(clippy::too_many_arguments)]
fn drain_server_response_sender_ready(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    data_ack_outstanding_bytes: usize,
    frontier_state: ReliableDataAckFrontierState,
    send_stream: &mut ReliableSendStream,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    sender_dispatch_byte_budget: usize,
    sender_dispatch_item_budget: usize,
    tail_copy_wake_at: &mut Option<Instant>,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] session_id: SessionId,
) -> Result<bool, RuntimeError> {
    let mut dispatched_items = 0usize;
    let mut dispatched_payload_bytes = 0usize;
    let mut blocked_by_carrier = false;

    while response_sender.queued_nondata_ready()
        && dispatched_items < sender_dispatch_item_budget
        && (dispatched_payload_bytes < sender_dispatch_byte_budget || dispatched_items == 0)
    {
        let dispatch = match response_sender.dispatch_next_nondata_at_frontier(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            data_ack_outstanding_bytes,
            frontier_state,
        ) {
            Ok(dispatch) => dispatch,
            Err(RuntimeError::SenderServiceBlocked) => {
                blocked_by_carrier = true;
                break;
            }
            Err(err) => return Err(err),
        };
        dispatched_items = dispatched_items.saturating_add(1);
        if let Some(deadline) = dispatch.accepted_copy_deadline {
            retain_accepted_copy_wake(tail_copy_wake_at, deadline);
        }
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
            // These are queue-accounting control units, not source payload.
            // Original bytes and their clocks come only from actual claims.
            debug_assert_ne!(dispatch.lane, ReliableWorkClass::Data);
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

    Ok(blocked_by_carrier)
}

/// Observe actual writer commitment once, using its producer clock even when
/// target I/O delayed this actor. Opposite-direction delivery may already have
/// recorded a newer event, so neither endpoint of the observed span regresses.
fn observe_server_response_claims(
    product: &ResponseProductState,
    observed_offset: &mut u64,
    stats: &mut PathDeliveryStats,
) -> bool {
    let offset = product.send_stream.next_offset();
    if offset <= *observed_offset {
        return false;
    }
    let claimed_at = product
        .prepared
        .last_claimed_at
        .expect("committed response source has a producer timestamp");
    let first_claimed_at = product
        .prepared
        .first_claimed_at
        .expect("committed response source retains its first producer timestamp");
    stats.payload_bytes = stats
        .payload_bytes
        .saturating_add(offset - *observed_offset);
    stats.first_payload_at = Some(
        stats
            .first_payload_at
            .map_or(first_claimed_at, |at| at.min(first_claimed_at)),
    );
    stats.last_payload_at = Some(
        stats
            .last_payload_at
            .map_or(claimed_at, |at| at.max(claimed_at)),
    );
    *observed_offset = offset;
    true
}

#[allow(clippy::too_many_arguments)]
fn drain_shared_server_response_sender_ready(
    owner: &SharedResponseProduct,
    path_stream: &ReliablePathStream,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    sender_dispatch_byte_budget: usize,
    sender_dispatch_item_budget: usize,
    tail_copy_wake_at: &mut Option<Instant>,
    session_id: SessionId,
) -> Result<bool, RuntimeError> {
    let mut product = owner.lock();
    let previous_queue_bytes = product.sender.bytes();
    let ResponseProductState {
        sender,
        send_stream,
        last_send_ack,
        ..
    } = &mut *product;
    let outstanding = reliable_relay_current_data_ack_outstanding_bytes(
        relay_lane,
        send_stream,
        send_stream.data_ack_frontier(),
    );
    let result = drain_server_response_sender_ready(
        sender,
        path_stream,
        outstanding,
        server_data_ack_frontier_state(last_send_ack, send_stream),
        send_stream,
        relay_lane,
        mux_limits,
        sender_dispatch_byte_budget,
        sender_dispatch_item_budget,
        tail_copy_wake_at,
        session_id,
    );
    if product.sender.bytes() != previous_queue_bytes {
        product.prepared.work_changed.notify_waiters();
    }
    result
}

fn refresh_server_response_flow_demand(
    response_flow_demand: &mut ReliableRelayFlowDemandTracker,
    response_sender: &ServerResponseSenderService,
    send_stream: &ReliableSendStream,
    classifier_path: Option<PathSnapshot>,
    mux_limits: MuxLimits,
) -> ReliableRelayFlowDecision {
    let queued_unique_original_bytes = response_sender.data_bytes();
    let response_observed_bytes = send_stream
        .next_offset()
        .saturating_add(queued_unique_original_bytes as u64);
    response_flow_demand.refresh(
        ReliableRelayFlowSignals::new(response_observed_bytes).with_product_work(
            queued_unique_original_bytes,
            send_stream.reinjection_bytes(),
        ),
        ReliableRelayFlowPathEvidence::measured(classifier_path),
        mux_limits,
    )
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
    let activity = ProductFlowActivity::new();
    let local = ProductFlowActivityIo::new(local, activity.clone());
    let result = {
        let body = relay_reliable_stream_body(
            local,
            &mut path_stream,
            context,
            session_id,
            session_send_buffer,
            &mut close,
        );
        let idle = activity.wait_until_idle(context.flow_idle_timeout);
        tokio::pin!(body);
        tokio::pin!(idle);
        tokio::select! {
            result = &mut body => result,
            () = &mut idle => Err(RuntimeError::ProductIdleTimeout),
        }
    };
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
        Err(RuntimeError::ProductIdleTimeout) => {
            path_stream.retire_with_reset(ResetReason::TimedOut);
        }
        _ if close.sent => path_stream.close_ordered(close.lane).await,
        _ => path_stream.close().await,
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("stream_close");
    #[cfg(feature = "lab-diagnostics")]
    lab_assert_server_sender_service_balanced(session_id.0, stream_id.0);
    match result {
        Err(RuntimeError::ProductIdleTimeout) => Ok(PathDeliveryStats::default()),
        result => result,
    }
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
    let mut request_ack_publication = ServerAckPublicationState::default();
    let mut request_ack_capacity_wait = None;
    let mut request_ack_capacity_wait_generation = 0_u64;
    let mut ack_gap_reinjection = ReliableAckGapReinjectionProgress::default();
    let mut response_path_staleness = ReliableResponsePathStaleness::default();
    let mut response_data_ack_progress_outputs = Vec::<ServerReinjectionOutputIdentity>::new();
    let mut response_path_staleness_dirty = true;
    let mut response_recovery_dirty = true;
    let mut response_range_recovery_deadline = None::<Instant>;
    let mut tail_copy_wake_at = None::<Instant>;
    let mut response_recovery_capacity_blocked = false;
    let mut last_recv_progress_sent_at = Instant::now();
    let mut last_send_ack_progress_at = Instant::now();
    let mut last_send_ack_frontier = 0_u64;
    let last_send_ack = AuthoritativeStreamAckSnapshot::default();
    let mut tail_reinjection_timer = ReliableRelayTailReinjectionTimer::default();
    let mut request_flow_demand =
        ReliableRelayFlowDemandTracker::with_initial_lane(path_stream.current_lane());
    let mut response_flow_demand =
        ReliableRelayFlowDemandTracker::with_initial_lane(path_stream.current_lane());
    let mut output_updates = path_stream.subscribe_output_updates();
    let mut observed_output_membership_generation = path_stream.output_membership_generation();
    let mut multipath_reinjection_alternative_available =
        path_stream.has_multipath_reinjection_alternative();
    let response_sender =
        ServerResponseSenderService::new_with_performance(session_id, stream_id, performance);
    let mut observed_response_recovery_generation =
        response_sender.stale_response_recovery_generation();
    let binding = match &path_stream.output {
        ReliablePathStreamOutput::Switchable(binding) => binding.clone(),
        ReliablePathStreamOutput::Fixed(_) => {
            return Err(RuntimeError::Protocol(
                "server response relay requires a response binding",
            ));
        }
    };
    let response_product = SharedResponseProduct::new(
        ResponseProductState {
            sender: response_sender,
            send_stream,
            last_send_ack,
            prepared: PreparedResponseSource::new(path_stream.current_lane(), chunk_size),
        },
        binding,
    );
    let _response_actor_lifetime = response_product.actor_lifetime();
    let mut observed_response_claimed_offset = 0;
    let mut deferred_path_frame = None::<Result<Frame, RuntimeError>>;
    let mut ready_path_data = super::io::ReadyStreamDataBatch::new();
    let mut send_buffer_reservation = session_send_buffer.stream_reservation();
    let mut send_buffer_updates = session_send_buffer.subscribe();
    let mut response_sender_retry_at: Option<tokio::time::Instant> = None;
    let mut response_requalification_capacity_wait = None;
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
        let previous_request_lane = request_flow_demand.current_lane();
        let request_classifier_path =
            path_stream.request_feedback_path_snapshot(previous_request_lane);
        let request_demand_update = request_flow_demand.refresh(
            ReliableRelayFlowSignals::new(recv_stream.next_offset())
                .with_product_work(0, recv_stream.reorder_bytes()),
            ReliableRelayFlowPathEvidence::timing_only(request_classifier_path),
            mux_limits,
        );
        let request_lane = request_demand_update.lane;
        #[cfg(feature = "lab-diagnostics")]
        if request_lane != previous_request_lane {
            lab_diagnostic(
                "server_request_lane_changed",
                format_args!(
                    "stream_id={} previous={:?} lane={:?} received_offset={} reorder_bytes={} byte_proven={} rate_proven={}",
                    stream_id.0,
                    previous_request_lane,
                    request_lane,
                    recv_stream.next_offset(),
                    recv_stream.reorder_bytes(),
                    request_demand_update.byte_proven_bulk,
                    request_demand_update.rate_proven_sustained_bulk,
                ),
            );
        }
        let (
            previous_response_lane,
            response_lane,
            response_classifier_path,
            send_path_snapshot,
            inflight_limit,
            tail_reinjection_path_snapshot,
            response_requalification_capacity_blocked,
            tail_copy_due,
            response_recovery_due,
            output_membership_changed,
            authoritative_data_ack_gap,
            ack_gap_capacity_wait_arm_active,
            retained_frontier_candidate,
        ) = {
            let mut product = response_product.lock();
            if let Some(error) = product.prepared.pending_error.take() {
                break Err(error);
            }
            if observe_server_response_claims(
                &product,
                &mut observed_response_claimed_offset,
                &mut stats,
            ) {
                response_recovery_dirty = true;
            }
            let ResponseProductState {
                sender: response_sender,
                send_stream,
                last_send_ack,
                ..
            } = &mut *product;
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
            let previous_response_lane = path_stream.current_lane();
            response_sender.publish_queue_bytes(path_stream);
            let classifier_payload_hint =
                relay_lane_startup_chunk_bytes(previous_response_lane, mux_limits)
                    .min(path_stream.max_frame_payload_bytes);
            let (response_classifier_path, classifier_inflight_limit) = path_stream
                .send_path_snapshot_and_source_window(
                    previous_response_lane,
                    classifier_payload_hint,
                );
            let response_demand_update = refresh_server_response_flow_demand(
                &mut response_flow_demand,
                response_sender,
                send_stream,
                response_classifier_path,
                mux_limits,
            );
            let response_lane = response_demand_update.lane;
            if response_lane != previous_response_lane {
                path_stream.set_lane(response_lane);
                response_path_staleness_dirty = true;
                response_recovery_dirty = true;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_response_lane_changed",
                    format_args!(
                        "stream_id={} previous={:?} lane={:?} sent_offset={} reinjection_bytes={} byte_proven={} rate_proven={} buffered_data={}",
                        stream_id.0,
                        previous_response_lane,
                        response_lane,
                        send_stream.next_offset(),
                        send_stream.reinjection_bytes(),
                        response_demand_update.byte_proven_bulk,
                        response_demand_update.rate_proven_sustained_bulk,
                        response_demand_update.buffered_bulk,
                    ),
                );
            }
            let payload_hint = relay_lane_startup_chunk_bytes(response_lane, mux_limits)
                .min(path_stream.max_frame_payload_bytes);
            let (send_path_snapshot, inflight_limit) = if response_lane == previous_response_lane {
                (response_classifier_path, classifier_inflight_limit)
            } else {
                path_stream.send_path_snapshot_and_source_window(response_lane, payload_hint)
            };
            let tail_reinjection_path_snapshot = path_stream.tail_reinjection_snapshot(
                last_send_ack_frontier,
                response_lane,
                relay_lane_startup_chunk_bytes(response_lane, mux_limits)
                    .min(path_stream.max_frame_payload_bytes),
            );
            let response_path_staleness_due = response_path_staleness
                .next_deadline()
                .is_some_and(|deadline| deadline <= Instant::now());
            if response_path_staleness_dirty || response_path_staleness_due {
                let response_path_staleness_candidates =
                    path_stream.data_ack_recovery_candidates(last_send_ack.gaps(), response_lane);
                if mark_response_path_staleness(
                    &mut response_path_staleness,
                    path_stream,
                    &response_path_staleness_candidates,
                    &response_data_ack_progress_outputs,
                    response_lane,
                ) {
                    response_recovery_dirty = true;
                }
                response_data_ack_progress_outputs.clear();
                response_path_staleness_dirty = false;
            }
            if response_requalification_capacity_wait.is_none() {
                let response_requalification_attempt = match response_sender
                    .try_send_requalification_probe(
                        path_stream,
                        send_stream,
                        response_lane,
                        mux_limits,
                    ) {
                    Ok(attempt) => attempt,
                    Err(err) if reliable_path_error_is_migratable(&err) => {
                        RequalificationAttempt::Idle
                    }
                    Err(err) => break Err(err),
                };
                response_requalification_capacity_wait =
                    response_requalification_attempt.into_capacity_wait();
            }
            let response_requalification_capacity_blocked =
                response_requalification_capacity_wait.is_some();
            let response_recovery_generation = response_sender.stale_response_recovery_generation();
            if response_recovery_generation != observed_response_recovery_generation {
                observed_response_recovery_generation = response_recovery_generation;
                response_recovery_dirty = true;
            }
            let accepted_copy_observation = path_stream.earliest_reinjection_suppression_deadline();
            #[cfg(feature = "lab-diagnostics")]
            let accepted_copy_wake_before = tail_copy_wake_at;
            let accepted_copy_observed_at = Instant::now();
            let tail_copy_due = reconcile_accepted_copy_wake(
                &mut tail_copy_wake_at,
                accepted_copy_observation,
                accepted_copy_observed_at,
            );
            if tail_copy_due {
                #[cfg(feature = "lab-diagnostics")]
                lab_server_response_recovery_wake(
                    stream_id,
                    "accepted_copy",
                    last_send_ack_frontier,
                    send_stream.next_offset(),
                    accepted_copy_wake_before,
                    tail_copy_wake_at,
                    accepted_copy_observed_at,
                );
                // Stale-owner recovery and generic/failure tail recovery share the
                // same accepted-copy expiry. Make the stale range driver consume
                // this serialized turn before ACK-gap and tail evaluation.
                response_recovery_dirty = true;
            }
            let response_range_observed_at = Instant::now();
            let response_range_recovery_due = response_range_recovery_deadline
                .is_some_and(|deadline| deadline <= response_range_observed_at);
            #[cfg(feature = "lab-diagnostics")]
            if response_range_recovery_due {
                lab_server_response_recovery_wake(
                    stream_id,
                    "stale_range",
                    last_send_ack_frontier,
                    send_stream.next_offset(),
                    response_range_recovery_deadline,
                    None,
                    response_range_observed_at,
                );
            }
            let response_recovery_due = response_recovery_dirty || response_range_recovery_due;
            let output_membership_generation = path_stream.output_membership_generation();
            let output_membership_changed =
                output_membership_generation != observed_output_membership_generation;
            if output_membership_changed {
                observed_output_membership_generation = output_membership_generation;
            }
            let authoritative_data_ack_gap =
                stream_ack_ranges_expose_authoritative_gap(last_send_ack.gaps());
            let has_distinct_ack_gap_reinjection_alternative = authoritative_data_ack_gap
                && has_distinct_response_reinjection_alternative(
                    path_stream,
                    send_stream,
                    last_send_ack.gaps(),
                    last_send_ack_frontier,
                );
            let ack_gap_capacity_wait_arm_active = server_ack_gap_capacity_wait_arm_active(
                authoritative_data_ack_gap,
                has_distinct_ack_gap_reinjection_alternative,
            );
            let retained_frontier_candidate =
                response_retained_frontier_candidate(path_stream, send_stream);
            (
                previous_response_lane,
                response_lane,
                response_classifier_path,
                send_path_snapshot,
                inflight_limit,
                tail_reinjection_path_snapshot,
                response_requalification_capacity_blocked,
                tail_copy_due,
                response_recovery_due,
                output_membership_changed,
                authoritative_data_ack_gap,
                ack_gap_capacity_wait_arm_active,
                retained_frontier_candidate,
            )
        };
        let applied_peer_max_offset = response_product.lock().send_stream.peer_max_offset();
        request_ack_publication.record_feedback(
            path_stream.service_feedback_route(applied_peer_max_offset),
            &mut recv_stream,
        );
        // Attachment replay can service feedback outside this actor. Observe
        // its durable advertised credit and current exact-recipient fences;
        // this is reconciliation, not a new send/progress event.
        request_ack_publication.record_feedback(path_stream.feedback_status(), &mut recv_stream);
        let request_ack_generation = recv_progress.ack_generation();
        if output_membership_changed
            || request_ack_capacity_wait_generation != request_ack_generation
            || !request_ack_publication.pending
        {
            // A retained wait covers only the exact attachment set and ACK
            // generation for which it was armed.
            request_ack_capacity_wait = None;
        }
        let request_ack_reconciliation_due = request_ack_generation != 0
            && (output_membership_changed
                || (request_ack_publication.pending && request_ack_capacity_wait.is_none()));
        if request_ack_reconciliation_due {
            // Arm before retry: Notify::notify_waiters is not retained if a
            // writer releases capacity between the retry and waiter creation.
            let capacity_wait = arm_carrier_capacity_notifies(
                path_stream.pending_ack_capacity_notifies(request_ack_generation),
            );
            debug_assert_eq!(request_ack_publication.generation, request_ack_generation);
            let publication = path_stream.retry_pending_ack();
            request_ack_publication.record_feedback(publication, &mut recv_stream);
            if request_ack_publication.pending {
                request_ack_capacity_wait = capacity_wait;
                request_ack_capacity_wait_generation = request_ack_generation;
            }
        }
        let max_data_publication_pending = path_stream.has_pending_max_data_publication();
        let request_requalification_ack_pending =
            path_stream.has_pending_request_requalification_ack();
        let mut response_state_capacity_notifies = if response_recovery_due
            || response_recovery_capacity_blocked
            || ack_gap_capacity_wait_arm_active
            || retained_frontier_candidate
        {
            path_stream.response_recovery_capacity_notifies()
        } else {
            Vec::new()
        };
        if max_data_publication_pending {
            for notify in path_stream.pending_max_data_capacity_notifies() {
                if !response_state_capacity_notifies
                    .iter()
                    .any(|current| Arc::ptr_eq(current, &notify))
                {
                    response_state_capacity_notifies.push(notify);
                }
            }
        }
        if request_requalification_ack_pending {
            for notify in path_stream.pending_request_requalification_ack_capacity_notifies() {
                if !response_state_capacity_notifies
                    .iter()
                    .any(|current| Arc::ptr_eq(current, &notify))
                {
                    response_state_capacity_notifies.push(notify);
                }
            }
        }
        let response_state_capacity_wait =
            arm_carrier_capacity_notifies(response_state_capacity_notifies);
        let has_response_state_capacity_wait = response_state_capacity_wait.is_some();
        if request_requalification_ack_pending {
            let _ = path_stream.retry_pending_request_requalification_ack()?;
        }
        if max_data_publication_pending {
            let publication = path_stream.retry_pending_max_data();
            request_ack_publication.record_feedback(publication, &mut recv_stream);
            if !request_ack_publication.pending {
                request_ack_capacity_wait = None;
            }
        }
        let request_feedback_path_snapshot =
            path_stream.request_feedback_path_snapshot(request_lane);
        let feedback_route_capacity_wait =
            arm_carrier_capacity_notifies(path_stream.feedback_route_capacity_notifies());
        let has_feedback_route_capacity_wait = feedback_route_capacity_wait.is_some();
        request_ack_publication.record_feedback(
            path_stream.service_feedback_route(applied_peer_max_offset),
            &mut recv_stream,
        );
        let feedback_route_deadline = path_stream.feedback_route_deadline();
        let request_feedback_underlay = request_feedback_path_snapshot
            .map(|snapshot| snapshot.underlay)
            .or_else(|| path_stream.request_feedback_underlay())
            .unwrap_or(path_stream.underlay);
        let recv_progress_observed_at = recv_progress
            .last_ack_at()
            .map_or(last_recv_progress_sent_at, |ack_at| {
                ack_at.max(last_recv_progress_sent_at)
            });
        let recv_progress_deadline = tokio::time::Instant::from_std(
            recv_progress_observed_at
                + reliable_stream_recv_progress_interval(request_feedback_path_snapshot),
        );
        let recv_progress_ack_update_pending = remote_open && recv_progress.ack_update_pending();
        let (
            response_state_capacity_blocked,
            has_request_ack_capacity_wait,
            response_path_recovery_deadline,
            failed_original_recovery,
            failed_original_tail_reinjection_ready,
            final_offset_known,
            retained_frontier_phase,
            tail_timer_active,
            tail_timer_deadline,
            ack_gap_reinjection_deadline,
            live_tail_deadline,
            tail_reinjection_deadline,
            tail_reinjection_active,
            adaptive_chunk,
            sender_queue_limit,
            sender_dispatch_byte_budget,
            sender_dispatch_item_budget,
            queued_send_blocked,
            queued_send_ready,
            queued_send_retry_deadline,
            has_carrier_capacity_wait,
            carrier_capacity_wait,
            can_read_local,
            read_budget,
            can_send_pending_fin,
            prepared_work_wait,
        ) = {
            let mut product = response_product.lock();
            if let Some(error) = product.prepared.pending_error.take() {
                break Err(error);
            }
            if observe_server_response_claims(
                &product,
                &mut observed_response_claimed_offset,
                &mut stats,
            ) {
                response_recovery_dirty = true;
            }
            let ResponseProductState {
                sender: response_sender,
                send_stream,
                last_send_ack,
                ..
            } = &mut *product;
            let queued_bytes_before_recovery = response_sender.bytes();
            let response_recovery_due = response_recovery_due || response_recovery_dirty;
            if response_recovery_due {
                response_sender.discard_resolved_stale_output_reinjections(path_stream);
                let response_recovery = response_sender.drive_stale_output_recovery(
                    path_stream,
                    send_stream,
                    mux_limits,
                );
                if response_recovery.queued {
                    response_sender_retry_at = None;
                }
                response_range_recovery_deadline = response_recovery.retry_deadline;
                response_recovery_capacity_blocked = response_recovery.blocked_for_carrier_capacity;
                response_recovery_dirty = false;
            }
            response_sender.discard_unusable_tail_reinjections(path_stream);
            if response_sender.discard_stale_bound_reinjections(path_stream) > 0 {
                response_sender_retry_at = None;
            }
            let ack_gap_recovery = evaluate_server_data_ack_reinjection(
                response_sender,
                path_stream,
                send_stream,
                &mut ack_gap_reinjection,
                last_send_ack,
                last_send_ack_frontier,
                send_path_snapshot,
                response_lane,
                mux_limits,
                stream_id,
            );
            let ack_gap_observed_at = ack_gap_recovery.observed_at;
            if ack_gap_recovery.queued > 0 {
                response_sender_retry_at = None;
            }
            let ack_gap_missing_target_wait_active = server_ack_gap_missing_target_wait_active(
                authoritative_data_ack_gap,
                ack_gap_recovery.has_multipath_alternative,
                ack_gap_recovery.has_measured_target,
            );
            let max_data_publication_blocked = path_stream.has_pending_max_data_publication();
            let request_requalification_ack_blocked =
                path_stream.has_pending_request_requalification_ack();
            let mut response_state_capacity_blocked = response_recovery_capacity_blocked
                || max_data_publication_blocked
                || request_requalification_ack_blocked
                || ack_gap_missing_target_wait_active
                || ack_gap_recovery.target_service_exhausted;
            let has_request_ack_capacity_wait = request_ack_capacity_wait.is_some();
            let response_requalification_deadline = (!response_requalification_capacity_blocked)
                .then(|| path_stream.response_requalification_deadline())
                .flatten()
                .map(tokio::time::Instant::from_std);
            let response_path_recovery_deadline = response_path_staleness
                .next_deadline()
                .map(tokio::time::Instant::from_std)
                .into_iter()
                .chain(response_range_recovery_deadline.map(tokio::time::Instant::from_std))
                .chain(response_requalification_deadline)
                .min();
            let data_ack_recovery_candidate =
                path_stream.data_ack_recovery_candidate(last_send_ack_frontier);
            let data_ack_recovery_candidate =
                data_ack_recovery_candidate.map(ReliableRelayTailRecoveryCandidate::Tracked);
            let has_tail_reinjection_alternative = has_distinct_response_reinjection_alternative(
                path_stream,
                send_stream,
                last_send_ack.gaps(),
                last_send_ack_frontier,
            );
            let failed_original_recovery = path_stream.failed_original_recovery_state();
            let accepted_copy_deadline = tail_copy_wake_at
                .map(tokio::time::Instant::from_std)
                .into_iter()
                .chain(
                    failed_original_recovery
                        .retry_deadline
                        .map(tokio::time::Instant::from_std),
                )
                .min();
            let failed_original_tail_reinjection_ready =
                reliable_failed_original_tail_reinjection_ready(
                    &failed_original_recovery,
                    send_stream,
                );
            let final_offset_known = close.sent || pending_local_fin;
            let retained_frontier_phase = if final_offset_known {
                ResponseRetainedFrontierPhase::FinalDrain
            } else {
                ResponseRetainedFrontierPhase::Active
            };
            // Active and final sending share one exact retained-owner obligation.
            // The capacity edge above is armed before this helper checks target
            // credit. ACK-gap and exact failed/unknown-owner recovery retain their
            // separate authority; historical ACK completeness does not gate F.
            let retained_frontier_outcome =
                if !failed_original_tail_reinjection_ready && ack_gap_recovery.frame_count == 0 {
                    enqueue_live_response_retained_frontier_reinjection(
                        response_sender,
                        path_stream,
                        send_stream,
                        ack_gap_recovery.base_limit,
                        retained_frontier_phase,
                        mux_limits,
                        Instant::now(),
                    )
                } else {
                    LiveResponseRetainedFrontierEnqueueOutcome::default()
                };
            if retained_frontier_outcome.queued > 0 {
                response_sender_retry_at = None;
            }
            response_state_capacity_blocked |=
                retained_frontier_outcome.blocked_for_carrier_capacity;
            let tail_reinjection_candidate = has_tail_reinjection_alternative
                && last_send_ack.has_gaps()
                && stream_ack_ranges_expose_authoritative_gap(last_send_ack.gaps());
            let tail_timer_active = reliable_relay_tail_reinjection_timer_active(
                send_stream.reinjection_bytes(),
                tail_reinjection_candidate,
                failed_original_tail_reinjection_ready,
            );
            let data_ack_recovery_candidate = tail_timer_active.then(|| {
                data_ack_recovery_candidate.unwrap_or(
                    ReliableRelayTailRecoveryCandidate::Untracked {
                        start: last_send_ack_frontier,
                        end: send_stream.next_offset(),
                        sent_at: last_send_ack_progress_at,
                    },
                )
            });
            let data_ack_outstanding_bytes = reliable_relay_current_data_ack_outstanding_bytes(
                response_lane,
                send_stream,
                last_send_ack_frontier,
            );
            let tail_timer_deadline = tail_reinjection_timer.observe(
                data_ack_recovery_candidate,
                last_send_ack_progress_at,
                tail_reinjection_path_snapshot,
                failed_original_tail_reinjection_ready,
            );
            let ack_gap_candidate_deadline = (has_tail_reinjection_alternative
                && stream_ack_ranges_expose_authoritative_gap(last_send_ack.gaps()))
            .then(|| ack_gap_reinjection.next_reinjection_deadline())
            .flatten();
            let live_owner_epoch_deadline = response_sender.live_owner_frontier_floor_deadline();
            let ack_gap_live_owner_wake = live_owner_gap_recovery_wake(
                ack_gap_candidate_deadline,
                ack_gap_reinjection.original_owner_recovery_deadline(),
                live_owner_epoch_deadline,
                ack_gap_observed_at,
            );
            let live_tail_wake = server_live_owner_recovery_wake(
                retained_frontier_outcome
                    .owner_fallback_deadline
                    .map(tokio::time::Instant::from_std),
                live_owner_epoch_deadline,
                ack_gap_observed_at,
            );
            let failed_tail_deadline = (tail_timer_active
                && failed_original_tail_reinjection_ready)
                .then_some(tail_timer_deadline);
            let ack_gap_reinjection_deadline = ack_gap_live_owner_wake
                .deadline
                .map(tokio::time::Instant::from_std);
            let live_tail_deadline = live_tail_wake.deadline.map(tokio::time::Instant::from_std);
            let tail_reinjection_deadline = ack_gap_reinjection_deadline
                .into_iter()
                .chain(live_tail_deadline)
                .chain(failed_tail_deadline)
                .chain(accepted_copy_deadline)
                .min()
                .unwrap_or(tail_timer_deadline);
            let tail_reinjection_active = ack_gap_reinjection_deadline.is_some()
                || live_tail_deadline.is_some()
                || failed_tail_deadline.is_some()
                || accepted_copy_deadline.is_some();
            if tail_copy_due || live_tail_wake.due {
                let outcome = enqueue_reliable_tail_reinjection_with_ack_gaps(
                    response_sender,
                    path_stream,
                    &failed_original_recovery.uncovered_ranges,
                    stream_id,
                    send_stream,
                    last_send_ack.gaps(),
                    tail_reinjection_path_snapshot,
                    response_lane,
                    mux_limits,
                    performance,
                    path_stream.max_frame_payload_bytes,
                    (!final_offset_known)
                        .then(|| ack_gap_reinjection.original_owner_recovery_deadline())
                        .flatten(),
                    last_send_ack_frontier,
                );
                if outcome.queued > 0 {
                    response_sender_retry_at = None;
                }
                response_state_capacity_blocked |= outcome.blocked_for_carrier_capacity;
            }
            let adaptive_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
                send_path_snapshot,
                response_lane,
                mux_limits,
                path_stream.max_frame_payload_bytes,
            );
            let sender_queue_limit = reliable_relay_sender_queue_limit(mux_limits, inflight_limit);
            let latency_startup_credit = response_flow_demand
                .latency_startup_credit_remaining_bytes(
                    response_lane,
                    response_classifier_path,
                    mux_limits,
                );
            let source_staging_headroom = reliable_relay_response_source_staging_headroom(
                response_lane,
                inflight_limit,
                data_ack_outstanding_bytes,
                response_sender.data_bytes(),
            );
            // Source bytes do not receive a data sequence or path assignment until
            // dispatch; exact chosen-tier Product P and retained/queued Product O
            // bound staging under the shared stream/reorder/repair envelope.
            let source_read_ceiling = reliable_relay_buffer_len(mux_limits)
                .min(path_stream.max_frame_payload_bytes)
                .min(sender_queue_limit)
                .min(latency_startup_credit)
                .min(source_staging_headroom);
            let (sender_dispatch_byte_budget, sender_dispatch_item_budget) =
                reliable_relay_sender_dispatch_budget(
                    mux_limits,
                    response_lane,
                    adaptive_chunk,
                    inflight_limit,
                    sender_queue_limit,
                );
            close.lane = response_lane;
            last_sender_dispatch_byte_budget = sender_dispatch_byte_budget;
            last_sender_dispatch_item_budget = sender_dispatch_item_budget;
            #[cfg(feature = "lab-diagnostics")]
            if last_reported_budget != Some((response_lane, adaptive_chunk, inflight_limit)) {
                let snapshot = send_path_snapshot;
                lab_diagnostic(
                    "server_relay_budget",
                    format_args!(
                        "stream_id={} underlay={:?} lane={:?} chunk_bytes={} inflight_bytes={} max_frame_payload_bytes={} snapshot={} rate_mbps={:.3} pacing_mbps={:.3} product_progress_mbps={:.3} queue_bytes={} data_level_queue_bytes={} carrier_flight_bytes={} product_flight_bytes={} confidence_ppm={}",
                        stream_id.0,
                        path_stream.underlay,
                        response_lane,
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
                last_reported_budget = Some((response_lane, adaptive_chunk, inflight_limit));
            }
            let now = tokio::time::Instant::now();
            if response_sender_retry_at.is_some_and(|deadline| deadline <= now) {
                response_sender_retry_at = None;
            }
            let response_sender_queue_nonempty = response_sender.queued_nondata_ready();
            let carrier_capacity_wait = if response_sender_queue_nonempty {
                arm_response_sender_capacity_wait(path_stream.capacity_notifies())
            } else {
                None
            };
            let queued_front_has_carrier_credit = response_sender
                .front_nondata_has_carrier_credit_at_frontier(
                    path_stream,
                    send_stream,
                    response_lane,
                    mux_limits,
                    data_ack_outstanding_bytes,
                    server_data_ack_frontier_state(last_send_ack, send_stream),
                );
            let sender_wait = response_sender_wait_state(
                response_sender_queue_nonempty,
                response_sender.queued_nondata_ready(),
                queued_front_has_carrier_credit,
                response_sender_retry_at,
                now,
                sender_service_retry_delay(send_path_snapshot),
            );
            response_sender_retry_at = sender_wait.retry_at;
            let queued_send_blocked = sender_wait.blocked;
            let queued_send_ready = sender_wait.ready;
            let queued_send_retry_deadline = sender_wait.retry_at.unwrap_or(now);
            let has_carrier_capacity_wait =
                sender_wait.subscribe_capacity && carrier_capacity_wait.is_some();
            let queued_send_blocks_source_read =
                queued_send_blocked || response_recovery_capacity_blocked;
            let can_read_by_flow = source_read_ceiling > 0
                && source_staging_headroom > 0
                && response_sender.can_read_product_source(
                    local_open,
                    queued_send_blocks_source_read,
                    send_stream,
                    sender_queue_limit,
                );
            let read_budget = if can_read_by_flow {
                response_sender.read_budget(send_stream, sender_queue_limit, source_read_ceiling)
            } else {
                0
            };
            // A target socket can stay established while every MPP carrier is down.
            // Stop reading so ordinary socket backpressure bounds retained response data.
            let can_read_local =
                send_path_snapshot.is_some() && can_read_by_flow && read_budget > 0;
            let can_send_pending_fin =
                pending_local_fin && response_sender.is_empty() && !close.sent;
            // Membership and pending control publication can become reconciled in
            // this turn without producing another wake. Reconsider completion only
            // after that work, while retaining every exact-recipient obligation.
            if !local_open && !remote_open {
                request_ack_publication
                    .record_feedback(path_stream.feedback_status(), &mut recv_stream);
            }
            if !local_open
                && !remote_open
                && send_stream.reinjection_bytes() == 0
                && response_sender.is_empty()
                && (!pending_local_fin || close.sent)
                && (!path_stream.has_live_output()
                    || request_ack_publication.current_generation_is_fully_published())
                && !path_stream.has_pending_request_requalification_ack()
                && path_stream.output_membership_generation()
                    == observed_output_membership_generation
            {
                break Ok(stats);
            }

            let recovery_work_changed = response_sender.bytes() != queued_bytes_before_recovery;
            // Publish only real source/policy/membership changes. Writer commits
            // notify this actor; an unchanged observation must not wake itself.
            publish_prepared_response_work(
                &mut product,
                &response_product,
                response_lane,
                adaptive_chunk,
                output_membership_changed
                    || response_lane != previous_response_lane
                    || recovery_work_changed,
            );
            let mut prepared_work_wait =
                Box::pin(product.prepared.work_changed.clone().notified_owned());
            prepared_work_wait.as_mut().enable();
            (
                response_state_capacity_blocked,
                has_request_ack_capacity_wait,
                response_path_recovery_deadline,
                failed_original_recovery,
                failed_original_tail_reinjection_ready,
                final_offset_known,
                retained_frontier_phase,
                tail_timer_active,
                tail_timer_deadline,
                ack_gap_reinjection_deadline,
                live_tail_deadline,
                tail_reinjection_deadline,
                tail_reinjection_active,
                adaptive_chunk,
                sender_queue_limit,
                sender_dispatch_byte_budget,
                sender_dispatch_item_budget,
                queued_send_blocked,
                queued_send_ready,
                queued_send_retry_deadline,
                has_carrier_capacity_wait,
                carrier_capacity_wait,
                can_read_local,
                read_budget,
                can_send_pending_fin,
                prepared_work_wait,
            )
        };
        if !can_read_local {
            // Eligibility ended. An inactive source owns no admission position;
            // a losing select alone preserves its unsatisfied position instead.
            send_buffer_updates.withdraw();
        }

        // Carrier input and target responses can both remain continuously
        // ready during an upload. Fair polling keeps response progress from
        // being hidden behind an unbounded run of incoming STREAM_DATA.
        tokio::select! {
        _ = async {
            match feedback_route_deadline {
                Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
                None => std::future::pending().await,
            }
        } => { continue; }
        _ = async move {
            if let Some(wait) = feedback_route_capacity_wait { wait.await; }
        }, if has_feedback_route_capacity_wait => { continue; }
        () = prepared_work_wait => {
            // Re-read C, source credit and error/terminal state under the owner.
            continue;
        }
        _ = async {
            match response_path_recovery_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        }, if response_path_recovery_deadline.is_some() => {
            // Re-evaluate exact attachment and range recovery clocks before
            // assigning more OriginalData; native recovery continues.
            continue;
        }
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
            let now = tokio::time::Instant::now();
            if tail_copy_wake_at
                .is_some_and(|deadline| tokio::time::Instant::from_std(deadline) <= now)
            {
                // The durable actor-owned one-shot is consumed only at the
                // loop's serialized recovery point. Committing a successor
                // copy here could otherwise let this expired deadline mask
                // the successor's later immutable wake.
                continue;
            }
            {
            let mut product = response_product.lock();
            let ResponseProductState {
                sender: response_sender, send_stream, last_send_ack, ..
            } = &mut *product;
            if ack_gap_reinjection_deadline.is_some_and(|deadline| deadline <= now) {
                #[cfg(feature = "lab-diagnostics")]
                lab_server_response_recovery_wake(
                    stream_id,
                    "ack_gap",
                    last_send_ack_frontier,
                    send_stream.next_offset(),
                    ack_gap_reinjection_deadline.map(tokio::time::Instant::into_std),
                    None,
                    now.into_std(),
                );
                // Retained ACK recovery is evaluated only at the loop's single
                // ownership point, including when this deadline is the wake.
                continue;
            }
            if live_tail_deadline.is_some_and(|deadline| deadline <= now) {
                // The next loop observation retains this as explicit due
                // state and evaluates the live tail once. Keeping a past
                // epoch deadline in select would otherwise busy-spin.
                continue;
            }
            let tail_timer_due = failed_original_tail_reinjection_ready
                && tail_timer_active
                && tail_timer_deadline <= now;
            #[cfg(feature = "lab-diagnostics")]
            lab_server_response_recovery_wake(
                stream_id,
                "tail",
                last_send_ack_frontier,
                send_stream.next_offset(),
                Some(tail_reinjection_deadline.into_std()),
                None,
                now.into_std(),
            );
            enqueue_reliable_tail_reinjection_with_ack_gaps(
                response_sender,
                path_stream,
                &failed_original_recovery.uncovered_ranges,
                stream_id,
                send_stream,
                last_send_ack.gaps(),
                tail_reinjection_path_snapshot,
                response_lane,
                mux_limits,
                performance,
                path_stream.max_frame_payload_bytes,
                (!final_offset_known)
                    .then(|| ack_gap_reinjection.original_owner_recovery_deadline())
                    .flatten(),
                last_send_ack_frontier,
            );
            if tail_timer_due {
                tail_reinjection_timer.record_scan();
            }
            }
            if drain_shared_server_response_sender_ready(
                &response_product, path_stream, response_lane, mux_limits,
                sender_dispatch_byte_budget, sender_dispatch_item_budget, &mut tail_copy_wake_at, session_id,
            )?
            {
                response_sender_retry_at =
                    Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
            }
            continue;
        }
        // Server input interleaves Product frames with ordered attachment
        // lifecycle. It must remain polled after Product half-close and while
        // response flight is empty so carrier retirement can complete.
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
        } => {
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
                    // Reconcile after dequeueing the complete ready batch:
                    // registry-side MAX replay can race its collection. The
                    // binding retains actually advertised credit even when
                    // its advertising attachment has already been removed.
                    request_ack_publication
                        .record_feedback(path_stream.feedback_status(), &mut recv_stream);
                    let applied = apply_ready_stream_data_batch(
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
                    );
                    let peer_max_offset = response_product.lock().send_stream.peer_max_offset();
                    service_server_feedback_while_pending(
                        write_applied_ready_stream_data_batch(&mut local, &mut ready_path_data, applied),
                        path_stream, &mut recv_stream, &mut request_ack_publication, peer_max_offset,
                        Some(ServerPendingReceiveAck {
                            progress: &mut recv_progress,
                            path: request_feedback_path_snapshot,
                            lane: request_lane,
                            limits: mux_limits,
                        }),
                    ).await?;
                    if enqueue_tcp_recv_progress(
                        path_stream,
                        &mut recv_stream,
                        &mut recv_progress,
                        &mut request_ack_publication,
                        request_feedback_path_snapshot,
                        request_lane,
                        mux_limits,
                        false,
                        true,
                        false,
                    )
                    {
                        response_sender_retry_at = None;
                        last_recv_progress_sent_at = Instant::now();
                    }
                    if pending_stream_fin_ready(&recv_stream, pending_remote_fin_offset) {
                        path_stream.finish_feedback_route();
                        if enqueue_tcp_recv_progress(
                            path_stream,
                            &mut recv_stream,
                            &mut recv_progress,
                            &mut request_ack_publication,
                            request_feedback_path_snapshot,
                            request_lane,
                            mux_limits,
                            true,
                            false,
                            false,
                        ) {
                            response_sender_retry_at = None;
                            last_recv_progress_sent_at = Instant::now();
                        }
                        let peer_max_offset = response_product.lock().send_stream.peer_max_offset();
                        service_server_feedback_while_pending(
                            async { local.shutdown().await.map_err(RuntimeError::Io) },
                            path_stream, &mut recv_stream, &mut request_ack_publication, peer_max_offset,
                            None,
                        ).await?;
                        remote_open = false;
                        pending_remote_fin_offset = None;
                    }
                }
                Frame::StreamAck {
                    stream_id: ack_stream_id,
                    scope_start,
                    ranges,
                } if ack_stream_id == stream_id => {
                    let released_bytes = {
            let mut product = response_product.lock();
            let ResponseProductState {
                sender: response_sender, send_stream, last_send_ack, ..
            } = &mut *product;
                    // Freeze the send-assignment extent and validate every
                    // original range before any cache, flight, queue,
                    // reservation, or recovery-evidence mutation.
                    let validated_ack =
                        match begin_reliable_stream_ack(send_stream, scope_start, ranges) {
                            Ok(ack) => ack,
                            Err(err) => break Err(err.into()),
                        };
                    if last_send_ack.subsumes(&validated_ack, send_stream) {
                        continue;
                    }
                    let normalized_ranges = validated_ack.ranges();
                    #[cfg(feature = "lab-diagnostics")]
                    let mux_started = Instant::now();
                    let ack = match send_stream.apply_validated_ack(&validated_ack) {
                        Ok(ack) => ack,
                        Err(err) => break Err(err.into()),
                    };
                    if ack.released_bytes > 0 {
                        response_sender.record_delivered_data(ack.released_bytes);
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("mux.apply_ack", mux_started.elapsed(), ack.released_bytes);
                    let response_data_ack_release =
                        path_stream.release_normalized_acked_ranges(normalized_ranges);
                    response_data_ack_progress_outputs =
                        response_data_ack_release.path_progress_outputs.into_vec();
                    response_path_staleness_dirty = true;
                    response_recovery_dirty = true;
                    response_sender.release_normalized_acked_reinjections(normalized_ranges);
                    // The next dirty observation includes every binding
                    // mutation completed before this borrow. A later external
                    // attachment change remains pending on the watch.
                    if let Some(updates) = output_updates.as_mut() {
                        updates.borrow_and_update();
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    let largest_ack_end = normalized_ranges.last().map_or(0, |range| range.end);
                    #[cfg(feature = "lab-diagnostics")]
                    let incoming_ack_frontier =
                        stream_ack_contiguous_frontier(normalized_ranges);
                    let previous_ack_frontier = last_send_ack_frontier;
                    update_reinjection_authoritative_ack_snapshot(
                        last_send_ack,
                        &validated_ack,
                        send_stream,
                    );
                    // Positive coverage always releases bytes; only explicitly
                    // scoped omissions update gap authority. Flow control follows
                    // the exact lowest outstanding Data Sequence offset.
                    last_send_ack_frontier = send_stream.data_ack_frontier();
                    let ack_made_progress = last_send_ack_frontier > previous_ack_frontier;
                    if ack_made_progress {
                        let progressed_at = Instant::now();
                        last_send_ack_progress_at = progressed_at;
                        response_sender
                            .record_live_owner_data_ack_frontier_progress(progressed_at);
                    }
                    let reinjection = evaluate_server_data_ack_reinjection(
                        response_sender,
                        path_stream,
                        send_stream,
                        &mut ack_gap_reinjection,
                        last_send_ack,
                        last_send_ack_frontier,
                        send_path_snapshot,
                        response_lane,
                        mux_limits,
                        stream_id,
                    );
                    if let (Some(candidate), Some(deadline)) = (
                        reinjection.tail_recovery_candidate,
                        reinjection.tail_recovery_deadline,
                    ) {
                        tail_reinjection_timer.arm_recovery_deadline(
                            ReliableRelayTailRecoveryCandidate::Tracked(candidate),
                            deadline,
                        );
                    }
                    if reinjection.queued > 0 {
                        response_sender_retry_at = None;
                    }
                    let base_reinjection_limit = reinjection.base_limit;
                    let retained_frontier_observed_at = Instant::now();
                    // Apply positive ACK release before observing the exact
                    // retained owner, then queue recovery before this branch's
                    // sender drain regardless of source EOF.
                    let retained_frontier_outcome = if reinjection.frame_count == 0
                        && !failed_original_tail_reinjection_ready
                    {
                        enqueue_live_response_retained_frontier_reinjection(
                            response_sender,
                            path_stream,
                            send_stream,
                            base_reinjection_limit,
                            retained_frontier_phase,
                            mux_limits,
                            retained_frontier_observed_at,
                        )
                    } else {
                        LiveResponseRetainedFrontierEnqueueOutcome::default()
                    };
                    if retained_frontier_outcome.queued > 0 {
                        response_sender_retry_at = None;
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    let reinjection_limit = if retained_frontier_outcome.service_limit > 0 {
                        retained_frontier_outcome.service_limit
                    } else {
                        reinjection.service_limit
                    };
                    let reinjection_kind = if reinjection.frame_count > 0 {
                        "persistent_ack_gap"
                    } else if retained_frontier_outcome.queued > 0 || retained_frontier_outcome.pending {
                        "retained_frontier"
                    } else {
                        "none"
                    };
                    #[cfg(feature = "lab-diagnostics")]
                    if retained_frontier_outcome.blocked_frontier_offset.is_some() {
                        lab_diagnostic(
                            "tail_stall_reinjection_blocked_frontier",
                            format_args!(
                                "stream_id={} blocked_frontier_offset={:?} reinjection_kind=retained_frontier",
                                stream_id.0, retained_frontier_outcome.blocked_frontier_offset,
                            ),
                        );
                    }
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = reinjection_kind;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "stream_ack_received",
                        format_args!(
                            "stream_id={} scope_start={:?} ranges={} incoming_frontier={} stored_frontier={} largest_end={} released_bytes={} sent_offset={} sender_queue_bytes={} reinjection_bytes_after={} reinjection_frames={} reinjection_kind={} active_underlay={:?} multipath_reinjection_alternative={} ack_gap_reinjection_ready={} base_reinjection_limit={} reinjection_limit={} optional_reinjection_budget_percent={}",
                            stream_id.0,
                            scope_start,
                            normalized_ranges.len(),
                            incoming_ack_frontier,
                            last_send_ack_frontier,
                            largest_ack_end,
                            ack.released_bytes,
                            send_stream.next_offset(),
                            response_sender.bytes(),
                            ack.remaining_reinjection_bytes,
                            reinjection.frame_count.saturating_add(retained_frontier_outcome.queued),
                            reinjection_kind,
                            Some(path_stream.underlay),
                            reinjection.has_multipath_alternative,
                            reinjection.persistent_ready,
                            base_reinjection_limit,
                            reinjection_limit,
                            performance.optional_reinjection_budget_percent,
                        ),
                    );
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
                    let released_bytes = ack.released_bytes;
                    publish_prepared_response_work(
                        &mut product, &response_product, response_lane, adaptive_chunk, true,
                    );
                    released_bytes
                    };
                    send_buffer_reservation.release(released_bytes);
                }
                Frame::StreamFeedbackProbe { stream_id: probe_stream_id, token, max_offset }
                    if probe_stream_id == stream_id => {
                    // The handle retained the exact reply tuple only after
                    // dequeuing this boundary behind its preceding ACK/MAX.
                    let peer_max_offset = response_product.lock().send_stream.peer_max_offset();
                    #[cfg(feature = "lab-diagnostics")]
                    crate::lab_diagnostics::lab_diagnostic("feedback_return", format_args!(
                        "session_id={} stream_id={} kind=owner_received token={} required_max_offset={} applied_peer_max_offset={}", session_id.0, stream_id.0, token, max_offset, peer_max_offset,
                    ));
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = (token, max_offset);
                    request_ack_publication.record_feedback(
                        path_stream.service_feedback_route(peer_max_offset), &mut recv_stream,
                    );
                }
                Frame::StreamFeedbackReceipt { stream_id: receipt_stream_id, token }
                    if receipt_stream_id == stream_id => {
                    request_ack_publication.record_feedback(
                        path_stream.receive_feedback_receipt(token), &mut recv_stream,
                    );
                    request_ack_capacity_wait = None;
                }
                Frame::StreamMaxData {
                    stream_id: max_stream_id,
                    max_offset,
                } if max_stream_id == stream_id => {
                    let mut product = response_product.lock();
                    let previous_credit = product.send_stream.send_credit_bytes();
                    product.send_stream.update_max_offset(max_offset);
                    let changed = product.send_stream.send_credit_bytes() != previous_credit;
                    publish_prepared_response_work(
                        &mut product, &response_product, response_lane, adaptive_chunk, changed,
                    );
                }
                Frame::StreamFin {
                    stream_id: fin_stream_id,
                    final_offset,
                } if fin_stream_id == stream_id => {
                    path_stream.finish_feedback_route();
                    if receive_stream_fin(
                        &recv_stream,
                        &mut pending_remote_fin_offset,
                        final_offset,
                    )? {
                        if enqueue_tcp_recv_progress(
                            path_stream,
                            &mut recv_stream,
                            &mut recv_progress,
                            &mut request_ack_publication,
                            request_feedback_path_snapshot,
                            request_lane,
                            mux_limits,
                            true,
                            false,
                            false,
                        ) {
                            response_sender_retry_at = None;
                            last_recv_progress_sent_at = Instant::now();
                        }
                        let peer_max_offset = response_product.lock().send_stream.peer_max_offset();
                        service_server_feedback_while_pending(
                            async { local.shutdown().await.map_err(RuntimeError::Io) },
                            path_stream, &mut recv_stream, &mut request_ack_publication, peer_max_offset,
                            None,
                        ).await?;
                        remote_open = false;
                        pending_remote_fin_offset = None;
                    }
                }
                Frame::StreamReset {
                    stream_id: reset_stream_id,
                    reason,
                } if reset_stream_id == stream_id => {
                    return Err(RuntimeError::RemoteReset(reason));
                },
                Frame::StreamData {
                    stream_id: received_stream_id,
                    offset,
                    payload,
                    ..
                } if received_stream_id == stream_id
                    && stream_data_range_already_delivered(&recv_stream, offset, payload.len()) =>
                {
                    if enqueue_tcp_recv_progress(
                        path_stream,
                        &mut recv_stream,
                        &mut recv_progress,
                        &mut request_ack_publication,
                        request_feedback_path_snapshot,
                        request_lane,
                        mux_limits,
                        true,
                        true,
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
            let queued_nondata_ready = response_product.lock().sender.queued_nondata_ready();
            if queued_nondata_ready
                && drain_shared_server_response_sender_ready(
                    &response_product, path_stream, response_lane, mux_limits,
                    sender_dispatch_byte_budget, sender_dispatch_item_budget, &mut tail_copy_wake_at, session_id,
                )?
            {
                response_sender_retry_at =
                    Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
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
            response_path_staleness_dirty = true;
            response_recovery_dirty = true;
            let now_has_reinjection_alternative = path_stream.has_multipath_reinjection_alternative();
            let gained_reinjection_alternative =
                now_has_reinjection_alternative && !multipath_reinjection_alternative_available;
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = gained_reinjection_alternative;
            multipath_reinjection_alternative_available = now_has_reinjection_alternative;
            response_sender_retry_at = None;
            {
            let mut product = response_product.lock();
            let ResponseProductState {
                sender: response_sender, send_stream, last_send_ack, ..
            } = &mut *product;
            let output_observed_at = Instant::now();
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = last_send_ack;
            let retained_frontier_candidate = !failed_original_tail_reinjection_ready
                && response_retained_frontier_candidate(path_stream, send_stream);
            let retained_frontier_outcome = if retained_frontier_candidate {
                enqueue_live_response_retained_frontier_reinjection(
                    response_sender,
                    path_stream,
                    send_stream,
                    adaptive_reliable_relay_reinjection_bytes(
                        tail_reinjection_path_snapshot,
                        response_lane,
                        mux_limits,
                    ),
                    retained_frontier_phase,
                    mux_limits,
                    output_observed_at,
                )
            } else {
                LiveResponseRetainedFrontierEnqueueOutcome::default()
            };
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_output_update",
                format_args!(
                    "stream_id={} now_has_reinjection_alternative={} gained_reinjection_alternative={} retained_frontier_candidate={} close_sent={} pending_local_fin={} reinjection_bytes={} ack_ranges={} ack_frontier={} sent_offset={} queue_bytes={}",
                    stream_id.0,
                    now_has_reinjection_alternative,
                    gained_reinjection_alternative,
                    retained_frontier_candidate,
                    close.sent,
                    pending_local_fin,
                    send_stream.reinjection_bytes(),
                    last_send_ack.gaps().len(),
                    last_send_ack_frontier,
                    send_stream.next_offset(),
                    response_sender.bytes(),
                ),
            );
            if retained_frontier_candidate {
                #[cfg(feature = "lab-diagnostics")]
                if retained_frontier_outcome.blocked_frontier_offset.is_some() {
                    lab_diagnostic(
                        "tail_stall_reinjection_blocked_frontier",
                        format_args!(
                            "stream_id={} blocked_frontier_offset={:?} reinjection_kind=retained_frontier",
                            stream_id.0, retained_frontier_outcome.blocked_frontier_offset,
                        ),
                    );
                }
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "tail_stall_reinjection",
                    format_args!(
                        "stream_id={} lane={:?} ack_frontier={} sent_offset={} reinjection_bytes={} reinjection_frames={} blocked_frontier_offset={:?} same_output_frontier_retransmit={} base_reinjection_limit={} reinjection_limit={} optional_reinjection_budget_percent={} reinjection_kind=retained_frontier",
                        stream_id.0,
                        response_lane,
                        last_send_ack_frontier,
                        send_stream.next_offset(),
                        send_stream.reinjection_bytes(),
                        retained_frontier_outcome.queued,
                        retained_frontier_outcome.blocked_frontier_offset,
                        false,
                        retained_frontier_outcome.frontier_limit,
                        retained_frontier_outcome.service_limit,
                        performance.optional_reinjection_budget_percent,
                    ),
                );
                if retained_frontier_outcome.queued > 0 {
                    response_sender_retry_at = None;
                }
            }
                publish_prepared_response_work(
                    &mut product, &response_product, response_lane, adaptive_chunk, true,
                );
            }
            let queued_nondata_ready = response_product.lock().sender.queued_nondata_ready();
            if queued_nondata_ready
                && drain_shared_server_response_sender_ready(
                    &response_product, path_stream, response_lane, mux_limits,
                    sender_dispatch_byte_budget, sender_dispatch_item_budget, &mut tail_copy_wake_at, session_id,
                )?
            {
                response_sender_retry_at =
                    Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
            }
            continue;
        }
        _ = async move {
            if let Some(wait) = carrier_capacity_wait {
                wait.await;
            }
        }, if queued_send_blocked && has_carrier_capacity_wait => {
            response_sender_retry_at = None;
            if response_recovery_capacity_blocked {
                response_recovery_dirty = true;
            }
            continue;
        }
        _ = async {
            if let Some(wait) = request_ack_capacity_wait.as_mut() {
                wait.as_mut().await;
            }
        }, if request_ack_publication.pending && has_request_ack_capacity_wait => {
            request_ack_capacity_wait = None;
            continue;
        }
        _ = async move {
            if let Some(wait) = response_state_capacity_wait {
                wait.await;
            }
        }, if response_state_capacity_blocked && has_response_state_capacity_wait => {
            // ACK-gap target selection reads the bounded reinjection queues
            // after this waiter is enabled. A release between a negative
            // selection and this poll is therefore retained rather than lost.
            if response_recovery_capacity_blocked {
                response_recovery_dirty = true;
            }
            continue;
        }
        _ = async {
            if let Some(wait) = response_requalification_capacity_wait.as_mut() {
                wait.as_mut().await;
            }
        }, if response_requalification_capacity_blocked => {
            // Requalification owns only this exact maintenance wake. Ordinary
            // Product work never inherits its blockage or retry state.
            response_requalification_capacity_wait = None;
            continue;
        }
        _ = tokio::time::sleep_until(queued_send_retry_deadline), if queued_send_blocked => {
            response_sender_retry_at = None;
            continue;
        }
        _ = tokio::time::sleep_until(recv_progress_deadline), if (reliable_relay_recv_progress_timer_enabled(
                request_feedback_underlay,
                multipath_reinjection_alternative_available,
            )
            && reliable_relay_recv_progress_resend_active(
                &recv_stream,
                remote_open,
                Some(request_feedback_underlay),
            )) || recv_progress_ack_update_pending => {
            let resend_progress = reliable_relay_recv_progress_timer_enabled(
                    request_feedback_underlay,
                    multipath_reinjection_alternative_available,
                ) && reliable_relay_recv_progress_resend_active(
                    &recv_stream,
                    remote_open,
                    Some(request_feedback_underlay),
                );
            if enqueue_tcp_recv_progress(
                path_stream,
                &mut recv_stream,
                &mut recv_progress,
                &mut request_ack_publication,
                request_feedback_path_snapshot,
                request_lane,
                mux_limits,
                true,
                resend_progress,
                resend_progress,
            ) {
                response_sender_retry_at = None;
                last_recv_progress_sent_at = Instant::now();
            }
            let queued_nondata_ready = response_product.lock().sender.queued_nondata_ready();
            if queued_nondata_ready
                && drain_shared_server_response_sender_ready(
                    &response_product, path_stream, response_lane, mux_limits,
                    sender_dispatch_byte_budget, sender_dispatch_item_budget, &mut tail_copy_wake_at, session_id,
                )?
            {
                response_sender_retry_at =
                    Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
            }
        }
        _ = std::future::ready(()), if can_send_pending_fin => {
            {
            let mut product = response_product.lock();
            let ResponseProductState {
                sender: response_sender, send_stream, ..
            } = &mut *product;
                if !pending_local_fin || !response_sender.is_empty() || close.sent {
                    continue;
                }
            let frame = Frame::StreamFin {
                stream_id,
                final_offset: send_stream.next_offset(),
            };
            response_sender.enqueue_final_control_frame(frame);
            response_sender_retry_at = None;
            close.sent = true;
            pending_local_fin = false;
                product.prepared.work_changed.notify_waiters();
            }
            if drain_shared_server_response_sender_ready(
                &response_product, path_stream, response_lane, mux_limits,
                sender_dispatch_byte_budget, sender_dispatch_item_budget, &mut tail_copy_wake_at, session_id,
            )?
            {
                response_sender_retry_at =
                    Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot));
            }
        }
        _ = std::future::ready(()), if queued_send_ready => {
            if drain_shared_server_response_sender_ready(
                &response_product, path_stream, response_lane, mux_limits,
                sender_dispatch_byte_budget, sender_dispatch_item_budget, &mut tail_copy_wake_at, session_id,
            )?
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
                read_reliable_relay_payload(&mut local, &mut buf, reserved_read_budget, read_budget).await;
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
                // EOF does not revoke prepared data. FIN waits until U is zero.
                response_product.lock().prepared.work_changed.notify_waiters();
            } else {
                {
                    let mut product = response_product.lock();
                    let payload = payload.expect("positive read returns payload");
                    let enqueue_id = product.sender.enqueue_data_for_lane(payload, response_lane);
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = enqueue_id;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "server_sender_enqueue",
                        format_args!(
                            "session_id={} stream_id={} enqueue_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} send_credit_bytes={} reinjection_bytes={}",
                            session_id.0, stream_id.0, enqueue_id, response_lane, read,
                            product.sender.bytes(), sender_queue_limit,
                            product.send_stream.send_credit_bytes(),
                            product.send_stream.reinjection_bytes(),
                        ),
                    );
                    publish_prepared_response_work(
                        &mut product, &response_product, response_lane, adaptive_chunk, true,
                    );
                }
                let mut opportunistic_reads = 1usize;
                while local_open && opportunistic_reads < sender_dispatch_item_budget {
                    // A writer may have claimed source while the socket was
                    // serviced. Re-read U/O/credit, then release Product before
                    // the actual reservation/read future is even constructed.
                    let next_read_budget = {
                        let product = response_product.lock();
                        if !product.sender.can_read_product_source(
                            local_open, false, &product.send_stream, sender_queue_limit,
                        ) || product.sender.data_bytes() >= sender_dispatch_byte_budget {
                            0
                        } else {
                            let outstanding = reliable_relay_current_data_ack_outstanding_bytes(
                                response_lane, &product.send_stream,
                                product.send_stream.data_ack_frontier(),
                            );
                            let headroom = reliable_relay_response_source_staging_headroom(
                                response_lane, inflight_limit, outstanding, product.sender.data_bytes(),
                            );
                            product.sender.read_budget(
                                &product.send_stream, sender_queue_limit, buf.len(),
                            ).min(headroom)
                        }
                    };
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
                                &mut local, &mut buf, permit.bytes(), next_read_budget,
                            ).await;
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
                        response_product.lock().prepared.work_changed.notify_waiters();
                        break;
                    }
                    {
                        let mut product = response_product.lock();
                        let payload = payload.expect("positive read returns payload");
                        let enqueue_id = product.sender.enqueue_data_for_lane(payload, response_lane);
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = enqueue_id;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "server_sender_enqueue",
                            format_args!(
                                "session_id={} stream_id={} enqueue_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} send_credit_bytes={} reinjection_bytes={} opportunistic=true",
                                session_id.0, stream_id.0, enqueue_id, response_lane, read,
                                product.sender.bytes(), sender_queue_limit,
                                product.send_stream.send_credit_bytes(),
                                product.send_stream.reinjection_bytes(),
                            ),
                        );
                        publish_prepared_response_work(
                            &mut product, &response_product, response_lane, adaptive_chunk, true,
                        );
                    }
                    opportunistic_reads = opportunistic_reads.saturating_add(1);
                }
                {
                    let product = response_product.lock();
                    refresh_server_response_flow_demand(
                        &mut response_flow_demand, &product.sender, &product.send_stream,
                        response_classifier_path, mux_limits,
                    );
                }
                if drain_shared_server_response_sender_ready(
                    &response_product, path_stream, response_lane, mux_limits,
                    sender_dispatch_byte_budget, sender_dispatch_item_budget,
                    &mut tail_copy_wake_at, session_id,
                )? {
                    response_sender_retry_at = Some(
                        tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot),
                    );
                }
            }
        }
        else => break Ok(stats),
        }
    };
    if result.is_ok() && pending_local_fin && !close.sent {
        while result.is_ok() {
            let (queue_empty, nondata_ready, bound_deadline, prepared_work_wait) = {
                let mut product = response_product.lock();
                product.sender.discard_stale_bound_reinjections(path_stream);
                let quantum = product.prepared.data_quantum_bytes;
                publish_prepared_response_work(
                    &mut product,
                    &response_product,
                    close.lane,
                    quantum,
                    false,
                );
                let mut wait = Box::pin(product.prepared.work_changed.clone().notified_owned());
                wait.as_mut().enable();
                (
                    product.sender.is_empty(),
                    product.sender.queued_nondata_ready(),
                    product.sender.bound_reinjection_deadline(),
                    wait,
                )
            };
            if queue_empty {
                break;
            }
            let drained = drain_shared_server_response_sender_ready(
                &response_product,
                path_stream,
                close.lane,
                mux_limits,
                last_sender_dispatch_byte_budget,
                last_sender_dispatch_item_budget,
                &mut tail_copy_wake_at,
                session_id,
            );
            match drained {
                Ok(false) if nondata_ready => {}
                Ok(_) => {
                    let capacity_notifies = path_stream.capacity_notifies();
                    let has_capacity_notify = !capacity_notifies.is_empty();
                    let retry_at = tokio::time::Instant::now()
                        + sender_service_retry_delay(path_stream.send_path_snapshot(close.lane, 0));
                    let wake_at = bound_deadline
                        .map(tokio::time::Instant::from_std)
                        .map_or(retry_at, |deadline| deadline.min(retry_at));
                    tokio::select! {
                        () = prepared_work_wait => {}
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
                Err(err) => result = Err(err),
            }
        }
        let final_queued = if result.is_ok() {
            let mut product = response_product.lock();
            if product.sender.is_empty() {
                let frame = Frame::StreamFin {
                    stream_id,
                    final_offset: product.send_stream.next_offset(),
                };
                product.sender.enqueue_final_control_frame(frame);
                true
            } else {
                false
            }
        } else {
            false
        };
        while final_queued && result.is_ok() && !close.sent {
            let dispatched = {
                let mut product = response_product.lock();
                let ResponseProductState {
                    sender,
                    send_stream,
                    last_send_ack,
                    ..
                } = &mut *product;
                let outstanding = reliable_relay_current_data_ack_outstanding_bytes(
                    close.lane,
                    send_stream,
                    send_stream.data_ack_frontier(),
                );
                sender.dispatch_next_nondata_at_frontier(
                    path_stream,
                    send_stream,
                    close.lane,
                    mux_limits,
                    outstanding,
                    server_data_ack_frontier_state(last_send_ack, send_stream),
                )
            };
            match dispatched {
                Ok(dispatch) if dispatch.lane == ReliableWorkClass::Control => {
                    close.sent = true;
                }
                Ok(_) => {
                    result = Err(RuntimeError::Protocol(
                        "server response sender dispatched non-control final close",
                    ));
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    let capacity_notifies = path_stream.capacity_notifies();
                    let has_capacity_notify = !capacity_notifies.is_empty();
                    let retry_at = tokio::time::Instant::now()
                        + sender_service_retry_delay(path_stream.send_path_snapshot(close.lane, 0));
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
                        _ = tokio::time::sleep_until(retry_at) => {}
                    }
                }
                Err(err) => result = Err(err),
            }
        }
    }
    {
        let mut product = response_product.lock();
        observe_server_response_claims(&product, &mut observed_response_claimed_offset, &mut stats);
        if let Some(error) = product.prepared.pending_error.take() {
            result = Err(error);
        }
        // Revoke before ordinary-return cleanup awaits; actor_lifetime also
        // performs this transition if the whole future is cancelled.
        product.prepared.claims_active = false;
        product.prepared.registrations.clear();
        product.prepared.work_changed.notify_waiters();
    }
    result.map(|_| stats)
}

#[cfg(test)]
#[path = "tests_server.rs"]
mod tests;
