//! Client relay progress, additional-path establishment, and recovery lifecycle.
//!
//! The bidirectional actor remains in `control`; this owner decides when
//! progress is authoritative, when completion is safe, and how path loss or
//! stalls open and attach replacement carriers.

use super::client::ClientRelayPathOpenSuppressions;
use super::open::{
    ReliableRelayOpenSpec, open_remote_stream_for_relay_path, relay_path_open_error_is_retryable,
};
use super::remote::{
    ReliableRelayAttachMode, ReliableRelayPathLanes,
    attach_reliable_relay_paths_with_claims_and_suppressions,
    reliable_relay_additional_path_open_payload_bytes, reliable_relay_attach_payload_bytes,
    reliable_relay_path_open_candidates_after_suppression,
    reliable_relay_reinjection_path_candidates,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{QUIC_PERSISTENT_CONGESTION_THRESHOLD, reliable_relay_buffer_len};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::protocol::{Frame, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::sender::{ReliableRelaySenderQueue, RequestSenderService};
use crate::runtime::stream::{
    OpenedRemoteStream, ReliableRelayAttachOutcome, ReliableRelayOpenedStartup,
    ReliableRelayRemoteSet, ReliableRelayReturnCandidate, ReliableRelayReturnPlan,
};
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReliableReturnCandidateSettlement {
    Unresolved,
    Opening,
    Accepted(RelayPathInstance),
    Failed,
}

/// Serialized requester state for the one-shot response return-plan round.
///
/// This state owns no Product score, rate, queue, or retry clock. It only
/// resolves the immutable exact carrier transcript and retains FINAL until
/// response progress proves that the peer applied it.
pub(super) struct ClientReliableReturnPlan {
    plan: Arc<ReliableRelayReturnPlan>,
    bound_instances: Vec<Option<CarrierPathInstanceId>>,
    settlements: Vec<ReliableReturnCandidateSettlement>,
    response_triggered: bool,
    final_retained: Option<Vec<u8>>,
    done: bool,
}

impl ClientReliableReturnPlan {
    pub(super) fn from_initial_open(
        startup: ReliableRelayOpenedStartup,
        opening_instance: RelayPathInstance,
    ) -> Result<Self, RuntimeError> {
        let opening =
            startup
                .plan
                .candidate(startup.opening_ordinal)
                .ok_or(RuntimeError::Protocol(
                    "opening return ordinal is out of range",
                ))?;
        if opening.key != opening_instance.key
            || opening
                .path_instance_id
                .is_some_and(|frozen| frozen != opening_instance.path_instance_id)
        {
            return Err(RuntimeError::Protocol(
                "opening return ordinal does not bind the accepted exact carrier",
            ));
        }
        let mut settlements =
            vec![ReliableReturnCandidateSettlement::Unresolved; startup.plan.candidates().len()];
        settlements[usize::from(startup.opening_ordinal)] =
            ReliableReturnCandidateSettlement::Accepted(opening_instance);
        for ordinal in startup.failed_ordinals {
            let settlement =
                settlements
                    .get_mut(usize::from(ordinal))
                    .ok_or(RuntimeError::Protocol(
                        "failed return ordinal is out of range",
                    ))?;
            if !matches!(settlement, ReliableReturnCandidateSettlement::Unresolved) {
                return Err(RuntimeError::Protocol(
                    "initial return ordinal settled more than once",
                ));
            }
            *settlement = ReliableReturnCandidateSettlement::Failed;
        }
        let done = settlements.len() == 1;
        let mut bound_instances = startup
            .plan
            .candidates()
            .iter()
            .map(|candidate| candidate.path_instance_id)
            .collect::<Vec<_>>();
        bound_instances[usize::from(startup.opening_ordinal)] =
            Some(opening_instance.path_instance_id);
        Ok(Self {
            plan: startup.plan,
            bound_instances,
            settlements,
            response_triggered: false,
            final_retained: None,
            done,
        })
    }

    #[cfg(test)]
    pub(super) fn plan(&self) -> &Arc<ReliableRelayReturnPlan> {
        &self.plan
    }

    pub(super) fn is_done(&self) -> bool {
        self.done
    }

    pub(super) fn observe_response_frontier(&mut self, frontier: u64) -> bool {
        if self.done {
            return false;
        }
        if self.final_retained.is_some() {
            if frontier > self.plan.trigger_bytes() {
                self.done = true;
                self.final_retained = None;
            }
            return false;
        }
        if !self.response_triggered && frontier >= self.plan.trigger_bytes() {
            self.response_triggered = true;
            return true;
        }
        false
    }

    pub(super) fn observe_response_terminal(
        &mut self,
        final_offset: u64,
        contiguous_frontier: u64,
    ) {
        if self.done {
            return;
        }
        if final_offset > self.plan.trigger_bytes() || contiguous_frontier >= final_offset {
            self.done = true;
            self.final_retained = None;
            return;
        }
        // A terminal declaration received ahead of missing low response bytes
        // does not prove that a server-enrolled attachment has no exclusive
        // copy. Do not create new startup work after FIN, but let operations
        // already in flight settle and publish the exact retained/omitted
        // receipt before the peer recovers any ghost-owned range.
        self.response_triggered = true;
        for settlement in &mut self.settlements {
            if matches!(settlement, ReliableReturnCandidateSettlement::Unresolved) {
                *settlement = ReliableReturnCandidateSettlement::Failed;
            }
        }
    }

    fn candidate_settlement_mut(
        &mut self,
        ordinal: u8,
    ) -> Result<&mut ReliableReturnCandidateSettlement, RuntimeError> {
        self.settlements
            .get_mut(usize::from(ordinal))
            .ok_or(RuntimeError::Protocol(
                "return startup ordinal is out of range",
            ))
    }

    pub(super) fn begin_candidate_for_open(
        &mut self,
        key: RelayPathKey,
        path_instance_id: Option<CarrierPathInstanceId>,
    ) -> Option<u8> {
        if self.done || self.final_retained.is_some() {
            return None;
        }
        let candidate = self.plan.candidate_for_key(key)?;
        let ordinal = candidate.ordinal;
        let index = usize::from(ordinal);
        if let Some(frozen) = self.bound_instances[index]
            && path_instance_id != Some(frozen)
        {
            return None;
        }
        if !matches!(
            self.settlements[index],
            ReliableReturnCandidateSettlement::Unresolved
        ) {
            return None;
        }
        if let Some(path_instance_id) = path_instance_id {
            self.bound_instances[index] = Some(path_instance_id);
        }
        self.settlements[index] = ReliableReturnCandidateSettlement::Opening;
        Some(ordinal)
    }

    pub(super) fn begin_unresolved_after_response_trigger(
        &mut self,
    ) -> Vec<ReliableRelayReturnCandidate> {
        if self.done || !self.response_triggered {
            return Vec::new();
        }
        let mut candidates = Vec::new();
        for candidate in self.plan.candidates().iter().copied() {
            let settlement = &mut self.settlements[usize::from(candidate.ordinal)];
            if matches!(settlement, ReliableReturnCandidateSettlement::Unresolved) {
                *settlement = ReliableReturnCandidateSettlement::Opening;
                candidates.push(candidate);
            }
        }
        candidates
    }

    pub(super) fn settle_failed(&mut self, ordinal: u8) -> Result<(), RuntimeError> {
        let settlement = self.candidate_settlement_mut(ordinal)?;
        if !matches!(settlement, ReliableReturnCandidateSettlement::Opening) {
            return Err(RuntimeError::Protocol(
                "return startup failure did not settle one opening ordinal",
            ));
        }
        *settlement = ReliableReturnCandidateSettlement::Failed;
        Ok(())
    }

    pub(super) fn settle_accepted(
        &mut self,
        ordinal: u8,
        instance: RelayPathInstance,
    ) -> Result<(), RuntimeError> {
        let candidate = self.plan.candidate(ordinal).ok_or(RuntimeError::Protocol(
            "accepted return ordinal is out of range",
        ))?;
        if candidate.key != instance.key {
            return Err(RuntimeError::Protocol(
                "return startup successor cannot inherit a frozen ordinal",
            ));
        }
        let binding = &mut self.bound_instances[usize::from(ordinal)];
        if binding.is_some_and(|frozen| frozen != instance.path_instance_id) {
            return Err(RuntimeError::Protocol(
                "return startup successor cannot inherit a frozen ordinal",
            ));
        }
        *binding = Some(instance.path_instance_id);
        let settlement = self.candidate_settlement_mut(ordinal)?;
        if !matches!(settlement, ReliableReturnCandidateSettlement::Opening) {
            return Err(RuntimeError::Protocol(
                "return startup acceptance did not settle one opening ordinal",
            ));
        }
        *settlement = ReliableReturnCandidateSettlement::Accepted(instance);
        Ok(())
    }

    pub(super) fn bound_instance(&self, ordinal: u8) -> Option<CarrierPathInstanceId> {
        self.bound_instances
            .get(usize::from(ordinal))
            .copied()
            .flatten()
    }

    pub(super) fn bind_unresolved_slot(
        &mut self,
        ordinal: u8,
        path_instance_id: CarrierPathInstanceId,
    ) -> Result<bool, RuntimeError> {
        let binding =
            self.bound_instances
                .get_mut(usize::from(ordinal))
                .ok_or(RuntimeError::Protocol(
                    "return startup ordinal is out of range",
                ))?;
        if binding.is_some_and(|frozen| frozen != path_instance_id) {
            return Ok(false);
        }
        *binding = Some(path_instance_id);
        Ok(true)
    }

    pub(super) fn prepare_final(&mut self, remotes: &ReliableRelayRemoteSet) -> Option<&[u8]> {
        if self.done || self.final_retained.is_some() {
            return self.final_retained.as_deref();
        }
        if self.settlements.iter().any(|settlement| {
            matches!(
                settlement,
                ReliableReturnCandidateSettlement::Unresolved
                    | ReliableReturnCandidateSettlement::Opening
            )
        }) {
            return None;
        }
        let retained = self
            .settlements
            .iter()
            .enumerate()
            .filter_map(|(ordinal, settlement)| {
                let ReliableReturnCandidateSettlement::Accepted(instance) = settlement else {
                    return None;
                };
                remotes
                    .contains_path_instance(*instance)
                    .then_some(ordinal as u8)
            })
            .collect::<Vec<_>>();
        self.final_retained = Some(retained);
        self.final_retained.as_deref()
    }

    #[cfg(test)]
    fn settlement(&self, ordinal: u8) -> Option<ReliableReturnCandidateSettlement> {
        self.settlements.get(usize::from(ordinal)).copied()
    }
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
    lane: TrafficClass,
) -> Result<Option<bool>, RuntimeError> {
    if remotes.is_empty() {
        return Ok(None);
    }

    send_stream.update_max_offset(remotes.max_offset());
    let recovery =
        sender.drive_request_path_recovery(sender_queue, context, remotes, send_stream, lane);
    Ok(Some(recovery.queued))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn switch_reliable_relay_to_best_path(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lanes: ReliableRelayPathLanes,
    remotes: &mut ReliableRelayRemoteSet,
    startup: &mut ClientReliableReturnPlan,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
    pending_additional_path_opens: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) -> Result<bool, RuntimeError> {
    let inflight_path_claims = pending_additional_path_opens
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    let attached = attach_reliable_relay_paths_with_claims_and_suppressions(
        context,
        spec,
        lanes,
        remotes,
        startup,
        send_stream,
        resend_fin,
        mode,
        path_open_suppressions,
        &inflight_path_claims,
    )
    .await?;
    if attached == 0 {
        return Ok(false);
    }
    Ok(true)
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
pub(super) fn try_handle_additional_path_open_result(
    stream_id: StreamId,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    resend_fin: bool,
    output_lane: TrafficClass,
    additional_path_open: RelayAdditionalPathOpenResult,
    pending_count: usize,
    last_stream_progress_at: &mut Instant,
) -> Result<Option<ReliableRelayAttachMode>, RuntimeError> {
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (stream_id, pending_count);
    let mode = additional_path_open.mode;
    match additional_path_open.result {
        Ok(opened) => {
            if additional_path_open
                .startup_expected_instance
                .is_some_and(|expected| opened.path_instance_id() != expected)
            {
                // A successor may use this configured slot as ordinary later
                // topology, but it cannot complete the predecessor's frozen
                // startup ordinal.
                opened.retire_uncommitted();
                return Ok(None);
            }
            #[cfg(feature = "lab-diagnostics")]
            let lane = opened.stream().lane;
            if resend_fin
                && let Err(err) =
                    opened
                        .stream()
                        .try_enqueue_request_control_frame(Frame::StreamFin {
                            stream_id,
                            final_offset: send_stream.next_offset(),
                        })
            {
                // The completed open never entered attachment-set ownership.
                // Retire it through the carrier mailbox so bounded control
                // capacity cannot delay an armed Product recovery deadline.
                opened.retire_uncommitted();
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
                return Ok(None);
            }
            match remotes.try_attach_candidate(opened)? {
                ReliableRelayAttachOutcome::Attached => {
                    // Demand may change while the open future is pending.
                    // Attachment commits to the stream owner's current output
                    // lane, never the stale lane captured by that future.
                    remotes.set_lane(output_lane);
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
                    Ok(Some(mode))
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
                    Ok(None)
                }
            }
        }
        Err(err @ RuntimeError::ExactIdentityExhausted) => Err(err),
        Err(RuntimeError::ReliablePathAttachmentRefused) => {
            // Attachment refusal is stream-local and says nothing about the
            // health of the authenticated carrier.
            Ok(None)
        }
        Err(err) if relay_path_open_error_is_retryable(additional_path_open.key.underlay, &err) => {
            // Retryability controls only Product work reselection. The exact
            // carrier actor independently owns carrier-instance failure.
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
            Ok(None)
        }
        Err(err) => {
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
            Ok(None)
        }
    }
}

#[cfg(test)]
// This test adapter mirrors the complete asynchronous path-open ownership
// envelope and deliberately adds no second fixture-only aggregate.
#[allow(clippy::too_many_arguments)]
pub(super) fn handle_additional_path_open_result(
    stream_id: StreamId,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    resend_fin: bool,
    output_lane: TrafficClass,
    additional_path_open: RelayAdditionalPathOpenResult,
    pending_count: usize,
    last_stream_progress_at: &mut Instant,
) -> Option<ReliableRelayAttachMode> {
    try_handle_additional_path_open_result(
        stream_id,
        remotes,
        send_stream,
        resend_fin,
        output_lane,
        additional_path_open,
        pending_count,
        last_stream_progress_at,
    )
    .expect("test request attachment identity space")
}

pub(super) fn settle_client_return_plan_open_result(
    startup: &mut ClientReliableReturnPlan,
    remotes: &ReliableRelayRemoteSet,
    key: RelayPathKey,
    startup_ordinal: Option<u8>,
    attached: bool,
) -> Result<(), RuntimeError> {
    let Some(ordinal) = startup_ordinal else {
        return Ok(());
    };
    if attached && let Some(instance) = remotes.path_instance_for_key(key) {
        return startup.settle_accepted(ordinal, instance);
    }
    startup.settle_failed(ordinal)
}

// Keep the relay and stream owners visible across the asynchronous attach boundary.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_drain_completed_additional_path_opens(
    stream_id: StreamId,
    startup: &mut ClientReliableReturnPlan,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    resend_fin: bool,
    output_lane: TrafficClass,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    additional_path_open_rx: &mut mpsc::Receiver<RelayAdditionalPathOpenResult>,
    last_stream_progress_at: &mut Instant,
) -> Result<bool, RuntimeError> {
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
                opened.retire_uncommitted();
            }
            continue;
        }
        let key = additional_path_open.key;
        let startup_ordinal = additional_path_open.startup_ordinal;
        let result_attached = try_handle_additional_path_open_result(
            stream_id,
            remotes,
            send_stream,
            resend_fin,
            output_lane,
            additional_path_open,
            pending.len(),
            last_stream_progress_at,
        )?
        .is_some();
        settle_client_return_plan_open_result(
            startup,
            remotes,
            key,
            startup_ordinal,
            result_attached,
        )?;
        attached |= result_attached;
    }
    Ok(attached)
}

#[cfg(test)]
// This test adapter mirrors the production drain ownership envelope exactly.
#[allow(clippy::too_many_arguments)]
pub(super) fn drain_completed_additional_path_opens(
    stream_id: StreamId,
    startup: &mut ClientReliableReturnPlan,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    resend_fin: bool,
    output_lane: TrafficClass,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    additional_path_open_rx: &mut mpsc::Receiver<RelayAdditionalPathOpenResult>,
    last_stream_progress_at: &mut Instant,
) -> bool {
    try_drain_completed_additional_path_opens(
        stream_id,
        startup,
        remotes,
        send_stream,
        resend_fin,
        output_lane,
        pending,
        additional_path_open_rx,
        last_stream_progress_at,
    )
    .expect("test request attachment identity space")
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

pub(super) struct RelayAdditionalPathOpenResult {
    pub(super) key: RelayPathKey,
    pub(super) generation: RelayAdditionalPathOpenGeneration,
    pub(super) mode: ReliableRelayAttachMode,
    pub(super) startup_ordinal: Option<u8>,
    pub(super) startup_expected_instance: Option<CarrierPathInstanceId>,
    pub(super) result: Result<OpenedRemoteStream, RuntimeError>,
}

pub(super) struct RelayAdditionalPathOpenTask {
    generation: RelayAdditionalPathOpenGeneration,
    #[cfg(test)]
    startup_ordinal: Option<u8>,
    #[cfg(test)]
    startup_expected_instance: Option<CarrierPathInstanceId>,
    #[cfg(feature = "lab-diagnostics")]
    lane: TrafficClass,
    handle: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy)]
struct RelayAdditionalPathOpenCandidate {
    key: RelayPathKey,
    startup_ordinal: Option<u8>,
    startup_expected_instance: Option<CarrierPathInstanceId>,
}

fn classify_reliable_relay_open_candidate(
    context: &ClientPathContext,
    startup: &mut ClientReliableReturnPlan,
    key: RelayPathKey,
) -> RelayAdditionalPathOpenCandidate {
    let current_instance = context
        .health()
        .lock()
        .expect("client path health lock")
        .path_record(key)
        .and_then(|record| record.path_instance_id());
    let startup_ordinal = startup.begin_candidate_for_open(key, current_instance);
    RelayAdditionalPathOpenCandidate {
        key,
        startup_ordinal,
        startup_expected_instance: startup_ordinal
            .and_then(|ordinal| startup.bound_instance(ordinal)),
    }
}

pub(super) fn take_matching_additional_path_open(
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    key: RelayPathKey,
    generation: RelayAdditionalPathOpenGeneration,
) -> Option<RelayAdditionalPathOpenTask> {
    if !matching_additional_path_open_pending(pending, key, generation) {
        return None;
    }
    pending.remove(&key)
}

pub(super) fn matching_additional_path_open_pending(
    pending: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    key: RelayPathKey,
    generation: RelayAdditionalPathOpenGeneration,
) -> bool {
    pending
        .get(&key)
        .is_some_and(|task| task.generation == generation)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_reliable_relay_additional_path_opens(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    startup: &mut ClientReliableReturnPlan,
    lanes: ReliableRelayPathLanes,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> Result<bool, RuntimeError> {
    if !lanes.selection.is_bulk() {
        return Ok(false);
    }
    if !pending.is_empty() {
        return Ok(false);
    }
    let stream_id = remotes.stream_id();
    let payload_bytes =
        reliable_relay_additional_path_open_payload_bytes(send_stream, context.mux_limits);
    let candidates = reliable_relay_additional_path_open_candidates(
        context,
        remotes,
        lanes.selection,
        payload_bytes,
    );
    let candidates = reliable_relay_bulk_path_open_candidates(
        context,
        candidates,
        path_open_suppressions,
        pending,
    );
    if candidates.is_empty() {
        return Ok(false);
    }
    let candidates = candidates
        .into_iter()
        .map(|key| classify_reliable_relay_open_candidate(context, startup, key))
        .collect::<Vec<_>>();
    Ok(spawn_reliable_relay_path_opens(
        context,
        spec,
        lanes.output,
        ReliableRelayAttachMode::BulkStriping,
        stream_id,
        candidates,
        pending,
        result_tx,
    ))
}

/// Opens the exact unresolved frozen candidates once the contiguous response
/// frontier reaches `h`. Stale exact instances settle as failed; a same-slot
/// successor remains outside the startup ordinal space.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_reliable_relay_response_startup_path_opens(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    startup: &mut ClientReliableReturnPlan,
    output_lane: TrafficClass,
    stream_id: StreamId,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> Result<bool, RuntimeError> {
    let frozen = startup.begin_unresolved_after_response_trigger();
    let mut candidates = Vec::new();
    for candidate in frozen {
        let current_instance = context
            .health()
            .lock()
            .expect("client path health lock")
            .path_record(candidate.key)
            .and_then(|record| record.path_instance_id());
        let expected_instance = startup.bound_instance(candidate.ordinal);
        if expected_instance.is_some() && current_instance != expected_instance
            || pending.contains_key(&candidate.key)
        {
            startup.settle_failed(candidate.ordinal)?;
            continue;
        }
        if let Some(current_instance) = current_instance
            && !startup.bind_unresolved_slot(candidate.ordinal, current_instance)?
        {
            startup.settle_failed(candidate.ordinal)?;
            continue;
        }
        candidates.push(RelayAdditionalPathOpenCandidate {
            key: candidate.key,
            startup_ordinal: Some(candidate.ordinal),
            startup_expected_instance: startup.bound_instance(candidate.ordinal),
        });
    }
    if candidates.is_empty() {
        return Ok(false);
    }
    let spawned = spawn_reliable_relay_path_opens(
        context,
        spec,
        output_lane,
        ReliableRelayAttachMode::Startup,
        stream_id,
        candidates,
        pending,
        result_tx,
    );
    #[cfg(test)]
    if spawned {
        context.record_response_startup_open_round_for_test();
    }
    Ok(spawned)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_reliable_relay_recovery_path_open(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    startup: &mut ClientReliableReturnPlan,
    lanes: ReliableRelayPathLanes,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> bool {
    if !reliable_relay_should_open_recovery_path(remotes) || !pending.is_empty() {
        return false;
    }
    let payload_bytes =
        reliable_relay_attach_payload_bytes(send_stream, lanes.selection, context.mux_limits);
    let candidates = reliable_relay_reinjection_path_candidates(
        context,
        remotes,
        lanes.selection,
        payload_bytes,
    );
    let candidates = reliable_relay_recovery_path_open_candidates(
        context,
        candidates,
        path_open_suppressions,
        pending,
    );
    if candidates.is_empty() {
        return false;
    }
    spawn_reliable_relay_path_opens(
        context,
        spec,
        lanes.output,
        ReliableRelayAttachMode::Recovery,
        remotes.stream_id(),
        candidates
            .into_iter()
            .map(|key| classify_reliable_relay_open_candidate(context, startup, key))
            .collect(),
        pending,
        result_tx,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_reliable_relay_disconnected_path_open(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    startup: &mut ClientReliableReturnPlan,
    lanes: ReliableRelayPathLanes,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
    attempted_paths: &mut HashSet<RelayPathKey>,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> bool {
    if !remotes.is_empty() || !pending.is_empty() {
        return false;
    }
    let payload_bytes =
        reliable_relay_attach_payload_bytes(send_stream, lanes.selection, context.mux_limits);
    let candidates = reliable_relay_reinjection_path_candidates(
        context,
        remotes,
        lanes.selection,
        payload_bytes,
    );
    let candidates = reliable_relay_path_open_candidates_after_suppression(
        context,
        candidates,
        path_open_suppressions,
    );
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
        lanes.output,
        ReliableRelayAttachMode::Recovery,
        remotes.stream_id(),
        vec![classify_reliable_relay_open_candidate(
            context, startup, key,
        )],
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
    candidates: Vec<RelayAdditionalPathOpenCandidate>,
    pending: &mut HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
    result_tx: &mpsc::Sender<RelayAdditionalPathOpenResult>,
) -> bool {
    let mut spawned = false;
    for candidate in candidates {
        let key = candidate.key;
        match key.underlay {
            UnderlayProtocol::Tcp if context.tcp_paths.get(key.index).is_some() => {}
            UnderlayProtocol::Udp if context.udp_paths.get(key.index).is_some() => {}
            _ => continue,
        }
        let context = context.clone();
        let spec = candidate.startup_ordinal.map_or_else(
            || spec.for_ordinary_attachment(),
            |ordinal| spec.for_startup_ordinal(ordinal),
        );
        let result_tx = result_tx.clone();
        let generation = next_relay_additional_path_open_generation();
        #[cfg(test)]
        let fail_response_startup_open = matches!(mode, ReliableRelayAttachMode::Startup)
            && context.response_startup_open_failure_for_test();
        let handle = tokio::spawn(async move {
            #[cfg(test)]
            let result = if fail_response_startup_open {
                Err(RuntimeError::NoSchedulableTcpPath)
            } else {
                open_remote_stream_for_relay_path(&context, stream_id, &spec, lane, key).await
            };
            #[cfg(not(test))]
            let result =
                open_remote_stream_for_relay_path(&context, stream_id, &spec, lane, key).await;
            let message = RelayAdditionalPathOpenResult {
                key,
                generation,
                mode,
                startup_ordinal: candidate.startup_ordinal,
                startup_expected_instance: candidate.startup_expected_instance,
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
                #[cfg(test)]
                startup_ordinal: candidate.startup_ordinal,
                #[cfg(test)]
                startup_expected_instance: candidate.startup_expected_instance,
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
    context: &ClientPathContext,
    candidates: Vec<RelayPathKey>,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
    pending: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) -> Vec<RelayPathKey> {
    let candidates = reliable_relay_path_open_candidates_after_suppression(
        context,
        candidates,
        path_open_suppressions,
    );
    let mut candidates = reliable_relay_available_path_open_candidates(candidates, pending);
    candidates.truncate(1);
    candidates
}

fn reliable_relay_bulk_path_open_candidates(
    context: &ClientPathContext,
    candidates: Vec<RelayPathKey>,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
    pending: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) -> Vec<RelayPathKey> {
    let candidates = reliable_relay_path_open_candidates_after_suppression(
        context,
        candidates,
        path_open_suppressions,
    );
    reliable_relay_available_path_open_candidates(candidates, pending)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn attach_reliable_relay_paths_with_suppressions(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lanes: ReliableRelayPathLanes,
    remotes: &mut ReliableRelayRemoteSet,
    startup: &mut ClientReliableReturnPlan,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    path_open_suppressions: &ClientRelayPathOpenSuppressions,
    pending_additional_path_opens: &HashMap<RelayPathKey, RelayAdditionalPathOpenTask>,
) -> Result<usize, RuntimeError> {
    // A pending open owns logical (stream, path) membership. Synchronous
    // recovery must not race that claim through either carrier.
    let inflight_path_claims = pending_additional_path_opens
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    attach_reliable_relay_paths_with_claims_and_suppressions(
        context,
        spec,
        lanes,
        remotes,
        startup,
        send_stream,
        resend_fin,
        mode,
        path_open_suppressions,
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
        && (lane == TrafficClass::Throughput
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
    remotes.accepted_path_count() <= 1 && !remotes.is_empty()
}

pub(in crate::runtime) fn reliable_relay_should_open_recovery_path(
    remotes: &ReliableRelayRemoteSet,
) -> bool {
    !remotes.is_empty()
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
#[path = "tests_lifecycle.rs"]
mod tests;
