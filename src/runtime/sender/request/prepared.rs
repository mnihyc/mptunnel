//! Unnumbered request source is claimed only by an imminent native writer.

use super::multipath::{RequestMultipathPlan, RequestRelayNativeCapture};
use super::{
    RequestFrameAdmissionError, RequestFrameProductCommit, RequestProductState,
    RequestQueuedSourceCommit, SharedRequestProduct,
};
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::capacity::adaptive_reliable_relay_reinjection_bytes;
use crate::model::path::RelayPathInstance;
use crate::model::work::{flight_interval_bytes, reliable_live_frontier_reinjection_limit_bytes};
use crate::mux::stream::StreamError;
use crate::protocol::frame::{
    normalize_offset_ranges, offset_ranges_not_covered, reliable_stream_frame_extent,
};
use crate::protocol::{Frame, OffsetRange};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::prepared::{PreparedOriginalClaim, PreparedOriginalRegistration};
use crate::runtime::path::writer_boundary::ReliableWriterReadyGuard;
use crate::runtime::sender::queue::ReliableRelayQueuedWorkKind;
use crate::runtime::sender::work::RelaySendCause;
use crate::runtime::stream::ReliablePathStreamOutput;
use crate::scheduler::TrafficClass;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;

pub(in crate::runtime) type RequestPreparedWake =
    Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Policy and wake ownership live beside the source they govern. Neither a
/// queued notice nor a writer carries a second mutable copy of these values.
pub(in crate::runtime) struct RequestPreparedSource {
    pub(in crate::runtime) request_lane: TrafficClass,
    pub(in crate::runtime) data_quantum_bytes: usize,
    pub(in crate::runtime) claims_active: bool,
    pub(in crate::runtime) registrations: Vec<Arc<PreparedOriginalRegistration>>,
    pub(in crate::runtime) work_changed: Arc<Notify>,
    pub(in crate::runtime) pending_error: Option<RuntimeError>,
    pub(in crate::runtime) last_claimed_at: Option<Instant>,
}

impl RequestPreparedSource {
    pub(in crate::runtime) fn new(request_lane: TrafficClass, data_quantum_bytes: usize) -> Self {
        Self {
            request_lane,
            data_quantum_bytes,
            claims_active: true,
            registrations: Vec::new(),
            work_changed: Arc::new(Notify::new()),
            pending_error: None,
            last_claimed_at: None,
        }
    }
}

fn registration_is_current(
    state: &RequestProductState,
    instance: RelayPathInstance,
    registration: &PreparedOriginalRegistration,
) -> bool {
    state.prepared.claims_active
        && registration.request_instance() == Some(instance)
        && registration.lane() == state.prepared.request_lane
        && state
            .prepared
            .registrations
            .iter()
            .any(|current| std::ptr::eq(current.as_ref(), registration))
        && state
            .remotes
            .paths
            .iter()
            .any(|path| path.instance() == instance && path.stream.product_admission_active())
}

fn frontier(state: &RequestProductState) -> ReliableDataAckFrontierState {
    ReliableDataAckFrontierState::from_authoritative_gap(
        state
            .last_send_ack
            .gap_at(state.send_stream.data_ack_frontier())
            .is_some(),
    )
}

fn arm_notify(notify: Arc<Notify>) -> RequestPreparedWake {
    let mut wait = Box::pin(notify.notified_owned());
    wait.as_mut().enable();
    wait
}

/// Arm before collecting any advisory evidence. Own readiness withdrawal is
/// deliberately excluded: dropping this claim's guard must not wake itself.
fn arm_work_change(
    state: &RequestProductState,
    context: &ClientPathContext,
    instance: RelayPathInstance,
) -> RequestPreparedWake {
    let mut waits = vec![
        arm_notify(state.prepared.work_changed.clone()),
        context.arm_path_model_publication(context.path_model_generation()),
    ];
    for remote in &state.remotes.paths {
        let ReliablePathStreamOutput::Fixed(output) = &remote.stream.output else {
            continue;
        };
        let commands = output.commands();
        if remote.instance() != instance {
            waits.push(arm_notify(commands.writer_boundary().change_notify()));
        }
        if let Some(native) = commands.native_rate_authority() {
            let mut changes = native.accepted_change_cursor();
            waits.push(Box::pin(async move {
                let _ = changes.changed().await;
            }));
        }
    }
    Box::pin(async move {
        let _ = futures::future::select_all(waits).await;
    })
}

fn record_source_error(state: &mut RequestProductState, error: RuntimeError) {
    state.prepared.pending_error = Some(error);
    state.prepared.claims_active = false;
    state.prepared.work_changed.notify_waiters();
}

fn source_matches(
    state: &mut RequestProductState,
    frame: &Frame,
    lane: TrafficClass,
    quantum: usize,
) -> bool {
    state.prepared.request_lane == lane
        && state.prepared.data_quantum_bytes == quantum
        && matches!(frame, Frame::StreamData { offset, .. } if *offset == state.send_stream.next_offset())
        && RequestQueuedSourceCommit {
            send_stream: &mut state.send_stream,
            sender_queue: &mut state.sender_queue,
        }
        .validate(frame)
        .is_ok()
}

/// Other writers offer current scheduling opportunities, not ownership of
/// this source prefix. Sample them again at each selection; an unselected
/// writer withdrawing must not invalidate an otherwise unchanged winner.
fn current_ready_instances(state: &RequestProductState) -> Vec<RelayPathInstance> {
    state
        .remotes
        .paths
        .iter()
        .filter_map(|path| {
            let ReliablePathStreamOutput::Fixed(output) = &path.stream.output else {
                return None;
            };
            output
                .commands()
                .writer_boundary()
                .snapshot()
                .filter(|receipt| receipt.instance() == path.instance().path_instance_id)
                .map(|_| path.instance())
        })
        .collect()
}

struct PreparedRequestRepair {
    frame: Frame,
    selection_quantum: usize,
    avoid: Vec<RelayPathInstance>,
}

enum PreparedRequestRepairCandidate {
    Ready(PreparedRequestRepair),
    Immature(Instant),
    Empty,
}

/// Find only the first uncovered retained successor. Every accepted copy on a
/// current attachment remains coverage after its repeat delay expires; the
/// existing critical recovery producer alone owns same-range revisits.
fn prepared_request_repair_candidate(
    state: &mut RequestProductState,
    context: &ClientPathContext,
) -> PreparedRequestRepairCandidate {
    let head = state.send_stream.data_ack_frontier();
    let retained = state.send_stream.retained_ranges_in_scope(OffsetRange {
        start: head,
        end: state.send_stream.next_offset(),
    });
    let attached = state.remotes.path_instances();
    let mut covered = state
        .sender
        .multipath
        .recovery_copy_ranges()
        .into_iter()
        .filter(|(instance, _)| attached.contains(instance))
        .flat_map(|(_, ranges)| ranges)
        .collect::<Vec<_>>();
    covered.extend(state.sender_queue.queued_reinjection_ranges());
    let uncovered = offset_ranges_not_covered(&retained, &normalize_offset_ranges(covered));
    let Some(range) = uncovered
        .first()
        .copied()
        .filter(|range| range.start > head)
    else {
        // An uncovered logical head retains its critical publication authority.
        return PreparedRequestRepairCandidate::Empty;
    };
    let lane = state.prepared.request_lane;
    let live = state
        .sender
        .multipath
        .owner_capable_instances(context, &state.remotes, lane);
    let selection_quantum = live
        .iter()
        .map(|instance| {
            adaptive_reliable_relay_reinjection_bytes(
                context.reliable_path_snapshot_for_instance(*instance),
                lane,
                context.mux_limits,
            )
        })
        .max()
        .unwrap_or(0);
    let limit = reliable_live_frontier_reinjection_limit_bytes(
        selection_quantum,
        selection_quantum,
        flight_interval_bytes(range.start, range.end),
        state.send_stream.reinjection_bytes(),
        context.mux_limits,
    );
    let Some(uniform) = state.sender.multipath.live_owner_uniform_frontier(
        OffsetRange {
            start: range.start,
            end: range.end.min(range.start.saturating_add(limit as u64)),
        },
        &attached,
    ) else {
        return PreparedRequestRepairCandidate::Empty;
    };
    if uniform.owners.len() != 1 || !live.contains(&uniform.owners[0]) {
        return PreparedRequestRepairCandidate::Empty;
    }
    let Some(frame) = state
        .send_stream
        .first_retransmission_frame_for_range(uniform.range, limit)
    else {
        return PreparedRequestRepairCandidate::Empty;
    };
    let Some((start, end, _)) = reliable_stream_frame_extent(&frame) else {
        return PreparedRequestRepairCandidate::Empty;
    };
    if start != range.start {
        return PreparedRequestRepairCandidate::Empty;
    }
    let Some(timing) = state
        .sender
        .multipath
        .observe_original_recovery_timing_for_range(OffsetRange { start, end }, |owner| {
            context.reliable_path_snapshot_for_instance(owner)
        })
    else {
        return PreparedRequestRepairCandidate::Empty;
    };
    if timing.fallback_at > Instant::now() {
        return PreparedRequestRepairCandidate::Immature(timing.fallback_at);
    }
    if state
        .sender
        .reinjection_suppression_deadline_for_frame(&frame, &state.remotes)
        .is_some()
    {
        return PreparedRequestRepairCandidate::Empty;
    }
    PreparedRequestRepairCandidate::Ready(PreparedRequestRepair {
        frame,
        selection_quantum,
        avoid: uniform.avoid,
    })
}

fn repair_maturity_wait(wake: RequestPreparedWake, deadline: Instant) -> RequestPreparedWake {
    Box::pin(async move {
        tokio::select! {
            () = wake => {}
            () = tokio::time::sleep_until(deadline.into()) => {}
        }
    })
}

fn same_repair_frame(left: &Frame, right: &Frame) -> bool {
    matches!((left, right), (
        Frame::StreamData { stream_id: left_stream, offset: left_offset, payload: left_payload },
        Frame::StreamData { stream_id: right_stream, offset: right_offset, payload: right_payload },
    ) if left_stream == right_stream && left_offset == right_offset
        && left_payload.len() == right_payload.len() && left_payload.as_ptr() == right_payload.as_ptr())
}

fn repair_claim_plan(
    state: &RequestProductState,
    context: &ClientPathContext,
    candidate: &PreparedRequestRepair,
    invoking: RelayPathInstance,
    inputs: super::multipath::RequestRelayNativeInputs,
) -> Option<(
    RequestMultipathPlan,
    super::RequestCompletionTailTarget,
    Frame,
)> {
    let (_, _, scoring_bytes) = reliable_stream_frame_extent(&candidate.frame)?;
    let ready_instances = state
        .remotes
        .paths
        .iter()
        .filter_map(|path| {
            let instance = path.instance();
            if instance == invoking {
                return Some(instance);
            }
            let ReliablePathStreamOutput::Fixed(output) = &path.stream.output else {
                return None;
            };
            output
                .commands()
                .background_repair_ready()
                .filter(|receipt| receipt.instance() == instance.path_instance_id)
                .map(|_| instance)
        })
        .collect::<Vec<_>>();
    let (plan, target) = state.sender.multipath.repair_claim_plan_from_inputs(
        context,
        &state.remotes,
        &candidate.frame,
        state.prepared.request_lane,
        &state.sender_queue,
        state.send_stream.reinjection_bytes(),
        scoring_bytes,
        &candidate.avoid,
        &ready_instances,
        inputs,
    )?;
    let quantum = adaptive_reliable_relay_reinjection_bytes(
        Some(target.snapshot),
        state.prepared.request_lane,
        context.mux_limits,
    );
    let limit = reliable_live_frontier_reinjection_limit_bytes(
        quantum,
        candidate.selection_quantum,
        scoring_bytes,
        state.send_stream.reinjection_bytes(),
        context.mux_limits,
    )
    .min(target.service_limit_bytes);
    let Frame::StreamData {
        stream_id,
        offset,
        payload,
    } = &candidate.frame
    else {
        return None;
    };
    if limit == 0 {
        return None;
    }
    let frame = Frame::StreamData {
        stream_id: *stream_id,
        offset: *offset,
        payload: payload.slice(..limit.min(payload.len())),
    };
    Some((plan, target, frame))
}

/// Low-priority, direct recovery acquisition. A notice owns no payload, queue
/// debt, range, or copy slot. Staged foreground is conservatively sufficient
/// to refuse extra work; an unavailable Product lock is never evidence of idle
/// service. The writer additionally fences carrier-wide foreground publication.
pub(in crate::runtime) fn claim_prepared_request_repair(
    owner: &SharedRequestProduct,
    context: &ClientPathContext,
    instance: RelayPathInstance,
    ready: &ReliableWriterReadyGuard,
    registration: &PreparedOriginalRegistration,
) -> PreparedOriginalClaim {
    if ready.receipt().instance() != instance.path_instance_id {
        return PreparedOriginalClaim::Empty;
    }
    let state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
    };
    if !registration_is_current(&state, instance, registration) {
        return PreparedOriginalClaim::Empty;
    }
    let wake = arm_work_change(&state, context, instance);
    if !state.sender_queue.is_empty() {
        return PreparedOriginalClaim::Blocked(wake);
    }
    let lane = state.prepared.request_lane;
    let capture =
        RequestRelayNativeCapture::new(state.remotes.membership_generation(), &state.remotes.paths);
    drop(state);
    #[cfg(test)]
    owner.run_before_prepared_native_resolve_for_test();
    let inputs = capture.resolve();
    let mut state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
    };
    if !registration_is_current(&state, instance, registration) {
        return PreparedOriginalClaim::Empty;
    }
    if !state.sender_queue.is_empty() || !ready.receipt().is_current() {
        return PreparedOriginalClaim::Blocked(wake);
    }
    let candidate = match prepared_request_repair_candidate(&mut state, context) {
        PreparedRequestRepairCandidate::Ready(candidate) => candidate,
        PreparedRequestRepairCandidate::Immature(deadline) => {
            return PreparedOriginalClaim::Blocked(repair_maturity_wait(wake, deadline));
        }
        PreparedRequestRepairCandidate::Empty => return PreparedOriginalClaim::Blocked(wake),
    };
    let Some((plan, _, frame)) =
        repair_claim_plan(&state, context, &candidate, instance, inputs.clone())
    else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    if plan.target().1 != instance {
        let selected = state
            .prepared
            .registrations
            .iter()
            .find(|current| current.request_instance() == Some(plan.target().1))
            .cloned();
        drop(state);
        if let Some(selected) = selected {
            selected.notify_repair();
        }
        return PreparedOriginalClaim::Blocked(wake);
    }
    let Some(commands) = state.remotes.paths.iter().find_map(|path| {
        if path.instance() != instance {
            return None;
        }
        match &path.stream.output {
            ReliablePathStreamOutput::Fixed(output) => Some(output.commands().clone()),
            ReliablePathStreamOutput::Switchable(_) => None,
        }
    }) else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    drop(state);

    let attempt = owner.arm_claim();
    let mut other_selected = None;
    let result = plan.commit_with_current_native_shape(&commands, |shape| {
        let mut state = match attempt.try_lock() {
            Ok(state) => state,
            Err(wait) => return Some(PreparedOriginalClaim::Busy(wait)),
        };
        if !registration_is_current(&state, instance, registration) {
            return Some(PreparedOriginalClaim::Empty);
        }
        if state.prepared.request_lane != lane
            || !state.sender_queue.is_empty()
            || !ready.receipt().is_current()
        {
            return None;
        }
        let PreparedRequestRepairCandidate::Ready(current) =
            prepared_request_repair_candidate(&mut state, context)
        else {
            return None;
        };
        if !same_repair_frame(&candidate.frame, &current.frame)
            || candidate.avoid.len() != current.avoid.len()
            || !candidate
                .avoid
                .iter()
                .all(|instance| current.avoid.contains(instance))
        {
            return None;
        }
        let current_inputs = shape.map_or(inputs.clone(), |shape| {
            inputs.with_fenced_target(instance, shape)
        });
        let (current_plan, target, current_frame) =
            repair_claim_plan(&state, context, &current, instance, current_inputs)?;
        if current_plan.target().1 != instance {
            other_selected = state
                .prepared
                .registrations
                .iter()
                .find(|current| current.request_instance() == Some(current_plan.target().1))
                .cloned();
            return None;
        }
        if !same_repair_frame(&frame, &current_frame)
            || !current_plan.target_retains_exact_eligibility(context, lane)
        {
            return None;
        }
        let position = current_plan.target_position_for_apply(&state.remotes, lane)?;
        // Final consumption also rejects foreground published after advisory
        // ranking. No fallible payload admission precedes this exact commit.
        if !ready.try_consume() {
            return None;
        }
        let cause = RelaySendCause::CompletionTailReinjection(target.identity);
        let RequestProductState {
            sender, remotes, ..
        } = &mut *state;
        let path_count = remotes.paths.len();
        let (bytes, _) = sender
            .commit_fenced_frame_product(
                context,
                remotes,
                RequestFrameProductCommit {
                    plan: &current_plan,
                    frame: &frame,
                    cause,
                    position,
                    path_count,
                    reinjection_target_snapshot: Some(target.snapshot),
                    request_load_claim: None,
                },
                None,
            )
            .ok()?;
        sender.optional_reinjection.record_reinjection(bytes);
        sender.record_decision(instance.key, bytes, &frame, cause);
        state.prepared.work_changed.notify_waiters();
        Some(PreparedOriginalClaim::Claimed(frame))
    });
    if let Some(selected) = other_selected {
        selected.notify_repair();
    }
    result
        .flatten()
        .unwrap_or(PreparedOriginalClaim::Blocked(wake))
}

/// Every owner acquisition is nonblocking so contention cannot park the native
/// writer's input service. No await or Native read occurs between the final
/// owner lock and Product commit. Refused source remains shared and unclaimed.
pub(in crate::runtime) fn claim_prepared_request_data(
    owner: &SharedRequestProduct,
    context: &ClientPathContext,
    instance: RelayPathInstance,
    ready: &ReliableWriterReadyGuard,
    registration: &PreparedOriginalRegistration,
) -> PreparedOriginalClaim {
    if ready.receipt().instance() != instance.path_instance_id {
        return PreparedOriginalClaim::Empty;
    }
    let mut state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
    };
    if !registration_is_current(&state, instance, registration) {
        return PreparedOriginalClaim::Empty;
    }
    let wake = arm_work_change(&state, context, instance);
    let lane = state.prepared.request_lane;
    let quantum = state.prepared.data_quantum_bytes;
    let Some((_, queued)) = state.sender_queue.front() else {
        return PreparedOriginalClaim::Empty;
    };
    let ReliableRelayQueuedWorkKind::Data(payload) = &queued.kind else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    let payload = payload.slice(..quantum.min(payload.len()).max(1));
    let frame = match state.send_stream.prepare_data(payload) {
        Ok(frame) => frame,
        Err(
            StreamError::FlowControlBlocked { .. }
            | StreamError::ReinjectionCacheFull { .. }
            | StreamError::TooManyReinjectionCacheChunks { .. },
        ) => return PreparedOriginalClaim::Blocked(wake),
        Err(error) => {
            record_source_error(&mut state, RuntimeError::Stream(error));
            return PreparedOriginalClaim::Empty;
        }
    };
    {
        let RequestProductState {
            sender, remotes, ..
        } = &mut *state;
        if sender
            .multipath
            .prepare_original_claim(context, remotes, &frame)
            .is_err()
        {
            return PreparedOriginalClaim::Blocked(wake);
        }
    }
    if !ready.receipt().is_current() {
        return PreparedOriginalClaim::Blocked(wake);
    }
    let capture =
        RequestRelayNativeCapture::new(state.remotes.membership_generation(), &state.remotes.paths);
    drop(state);
    #[cfg(test)]
    owner.run_before_prepared_native_resolve_for_test();
    let inputs = capture.resolve();
    // Arm anew after our earlier unlock; it is not a release credit for a
    // competing holder. Busy discards this advisory capture and retries fresh.
    let mut state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
    };
    if !registration_is_current(&state, instance, registration) {
        return PreparedOriginalClaim::Empty;
    }
    if !source_matches(&mut state, &frame, lane, quantum) || !ready.receipt().is_current() {
        return PreparedOriginalClaim::Blocked(wake);
    }
    let include_bulk = lane.is_bulk()
        && (state.remotes.paths.len() > 1
            || frontier(&state) == ReliableDataAckFrontierState::AuthoritativeGap);
    let Some(observation) = state.sender.multipath.observe_original_claim_from_inputs(
        context,
        &state.remotes,
        &frame,
        lane,
        include_bulk,
        inputs.clone(),
    ) else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    {
        let RequestProductState {
            sender, remotes, ..
        } = &mut *state;
        sender.multipath.reconcile_original_claim(context, remotes);
    }
    let full_authority = if lane.is_bulk() && !include_bulk {
        let Some(authority) = state.sender.multipath.observe_original_claim_from_inputs(
            context,
            &state.remotes,
            &frame,
            lane,
            true,
            inputs.clone(),
        ) else {
            return PreparedOriginalClaim::Blocked(wake);
        };
        Some(authority)
    } else {
        None
    };
    let ready_instances = current_ready_instances(&state);
    let plan = match state.sender.multipath.plan_original_claim_from_observation(
        context,
        &observation,
        full_authority.as_ref().unwrap_or(&observation),
        &state.remotes,
        &frame,
        lane,
        frontier(&state),
        &ready_instances,
    ) {
        Ok(plan) => plan,
        Err(_) => return PreparedOriginalClaim::Blocked(wake),
    };
    if plan.target().1 != instance {
        let selected = state
            .prepared
            .registrations
            .iter()
            .find(|current| current.request_instance() == Some(plan.target().1))
            .cloned();
        drop(state);
        if let Some(selected) = selected {
            selected.notify();
        }
        return PreparedOriginalClaim::Blocked(wake);
    }
    let Some(commands) = state.remotes.paths.iter().find_map(|path| {
        if path.instance() != instance {
            return None;
        }
        match &path.stream.output {
            ReliablePathStreamOutput::Fixed(output) => Some(output.commands().clone()),
            ReliablePathStreamOutput::Switchable(_) => None,
        }
    }) else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    drop(state);

    let attempt = owner.arm_claim();
    let mut other_selected = None;
    let result = plan.commit_with_current_native_shape(&commands, |shape| {
        let mut state = match attempt.try_lock() {
            Ok(state) => state,
            Err(wait) => return Some(PreparedOriginalClaim::Busy(wait)),
        };
        if !registration_is_current(&state, instance, registration) {
            return Some(PreparedOriginalClaim::Empty);
        }
        if !source_matches(&mut state, &frame, lane, quantum) || !ready.receipt().is_current() {
            return None;
        }
        let current_inputs = match shape {
            Some(shape) => inputs.with_fenced_target(instance, shape),
            None => inputs,
        };
        let include_bulk = lane.is_bulk()
            && (state.remotes.paths.len() > 1
                || frontier(&state) == ReliableDataAckFrontierState::AuthoritativeGap);
        let Some(observation) = state.sender.multipath.observe_original_claim_from_inputs(
            context,
            &state.remotes,
            &frame,
            lane,
            include_bulk,
            current_inputs.clone(),
        ) else {
            return None;
        };
        let full_authority = if lane.is_bulk() && !include_bulk {
            let Some(authority) = state.sender.multipath.observe_original_claim_from_inputs(
                context,
                &state.remotes,
                &frame,
                lane,
                true,
                current_inputs,
            ) else {
                return None;
            };
            Some(authority)
        } else {
            None
        };
        let ready_instances = current_ready_instances(&state);
        let current_plan: RequestMultipathPlan =
            match state.sender.multipath.plan_original_claim_from_observation(
                context,
                &observation,
                full_authority.as_ref().unwrap_or(&observation),
                &state.remotes,
                &frame,
                lane,
                frontier(&state),
                &ready_instances,
            ) {
                Ok(plan) => plan,
                Err(_) => return None,
            };
        if current_plan.target().1 != instance {
            other_selected = state
                .prepared
                .registrations
                .iter()
                .find(|current| current.request_instance() == Some(current_plan.target().1))
                .cloned();
            return None;
        }
        if current_plan
            .proof_expectation()
            .is_some_and(|proof| !context.relay_path_proof_epoch_is_current(instance.key, proof))
        {
            return None;
        }
        let request_load_claim =
            if let Some((_, active, latency_sensitive)) = current_plan.load_expectation() {
                let Some(claim) = context.try_reserve_relay_path_load_if_unchanged(
                    instance,
                    lane,
                    active,
                    latency_sensitive,
                ) else {
                    return None;
                };
                Some(claim)
            } else {
                None
            };
        let Some(position) = current_plan.target_position_for_apply(&state.remotes, lane) else {
            return None;
        };
        if !current_plan.target_retains_exact_eligibility(context, lane) {
            return None;
        }
        // This is an imminent native action, not admission to a future Data
        // command queue. Consuming the writer epoch cannot create W/P/E credit.
        if !ready.try_consume() {
            return None;
        }
        let RequestProductState {
            sender,
            sender_queue,
            send_stream,
            remotes,
            ..
        } = &mut *state;
        let path_count = remotes.paths.len();
        let result = sender.commit_fenced_frame_product(
            context,
            remotes,
            RequestFrameProductCommit {
                plan: &current_plan,
                frame: &frame,
                cause: RelaySendCause::StreamData,
                position,
                path_count,
                reinjection_target_snapshot: None,
                request_load_claim,
            },
            Some(&mut RequestQueuedSourceCommit {
                send_stream,
                sender_queue,
            }),
        );
        match result {
            Ok(_) => {
                state.prepared.last_claimed_at = Some(Instant::now());
                state.prepared.work_changed.notify_waiters();
                Some(PreparedOriginalClaim::Claimed(frame.clone()))
            }
            Err(RequestFrameAdmissionError::Source(error)) => {
                record_source_error(&mut state, RuntimeError::Stream(error));
                Some(PreparedOriginalClaim::Empty)
            }
            Err(RequestFrameAdmissionError::Runtime(error)) => {
                record_source_error(&mut state, error);
                Some(PreparedOriginalClaim::Empty)
            }
            Err(
                RequestFrameAdmissionError::ServiceBlocked
                | RequestFrameAdmissionError::OrderedTerminalPending
                | RequestFrameAdmissionError::SourceChanged,
            ) => None,
        }
    });
    if let Some(selected) = other_selected {
        selected.notify();
    }
    result
        .flatten()
        .unwrap_or(PreparedOriginalClaim::Blocked(wake))
}

#[cfg(test)]
#[path = "tests_prepared_repair.rs"]
mod tests_repair;
