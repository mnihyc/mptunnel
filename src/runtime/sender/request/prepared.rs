//! Unnumbered request source is claimed only by an imminent native writer.

use super::multipath::{RequestMultipathPlan, RequestRelayNativeCapture};
use super::{
    RequestFrameAdmissionError, RequestFrameProductCommit, RequestProductState,
    RequestQueuedSourceCommit, SharedRequestProduct,
};
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::path::RelayPathInstance;
use crate::mux::stream::StreamError;
use crate::protocol::Frame;
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
