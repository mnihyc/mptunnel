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
use crate::runtime::path::prepared::{
    PreparedNativeCommitmentInputs, PreparedOriginalClaim, PreparedOriginalRegistration,
    prepared_wait_with_native_commitment, prepared_wait_with_recovery_deadline,
};
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
    let payload = state
        .sender_queue
        .front()
        .and_then(|(_, queued)| match &queued.kind {
            ReliableRelayQueuedWorkKind::Data(payload) => {
                Some(payload.slice(..quantum.min(payload.len()).max(1)))
            }
            _ => None,
        });
    let frame = match payload.map(|payload| state.send_stream.prepare_data(payload)) {
        Some(Ok(frame)) => Some(frame),
        None
        | Some(Err(
            StreamError::FlowControlBlocked { .. }
            | StreamError::ReinjectionCacheFull { .. }
            | StreamError::TooManyReinjectionCacheChunks { .. },
        )) => None,
        Some(Err(error)) => {
            record_source_error(&mut state, RuntimeError::Stream(error));
            return PreparedOriginalClaim::Empty;
        }
    };
    // Original preparation may publish proofs before Native sampling, but its
    // detached-predecessor refusal must not suppress separate recovery service.
    let original_prepared = frame.as_ref().is_some_and(|frame| {
        let RequestProductState {
            sender, remotes, ..
        } = &mut *state;
        sender
            .multipath
            .prepare_original_claim(context, remotes, frame)
            .is_ok()
    });
    if !ready.receipt().is_current() {
        return PreparedOriginalClaim::Blocked(wake);
    }
    let capture =
        RequestRelayNativeCapture::new(state.remotes.membership_generation(), &state.remotes.paths);
    let commitment_handles = state
        .remotes
        .paths
        .iter()
        .filter_map(|path| {
            let ReliablePathStreamOutput::Fixed(output) = &path.stream.output else {
                return None;
            };
            Some((path.instance(), output.commands().native_commitment()))
        })
        .collect::<Vec<_>>();
    drop(state);
    #[cfg(test)]
    owner.run_before_prepared_native_resolve_for_test();
    let inputs = capture.resolve();
    let commitments = PreparedNativeCommitmentInputs::capture(commitment_handles);
    let wake = prepared_wait_with_native_commitment(
        wake,
        commitments
            .selected_terminal_wait(instance)
            .into_iter()
            .collect(),
    );
    let mut commitment_waits = Vec::new();
    // Arm anew after our earlier unlock; it is not a release credit for a
    // competing holder. Busy discards this advisory capture and retries fresh.
    let mut state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
    };
    if !registration_is_current(&state, instance, registration) {
        return PreparedOriginalClaim::Empty;
    }
    if state.prepared.request_lane != lane || !ready.receipt().is_current() {
        return PreparedOriginalClaim::Blocked(wake);
    }
    if let Err(error) = commitments.check_selected(instance) {
        return PreparedOriginalClaim::CarrierFailed(RuntimeError::Io(std::io::Error::other(
            error,
        )));
    }
    let recovery_ready = current_ready_instances(&state);
    let recovery = {
        let RequestProductState {
            sender,
            remotes,
            send_stream,
            sender_queue,
            last_send_ack,
            ..
        } = &mut *state;
        sender.next_prepared_recovery(
            context,
            remotes,
            send_stream,
            sender_queue,
            last_send_ack,
            lane,
            inputs.clone(),
            &recovery_ready,
            Instant::now(),
        )
    };
    let wake = prepared_wait_with_recovery_deadline(wake, recovery.next_deadline);
    if let Some(candidate) = recovery.candidate {
        if candidate.target != instance {
            let selected = state
                .prepared
                .registrations
                .iter()
                .find(|registration| registration.request_instance() == Some(candidate.target))
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
            let ReliablePathStreamOutput::Fixed(output) = &path.stream.output else {
                return None;
            };
            Some(output.commands().clone())
        }) else {
            return PreparedOriginalClaim::Blocked(wake);
        };
        drop(state);
        let attempt = owner.arm_claim();
        let mut other_selected = None;
        let result = candidate
            .commit_with_current_native_shape(&commands, |shape| {
                let mut state = match attempt.try_lock() {
                    Ok(state) => state,
                    Err(wait) => return Some(PreparedOriginalClaim::Busy(wait)),
                };
                if !registration_is_current(&state, instance, registration) {
                    return Some(PreparedOriginalClaim::Empty);
                }
                if state.prepared.request_lane != lane || !ready.receipt().is_current() {
                    return None;
                }
                if let Err(error) = commitments.check_selected(instance) {
                    return Some(PreparedOriginalClaim::CarrierFailed(RuntimeError::Io(
                        std::io::Error::other(error),
                    )));
                }
                let current_inputs = match shape {
                    Some(shape) => inputs.with_fenced_target(instance, shape),
                    None => inputs,
                };
                let ready_instances = current_ready_instances(&state);
                let RequestProductState {
                    sender,
                    remotes,
                    send_stream,
                    sender_queue,
                    last_send_ack,
                    ..
                } = &mut *state;
                let current = sender
                    .next_prepared_recovery(
                        context,
                        remotes,
                        send_stream,
                        sender_queue,
                        last_send_ack,
                        lane,
                        current_inputs,
                        &ready_instances,
                        Instant::now(),
                    )
                    .candidate?;
                if current.target != candidate.target {
                    other_selected = state
                        .prepared
                        .registrations
                        .iter()
                        .find(|registration| {
                            registration.request_instance() == Some(current.target)
                        })
                        .cloned();
                    return None;
                }
                // The fresh query re-proves authority. Its newly constructed
                // batch validity deadline is not the identity of that authority;
                // compare cause kind while retaining exact target/range checks.
                if current.frame != candidate.frame
                    || std::mem::discriminant(&current.cause)
                        != std::mem::discriminant(&candidate.cause)
                {
                    return None;
                }
                match sender.commit_prepared_recovery(
                    context,
                    remotes,
                    send_stream,
                    sender_queue,
                    &current,
                    ready,
                    shape,
                ) {
                    Ok(_) => {
                        state.prepared.work_changed.notify_waiters();
                        Some(PreparedOriginalClaim::RecoveryQueued)
                    }
                    Err(RuntimeError::SenderServiceBlocked) => None,
                    Err(error) => {
                        record_source_error(&mut state, error);
                        Some(PreparedOriginalClaim::Empty)
                    }
                }
            })
            .flatten();
        if let Some(selected) = other_selected {
            selected.notify();
        }
        return result.unwrap_or(PreparedOriginalClaim::Blocked(wake));
    }
    let Some(frame) = frame else {
        return if state.sender_queue.front().is_none() && state.send_stream.reinjection_bytes() == 0
        {
            PreparedOriginalClaim::Empty
        } else {
            PreparedOriginalClaim::Blocked(wake)
        };
    };
    if !original_prepared || !source_matches(&mut state, &frame, lane, quantum) {
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
    let ready_instances =
        commitments.original_ready(current_ready_instances(&state), &mut commitment_waits);
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
        Err(_) => {
            return PreparedOriginalClaim::Blocked(prepared_wait_with_native_commitment(
                wake,
                commitment_waits,
            ));
        }
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
        return PreparedOriginalClaim::Blocked(prepared_wait_with_native_commitment(
            wake,
            commitment_waits,
        ));
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
        if let Err(error) = commitments.check_selected(instance) {
            return Some(PreparedOriginalClaim::CarrierFailed(RuntimeError::Io(
                std::io::Error::other(error),
            )));
        }
        let current_inputs = match shape {
            Some(shape) => inputs.with_fenced_target(instance, shape),
            None => inputs,
        };
        let include_bulk = lane.is_bulk()
            && (state.remotes.paths.len() > 1
                || frontier(&state) == ReliableDataAckFrontierState::AuthoritativeGap);
        let recovery_ready = current_ready_instances(&state);
        let recovery = {
            let RequestProductState {
                sender,
                remotes,
                send_stream,
                sender_queue,
                last_send_ack,
                ..
            } = &mut *state;
            sender.next_prepared_recovery(
                context,
                remotes,
                send_stream,
                sender_queue,
                last_send_ack,
                lane,
                current_inputs.clone(),
                &recovery_ready,
                Instant::now(),
            )
        };
        if let Some(candidate) = recovery.candidate {
            if candidate.target != instance {
                other_selected = state
                    .prepared
                    .registrations
                    .iter()
                    .find(|registration| registration.request_instance() == Some(candidate.target))
                    .cloned();
                return None;
            }
            let RequestProductState {
                sender,
                remotes,
                send_stream,
                sender_queue,
                ..
            } = &mut *state;
            return match sender.commit_prepared_recovery(
                context,
                remotes,
                send_stream,
                sender_queue,
                &candidate,
                ready,
                shape,
            ) {
                Ok(_) => {
                    state.prepared.work_changed.notify_waiters();
                    Some(PreparedOriginalClaim::RecoveryQueued)
                }
                Err(RuntimeError::SenderServiceBlocked) => None,
                Err(error) => {
                    record_source_error(&mut state, error);
                    Some(PreparedOriginalClaim::Empty)
                }
            };
        }
        let observation = state.sender.multipath.observe_original_claim_from_inputs(
            context,
            &state.remotes,
            &frame,
            lane,
            include_bulk,
            current_inputs.clone(),
        )?;
        let full_authority = if lane.is_bulk() && !include_bulk {
            let authority = state.sender.multipath.observe_original_claim_from_inputs(
                context,
                &state.remotes,
                &frame,
                lane,
                true,
                current_inputs,
            )?;
            Some(authority)
        } else {
            None
        };
        let ready_instances =
            commitments.original_ready(current_ready_instances(&state), &mut commitment_waits);
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
                let claim = context.try_reserve_relay_path_load_if_unchanged(
                    instance,
                    lane,
                    active,
                    latency_sensitive,
                )?;
                Some(claim)
            } else {
                None
            };
        let position = current_plan.target_position_for_apply(&state.remotes, lane)?;
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
    result.flatten().unwrap_or_else(|| {
        PreparedOriginalClaim::Blocked(prepared_wait_with_native_commitment(wake, commitment_waits))
    })
}
