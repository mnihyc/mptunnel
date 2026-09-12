//! Response bytes receive their first DSN and path owner at native claim.

use super::scheduling::select_prepared_response_data_path;
use super::service::response_data_dispatch_lane;
use super::{ServerResponseSenderService, SharedResponseProduct};
use crate::model::admission::ReliableDataAckFrontierState;
use crate::mux::stream::{ReliableSendStream, StreamError};
use crate::protocol::{Frame, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::prepared::{
    PreparedNativeCommitmentInputs, PreparedOriginalClaim, PreparedOriginalRegistration,
    PreparedOriginalWait, prepared_wait_with_native_commitment,
    prepared_wait_with_recovery_deadline,
};
use crate::runtime::path::writer_boundary::ReliableWriterReadyGuard;
use crate::runtime::relay::io::AuthoritativeStreamAckSnapshot;
use crate::runtime::sender::queue::{ReliableRelayQueuedWorkKind, ReliableRelaySenderQueue};
use crate::runtime::stream::response::{
    ResponseAcquisitionOutputId, ResponseDispatchTarget, ResponsePreparedNativeInputs,
    ResponsePreparedOutput,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;

pub(in crate::runtime) struct ResponseProductState {
    pub(in crate::runtime) sender: ServerResponseSenderService,
    pub(in crate::runtime) send_stream: ReliableSendStream,
    pub(in crate::runtime) last_send_ack: AuthoritativeStreamAckSnapshot,
    pub(in crate::runtime) prepared: PreparedResponseSource,
}

pub(in crate::runtime) struct PreparedResponseSource {
    pub(in crate::runtime) response_lane: TrafficClass,
    pub(in crate::runtime) data_quantum_bytes: usize,
    pub(in crate::runtime) claims_active: bool,
    pub(in crate::runtime) registrations: Vec<Arc<PreparedOriginalRegistration>>,
    pub(in crate::runtime) work_changed: Arc<Notify>,
    pub(in crate::runtime) pending_error: Option<RuntimeError>,
    pub(in crate::runtime) first_claimed_at: Option<Instant>,
    pub(in crate::runtime) last_claimed_at: Option<Instant>,
}

impl PreparedResponseSource {
    pub(in crate::runtime) fn new(response_lane: TrafficClass, data_quantum_bytes: usize) -> Self {
        Self {
            response_lane,
            data_quantum_bytes,
            claims_active: true,
            registrations: Vec::new(),
            work_changed: Arc::new(Notify::new()),
            pending_error: None,
            first_claimed_at: None,
            last_claimed_at: None,
        }
    }
}

pub(in crate::runtime) fn publish_prepared_response_work(
    state: &mut ResponseProductState,
    owner: &SharedResponseProduct,
    lane: TrafficClass,
    quantum: usize,
    changed: bool,
) {
    let lane = state
        .sender
        .queue
        .front()
        .filter(|(_, work)| matches!(work.kind, ReliableRelayQueuedWorkKind::Data(_)))
        .map_or(lane, |(_, work)| {
            response_data_dispatch_lane(work.data_lane, lane)
        });
    let policy_changed =
        state.prepared.response_lane != lane || state.prepared.data_quantum_bytes != quantum;
    state.prepared.response_lane = lane;
    state.prepared.data_quantum_bytes = quantum;
    if !state.prepared.claims_active {
        state.prepared.registrations.clear();
        state.prepared.work_changed.notify_waiters();
        return;
    }
    let outputs = owner.binding().prepared_outputs();
    let old_len = state.prepared.registrations.len();
    state.prepared.registrations.retain(|registration| {
        registration.lane() == lane
            && outputs
                .iter()
                .any(|output| registration.response_instance() == Some(output.identity))
    });
    let mut membership_changed = old_len != state.prepared.registrations.len();
    for output in outputs {
        if state
            .prepared
            .registrations
            .iter()
            .any(|registration| registration.response_instance() == Some(output.identity))
        {
            continue;
        }
        state
            .prepared
            .registrations
            .push(PreparedOriginalRegistration::new_response(
                owner.downgrade(),
                state.sender.stream_id(),
                output.identity,
                output.commands,
                lane,
            ));
        membership_changed = true;
    }
    if changed || policy_changed || membership_changed {
        state.prepared.work_changed.notify_waiters();
        // A newly attached writer must also discover retained recovery work
        // when the source has gone quiet. Notifications remain coalesced.
        if state.sender.data_bytes() > 0 || state.send_stream.reinjection_bytes() > 0 {
            for registration in &state.prepared.registrations {
                registration.notify();
            }
        }
    }
}

fn current(
    state: &ResponseProductState,
    identity: ResponseAcquisitionOutputId,
    registration: &PreparedOriginalRegistration,
) -> bool {
    state.prepared.claims_active
        && registration.response_instance() == Some(identity)
        && registration.lane() == state.prepared.response_lane
        && state
            .prepared
            .registrations
            .iter()
            .any(|current| std::ptr::eq(current.as_ref(), registration))
}

fn frontier(state: &ResponseProductState) -> ReliableDataAckFrontierState {
    ReliableDataAckFrontierState::from_authoritative_gap(
        state
            .last_send_ack
            .gap_at(state.send_stream.data_ack_frontier())
            .is_some(),
    )
}

fn arm_notify(notify: Arc<Notify>) -> PreparedOriginalWait {
    let mut wait = Box::pin(notify.notified_owned());
    wait.as_mut().enable();
    wait
}

fn arm_work_change(
    state: &ResponseProductState,
    identity: ResponseAcquisitionOutputId,
    outputs: &[ResponsePreparedOutput],
    mut updates: tokio::sync::watch::Receiver<u64>,
) -> PreparedOriginalWait {
    let mut waits = vec![
        arm_notify(state.prepared.work_changed.clone()),
        Box::pin(async move {
            let _ = updates.changed().await;
        }) as PreparedOriginalWait,
    ];
    for output in outputs {
        if output.identity != identity {
            waits.push(arm_notify(
                output.commands.writer_boundary().change_notify(),
            ));
        }
        if let Some(authority) = output.commands.native_rate_authority() {
            let mut updates = authority.accepted_change_cursor();
            waits.push(Box::pin(async move {
                let _ = updates.changed().await;
            }));
        }
    }
    Box::pin(async move {
        let _ = futures::future::select_all(waits).await;
    })
}

fn ready_outputs(outputs: &[ResponsePreparedOutput]) -> Vec<ResponseAcquisitionOutputId> {
    outputs
        .iter()
        .filter(|output| {
            output
                .commands
                .writer_boundary()
                .snapshot()
                .is_some_and(|ready| ready.instance() == output.identity.path_instance_id)
        })
        .map(|output| output.identity)
        .collect()
}

fn source_prefix(state: &ResponseProductState) -> Option<&Bytes> {
    let (_, work) = state.sender.queue.front()?;
    match &work.kind {
        ReliableRelayQueuedWorkKind::Data(payload) => Some(payload),
        _ => None,
    }
}

fn source_error(state: &mut ResponseProductState, error: RuntimeError) {
    state.prepared.pending_error = Some(error);
    state.prepared.claims_active = false;
    state.prepared.work_changed.notify_waiters();
}

pub(in crate::runtime) enum ResponsePreparedCommitError {
    Blocked,
    Source(RuntimeError),
}

/// Borrowed source transaction, never a second queue or owner. Qualification
/// refusal rolls back only the just-committed mux range before source removal.
pub(in crate::runtime) struct ResponsePreparedSourceCommit<'a> {
    pub(in crate::runtime) send_stream: &'a mut ReliableSendStream,
    pub(in crate::runtime) queue: &'a mut ReliableRelaySenderQueue,
}

impl ResponsePreparedSourceCommit<'_> {
    pub(in crate::runtime) fn finish(&mut self, bytes: usize) {
        self.queue
            .commit_front_data_prefix(bytes)
            .expect("validated prepared response source prefix");
    }
    pub(in crate::runtime) fn matches(&self, frame: &Frame) -> bool {
        let Frame::StreamData {
            offset, payload, ..
        } = frame
        else {
            return false;
        };
        let Some((_, work)) = self.queue.front() else {
            return false;
        };
        matches!(&work.kind, ReliableRelayQueuedWorkKind::Data(source)
            if *offset == self.send_stream.next_offset() && !payload.is_empty()
                && payload.len() <= source.len() && payload.as_ptr() == source.as_ptr())
    }
}

pub(in crate::runtime) fn claim_prepared_response_data(
    owner: &SharedResponseProduct,
    identity: ResponseAcquisitionOutputId,
    ready: &ReliableWriterReadyGuard,
    registration: &PreparedOriginalRegistration,
) -> PreparedOriginalClaim {
    if ready.receipt().instance() != identity.path_instance_id {
        return PreparedOriginalClaim::Empty;
    }
    let state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => {
            quinn::note_source_state("response_prepared_refusal", format_args!("stage=initial reason=ProductBusy"));
            return PreparedOriginalClaim::Busy(wait);
        }
    };
    if !current(&state, identity, registration) {
        return PreparedOriginalClaim::Empty;
    }
    // Subscribe before observing membership; a replacement between capture
    // and resolution must remain a pending wake if its receipt is rejected.
    let updates = owner.binding().subscribe_updates();
    let outputs = owner.binding().prepared_outputs();
    let wake = arm_work_change(&state, identity, &outputs, updates);
    let Some(output) = outputs
        .iter()
        .find(|output| output.identity == identity)
        .cloned()
    else {
        return PreparedOriginalClaim::Empty;
    };
    let lane = state.prepared.response_lane;
    let quantum = state.prepared.data_quantum_bytes;
    let credit = owner
        .binding()
        .mux_limits()
        .max_repair_bytes
        .saturating_sub(state.send_stream.reinjection_bytes());
    let source = source_prefix(&state).and_then(|source| {
        owner
            .binding()
            .response_startup_fresh_data_limit(
                state.send_stream.next_offset(),
                source.len().min(quantum).min(credit),
            )
            .filter(|bytes| *bytes > 0)
            .map(|proposed| source.slice(..proposed))
    });
    let offset = state.send_stream.next_offset();
    drop(state);
    let commitments = PreparedNativeCommitmentInputs::capture(
        outputs
            .iter()
            .map(|output| (output.identity, output.commands.native_commitment())),
    );
    let wake = prepared_wait_with_native_commitment(
        wake,
        commitments
            .selected_terminal_wait(identity)
            .into_iter()
            .collect(),
    );
    let mut commitment_waits = Vec::new();
    let mut inputs = ResponsePreparedNativeInputs::resolve(outputs.clone());
    let mut state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => {
            quinn::note_source_state("response_prepared_refusal", format_args!("stage=native_recheck reason=ProductBusy"));
            return PreparedOriginalClaim::Busy(wait);
        }
    };
    if !current(&state, identity, registration) {
        return PreparedOriginalClaim::Empty;
    }
    if state.prepared.data_quantum_bytes != quantum || !ready.receipt().is_current() {
        quinn::note_source_state("response_prepared_refusal", format_args!("stage=native_recheck reason=QuantumOrReadyChanged"));
        return PreparedOriginalClaim::Blocked(wake);
    }
    if let Err(error) = commitments.check_selected(identity) {
        return PreparedOriginalClaim::CarrierFailed(RuntimeError::Io(std::io::Error::other(
            error,
        )));
    }
    let Some(observation) = owner
        .binding()
        .observe_prepared_original(&inputs, lane, offset)
    else {
        quinn::note_source_state("response_prepared_refusal", format_args!("stage=native_recheck reason=ObservationUnavailable"));
        return PreparedOriginalClaim::Blocked(wake);
    };
    let recovery = state.sender.next_prepared_recovery(
        owner.binding(),
        &state.send_stream,
        &state.last_send_ack,
        lane,
        &observation.targets,
        &ready_outputs(&outputs),
        Instant::now(),
    );
    let wake = prepared_wait_with_recovery_deadline(wake, recovery.next_deadline);
    if let Some(candidate) = recovery.candidate {
        let selected = ResponseAcquisitionOutputId {
            key: candidate.target.key,
            path_instance_id: candidate.target.path_instance_id,
            incarnation: candidate.target.incarnation,
        };
        if selected != identity {
            let selected = state
                .prepared
                .registrations
                .iter()
                .find(|registration| registration.response_instance() == Some(selected))
                .cloned();
            drop(state);
            if let Some(selected) = selected {
                selected.notify();
            }
            return PreparedOriginalClaim::Blocked(wake);
        }
        drop(state);
        let attempt = owner.arm_claim();
        let mut other_selected = None;
        let commit = |shape| {
            let mut state = match attempt.try_lock() {
                Ok(state) => state,
                Err(wait) => return Some(PreparedOriginalClaim::Busy(wait)),
            };
            if !current(&state, identity, registration) {
                return Some(PreparedOriginalClaim::Empty);
            }
            if !ready.receipt().is_current() {
                return None;
            }
            if let Err(error) = commitments.check_selected(identity) {
                return Some(PreparedOriginalClaim::CarrierFailed(RuntimeError::Io(
                    std::io::Error::other(error),
                )));
            }
            if let Some(shape) = shape {
                inputs.replace_fenced_target(identity, shape);
            }
            let observation = owner.binding().observe_prepared_original(
                &inputs,
                lane,
                state.send_stream.next_offset(),
            )?;
            let current = state
                .sender
                .next_prepared_recovery(
                    owner.binding(),
                    &state.send_stream,
                    &state.last_send_ack,
                    lane,
                    &observation.targets,
                    &ready_outputs(&outputs),
                    Instant::now(),
                )
                .candidate?;
            if current.target != candidate.target {
                let selected = ResponseAcquisitionOutputId {
                    key: current.target.key,
                    path_instance_id: current.target.path_instance_id,
                    incarnation: current.target.incarnation,
                };
                other_selected = state
                    .prepared
                    .registrations
                    .iter()
                    .find(|registration| registration.response_instance() == Some(selected))
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
            let ResponseProductState {
                sender,
                send_stream,
                ..
            } = &mut *state;
            match sender.commit_prepared_recovery(
                owner.binding(),
                send_stream,
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
                    source_error(&mut state, error);
                    Some(PreparedOriginalClaim::Empty)
                }
            }
        };
        let result = match (
            output.commands.native_rate_authority(),
            candidate.target.native_authority_stamp,
        ) {
            (Some(authority), Some(stamp)) => authority
                .commit_with_current_scheduling_shape(stamp, |shape| commit(Some(shape)))
                .ok()
                .flatten(),
            (None, None) if identity.key.underlay == UnderlayProtocol::Tcp => commit(None),
            _ => None,
        };
        if let Some(selected) = other_selected {
            selected.notify();
        }
        return result.unwrap_or(PreparedOriginalClaim::Blocked(wake));
    }
    let Some(source) = source else {
        let queued_data_empty = state.sender.data_bytes() == 0;
        let return_empty = queued_data_empty && state.send_stream.reinjection_bytes() == 0;
        // This selection was made before Native resolution and also includes
        // startup/quantum/repair-credit restrictions. It is not producer EOF.
        quinn::note_source_state("response_source_selection_none", format_args!(
            "queued_data_empty={} return_empty={} selected_offset={} quantum={} repair_credit={}",
            queued_data_empty, return_empty, offset, quantum, credit,
        ));
        return if return_empty {
            PreparedOriginalClaim::Empty
        } else {
            PreparedOriginalClaim::Blocked(wake)
        };
    };
    if state.send_stream.next_offset() != offset
        || !source_prefix(&state).is_some_and(|current| {
            current.as_ptr() == source.as_ptr() && current.len() >= source.len()
        })
    {
        quinn::note_source_state("response_prepared_refusal", format_args!("stage=selection reason=SourceChanged"));
        return PreparedOriginalClaim::Blocked(wake);
    }
    let Some(selection) = select_prepared_response_data_path(
        &observation.targets,
        lane,
        source.len(),
        owner.binding().mux_limits(),
        &observation.lower_flights,
        state.send_stream.reinjection_bytes(),
        frontier(&state),
        &commitments.original_ready(ready_outputs(&outputs), &mut commitment_waits),
    ) else {
        quinn::note_source_state("response_prepared_refusal", format_args!(
            "stage=selection reason=NoOriginalSelection commitment_waits={}", commitment_waits.len(),
        ));
        return PreparedOriginalClaim::Blocked(prepared_wait_with_native_commitment(
            wake,
            commitment_waits,
        ));
    };
    let selected_identity = ResponseAcquisitionOutputId::from(&selection.target);
    if selected_identity != identity {
        quinn::note_source_state("response_prepared_refusal", format_args!("stage=selection reason=OtherTarget"));
        let selected = state
            .prepared
            .registrations
            .iter()
            .find(|registration| registration.response_instance() == Some(selected_identity))
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
    let frame = match state
        .send_stream
        .prepare_data(source.slice(..selection.payload_bytes))
    {
        Ok(frame) => frame,
        Err(
            StreamError::FlowControlBlocked { .. }
            | StreamError::ReinjectionCacheFull { .. }
            | StreamError::TooManyReinjectionCacheChunks { .. },
        ) => {
            quinn::note_source_state("response_prepared_refusal", format_args!("stage=prepare_data reason=FlowOrRetentionBlocked"));
            return PreparedOriginalClaim::Blocked(wake);
        }
        Err(error) => {
            source_error(&mut state, RuntimeError::Stream(error));
            return PreparedOriginalClaim::Empty;
        }
    };
    let target = ResponseDispatchTarget::from(&selection.target);
    drop(state);
    let attempt = owner.arm_claim();
    let mut selected_elsewhere = None;
    let commit = |shape| {
        let mut state = match attempt.try_lock() {
            Ok(state) => state,
            Err(wait) => return Some(PreparedOriginalClaim::Busy(wait)),
        };
        if !current(&state, identity, registration) {
            return Some(PreparedOriginalClaim::Empty);
        }
        if state.prepared.data_quantum_bytes != quantum || !ready.receipt().is_current() {
            return None;
        }
        {
            let ResponseProductState {
                sender,
                send_stream,
                ..
            } = &mut *state;
            if !(ResponsePreparedSourceCommit {
                send_stream,
                queue: &mut sender.queue,
            })
            .matches(&frame)
            {
                return None;
            }
        }
        if let Err(error) = commitments.check_selected(identity) {
            return Some(PreparedOriginalClaim::CarrierFailed(RuntimeError::Io(
                std::io::Error::other(error),
            )));
        }
        if let Some(shape) = shape {
            inputs.replace_fenced_target(identity, shape);
        }
        let observation = owner
            .binding()
            .observe_prepared_original(&inputs, lane, offset)?;
        let recovery = state.sender.next_prepared_recovery(
            owner.binding(),
            &state.send_stream,
            &state.last_send_ack,
            lane,
            &observation.targets,
            &ready_outputs(&outputs),
            Instant::now(),
        );
        if let Some(candidate) = recovery.candidate {
            let selected = ResponseAcquisitionOutputId {
                key: candidate.target.key,
                path_instance_id: candidate.target.path_instance_id,
                incarnation: candidate.target.incarnation,
            };
            if selected != identity {
                selected_elsewhere = state
                    .prepared
                    .registrations
                    .iter()
                    .find(|registration| registration.response_instance() == Some(selected))
                    .cloned();
                return None;
            }
            let ResponseProductState {
                sender,
                send_stream,
                ..
            } = &mut *state;
            return match sender.commit_prepared_recovery(
                owner.binding(),
                send_stream,
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
                    source_error(&mut state, error);
                    Some(PreparedOriginalClaim::Empty)
                }
            };
        }
        let current_plan = select_prepared_response_data_path(
            &observation.targets,
            lane,
            source.len(),
            owner.binding().mux_limits(),
            &observation.lower_flights,
            state.send_stream.reinjection_bytes(),
            frontier(&state),
            &commitments.original_ready(ready_outputs(&outputs), &mut commitment_waits),
        )?;
        let selected = ResponseAcquisitionOutputId::from(&current_plan.target);
        if selected != identity {
            selected_elsewhere = state
                .prepared
                .registrations
                .iter()
                .find(|registration| registration.response_instance() == Some(selected))
                .cloned();
            return None;
        }
        if current_plan.payload_bytes != selection.payload_bytes {
            return None;
        }
        let ResponseProductState {
            sender,
            send_stream,
            ..
        } = &mut *state;
        let result = owner
            .binding()
            .commit_prepared_original_for_dispatch_target(
                &ResponseDispatchTarget::from(&current_plan.target),
                &frame,
                lane,
                observation.model_generation,
                current_plan.position,
                shape,
                ready,
                &mut ResponsePreparedSourceCommit {
                    send_stream,
                    queue: &mut sender.queue,
                },
            );
        match result {
            Ok(()) => {
                let claimed_at = Instant::now();
                state.prepared.first_claimed_at.get_or_insert(claimed_at);
                state.prepared.last_claimed_at = Some(claimed_at);
                state.prepared.work_changed.notify_waiters();
                #[cfg(feature = "lab-diagnostics")]
                if let Frame::StreamData {
                    offset, payload, ..
                } = &frame
                {
                    crate::lab_diagnostics::lab_server_response_stream_data(
                        state.sender.session_id.0,
                        state.sender.stream_id.0,
                        *offset,
                        payload.len(),
                    );
                }
                Some(PreparedOriginalClaim::Claimed(frame.clone()))
            }
            Err(ResponsePreparedCommitError::Blocked) => None,
            Err(ResponsePreparedCommitError::Source(error)) => {
                source_error(&mut state, error);
                Some(PreparedOriginalClaim::Empty)
            }
        }
    };
    let result = match (
        output.commands.native_rate_authority(),
        target.native_authority_stamp,
    ) {
        (Some(authority), Some(stamp)) => authority
            .commit_with_current_scheduling_shape(stamp, |shape| commit(Some(shape)))
            .ok()
            .flatten(),
        (None, None) if identity.key.underlay == UnderlayProtocol::Tcp => commit(None),
        _ => None,
    };
    if let Some(selected) = selected_elsewhere {
        selected.notify();
    }
    result.unwrap_or_else(|| {
        PreparedOriginalClaim::Blocked(prepared_wait_with_native_commitment(wake, commitment_waits))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::path::CarrierPathKey;
    use crate::protocol::{PathId, StreamId};
    use crate::runtime::sender::{RelaySendCause, ServerReinjectionOutputIdentity};

    #[test]
    fn prepared_source_does_not_double_charge_selected_ordinary_repair() {
        let mut queue = ReliableRelaySenderQueue::default();
        queue.push_data(Bytes::from_static(b"unclaimed source"));
        let frame = Frame::StreamData {
            stream_id: StreamId(715),
            offset: 0,
            payload: Bytes::from_static(b"repair"),
        };
        queue.push_reinjection_with_cause(frame, RelaySendCause::TailReinjection);
        let target = ServerReinjectionOutputIdentity {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            },
            incarnation: 1,
        };
        assert_eq!(
            queue.response_target_queued_reinjection_bytes(target, true),
            6,
            "generic global-front accounting remains unchanged"
        );
        assert_eq!(
            queue.response_target_queued_reinjection_bytes_for_repair_dispatch(target),
            0,
            "the selected repair is excluded once even while Data stays shared"
        );
        queue.commit_front_reinjection().unwrap();
        assert_eq!(queue.data_bytes(), b"unclaimed source".len());
    }
}
