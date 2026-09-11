//! Response bytes receive their first DSN and path owner at native claim.

use super::scheduling::{
    response_completion_snapshot, select_prepared_response_data_path,
    select_response_frame_path_for_extent,
};
use super::service::response_data_dispatch_lane;
use super::{ServerResponseSenderService, SharedResponseProduct};
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::capacity::adaptive_reliable_relay_reinjection_bytes;
use crate::model::work::{
    ReliableReinjectionTargetWork, reliable_live_frontier_reinjection_limit_bytes,
    reliable_reinjection_service_limit_bytes,
};
use crate::mux::stream::{ReliableSendStream, StreamError};
use crate::protocol::frame::{
    normalize_offset_ranges, offset_ranges_not_covered, reliable_stream_frame_accounted_bytes,
};
use crate::protocol::{Frame, OffsetRange, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::prepared::{
    PreparedOriginalClaim, PreparedOriginalRegistration, PreparedOriginalWait,
};
use crate::runtime::path::writer_boundary::ReliableWriterReadyGuard;
use crate::runtime::relay::io::AuthoritativeStreamAckSnapshot;
use crate::runtime::sender::queue::{ReliableRelayQueuedWorkKind, ReliableRelaySenderQueue};
use crate::runtime::sender::{CarrierEmitMode, RelaySendCause, ServerReinjectionOutputIdentity};
use crate::runtime::stream::response::{
    ResponseAcquisitionOutputId, ResponseDispatchTarget, ResponsePreparedNativeInputs,
    ResponsePreparedOutput, ResponseStreamBinding,
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
        if state.sender.data_bytes() > 0 {
            for registration in &state.prepared.registrations {
                registration.notify();
            }
        }
    }
    // A successor requires existing copy coverage; an uncovered head remains
    // exclusively with critical recovery. This notice carries no payload/debt.
    if state.send_stream.reinjection_bytes() > 0 && owner.binding().has_current_copy_debt() {
        for registration in &state.prepared.registrations {
            registration.notify_repair();
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
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
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
    if state.sender.data_bytes() == 0 {
        return PreparedOriginalClaim::Empty;
    }
    let Some(source) = source_prefix(&state) else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    let lane = state.prepared.response_lane;
    let quantum = state.prepared.data_quantum_bytes;
    let credit = owner
        .binding()
        .mux_limits()
        .max_repair_bytes
        .saturating_sub(state.send_stream.reinjection_bytes());
    let Some(proposed) = owner
        .binding()
        .response_startup_fresh_data_limit(
            state.send_stream.next_offset(),
            source.len().min(quantum).min(credit),
        )
        .filter(|bytes| *bytes > 0)
    else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    let source = source.slice(..proposed);
    let offset = state.send_stream.next_offset();
    drop(state);
    let mut inputs = ResponsePreparedNativeInputs::resolve(outputs.clone());
    let mut state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
    };
    if !current(&state, identity, registration) {
        return PreparedOriginalClaim::Empty;
    }
    if state.prepared.data_quantum_bytes != quantum
        || state.send_stream.next_offset() != offset
        || !source_prefix(&state).is_some_and(|current| {
            current.as_ptr() == source.as_ptr() && current.len() >= source.len()
        })
        || !ready.receipt().is_current()
    {
        return PreparedOriginalClaim::Blocked(wake);
    }
    let Some(observation) = owner
        .binding()
        .observe_prepared_original(&inputs, lane, offset)
    else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    let Some(selection) = select_prepared_response_data_path(
        &observation.targets,
        lane,
        source.len(),
        owner.binding().mux_limits(),
        &observation.lower_flights,
        state.send_stream.reinjection_bytes(),
        frontier(&state),
        &ready_outputs(&outputs),
    ) else {
        return PreparedOriginalClaim::Blocked(wake);
    };
    let selected_identity = ResponseAcquisitionOutputId::from(&selection.target);
    if selected_identity != identity {
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
        return PreparedOriginalClaim::Blocked(wake);
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
        ) => return PreparedOriginalClaim::Blocked(wake),
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
        if let Some(shape) = shape {
            inputs.replace_fenced_target(identity, shape);
        }
        let observation = owner
            .binding()
            .observe_prepared_original(&inputs, lane, offset)?;
        let current_plan = select_prepared_response_data_path(
            &observation.targets,
            lane,
            source.len(),
            owner.binding().mux_limits(),
            &observation.lower_flights,
            state.send_stream.reinjection_bytes(),
            frontier(&state),
            &ready_outputs(&outputs),
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
    result.unwrap_or(PreparedOriginalClaim::Blocked(wake))
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

struct ResponseRepairPlan {
    frame: Frame,
    target: ResponseDispatchTarget,
    model_generation: u64,
}

enum ResponseRepairObservation {
    Ready(ResponseRepairPlan),
    Immature(Instant),
    Unavailable,
}

fn response_repair_plan(
    state: &ResponseProductState,
    binding: &ResponseStreamBinding,
    inputs: &ResponsePreparedNativeInputs,
    outputs: &[ResponsePreparedOutput],
    invoking: ResponseAcquisitionOutputId,
) -> ResponseRepairObservation {
    // Binding membership and flight ownership may change independently of the
    // source lock. Carry this receipt through the final output/flight commit.
    let model_generation = binding.response_model_generation();
    // Queue emptiness is a conservative Product guard, not an impairment test.
    // Carrier-wide foreground arbitration is separately fenced by the writer.
    if !state.sender.queue.is_empty() || state.send_stream.reinjection_bytes() == 0 {
        return ResponseRepairObservation::Unavailable;
    }
    let head = state.send_stream.data_ack_frontier();
    let mut covered = binding.current_reinjected_ranges();
    covered.extend(state.sender.queue.queued_reinjection_ranges());
    let retained = state.send_stream.retained_ranges_in_scope(OffsetRange {
        start: head,
        end: state.send_stream.next_offset(),
    });
    let Some(range) = offset_ranges_not_covered(&retained, &normalize_offset_ranges(covered))
        .into_iter()
        .next()
    else {
        return ResponseRepairObservation::Unavailable;
    };
    if range.start == head {
        return ResponseRepairObservation::Unavailable;
    }
    let lane = state.prepared.response_lane;
    let Some(observation) = binding.observe_prepared_original(inputs, lane, range.start) else {
        return ResponseRepairObservation::Unavailable;
    };
    let owner_capable = |target: &crate::runtime::stream::response::ResponseSenderPathTarget| {
        target.product_admission_active
            && !target.observation.stale_for_original_data
            && crate::scheduler::path_is_schedulable(response_completion_snapshot(target), lane)
    };
    let Some(uniform) = binding.live_owner_uniform_frontier(range) else {
        return ResponseRepairObservation::Unavailable;
    };
    if uniform.owners.len() != 1 {
        return ResponseRepairObservation::Unavailable;
    }
    let owner = uniform.owners[0];
    if !observation.targets.iter().any(|target| {
        target.observation.key == owner.key
            && target.observation.incarnation == owner.incarnation
            && owner_capable(target)
    }) {
        return ResponseRepairObservation::Unavailable;
    }
    // Both directions rank the same common live-path quantum, then apply the
    // selected target's own quantum and exact service limit without enlarging it.
    let quantum = observation
        .targets
        .iter()
        .filter(|target| owner_capable(target))
        .map(|target| {
            adaptive_reliable_relay_reinjection_bytes(
                Some(response_completion_snapshot(target)),
                lane,
                binding.mux_limits(),
            )
        })
        .max()
        .unwrap_or(0);
    let preview_range = OffsetRange {
        start: range.start,
        end: uniform
            .range
            .end
            .min(range.start.saturating_add(quantum as u64)),
    };
    let now = Instant::now();
    let Some(maturity) =
        binding.observe_prepared_live_owner_frontier(preview_range, &observation.targets, now)
    else {
        return ResponseRepairObservation::Unavailable;
    };
    let Some(mature) = maturity.mature_frontier else {
        return if maturity.owner_fallback_deadline > now {
            ResponseRepairObservation::Immature(maturity.owner_fallback_deadline)
        } else {
            ResponseRepairObservation::Unavailable
        };
    };
    let Some(frame) = state
        .send_stream
        .first_retransmission_frame_for_range(mature.range, quantum)
    else {
        return ResponseRepairObservation::Unavailable;
    };
    let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
    let available = |target: &crate::runtime::stream::response::ResponseSenderPathTarget| {
        let identity = ServerReinjectionOutputIdentity {
            key: target.observation.key,
            incarnation: target.observation.incarnation,
        };
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(
                Some(response_completion_snapshot(target)),
                state
                    .sender
                    .queue
                    .response_target_queued_reinjection_bytes(identity, false),
                binding.accepted_reinjected_data_in_flight_bytes_at(identity),
            ),
            state.send_stream.reinjection_bytes(),
            binding.mux_limits(),
        )
    };
    let targets = observation
        .targets
        .into_iter()
        .filter(|target| {
            let identity = ResponseAcquisitionOutputId::from(target);
            (identity == invoking
                || outputs.iter().any(|output| {
                    output.identity == identity
                        && output.commands.background_repair_ready().is_some()
                }))
                && available(target) > 0
        })
        .collect::<Vec<_>>();
    let avoid = binding.reinjection_avoid_outputs_for_frame(&frame);
    let Some(target) = select_response_frame_path_for_extent(
        &targets,
        lane,
        &frame,
        CarrierEmitMode::Classified,
        &avoid,
        Some(RelaySendCause::TailReinjection),
        payload_bytes,
    ) else {
        return ResponseRepairObservation::Unavailable;
    };
    let extent = available(&target).min(reliable_live_frontier_reinjection_limit_bytes(
        adaptive_reliable_relay_reinjection_bytes(
            Some(response_completion_snapshot(&target)),
            lane,
            binding.mux_limits(),
        ),
        quantum,
        payload_bytes,
        state.send_stream.reinjection_bytes(),
        binding.mux_limits(),
    ));
    let Frame::StreamData {
        stream_id,
        offset,
        payload,
    } = frame
    else {
        return ResponseRepairObservation::Unavailable;
    };
    if extent == 0 {
        return ResponseRepairObservation::Unavailable;
    }
    ResponseRepairObservation::Ready(ResponseRepairPlan {
        frame: Frame::StreamData {
            stream_id,
            offset,
            payload: payload.slice(..extent),
        },
        target: ResponseDispatchTarget::from(target),
        model_generation,
    })
}

fn response_repair_wait_until(
    wait: PreparedOriginalWait,
    deadline: Instant,
) -> PreparedOriginalWait {
    Box::pin(async move {
        tokio::select! {
            () = wait => {}
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
        }
    })
}

/// Acquires one successor only at an actual background writer opportunity.
/// The offered notice owns no retained bytes, copy slot, or native reservation.
pub(in crate::runtime) fn claim_prepared_response_repair(
    owner: &SharedResponseProduct,
    identity: ResponseAcquisitionOutputId,
    ready: &ReliableWriterReadyGuard,
    registration: &PreparedOriginalRegistration,
) -> PreparedOriginalClaim {
    if ready.receipt().instance() != identity.path_instance_id || !ready.receipt().is_current() {
        return PreparedOriginalClaim::Empty;
    }
    let state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
    };
    if !current(&state, identity, registration) {
        return PreparedOriginalClaim::Empty;
    }
    let outputs = owner.binding().prepared_outputs();
    let wake = arm_work_change(
        &state,
        identity,
        &outputs,
        owner.binding().subscribe_updates(),
    );
    if !state.sender.queue.is_empty() {
        return PreparedOriginalClaim::Blocked(wake);
    }
    let Some(output) = outputs
        .iter()
        .find(|output| output.identity == identity)
        .cloned()
    else {
        return PreparedOriginalClaim::Empty;
    };
    drop(state);
    let mut inputs = ResponsePreparedNativeInputs::resolve(outputs.clone());
    let state = match owner.arm_claim().try_lock() {
        Ok(state) => state,
        Err(wait) => return PreparedOriginalClaim::Busy(wait),
    };
    if !current(&state, identity, registration) || !ready.receipt().is_current() {
        return PreparedOriginalClaim::Empty;
    }
    let plan = match response_repair_plan(&state, owner.binding(), &inputs, &outputs, identity) {
        ResponseRepairObservation::Ready(plan) => plan,
        ResponseRepairObservation::Immature(deadline) => {
            return PreparedOriginalClaim::Blocked(response_repair_wait_until(wake, deadline));
        }
        ResponseRepairObservation::Unavailable => return PreparedOriginalClaim::Blocked(wake),
    };
    let selected = ResponseAcquisitionOutputId {
        key: plan.target.key,
        incarnation: plan.target.incarnation,
        path_instance_id: plan.target.path_instance_id,
    };
    if selected != identity {
        if let Some(other) = state
            .prepared
            .registrations
            .iter()
            .find(|other| other.response_instance() == Some(selected))
        {
            other.notify_repair();
        }
        return PreparedOriginalClaim::Blocked(wake);
    }
    drop(state);
    let mut commit = |shape| {
        if let Some(shape) = shape {
            inputs.replace_fenced_target(identity, shape);
        }
        let mut state = match owner.arm_claim().try_lock() {
            Ok(state) => state,
            Err(wait) => return Some(PreparedOriginalClaim::Busy(wait)),
        };
        if !current(&state, identity, registration) || !ready.receipt().is_current() {
            return None;
        }
        let ResponseRepairObservation::Ready(current_plan) =
            response_repair_plan(&state, owner.binding(), &inputs, &outputs, identity)
        else {
            return None;
        };
        if current_plan.target != plan.target || current_plan.frame != plan.frame {
            return None;
        }
        let target_identity = ServerReinjectionOutputIdentity {
            key: identity.key,
            incarnation: identity.incarnation,
        };
        owner
            .binding()
            .claim_reinjected_frame_for_target(
                &current_plan.target,
                &current_plan.frame,
                state.prepared.response_lane,
                state
                    .sender
                    .queue
                    .response_target_queued_reinjection_bytes(target_identity, false),
                state.send_stream.reinjection_bytes(),
                ready,
                shape,
                current_plan.model_generation,
            )
            .ok()?;
        state
            .sender
            .optional_reinjection
            .record_reinjection(reliable_stream_frame_accounted_bytes(&current_plan.frame));
        state.prepared.work_changed.notify_waiters();
        Some(PreparedOriginalClaim::Claimed(current_plan.frame))
    };
    let result = match (
        output.commands.native_rate_authority(),
        plan.target.native_authority_stamp,
    ) {
        (Some(authority), Some(stamp)) => authority
            .commit_with_current_scheduling_shape(stamp, |shape| commit(Some(shape)))
            .ok()
            .flatten(),
        (None, None) if identity.key.underlay == UnderlayProtocol::Tcp => commit(None),
        _ => None,
    };
    result.unwrap_or(PreparedOriginalClaim::Blocked(wake))
}
