//! Target-relay ownership for client-side TCP carrier validation.
//!
//! The coordinator stays outside generic relay state. It joins session-owned
//! admission, the validation-purpose carrier actor, exact Product assignment
//! and ACK provenance, and the RFC comparison state without publishing the
//! candidate into ordinary membership.

use crate::model::path::RelayPathInstance;
use crate::model::tcp_carrier::{
    TcpCarrierCandidateWorkState, TcpCarrierStableGenerations, TcpCarrierValidationPhase,
    TcpCarrierValidationState, TcpCarrierValidationUpdate,
};
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{Frame, OffsetRange, TcpCarrierValidationResult};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{ReliablePathCommandSender, reliable_path_command_queue};
use crate::runtime::path::tcp::client_validation::{
    ClientTcpValidationCandidate, ClientTcpValidationController, ClientTcpValidationEvent,
    ClientTcpValidationHandoff, ClientTcpValidationSession,
};
use crate::runtime::path::tcp::service::{
    ClientTcpCarrierDemand, ClientTcpCarrierObservation, ClientTcpCarrierOrdinaryService,
    ClientTcpCarrierSaturation, ClientTcpCarrierWorkloadLease,
};
use crate::runtime::sender::{
    ProductWorkloadIdentity, ReliableRelaySenderQueue, RequestOrdinarySaturationObservation,
    RequestProductAckOriginalResolution, RequestProductAckReceipt, RequestSenderService,
};
use crate::runtime::stream::{
    ReliableRelayAttachmentReservation, ReliableRelayRemoteFrame, ReliableRelayRemoteSet,
};
use std::num::NonZeroU64;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProductCohortKind {
    Reference,
    Assisted,
    Confirmation,
}

#[derive(Debug)]
pub(super) struct ProductCohort {
    pub(super) kind: ProductCohortKind,
    pub(super) opening_writer_at: Instant,
    pub(super) opening_ack_at: Instant,
    pub(super) target_bytes: u64,
    pub(super) aggregate_bytes: u64,
    pub(super) candidate_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CompleteProductCohort {
    pub(super) kind: ProductCohortKind,
    pub(super) opening_writer_at: Instant,
    pub(super) opening_ack_at: Instant,
    pub(super) closing_ack_at: Instant,
    pub(super) target_bytes: u64,
    pub(super) aggregate_bytes: u64,
    pub(super) candidate_bytes: u64,
}

pub(super) enum WriterBoundaryPurpose {
    Open {
        kind: ProductCohortKind,
        opening_ack_at: Instant,
    },
    Close(CompleteProductCohort),
}

struct PendingWriterBoundary {
    purpose: WriterBoundaryPurpose,
    task: JoinHandle<Result<Instant, RuntimeError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ValidationControlPurpose {
    Result(TcpCarrierValidationResult),
    CandidateWorkZero,
}

struct PendingValidationControl {
    purpose: ValidationControlPurpose,
    task: JoinHandle<Result<(), RuntimeError>>,
}

pub(super) enum ClientC2sTcpValidationInput {
    CarrierEvent(Option<ClientTcpValidationEvent>),
    Observation(Option<ClientTcpCarrierObservation>),
    PolicyChanged,
    WriterBoundary(
        WriterBoundaryPurpose,
        Result<Result<Instant, RuntimeError>, tokio::task::JoinError>,
    ),
    Control(
        ValidationControlPurpose,
        Result<Result<(), RuntimeError>, tokio::task::JoinError>,
    ),
    CarrierFinished(Result<Result<(), RuntimeError>, tokio::task::JoinError>),
}

pub(super) enum ClientC2sTcpValidationAction {
    None,
    RemoteFrame(ReliableRelayRemoteFrame),
    RecoverCandidate(RelayPathInstance),
    Retained {
        handoff: Box<ClientTcpValidationHandoff>,
        attachment: ReliableRelayAttachmentReservation,
    },
    Finished,
}

/// One target stream's exact C2S validation transaction.
pub(super) struct ClientC2sTcpValidation {
    target: ProductWorkloadIdentity,
    validation_id: NonZeroU64,
    admission_generation: NonZeroU64,
    stable: TcpCarrierStableGenerations,
    ordinary_instances: Box<[RelayPathInstance]>,
    workloads: Box<[ProductWorkloadIdentity]>,
    candidate_instance: RelayPathInstance,
    attachment_reservation: Option<ReliableRelayAttachmentReservation>,
    candidate: Option<ClientTcpValidationCandidate>,
    candidate_ranges: Vec<OffsetRange>,
    candidate_max_end: u64,
    validation: TcpCarrierValidationState,
    phase: TcpCarrierValidationPhase,
    controller: ClientTcpValidationController,
    validation_data: Option<ReliablePathCommandSender>,
    carrier_events: Option<mpsc::Receiver<ClientTcpValidationEvent>>,
    observations: Option<mpsc::Receiver<ClientTcpCarrierObservation>>,
    policy_changes: watch::Receiver<Option<crate::model::tcp_carrier::TcpCarrierPolicyEpochs>>,
    carrier_task: Option<JoinHandle<Result<(), RuntimeError>>>,
    writer_boundary: Option<PendingWriterBoundary>,
    control: Option<PendingValidationControl>,
    cohort: Option<ProductCohort>,
    awaiting_opening_ack: bool,
    assisted_cohort_closed: bool,
    result_serialized: Option<TcpCarrierValidationResult>,
    negative_acknowledged: bool,
    work_zero_confirmed: bool,
    finished: bool,
}

impl ClientC2sTcpValidation {
    pub(super) fn admit(
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        workload: &mut ClientTcpCarrierWorkloadLease,
        saturation: RequestOrdinarySaturationObservation,
    ) -> Result<Option<Self>, RuntimeError> {
        if saturation.stream_id != workload.identity().stream_id {
            return Ok(None);
        }
        let saturation = ClientTcpCarrierSaturation {
            stable: saturation.stable,
            ordinary_services: saturation
                .ordinary_services
                .into_iter()
                .map(|ordinary| ClientTcpCarrierOrdinaryService {
                    instance: ordinary.instance,
                    service_pipe_bytes: ordinary.service_pipe_bytes,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            eligible_tcp_groups: (0..context.configured_tcp_endpoint_count())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        };
        let Some(admission) = workload.try_admit_saturation(
            saturation,
            &context.tcp_carrier_groups,
            context.mux_limits,
        ) else {
            return Ok(None);
        };

        let target = admission.target();
        let validation_id = admission.validation_id();
        let admission_generation = admission.admission_generation();
        let stable = admission.stable();
        let ordinary_instances = admission
            .ordinary_services()
            .iter()
            .map(|ordinary| ordinary.instance)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let workloads = admission.workloads().to_vec().into_boxed_slice();
        let geometry = admission.geometry();
        let config_index = admission.config_index();
        let member_path_index = context
            .tcp_endpoint(config_index)
            .and_then(|group| group.members.first())
            .copied()
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let session = context
            .tcp_sessions
            .get(member_path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let attachment_reservation = remotes.reserve_attachment_incarnation();
        let actor_admission = session.c2s_validation_admission(
            admission,
            target.stream_id,
            attachment_reservation.attachment_id(),
            tokio::time::Instant::now() + context.path_probe_timeout,
        )?;
        let candidate_instance = actor_admission.instance();
        let attachment_reservation =
            attachment_reservation.bind_exact(target.stream_id, candidate_instance)?;
        let (carrier, controller, carrier_events) =
            ClientTcpValidationSession::new(actor_admission);
        let carrier_task = tokio::spawn(carrier.run());

        Ok(Some(Self {
            target,
            validation_id,
            admission_generation,
            stable,
            ordinary_instances,
            workloads,
            candidate_instance,
            attachment_reservation: Some(attachment_reservation),
            candidate: None,
            candidate_ranges: Vec::new(),
            candidate_max_end: 0,
            validation: TcpCarrierValidationState::new(geometry),
            phase: TcpCarrierValidationPhase::Reference,
            controller,
            validation_data: None,
            carrier_events: Some(carrier_events),
            observations: None,
            policy_changes: context.subscribe_tcp_carrier_policy_epochs(),
            carrier_task: Some(carrier_task),
            writer_boundary: None,
            control: None,
            cohort: None,
            awaiting_opening_ack: false,
            assisted_cohort_closed: false,
            result_serialized: None,
            negative_acknowledged: false,
            work_zero_confirmed: false,
            finished: false,
        }))
    }

    pub(super) async fn next_input(&mut self) -> ClientC2sTcpValidationInput {
        tokio::select! {
            biased;
            event = recv_optional(self.carrier_events.as_mut()) => {
                ClientC2sTcpValidationInput::CarrierEvent(event)
            }
            observation = recv_optional(self.observations.as_mut()) => {
                ClientC2sTcpValidationInput::Observation(observation)
            }
            _ = self.policy_changes.changed() => {
                ClientC2sTcpValidationInput::PolicyChanged
            }
            boundary = wait_optional_task(
                self.writer_boundary.as_mut().map(|pending| &mut pending.task)
            ) => {
                let purpose = self.writer_boundary
                    .take()
                    .expect("completed writer boundary remains owned")
                    .purpose;
                ClientC2sTcpValidationInput::WriterBoundary(purpose, boundary)
            }
            completion = wait_optional_task(
                self.control.as_mut().map(|pending| &mut pending.task)
            ) => {
                let purpose = self.control
                    .take()
                    .expect("completed validation control remains owned")
                    .purpose;
                ClientC2sTcpValidationInput::Control(purpose, completion)
            }
            completion = wait_optional_task(self.carrier_task.as_mut()) => {
                self.carrier_task = None;
                ClientC2sTcpValidationInput::CarrierFinished(completion)
            }
        }
    }

    pub(super) fn handle_input(
        &mut self,
        context: &ClientPathContext,
        input: ClientC2sTcpValidationInput,
    ) -> ClientC2sTcpValidationAction {
        match input {
            ClientC2sTcpValidationInput::CarrierEvent(Some(event)) => {
                self.handle_carrier_event(context, event)
            }
            ClientC2sTcpValidationInput::CarrierEvent(None) => {
                self.carrier_events = None;
                ClientC2sTcpValidationAction::None
            }
            ClientC2sTcpValidationInput::Observation(Some(observation)) => {
                self.handle_observation(observation);
                ClientC2sTcpValidationAction::None
            }
            ClientC2sTcpValidationInput::Observation(None) => {
                self.observations = None;
                self.withdraw();
                ClientC2sTcpValidationAction::None
            }
            ClientC2sTcpValidationInput::PolicyChanged => {
                self.withdraw();
                ClientC2sTcpValidationAction::None
            }
            ClientC2sTcpValidationInput::WriterBoundary(purpose, completion) => {
                match completion {
                    Ok(Ok(completed_at)) => self.complete_writer_boundary(purpose, completed_at),
                    Ok(Err(_)) | Err(_) => self.withdraw(),
                }
                ClientC2sTcpValidationAction::None
            }
            ClientC2sTcpValidationInput::Control(purpose, completion) => {
                if !matches!(completion, Ok(Ok(()))) {
                    self.withdraw();
                    return ClientC2sTcpValidationAction::RecoverCandidate(self.candidate_instance);
                }
                match purpose {
                    ValidationControlPurpose::Result(result) => {
                        self.result_serialized = Some(result);
                    }
                    ValidationControlPurpose::CandidateWorkZero => {
                        self.work_zero_confirmed = true;
                    }
                }
                ClientC2sTcpValidationAction::None
            }
            ClientC2sTcpValidationInput::CarrierFinished(completion) => {
                if !self.finished {
                    let _ = completion;
                    self.finished = true;
                    ClientC2sTcpValidationAction::RecoverCandidate(self.candidate_instance)
                } else {
                    ClientC2sTcpValidationAction::Finished
                }
            }
        }
    }

    fn handle_carrier_event(
        &mut self,
        context: &ClientPathContext,
        event: ClientTcpValidationEvent,
    ) -> ClientC2sTcpValidationAction {
        match event {
            ClientTcpValidationEvent::Admitted {
                candidate,
                validation_data,
            } => {
                if !self.exact_candidate(candidate) {
                    self.withdraw();
                    return ClientC2sTcpValidationAction::None;
                }
                self.candidate = Some(candidate);
                self.validation_data = Some(validation_data);
                self.observations = context.activate_tcp_carrier_observations(
                    self.validation_id,
                    self.admission_generation,
                    reliable_path_command_queue(context.mux_limits),
                );
                if self.observations.is_none() {
                    self.withdraw();
                } else {
                    // The reference opens only after the next fully processed
                    // target ACK and its following exact writer boundary.
                    self.awaiting_opening_ack = true;
                }
                ClientC2sTcpValidationAction::None
            }
            ClientTcpValidationEvent::Control { candidate, frame } => {
                if !self.exact_candidate(candidate) {
                    self.withdraw();
                    return ClientC2sTcpValidationAction::None;
                }
                match frame {
                    frame @ (Frame::StreamAck { .. }
                    | Frame::StreamMaxData { .. }
                    | Frame::StreamFin { .. }
                    | Frame::StreamReset { .. }
                    | Frame::StreamDetach { .. }) => {
                        ClientC2sTcpValidationAction::RemoteFrame(ReliableRelayRemoteFrame {
                            instance: candidate.instance,
                            frame: Ok(frame),
                        })
                    }
                    Frame::PathStatus { .. } => ClientC2sTcpValidationAction::None,
                    _ => {
                        self.withdraw();
                        ClientC2sTcpValidationAction::None
                    }
                }
            }
            ClientTcpValidationEvent::ResultAcknowledged { candidate, result } => {
                if !self.exact_candidate(candidate)
                    || self.result_serialized != Some(result)
                    || result == TcpCarrierValidationResult::Retain
                {
                    self.withdraw();
                } else {
                    self.negative_acknowledged = true;
                }
                ClientC2sTcpValidationAction::None
            }
            ClientTcpValidationEvent::Retained(handoff) => {
                if !self.exact_candidate(handoff.candidate)
                    || self.result_serialized != Some(TcpCarrierValidationResult::Retain)
                {
                    self.withdraw();
                    ClientC2sTcpValidationAction::RecoverCandidate(self.candidate_instance)
                } else {
                    self.finished = true;
                    ClientC2sTcpValidationAction::Retained {
                        handoff,
                        attachment: self
                            .attachment_reservation
                            .take()
                            .expect("live validation owns its attachment reservation"),
                    }
                }
            }
            ClientTcpValidationEvent::Drained { candidate } => {
                if !self.exact_candidate(candidate)
                    || !self.negative_acknowledged
                    || !self.work_zero_confirmed
                {
                    self.withdraw();
                    return ClientC2sTcpValidationAction::RecoverCandidate(self.candidate_instance);
                }
                self.finished = true;
                ClientC2sTcpValidationAction::Finished
            }
            ClientTcpValidationEvent::ReceiverAdmitted { .. }
            | ClientTcpValidationEvent::ResultReceived { .. } => {
                self.withdraw();
                ClientC2sTcpValidationAction::RecoverCandidate(self.candidate_instance)
            }
        }
    }

    fn exact_candidate(&self, candidate: ClientTcpValidationCandidate) -> bool {
        candidate.validation_id == self.validation_id
            && candidate.stream_id == self.target.stream_id
            && candidate.instance == self.candidate_instance
    }

    fn handle_observation(&mut self, observation: ClientTcpCarrierObservation) {
        match observation {
            ClientTcpCarrierObservation::ProductAck(receipt) => {
                self.handle_product_ack(receipt);
            }
        }
    }

    fn handle_product_ack(&mut self, receipt: RequestProductAckReceipt) {
        if !self.workloads.contains(&receipt.identity) {
            self.withdraw();
            return;
        }

        let mut candidate_resolved = 0_u64;
        let mut candidate_qualified = 0_u64;
        for release in &receipt.original_releases {
            if release.instance != self.candidate_instance {
                continue;
            }
            candidate_resolved = candidate_resolved.saturating_add(release.bytes as u64);
            if release.resolution == RequestProductAckOriginalResolution::Unambiguous {
                candidate_qualified = candidate_qualified.saturating_add(release.bytes as u64);
            }
        }
        if candidate_resolved != 0
            && self
                .validation
                .observe_candidate_resolution(candidate_resolved, candidate_qualified)
                != TcpCarrierValidationUpdate::Pending
        {
            self.phase = self
                .validation
                .result()
                .map(TcpCarrierValidationPhase::Settled)
                .unwrap_or(self.phase);
        }

        if self.awaiting_opening_ack
            && receipt.identity == self.target
            && self.writer_boundary.is_none()
        {
            self.awaiting_opening_ack = false;
            let kind = match self.phase {
                TcpCarrierValidationPhase::Reference => ProductCohortKind::Reference,
                TcpCarrierValidationPhase::Assisted => ProductCohortKind::Assisted,
                TcpCarrierValidationPhase::Confirmation => ProductCohortKind::Confirmation,
                _ => return,
            };
            self.start_writer_boundary(WriterBoundaryPurpose::Open {
                kind,
                opening_ack_at: receipt.completed_at,
            });
            return;
        }

        let Some(cohort) = self.cohort.as_mut() else {
            return;
        };
        if receipt.completed_at < cohort.opening_ack_at
            || receipt.completed_at < cohort.opening_writer_at
        {
            return;
        }

        let mut qualified = 0_u64;
        let mut candidate = 0_u64;
        for release in &receipt.original_releases {
            if release.resolution != RequestProductAckOriginalResolution::Unambiguous {
                continue;
            }
            let release_bytes = if release.sent_at >= cohort.opening_writer_at {
                release.bytes as u64
            } else {
                0
            };
            qualified = qualified.saturating_add(release_bytes);
            if release.instance == self.candidate_instance {
                candidate = candidate.saturating_add(release_bytes);
            }
        }
        cohort.aggregate_bytes = cohort.aggregate_bytes.saturating_add(qualified);
        if receipt.identity == self.target {
            cohort.target_bytes = cohort.target_bytes.saturating_add(qualified);
            cohort.candidate_bytes = cohort.candidate_bytes.saturating_add(candidate);
        }

        if cohort.target_bytes >= self.validation_geometry_cohort_coverage()
            && self.writer_boundary.is_none()
        {
            let cohort = self.cohort.take().expect("covered cohort remains active");
            self.start_writer_boundary(WriterBoundaryPurpose::Close(CompleteProductCohort {
                kind: cohort.kind,
                opening_writer_at: cohort.opening_writer_at,
                opening_ack_at: cohort.opening_ack_at,
                closing_ack_at: receipt.completed_at,
                target_bytes: cohort.target_bytes,
                aggregate_bytes: cohort.aggregate_bytes,
                candidate_bytes: cohort.candidate_bytes,
            }));
        }
    }

    fn validation_geometry_cohort_coverage(&self) -> u64 {
        // Credit outside CandidateStartup/Assisted is zero, so the geometry's
        // cohort floor is exposed through the immutable admission model.
        self.validation.cohort_coverage_bytes()
    }

    fn start_writer_boundary(&mut self, purpose: WriterBoundaryPurpose) {
        if self.writer_boundary.is_some() {
            self.withdraw();
            return;
        }
        let controller = self.controller.clone();
        self.writer_boundary = Some(PendingWriterBoundary {
            purpose,
            task: tokio::spawn(async move { controller.writer_boundary().await }),
        });
    }

    fn complete_writer_boundary(&mut self, purpose: WriterBoundaryPurpose, completed_at: Instant) {
        match purpose {
            WriterBoundaryPurpose::Open {
                kind,
                opening_ack_at,
            } => {
                let expected = matches!(
                    (kind, self.phase),
                    (
                        ProductCohortKind::Reference,
                        TcpCarrierValidationPhase::Reference
                    ) | (
                        ProductCohortKind::Assisted,
                        TcpCarrierValidationPhase::Assisted
                    ) | (
                        ProductCohortKind::Confirmation,
                        TcpCarrierValidationPhase::Confirmation
                    )
                );
                if !expected || self.cohort.is_some() {
                    self.withdraw();
                    return;
                }
                self.cohort = Some(ProductCohort {
                    kind,
                    opening_writer_at: completed_at,
                    opening_ack_at,
                    target_bytes: 0,
                    aggregate_bytes: 0,
                    candidate_bytes: 0,
                });
            }
            WriterBoundaryPurpose::Close(cohort) => {
                let writer_elapsed = completed_at
                    .checked_duration_since(cohort.opening_writer_at)
                    .unwrap_or(Duration::ZERO);
                let ack_elapsed = cohort
                    .closing_ack_at
                    .checked_duration_since(cohort.opening_ack_at)
                    .unwrap_or(Duration::ZERO);
                let update = self.validation.observe_cohort(
                    cohort.target_bytes,
                    cohort.aggregate_bytes,
                    cohort.candidate_bytes,
                    writer_elapsed,
                    ack_elapsed,
                );
                if let TcpCarrierValidationUpdate::Settled(result) = update {
                    self.phase = TcpCarrierValidationPhase::Settled(result);
                    return;
                }
                match cohort.kind {
                    ProductCohortKind::Reference => {
                        self.advance_phase(TcpCarrierCandidateWorkState::default());
                    }
                    ProductCohortKind::Assisted => {
                        self.assisted_cohort_closed = true;
                    }
                    ProductCohortKind::Confirmation => {
                        self.advance_phase(TcpCarrierCandidateWorkState::default());
                    }
                }
            }
        }
    }

    fn advance_phase(&mut self, candidate_work: TcpCarrierCandidateWorkState) {
        match self.validation.advance_at_causal_boundary(candidate_work) {
            TcpCarrierValidationUpdate::Advanced(phase) => {
                self.phase = phase;
                match phase {
                    TcpCarrierValidationPhase::Assisted
                    | TcpCarrierValidationPhase::Confirmation => {
                        self.awaiting_opening_ack = true;
                    }
                    _ => {}
                }
            }
            TcpCarrierValidationUpdate::Settled(result) => {
                self.phase = TcpCarrierValidationPhase::Settled(result);
            }
            TcpCarrierValidationUpdate::Pending => {}
        }
    }

    pub(super) fn revalidate(&mut self, context: &ClientPathContext, membership_generation: u64) {
        if self.finished || self.result_serialized.is_some() {
            return;
        }
        let mut current = self.stable;
        current.membership_generation = membership_generation;
        if !context.revalidate_tcp_carrier_candidate(
            self.validation_id,
            self.admission_generation,
            current,
            &self.ordinary_instances,
        ) {
            self.withdraw();
        }
    }

    pub(super) fn drive(
        &mut self,
        sender: &RequestSenderService,
        sender_queue: &ReliableRelaySenderQueue,
        send_stream: &ReliableSendStream,
    ) {
        if self.finished {
            return;
        }
        let work = self.candidate_work(sender, sender_queue, send_stream);
        match self.phase {
            TcpCarrierValidationPhase::CandidateStartup
                if self.validation.candidate_assignment_credit_bytes() == 0
                    && work == TcpCarrierCandidateWorkState::default() =>
            {
                self.advance_phase(work);
            }
            TcpCarrierValidationPhase::Assisted
                if self.assisted_cohort_closed
                    && work == TcpCarrierCandidateWorkState::default() =>
            {
                self.assisted_cohort_closed = false;
                self.advance_phase(work);
            }
            _ => {}
        }

        if let Some(result) = self.validation.result()
            && self.result_serialized.is_none()
            && self.control.is_none()
        {
            let controller = self.controller.clone();
            self.control = Some(PendingValidationControl {
                purpose: ValidationControlPurpose::Result(result),
                task: tokio::spawn(async move { controller.serialize_result(result).await }),
            });
        }

        if self.negative_acknowledged
            && !self.work_zero_confirmed
            && self.control.is_none()
            && work == TcpCarrierCandidateWorkState::default()
        {
            let controller = self.controller.clone();
            self.control = Some(PendingValidationControl {
                purpose: ValidationControlPurpose::CandidateWorkZero,
                task: tokio::spawn(async move { controller.confirm_candidate_work_zero().await }),
            });
        }
    }

    pub(super) fn dispatch_candidate(
        &mut self,
        sender: &mut RequestSenderService,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        data_quantum_bytes: usize,
    ) -> Result<Option<usize>, RuntimeError> {
        let can_assign = match self.phase {
            TcpCarrierValidationPhase::CandidateStartup => self.writer_boundary.is_none(),
            TcpCarrierValidationPhase::Assisted => {
                self.cohort
                    .as_ref()
                    .is_some_and(|cohort| cohort.kind == ProductCohortKind::Assisted)
                    && self.writer_boundary.is_none()
            }
            _ => false,
        };
        if !can_assign || self.control.is_some() {
            return Ok(None);
        }
        let Some(validation_data) = self.validation_data.as_ref() else {
            return Ok(None);
        };
        let dispatch = sender.dispatch_client_tcp_carrier_validation_data(
            self.validation_id,
            self.candidate_instance,
            validation_data,
            &mut self.validation,
            send_stream,
            sender_queue,
            data_quantum_bytes,
        )?;
        let Some(dispatch) = dispatch else {
            if let Some(result) = self.validation.result() {
                self.phase = TcpCarrierValidationPhase::Settled(result);
            }
            return Ok(None);
        };
        self.candidate_max_end = self.candidate_max_end.max(dispatch.range.end);
        self.candidate_ranges.push(dispatch.range);
        Ok(Some(dispatch.payload_bytes))
    }

    fn candidate_work(
        &self,
        sender: &RequestSenderService,
        sender_queue: &ReliableRelaySenderQueue,
        send_stream: &ReliableSendStream,
    ) -> TcpCarrierCandidateWorkState {
        let queued_bytes = self.validation_data.as_ref().map_or(0, |commands| {
            commands
                .pending_bytes()
                .saturating_add(commands.writer_pending_bytes())
        });
        let original_flight_bytes =
            sender.tcp_carrier_candidate_original_flight_bytes(self.candidate_instance);
        let recovery_bytes =
            u64::from(sender_queue.has_queued_reinjection_range_overlap(&self.candidate_ranges));
        let reorder_debt_bytes = self
            .candidate_max_end
            .saturating_sub(send_stream.data_ack_frontier());
        TcpCarrierCandidateWorkState {
            queued_bytes,
            original_flight_bytes,
            recovery_bytes,
            reorder_debt_bytes,
        }
    }

    fn withdraw(&mut self) {
        if self.result_serialized.is_some() {
            return;
        }
        if let TcpCarrierValidationUpdate::Settled(result) = self.validation.withdraw() {
            self.phase = TcpCarrierValidationPhase::Settled(result);
        }
        self.cohort = None;
        self.awaiting_opening_ack = false;
    }
}

async fn recv_optional<T>(receiver: Option<&mut mpsc::Receiver<T>>) -> Option<T> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

async fn wait_optional_task<T>(
    task: Option<&mut JoinHandle<T>>,
) -> Result<T, tokio::task::JoinError> {
    match task {
        Some(task) => task.await,
        None => std::future::pending().await,
    }
}

pub(super) async fn receive_client_c2s_tcp_validation(
    validation: &mut Option<ClientC2sTcpValidation>,
) -> Option<ClientC2sTcpValidationInput> {
    match validation.as_mut() {
        Some(validation) => Some(validation.next_input().await),
        None => std::future::pending().await,
    }
}

pub(super) enum ClientS2cTcpValidationInput {
    CarrierEvent(Option<ClientTcpValidationEvent>),
    Acknowledgment(
        TcpCarrierValidationResult,
        Result<Result<(), RuntimeError>, tokio::task::JoinError>,
    ),
    CarrierFinished(Result<Result<(), RuntimeError>, tokio::task::JoinError>),
}

pub(super) enum ClientS2cTcpValidationAction {
    None,
    RemoteFrame(ReliableRelayRemoteFrame),
    Retained(Box<ClientTcpValidationHandoff>),
    Finished,
}

/// Receiver-side owner for one exact server-issued S2C demand. Product
/// comparison and verdict remain entirely server-owned; this coordinator only
/// preserves target-frame FIFO ordering before asking the carrier actor to
/// serialize the matching acknowledgment.
pub(super) struct ClientS2cTcpValidation {
    request_id: NonZeroU64,
    stream_id: crate::protocol::StreamId,
    candidate_instance: RelayPathInstance,
    validation_id: NonZeroU64,
    controller: ClientTcpValidationController,
    carrier_events: Option<mpsc::Receiver<ClientTcpValidationEvent>>,
    carrier_task: Option<JoinHandle<Result<(), RuntimeError>>>,
    acknowledgment: Option<(
        TcpCarrierValidationResult,
        JoinHandle<Result<(), RuntimeError>>,
    )>,
    result: Option<TcpCarrierValidationResult>,
    finished: bool,
}

impl ClientS2cTcpValidation {
    pub(super) fn admit(
        context: &ClientPathContext,
        demand: ClientTcpCarrierDemand,
        target_stream_id: crate::protocol::StreamId,
    ) -> Result<Option<Self>, RuntimeError> {
        if demand.stream_id != Some(target_stream_id) {
            return Ok(None);
        }
        let Some(admission) = context.claim_server_to_client_tcp_carrier(demand) else {
            return Ok(None);
        };
        let request_id = admission.request_id();
        let validation_id = admission.validation_id();
        let stream_id = admission.stream_id();
        let config_index = admission.config_index();
        let member_path_index = context
            .tcp_endpoint(config_index)
            .and_then(|group| group.members.first())
            .copied()
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let session = context
            .tcp_sessions
            .get(member_path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let actor_admission = session.s2c_validation_admission(
            admission,
            tokio::time::Instant::now() + context.path_probe_timeout,
        )?;
        let candidate_instance = actor_admission.instance();
        let (carrier, controller, carrier_events) =
            ClientTcpValidationSession::new(actor_admission);
        let carrier_task = tokio::spawn(carrier.run());
        Ok(Some(Self {
            request_id,
            stream_id,
            candidate_instance,
            validation_id,
            controller,
            carrier_events: Some(carrier_events),
            carrier_task: Some(carrier_task),
            acknowledgment: None,
            result: None,
            finished: false,
        }))
    }

    pub(super) fn matches_demand(&self, demand: ClientTcpCarrierDemand) -> bool {
        demand.request_id == self.request_id && demand.stream_id == Some(self.stream_id)
    }

    pub(super) async fn next_input(&mut self) -> ClientS2cTcpValidationInput {
        tokio::select! {
            biased;
            event = recv_optional(self.carrier_events.as_mut()) => {
                ClientS2cTcpValidationInput::CarrierEvent(event)
            }
            completion = wait_optional_task(
                self.acknowledgment.as_mut().map(|(_, task)| task)
            ) => {
                let (result, _) = self.acknowledgment
                    .take()
                    .expect("completed S2C acknowledgment remains owned");
                ClientS2cTcpValidationInput::Acknowledgment(result, completion)
            }
            completion = wait_optional_task(self.carrier_task.as_mut()) => {
                self.carrier_task = None;
                ClientS2cTcpValidationInput::CarrierFinished(completion)
            }
        }
    }

    pub(super) fn handle_input(
        &mut self,
        input: ClientS2cTcpValidationInput,
    ) -> ClientS2cTcpValidationAction {
        match input {
            ClientS2cTcpValidationInput::CarrierEvent(Some(event)) => {
                self.handle_carrier_event(event)
            }
            ClientS2cTcpValidationInput::CarrierEvent(None) => {
                self.carrier_events = None;
                ClientS2cTcpValidationAction::None
            }
            ClientS2cTcpValidationInput::Acknowledgment(result, completion) => {
                if !matches!(completion, Ok(Ok(()))) || self.result != Some(result) {
                    self.finished = true;
                    ClientS2cTcpValidationAction::Finished
                } else {
                    ClientS2cTcpValidationAction::None
                }
            }
            ClientS2cTcpValidationInput::CarrierFinished(completion) => {
                drop(completion);
                self.carrier_task = None;
                self.finished = true;
                ClientS2cTcpValidationAction::Finished
            }
        }
    }

    fn handle_carrier_event(
        &mut self,
        event: ClientTcpValidationEvent,
    ) -> ClientS2cTcpValidationAction {
        match event {
            ClientTcpValidationEvent::ReceiverAdmitted { candidate } => {
                if !self.exact_candidate(candidate) {
                    self.finished = true;
                    return ClientS2cTcpValidationAction::Finished;
                }
                ClientS2cTcpValidationAction::None
            }
            ClientTcpValidationEvent::Control { candidate, frame } => {
                if !self.exact_candidate(candidate) {
                    self.finished = true;
                    return ClientS2cTcpValidationAction::Finished;
                }
                match frame {
                    frame @ Frame::StreamData { stream_id, .. } if stream_id == self.stream_id => {
                        ClientS2cTcpValidationAction::RemoteFrame(ReliableRelayRemoteFrame {
                            instance: candidate.instance,
                            frame: Ok(frame),
                        })
                    }
                    Frame::PathStatus { .. } => ClientS2cTcpValidationAction::None,
                    _ => {
                        self.finished = true;
                        ClientS2cTcpValidationAction::Finished
                    }
                }
            }
            ClientTcpValidationEvent::ResultReceived { candidate, result } => {
                if !self.exact_candidate(candidate)
                    || self.result.replace(result).is_some()
                    || self.acknowledgment.is_some()
                {
                    self.finished = true;
                    return ClientS2cTcpValidationAction::Finished;
                }
                let controller = self.controller.clone();
                self.acknowledgment = Some((
                    result,
                    tokio::spawn(async move {
                        controller.acknowledge_server_to_client_result(result).await
                    }),
                ));
                ClientS2cTcpValidationAction::None
            }
            ClientTcpValidationEvent::Retained(handoff) => {
                if !self.exact_candidate(handoff.candidate)
                    || self.result != Some(TcpCarrierValidationResult::Retain)
                {
                    self.finished = true;
                    ClientS2cTcpValidationAction::Finished
                } else {
                    self.finished = true;
                    ClientS2cTcpValidationAction::Retained(handoff)
                }
            }
            ClientTcpValidationEvent::Drained { candidate } => {
                if !self.exact_candidate(candidate)
                    || !matches!(
                        self.result,
                        Some(
                            TcpCarrierValidationResult::NoGain
                                | TcpCarrierValidationResult::Withdrawn
                        )
                    )
                {
                    self.finished = true;
                    return ClientS2cTcpValidationAction::Finished;
                }
                self.finished = true;
                ClientS2cTcpValidationAction::Finished
            }
            ClientTcpValidationEvent::Admitted { .. }
            | ClientTcpValidationEvent::ResultAcknowledged { .. } => {
                self.finished = true;
                ClientS2cTcpValidationAction::Finished
            }
        }
    }

    fn exact_candidate(&self, candidate: ClientTcpValidationCandidate) -> bool {
        candidate.validation_id == self.validation_id
            && candidate.stream_id == self.stream_id
            && candidate.instance == self.candidate_instance
    }
}

pub(super) async fn receive_client_s2c_tcp_validation(
    validation: &mut Option<ClientS2cTcpValidation>,
) -> Option<ClientS2cTcpValidationInput> {
    match validation.as_mut() {
        Some(validation) => Some(validation.next_input().await),
        None => std::future::pending().await,
    }
}
