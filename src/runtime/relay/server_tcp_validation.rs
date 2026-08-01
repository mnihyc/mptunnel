//! Server response-relay ownership for one S2C TCP carrier validation.
//!
//! The validation-purpose carrier actor remains the sole wire writer. This
//! coordinator joins its exact offer to the existing response sender, Product
//! Data-ACK receipts, and the transport-neutral RFC comparison state without
//! publishing the candidate into ordinary response membership.

use super::tcp_validation::{
    CompleteProductCohort, ProductCohort, ProductCohortKind, ValidationControlPurpose,
    WriterBoundaryPurpose,
};
use crate::model::tcp_carrier::{
    TcpCarrierCandidateWorkState, TcpCarrierValidationPhase, TcpCarrierValidationState,
    TcpCarrierValidationUpdate,
};
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{OffsetRange, TcpCarrierValidationResult};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::reliable_path_command_queue;
use crate::runtime::path::tcp::server_service::{
    ResponseProductAckReceipt, ServerTcpCarrierObservation, ServerTcpCarrierOutputInstance,
    ServerTcpCarrierValidationAdmission, ServerTcpCarrierValidationOffer,
    ServerTcpValidationController, ServerTcpValidationEvent, candidate_original_release_progress,
};
use crate::runtime::sender::{ProductWorkloadIdentity, ServerResponseSenderService};
use crate::runtime::stream::response::{
    ResponseProductAckOriginalRelease, ResponseProductAckOriginalResolution,
    ServerTcpValidationOutput,
};
use std::num::NonZeroU64;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

struct PendingWriterBoundary {
    purpose: WriterBoundaryPurpose,
    task: JoinHandle<Result<Instant, RuntimeError>>,
}

struct PendingValidationControl {
    purpose: ValidationControlPurpose,
    task: JoinHandle<Result<(), RuntimeError>>,
}

pub(super) enum ServerS2cTcpValidationInput {
    CarrierEvent(Option<ServerTcpValidationEvent>),
    Observation(Option<ServerTcpCarrierObservation>),
    WriterBoundary(
        WriterBoundaryPurpose,
        Result<Result<Instant, RuntimeError>, tokio::task::JoinError>,
    ),
    Control(
        ValidationControlPurpose,
        Result<Result<(), RuntimeError>, tokio::task::JoinError>,
    ),
}

/// Exact sender-side S2C validation owned by the named response relay.
pub(super) struct ServerS2cTcpValidation {
    target: ProductWorkloadIdentity,
    validation_id: NonZeroU64,
    candidate: ServerTcpCarrierOutputInstance,
    workloads: Box<[ProductWorkloadIdentity]>,
    admission: Option<ServerTcpCarrierValidationAdmission>,
    output: ServerTcpValidationOutput,
    controller: ServerTcpValidationController,
    carrier_events: Option<mpsc::Receiver<ServerTcpValidationEvent>>,
    observations: Option<mpsc::Receiver<ServerTcpCarrierObservation>>,
    candidate_ranges: Vec<OffsetRange>,
    candidate_max_end: u64,
    validation: TcpCarrierValidationState,
    phase: TcpCarrierValidationPhase,
    writer_boundary: Option<PendingWriterBoundary>,
    control: Option<PendingValidationControl>,
    cohort: Option<ProductCohort>,
    awaiting_opening_ack: bool,
    assisted_cohort_closed: bool,
    result_serialized: Option<TcpCarrierValidationResult>,
    finished: bool,
}

impl ServerS2cTcpValidation {
    pub(super) fn admit(
        offer: ServerTcpCarrierValidationOffer,
        expected_target: ProductWorkloadIdentity,
        mux_limits: MuxLimits,
    ) -> Option<Self> {
        let ServerTcpCarrierValidationOffer {
            admission,
            output,
            controller,
            events,
        } = offer;
        let target = admission.target();
        let candidate = admission.candidate();
        let output_identity = output.identity();
        if target != expected_target
            || candidate.key != output_identity.key
            || candidate.path_instance_id != output_identity.path_instance_id
            || candidate.output_incarnation != output_identity.incarnation
            || !output.is_current()
            || !output.peer_available()
        {
            return None;
        }
        let observations =
            admission.activate_observations(reliable_path_command_queue(mux_limits).max(1))?;
        let validation_id = admission.validation_id();
        let workloads = admission.workloads().to_vec().into_boxed_slice();
        let geometry = admission.geometry();
        Some(Self {
            target,
            validation_id,
            candidate,
            workloads,
            admission: Some(admission),
            output,
            controller,
            carrier_events: Some(events),
            observations: Some(observations),
            candidate_ranges: Vec::new(),
            candidate_max_end: 0,
            validation: TcpCarrierValidationState::new(geometry),
            phase: TcpCarrierValidationPhase::Reference,
            writer_boundary: None,
            control: None,
            cohort: None,
            awaiting_opening_ack: true,
            assisted_cohort_closed: false,
            result_serialized: None,
            finished: false,
        })
    }

    pub(super) async fn next_input(&mut self) -> ServerS2cTcpValidationInput {
        tokio::select! {
            biased;
            event = recv_optional(self.carrier_events.as_mut()) => {
                ServerS2cTcpValidationInput::CarrierEvent(event)
            }
            observation = recv_optional(self.observations.as_mut()) => {
                ServerS2cTcpValidationInput::Observation(observation)
            }
            boundary = wait_optional_task(
                self.writer_boundary.as_mut().map(|pending| &mut pending.task)
            ) => {
                let purpose = self.writer_boundary
                    .take()
                    .expect("completed writer boundary remains owned")
                    .purpose;
                ServerS2cTcpValidationInput::WriterBoundary(purpose, boundary)
            }
            completion = wait_optional_task(
                self.control.as_mut().map(|pending| &mut pending.task)
            ) => {
                let purpose = self.control
                    .take()
                    .expect("completed validation control remains owned")
                    .purpose;
                ServerS2cTcpValidationInput::Control(purpose, completion)
            }
        }
    }

    pub(super) fn handle_input(&mut self, input: ServerS2cTcpValidationInput) {
        match input {
            ServerS2cTcpValidationInput::CarrierEvent(Some(event)) => {
                self.handle_carrier_event(event);
            }
            ServerS2cTcpValidationInput::CarrierEvent(None) => self.finish(),
            ServerS2cTcpValidationInput::Observation(Some(
                ServerTcpCarrierObservation::ProductAck(receipt),
            )) => self.handle_product_ack(receipt),
            ServerS2cTcpValidationInput::Observation(None) => {
                self.observations = None;
                self.withdraw();
            }
            ServerS2cTcpValidationInput::WriterBoundary(purpose, completion) => match completion {
                Ok(Ok(completed_at)) => self.complete_writer_boundary(purpose, completed_at),
                Ok(Err(_)) | Err(_) => self.withdraw(),
            },
            ServerS2cTcpValidationInput::Control(
                ValidationControlPurpose::Result(result),
                completion,
            ) => {
                if matches!(completion, Ok(Ok(()))) {
                    self.result_serialized = Some(result);
                } else {
                    self.finish();
                }
            }
            ServerS2cTcpValidationInput::Control(
                ValidationControlPurpose::CandidateWorkZero,
                _,
            ) => {
                // S2C settlement is completed by the server carrier actor at
                // exact RESULT_ACK receipt; this client-sender control is not
                // part of the server transaction.
                self.finish();
            }
        }
    }

    fn handle_carrier_event(&mut self, event: ServerTcpValidationEvent) {
        let _ = event;
        // The carrier actor publishes an event only after exact validation-ID,
        // result, output, and ACK settlement. Either terminal event therefore
        // releases this coordinator's observation admission.
        self.finish();
    }

    pub(super) fn revalidate(&mut self) {
        if self.finished || self.result_serialized.is_some() {
            return;
        }
        let current = self.output.is_current()
            && self.output.peer_available()
            && self
                .admission
                .as_ref()
                .is_some_and(|admission| !admission.is_withdrawn())
            && self
                .admission
                .as_ref()
                .is_some_and(|admission| admission.revalidate_current(&self.output));
        if !current {
            self.withdraw();
        }
    }

    pub(super) fn drive(
        &mut self,
        sender: &ServerResponseSenderService,
        send_stream: &ReliableSendStream,
    ) {
        if self.finished {
            return;
        }
        let work = self.candidate_work(sender, send_stream);
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
            && work == TcpCarrierCandidateWorkState::default()
            && self.result_serialized.is_none()
            && self.control.is_none()
        {
            let controller = self.controller.clone();
            self.control = Some(PendingValidationControl {
                purpose: ValidationControlPurpose::Result(result),
                task: tokio::spawn(async move { controller.serialize_result(result).await }),
            });
        }
    }

    pub(super) fn dispatch_candidate(
        &mut self,
        sender: &mut ServerResponseSenderService,
        send_stream: &mut ReliableSendStream,
        data_quantum_bytes: usize,
    ) -> Result<Option<usize>, RuntimeError> {
        if !self.candidate_dispatch_ready() {
            return Ok(None);
        }
        let dispatch = match sender.dispatch_server_tcp_carrier_validation_data(
            self.validation_id,
            &self.output,
            &mut self.validation,
            send_stream,
            data_quantum_bytes,
        ) {
            Ok(dispatch) => dispatch,
            Err(RuntimeError::ReliablePathRetired) => {
                self.withdraw();
                return Ok(None);
            }
            Err(RuntimeError::SenderServiceBlocked) => return Ok(None),
            Err(error) => return Err(error),
        };
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

    pub(super) fn is_finished(&self) -> bool {
        self.finished
    }

    pub(super) fn candidate_dispatch_ready(&self) -> bool {
        !self.finished
            && self.result_serialized.is_none()
            && self.control.is_none()
            && self.writer_boundary.is_none()
            && self.validation.candidate_assignment_credit_bytes() > 0
            && match self.phase {
                TcpCarrierValidationPhase::CandidateStartup => true,
                TcpCarrierValidationPhase::Assisted => self
                    .cohort
                    .as_ref()
                    .is_some_and(|cohort| cohort.kind == ProductCohortKind::Assisted),
                _ => false,
            }
    }

    fn handle_product_ack(&mut self, receipt: ResponseProductAckReceipt) {
        if !self.workloads.contains(&receipt.identity) {
            self.withdraw();
            return;
        }

        let Some(candidate_progress) =
            candidate_original_release_progress(&receipt, self.candidate)
        else {
            self.withdraw();
            return;
        };
        if candidate_progress.resolved_bytes != 0
            && self.validation.observe_candidate_resolution(
                candidate_progress.resolved_bytes,
                candidate_progress.qualified_bytes,
            ) != TcpCarrierValidationUpdate::Pending
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
            if release.resolution != ResponseProductAckOriginalResolution::Unambiguous
                || release.sent_at < cohort.opening_writer_at
            {
                continue;
            }
            let bytes = release.bytes as u64;
            qualified = qualified.saturating_add(bytes);
            if release_matches_candidate(release, self.candidate) {
                candidate = candidate.saturating_add(bytes);
            }
        }
        cohort.aggregate_bytes = cohort.aggregate_bytes.saturating_add(qualified);
        if receipt.identity == self.target {
            cohort.target_bytes = cohort.target_bytes.saturating_add(qualified);
            cohort.candidate_bytes = cohort.candidate_bytes.saturating_add(candidate);
        }

        if cohort.target_bytes >= self.validation.cohort_coverage_bytes()
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
                if matches!(
                    phase,
                    TcpCarrierValidationPhase::Assisted | TcpCarrierValidationPhase::Confirmation
                ) {
                    self.awaiting_opening_ack = true;
                }
            }
            TcpCarrierValidationUpdate::Settled(result) => {
                self.phase = TcpCarrierValidationPhase::Settled(result);
            }
            TcpCarrierValidationUpdate::Pending => {}
        }
    }

    fn candidate_work(
        &self,
        sender: &ServerResponseSenderService,
        send_stream: &ReliableSendStream,
    ) -> TcpCarrierCandidateWorkState {
        TcpCarrierCandidateWorkState {
            queued_bytes: self.controller.pending_bytes(),
            original_flight_bytes: self.output.original_flight_bytes(),
            recovery_bytes: u64::from(
                sender.has_queued_reinjection_range_overlap(&self.candidate_ranges),
            ),
            reorder_debt_bytes: self
                .candidate_max_end
                .saturating_sub(send_stream.data_ack_frontier()),
        }
    }

    fn withdraw(&mut self) {
        if self.result_serialized.is_some() || self.finished {
            return;
        }
        if let TcpCarrierValidationUpdate::Settled(result) = self.validation.withdraw() {
            self.phase = TcpCarrierValidationPhase::Settled(result);
        }
        self.cohort = None;
        self.awaiting_opening_ack = false;
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.carrier_events = None;
        self.observations = None;
        self.writer_boundary = None;
        self.control = None;
        if let Some(admission) = self.admission.take() {
            admission.release();
        }
    }
}

fn release_matches_candidate(
    release: &ResponseProductAckOriginalRelease,
    candidate: ServerTcpCarrierOutputInstance,
) -> bool {
    release.key == candidate.key
        && release.path_instance_id == Some(candidate.path_instance_id)
        && release.output_incarnation == candidate.output_incarnation
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

pub(super) async fn receive_server_s2c_tcp_validation(
    validation: &mut Option<ServerS2cTcpValidation>,
) -> Option<ServerS2cTcpValidationInput> {
    match validation.as_mut() {
        Some(validation) => Some(validation.next_input().await),
        None => std::future::pending().await,
    }
}
