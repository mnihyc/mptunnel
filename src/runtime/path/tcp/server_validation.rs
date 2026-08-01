//! Server ownership of one validation-purpose TCP carrier.
//!
//! A validation carrier is deliberately kept out of the ordinary TCP actor:
//! before an acknowledged directional retain it may route only the finite
//! Product work named by the exact validation transaction.  This actor owns
//! the wire ordering, absolute validation lifetime, and ordered carrier drain;
//! the existing Product stream remains owned by the stream registry.

use super::io::encrypted_framed_peer_closed;
use super::server_evidence::ServerTcpEvidenceState;
use super::server_service::{
    ServerTcpCarrierDemand, ServerTcpCarrierDemandSubscription, ServerTcpCarrierValidationOffer,
    ServerTcpValidationControl, ServerTcpValidationController, ServerTcpValidationEvent,
};
use super::server_writer::ServerTcpWriter;
use crate::protocol::{
    CloseReason, Frame, PathId, PathMetricDirection, PeerPathState, SessionId, StreamId,
    TcpCarrierValidationResult,
};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    recv_reliable_path_command, recv_reliable_path_command_during_drain,
    reliable_path_command_pending_bytes, reliable_path_command_queue,
    reliable_path_command_writer_run_budget_bytes, reliable_path_command_writer_run_budget_items,
    reliable_path_command_writer_run_bytes, try_recv_reliable_path_command,
};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    ServerCarrierPathRegistration, ServerStreamFrameRoute, ServerTcpCarrierValidationLease,
    ServerValidationStreamBinding,
};
use crate::runtime::peer_status::PeerStatusCarrier;
use crate::runtime::stream::response::ServerTcpValidationOutput;
use crate::transport::encrypted::EncryptedFramedTransportError;
use futures::FutureExt;
use std::future::Future;
use std::num::NonZeroU64;
use std::time::Instant;
use tokio::sync::mpsc;

pub(super) struct ServerTcpValidationAdmission {
    pub(super) context: ServerPathContext,
    pub(super) session_id: SessionId,
    pub(super) path_id: PathId,
    pub(super) path_registration: ServerCarrierPathRegistration,
    pub(super) writer: ServerTcpWriter,
    pub(super) path_frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    pub(super) commands_tx: ReliablePathCommandSender,
    pub(super) commands_rx: ReliablePathCommandReceivers,
    pub(super) evidence: ServerTcpEvidenceState,
    pub(super) peer_status: PeerStatusCarrier,
    pub(super) carrier_demands: ServerTcpCarrierDemandSubscription,
}

struct ActiveClientToServerValidation {
    validation_id: NonZeroU64,
    stream_id: StreamId,
    binding: ServerValidationStreamBinding,
    lease: ServerTcpCarrierValidationLease,
}

struct SettlingClientToServerValidation {
    binding: Option<ServerValidationStreamBinding>,
}

struct ActiveServerToClientValidation {
    validation_id: NonZeroU64,
    stream_id: StreamId,
    output: ServerTcpValidationOutput,
    lease: ServerTcpCarrierValidationLease,
    controls: mpsc::Receiver<ServerTcpValidationControl>,
    events: mpsc::Sender<ServerTcpValidationEvent>,
    immutable_result: Option<TcpCarrierValidationResult>,
}

struct SettlingServerToClientValidation {
    stream_id: StreamId,
    output: ServerTcpValidationOutput,
}

enum ServerTcpValidationLifecycle {
    AwaitingValidation,
    ClientToServerActive(ActiveClientToServerValidation),
    ClientToServerSettling(SettlingClientToServerValidation),
    ServerToClientActive(ActiveServerToClientValidation),
    ServerToClientSettling(SettlingServerToClientValidation),
    ServerToClientRetained {
        stream_id: StreamId,
        output: ServerTcpValidationOutput,
    },
    Retained {
        /// The measured attachment remains usable after RETAIN but is not the
        /// authority itself. Detaching it does not revoke the registry-owned
        /// directional authority.
        validation_attachment: Option<ServerValidationStreamBinding>,
    },
    Draining,
}

pub(super) struct ServerTcpValidationSession {
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
    writer: ServerTcpWriter,
    path_frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    commands_tx: Option<ReliablePathCommandSender>,
    commands_rx: ReliablePathCommandReceivers,
    evidence: ServerTcpEvidenceState,
    peer_status: PeerStatusCarrier,
    carrier_demands: ServerTcpCarrierDemandSubscription,
    last_validation_id: u64,
    lifecycle: ServerTcpValidationLifecycle,
    validation_deadline: Option<tokio::time::Instant>,
}

impl ServerTcpValidationSession {
    pub(super) fn new(admission: ServerTcpValidationAdmission) -> Self {
        Self {
            context: admission.context,
            session_id: admission.session_id,
            path_id: admission.path_id,
            path_registration: admission.path_registration,
            writer: admission.writer,
            path_frames: admission.path_frames,
            commands_tx: Some(admission.commands_tx),
            commands_rx: admission.commands_rx,
            evidence: admission.evidence,
            peer_status: admission.peer_status,
            carrier_demands: admission.carrier_demands,
            last_validation_id: 0,
            lifecycle: ServerTcpValidationLifecycle::AwaitingValidation,
            validation_deadline: None,
        }
    }

    pub(super) async fn run(mut self) -> Result<(), RuntimeError> {
        let retirement = self
            .context
            .wait_for_credential_retirement(self.path_registration.principal_permit().clone());
        tokio::pin!(retirement);

        if let Some(demand) = self.carrier_demands.current()
            && !self.write_frame(&demand.into_frame()).await?
        {
            return Ok(());
        }

        loop {
            if self
                .validation_deadline
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            {
                self.expire_validation();
                return Ok(());
            }
            self.evidence
                .observe_periodic(&self.context, &self.path_registration, self.path_id);
            let sender_observation_at = self.evidence.next_sender_observation_at();
            let validation_deadline = self.validation_deadline;
            let server_to_client_command_enabled = matches!(
                &self.lifecycle,
                ServerTcpValidationLifecycle::ServerToClientActive(active)
                    if active.immutable_result.is_none()
            ) || matches!(
                self.lifecycle,
                ServerTcpValidationLifecycle::ServerToClientRetained { .. }
            );
            let server_to_client_control_enabled = matches!(
                &self.lifecycle,
                ServerTcpValidationLifecycle::ServerToClientActive(active)
                    if active.immutable_result.is_none()
            );
            let event = tokio::select! {
                biased;
                () = &mut retirement => return Ok(()),
                () = sleep_until_optional_deadline(validation_deadline) => {
                    self.expire_validation();
                    return Ok(());
                }
                request_id = self.peer_status.recv_request() => {
                    match request_id {
                        Some(request_id) => ValidationEvent::PeerStatusRequest(request_id),
                        None => continue,
                    }
                }
                frame = self.path_frames.recv() => {
                    match frame {
                        Some(Ok(frame)) => ValidationEvent::Frame(frame),
                        Some(Err(error)) if encrypted_framed_peer_closed(&error) => return Ok(()),
                        Some(Err(error)) => return Err(RuntimeError::Encrypted(error)),
                        None => return Ok(()),
                    }
                }
                command = recv_reliable_path_command(&mut self.commands_rx), if server_to_client_command_enabled => ValidationEvent::Command(command),
                control = recv_server_to_client_control(&mut self.lifecycle), if server_to_client_control_enabled => {
                    ValidationEvent::Control(control)
                }
                demand = self.carrier_demands.changed() => {
                    match demand {
                        Some(demand) => ValidationEvent::CarrierDemand(demand),
                        None => return Ok(()),
                    }
                }
                () = wait_for_optional_std_deadline(sender_observation_at) => {
                    ValidationEvent::SenderObservationDue
                }
            };

            match event {
                ValidationEvent::Frame(frame) => {
                    let deadline = self.validation_deadline;
                    let Some(result) =
                        complete_before_optional_deadline(deadline, self.handle_frame(frame)).await
                    else {
                        return Ok(());
                    };
                    if !result? {
                        return Ok(());
                    }
                }
                ValidationEvent::PeerStatusRequest(request_id) => {
                    let frame = Frame::PeerStatusRequest { request_id };
                    let deadline = self.validation_deadline;
                    let Some(result) =
                        complete_before_optional_deadline(deadline, self.write_frame(&frame)).await
                    else {
                        return Ok(());
                    };
                    if !result? {
                        return Ok(());
                    }
                }
                ValidationEvent::SenderObservationDue => {}
                ValidationEvent::CarrierDemand(demand) => {
                    let deadline = self.validation_deadline;
                    let frame = demand.into_frame();
                    let Some(result) =
                        complete_before_optional_deadline(deadline, self.write_frame(&frame)).await
                    else {
                        return Ok(());
                    };
                    if !result? {
                        return Ok(());
                    }
                }
                ValidationEvent::Command(Some(command)) => {
                    if !self.write_server_to_client_command_run(command).await? {
                        return Ok(());
                    }
                }
                ValidationEvent::Command(None) => return Ok(()),
                ValidationEvent::Control(Some(control)) => {
                    if !self.handle_server_to_client_control(control).await? {
                        return Ok(());
                    }
                }
                ValidationEvent::Control(None) => return Ok(()),
            }
        }
    }

    async fn handle_frame(&mut self, frame: Frame) -> Result<bool, RuntimeError> {
        match frame {
            Frame::TcpCarrierValidate {
                validation_id,
                request_id: 0,
                direction: PathMetricDirection::ClientToServer,
                stream_id,
            } => self.admit_client_to_server_validation(validation_id, stream_id),
            Frame::TcpCarrierValidate {
                validation_id,
                request_id,
                direction: PathMetricDirection::ServerToClient,
                stream_id,
            } => self.admit_server_to_client_validation(validation_id, request_id, stream_id),
            Frame::TcpCarrierValidate { .. } => Err(RuntimeError::Protocol(
                "invalid TCP carrier validation request",
            )),
            frame @ (Frame::StreamData { stream_id, .. }
            | Frame::StreamAck { stream_id, .. }
            | Frame::StreamMaxData { stream_id, .. }
            | Frame::StreamFin { stream_id, .. }
            | Frame::StreamReset { stream_id, .. }) => {
                if self.server_to_client_stream_id() == Some(stream_id) {
                    if matches!(frame, Frame::StreamData { .. }) {
                        return Err(RuntimeError::Protocol(
                            "S2C-only TCP carrier received unauthorized request data",
                        ));
                    }
                    self.route_server_to_client_stream_control(stream_id, frame)
                        .await
                } else {
                    self.route_client_to_server_stream_frame(stream_id, frame)
                        .await
                }
            }
            Frame::TcpCarrierResult {
                validation_id,
                direction: PathMetricDirection::ClientToServer,
                result,
            } => {
                self.settle_client_to_server_validation(validation_id, result)
                    .await
            }
            Frame::TcpCarrierResultAck {
                validation_id,
                direction: PathMetricDirection::ServerToClient,
                result,
            } => {
                self.settle_server_to_client_validation(validation_id, result)
                    .await
            }
            Frame::TcpCarrierResult { .. } | Frame::TcpCarrierResultAck { .. } => Err(
                RuntimeError::Protocol("invalid TCP carrier validation result"),
            ),
            Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                self.evidence
                    .record_peer_metrics(&self.context, &self.path_registration, metrics);
                Ok(true)
            }
            Frame::PathStatus {
                path_id,
                sequence,
                usage,
            } if path_id == self.path_id => {
                self.context.reliable_streams.record_peer_path_usage(
                    &self.path_registration,
                    sequence,
                    usage,
                );
                Ok(true)
            }
            Frame::PathStatus { .. } => Err(RuntimeError::Protocol(
                "TCP path usage advertisement path mismatch",
            )),
            Frame::PathProofData {
                path_id,
                proof_id,
                payload,
            } if path_id == self.path_id => {
                let reply =
                    self.evidence
                        .handle_path_proof_data(self.path_id, proof_id, payload.len());
                self.write_frame(&reply).await
            }
            Frame::PathProofAck {
                path_id,
                proof_id,
                payload_bytes,
            } if path_id == self.path_id => {
                self.evidence.handle_path_proof_ack(
                    &self.context,
                    &self.path_registration,
                    self.path_id,
                    proof_id,
                    payload_bytes,
                );
                Ok(true)
            }
            Frame::PathProofData { .. } | Frame::PathProofAck { .. } => {
                Err(RuntimeError::Protocol("TCP path proof path mismatch"))
            }
            Frame::Ping { nonce } => self.write_frame(&Frame::Pong { nonce }).await,
            Frame::PeerStatusRequest { request_id } => {
                let response =
                    self.peer_status
                        .response_frame(request_id, self.context.codec_limits, || {
                            Some(self.context.peer_status_snapshot(self.session_id))
                        });
                self.write_frame(&response).await
            }
            Frame::PeerStatusResponse {
                request_id,
                code,
                paths,
            } => {
                let _ = self.peer_status.receive_response(request_id, code, paths);
                Ok(true)
            }
            Frame::StreamDetach { stream_id } => {
                if self.server_to_client_stream_id() == Some(stream_id) {
                    self.detach_server_to_client_attachment(stream_id)
                } else {
                    self.detach_client_to_server_validation_attachment(stream_id)
                }
            }
            Frame::PathDrain { path_id } if path_id == self.path_id => {
                self.complete_path_drain().await?;
                Ok(false)
            }
            Frame::PathDrain { .. } => Err(RuntimeError::Protocol(
                "TCP path drain request path mismatch",
            )),
            Frame::SessionClose { .. } => Ok(false),
            Frame::PathClose { .. } => Err(RuntimeError::Protocol(
                "TCP server received peer path close",
            )),
            _ => Err(RuntimeError::Protocol(
                "unexpected validation-purpose TCP path frame",
            )),
        }
    }

    fn admit_client_to_server_validation(
        &mut self,
        validation_id: u64,
        stream_id: StreamId,
    ) -> Result<bool, RuntimeError> {
        let validation_id = NonZeroU64::new(validation_id).ok_or(RuntimeError::Protocol(
            "invalid TCP carrier validation identifier",
        ))?;
        if self.path_registration.local_usage() != crate::protocol::PathUsage::Available {
            return Ok(false);
        }
        if validation_id.get() <= self.last_validation_id
            || !matches!(
                self.lifecycle,
                ServerTcpValidationLifecycle::AwaitingValidation
            )
        {
            return Err(RuntimeError::Protocol(
                "TCP carrier validation is not a fresh exact transaction",
            ));
        }
        self.last_validation_id = validation_id.get();
        let lease = self
            .path_registration
            .begin_tcp_carrier_validation(PathMetricDirection::ClientToServer)?;
        let Some(binding) = self
            .context
            .reliable_streams
            .bind_validation_input_existing(&self.path_registration, stream_id)?
        else {
            // A well-formed reference can race stream or carrier retirement.
            // The receiver cannot manufacture the sender-owned WITHDRAWN
            // result, so native close settles this exact candidate.
            return Ok(false);
        };
        self.lifecycle =
            ServerTcpValidationLifecycle::ClientToServerActive(ActiveClientToServerValidation {
                validation_id,
                stream_id,
                binding,
                lease,
            });
        self.validation_deadline =
            Some(tokio::time::Instant::now() + self.context.session_retention_timeout);
        Ok(true)
    }

    fn admit_server_to_client_validation(
        &mut self,
        validation_id: u64,
        request_id: u64,
        stream_id: StreamId,
    ) -> Result<bool, RuntimeError> {
        let validation_id = NonZeroU64::new(validation_id).ok_or(RuntimeError::Protocol(
            "invalid TCP carrier validation identifier",
        ))?;
        let request_id = NonZeroU64::new(request_id).ok_or(RuntimeError::Protocol(
            "invalid S2C TCP carrier demand identifier",
        ))?;
        if validation_id.get() <= self.last_validation_id
            || !matches!(
                self.lifecycle,
                ServerTcpValidationLifecycle::AwaitingValidation
            )
        {
            return Err(RuntimeError::Protocol(
                "TCP carrier validation is not a fresh exact transaction",
            ));
        }
        self.last_validation_id = validation_id.get();
        let lease = self
            .path_registration
            .begin_tcp_carrier_validation(PathMetricDirection::ServerToClient)?;
        let commands = self
            .commands_tx
            .take()
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let Some(output) = self
            .context
            .reliable_streams
            .bind_validation_output_existing(
                &self.path_registration,
                stream_id,
                commands.clone(),
            )?
        else {
            return Ok(false);
        };
        if !output.peer_available() {
            let _ = output.settle();
            return Ok(false);
        }
        let Some(admission) = self.carrier_demands.admit_validation_from_observation(
            request_id,
            validation_id,
            stream_id,
            &output,
        ) else {
            let _ = output.settle();
            return Ok(false);
        };
        let capacity = reliable_path_command_queue(self.context.mux_limits).max(1);
        let (control_tx, controls) = mpsc::channel(capacity);
        let (events, event_rx) = mpsc::channel(capacity);
        let controller = ServerTcpValidationController::new(control_tx, commands, validation_id);
        let offer = ServerTcpCarrierValidationOffer {
            admission,
            output: output.clone(),
            controller,
            events: event_rx,
        };
        if let Some(offer) = self.carrier_demands.publish_validation_offer(offer) {
            let _ = offer.output.settle();
            return Ok(false);
        }
        self.lifecycle =
            ServerTcpValidationLifecycle::ServerToClientActive(ActiveServerToClientValidation {
                validation_id,
                stream_id,
                output,
                lease,
                controls,
                events,
                immutable_result: None,
            });
        self.validation_deadline =
            Some(tokio::time::Instant::now() + self.context.session_retention_timeout);
        Ok(true)
    }

    fn server_to_client_stream_id(&self) -> Option<StreamId> {
        match &self.lifecycle {
            ServerTcpValidationLifecycle::ServerToClientActive(active) => Some(active.stream_id),
            ServerTcpValidationLifecycle::ServerToClientSettling(settling) => {
                Some(settling.stream_id)
            }
            ServerTcpValidationLifecycle::ServerToClientRetained { stream_id, .. } => {
                Some(*stream_id)
            }
            _ => None,
        }
    }

    async fn route_server_to_client_stream_control(
        &mut self,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<bool, RuntimeError> {
        match self.context.reliable_streams.try_route_frame(
            &self.path_registration,
            stream_id,
            frame,
        )? {
            ServerStreamFrameRoute::Routed => Ok(true),
            ServerStreamFrameRoute::Backpressured(frame) => match self
                .context
                .reliable_streams
                .route_frame(&self.path_registration, stream_id, frame)
                .await
            {
                Ok(()) => Ok(true),
                Err(RuntimeError::ReliablePathRetired) => Ok(false),
                Err(error) => Err(error),
            },
        }
    }

    async fn route_client_to_server_stream_frame(
        &mut self,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<bool, RuntimeError> {
        let binding = match &self.lifecycle {
            ServerTcpValidationLifecycle::ClientToServerActive(active)
                if active.stream_id == stream_id =>
            {
                &active.binding
            }
            ServerTcpValidationLifecycle::Retained {
                validation_attachment: Some(binding),
            } if binding.stream_id() == stream_id => binding,
            _ => {
                return Err(RuntimeError::Protocol(
                    "TCP validation stream frame has no directional authority",
                ));
            }
        };
        let frame = match binding.try_route_frame(frame) {
            Ok(ServerStreamFrameRoute::Routed) => return Ok(true),
            Ok(ServerStreamFrameRoute::Backpressured(frame)) => frame,
            Err(RuntimeError::ReliablePathRetired) => return Ok(false),
            Err(error) => return Err(error),
        };
        let route = binding.route_frame(frame);
        match route.await {
            Ok(()) => Ok(true),
            Err(RuntimeError::ReliablePathRetired) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn detach_client_to_server_validation_attachment(
        &mut self,
        stream_id: StreamId,
    ) -> Result<bool, RuntimeError> {
        let is_exact_attachment = match &self.lifecycle {
            ServerTcpValidationLifecycle::ClientToServerSettling(settling) => settling
                .binding
                .as_ref()
                .is_some_and(|binding| binding.stream_id() == stream_id),
            ServerTcpValidationLifecycle::Retained {
                validation_attachment,
            } => validation_attachment
                .as_ref()
                .is_some_and(|binding| binding.stream_id() == stream_id),
            _ => false,
        };
        if !is_exact_attachment {
            return Err(RuntimeError::Protocol(
                "TCP validation stream detach has no exact binding",
            ));
        }

        match &self.lifecycle {
            ServerTcpValidationLifecycle::ClientToServerSettling(settling) => settling
                .binding
                .as_ref()
                .expect("exact settling attachment was revalidated")
                .begin_detach(),
            ServerTcpValidationLifecycle::Retained {
                validation_attachment,
            } => validation_attachment
                .as_ref()
                .expect("exact retained attachment was revalidated")
                .begin_detach(),
            _ => unreachable!("validated attachment state changed without an await"),
        }
        match &mut self.lifecycle {
            ServerTcpValidationLifecycle::ClientToServerSettling(settling) => {
                settling.binding = None;
            }
            ServerTcpValidationLifecycle::Retained {
                validation_attachment,
            } => {
                *validation_attachment = None;
            }
            _ => unreachable!("validated attachment state changed without an await"),
        }
        Ok(true)
    }

    fn detach_server_to_client_attachment(
        &mut self,
        stream_id: StreamId,
    ) -> Result<bool, RuntimeError> {
        match &self.lifecycle {
            ServerTcpValidationLifecycle::ServerToClientSettling(settling)
                if settling.stream_id == stream_id =>
            {
                Ok(true)
            }
            ServerTcpValidationLifecycle::ServerToClientRetained {
                stream_id: retained_stream_id,
                ..
            } if *retained_stream_id == stream_id => {
                self.context
                    .reliable_streams
                    .detach_path(&self.path_registration, stream_id)?;
                Ok(true)
            }
            _ => Err(RuntimeError::Protocol(
                "S2C TCP validation detach has no exact binding",
            )),
        }
    }

    async fn settle_client_to_server_validation(
        &mut self,
        validation_id: u64,
        result: TcpCarrierValidationResult,
    ) -> Result<bool, RuntimeError> {
        let validation_id = NonZeroU64::new(validation_id).ok_or(RuntimeError::Protocol(
            "invalid TCP carrier validation identifier",
        ))?;
        let lifecycle =
            std::mem::replace(&mut self.lifecycle, ServerTcpValidationLifecycle::Draining);
        let ServerTcpValidationLifecycle::ClientToServerActive(active) = lifecycle else {
            return Err(RuntimeError::Protocol(
                "TCP carrier result has no active validation",
            ));
        };
        if active.validation_id != validation_id {
            return Err(RuntimeError::Protocol(
                "TCP carrier result validation identifier mismatch",
            ));
        }
        if active.lease.path_instance_id() != self.path_registration.path_instance_id()
            || active.lease.direction() != PathMetricDirection::ClientToServer
            || !active.binding.is_current()
            || (result == TcpCarrierValidationResult::Retain
                && self.path_registration.local_usage() != crate::protocol::PathUsage::Available)
        {
            return Ok(false);
        }

        let acknowledgment = Frame::TcpCarrierResultAck {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ClientToServer,
            result,
        };
        // The exact binding, local usage, and registry lease were revalidated
        // without awaiting. Serialize the immutable acknowledgment first,
        // then commit its exact local result before this actor can process any
        // following peer work. A write or flush failure retires the carrier
        // and therefore revokes any just-committed directional authority.
        if !self.writer.write_frame_unflushed(&acknowledgment).await? {
            return Ok(false);
        }
        match result {
            TcpCarrierValidationResult::Retain => {
                active.lease.commit_retain()?;
                if !self
                    .path_registration
                    .tcp_carrier_direction_authorized(PathMetricDirection::ClientToServer)
                {
                    return Ok(false);
                }
                self.lifecycle = ServerTcpValidationLifecycle::Retained {
                    validation_attachment: Some(active.binding),
                };
            }
            TcpCarrierValidationResult::NoGain | TcpCarrierValidationResult::Withdrawn => {
                active.lease.settle_without_retain()?;
                self.lifecycle = ServerTcpValidationLifecycle::ClientToServerSettling(
                    SettlingClientToServerValidation {
                        binding: Some(active.binding),
                    },
                );
            }
        }
        if !self.writer.flush().await? {
            return Ok(false);
        }
        if result == TcpCarrierValidationResult::Retain {
            self.validation_deadline = None;
        }
        self.evidence
            .observe_after_write(&self.context, &self.path_registration, self.path_id);
        Ok(true)
    }

    async fn handle_server_to_client_control(
        &mut self,
        control: ServerTcpValidationControl,
    ) -> Result<bool, RuntimeError> {
        let ServerTcpValidationControl::SerializeResult { result, response } = control;
        let lifecycle =
            std::mem::replace(&mut self.lifecycle, ServerTcpValidationLifecycle::Draining);
        let ServerTcpValidationLifecycle::ServerToClientActive(mut active) = lifecycle else {
            let _ = response.send(Err(RuntimeError::Protocol(
                "S2C TCP carrier result has no active validation",
            )));
            return Err(RuntimeError::Protocol(
                "S2C TCP carrier result has no active validation",
            ));
        };
        if active.immutable_result.is_some()
            || !active.output.is_current()
            || active.output.original_flight_bytes() != 0
        {
            let _ = response.send(Err(RuntimeError::ReliablePathRetired));
            return Ok(false);
        }
        let frame = Frame::TcpCarrierResult {
            validation_id: active.validation_id.get(),
            direction: PathMetricDirection::ServerToClient,
            result,
        };
        if !self.write_frame(&frame).await? {
            let _ = response.send(Err(RuntimeError::ReliablePathSessionClosed));
            return Ok(false);
        }
        active.immutable_result = Some(result);
        self.lifecycle = ServerTcpValidationLifecycle::ServerToClientActive(active);
        let _ = response.send(Ok(()));
        Ok(true)
    }

    async fn settle_server_to_client_validation(
        &mut self,
        validation_id: u64,
        result: TcpCarrierValidationResult,
    ) -> Result<bool, RuntimeError> {
        let validation_id = NonZeroU64::new(validation_id).ok_or(RuntimeError::Protocol(
            "invalid TCP carrier validation identifier",
        ))?;
        let lifecycle =
            std::mem::replace(&mut self.lifecycle, ServerTcpValidationLifecycle::Draining);
        let ServerTcpValidationLifecycle::ServerToClientActive(active) = lifecycle else {
            return Err(RuntimeError::Protocol(
                "S2C TCP carrier acknowledgment has no active validation",
            ));
        };
        if active.validation_id != validation_id
            || active.immutable_result != Some(result)
            || !active.output.is_current()
            || active.output.original_flight_bytes() != 0
            || (result == TcpCarrierValidationResult::Retain && !active.output.peer_available())
        {
            return Ok(false);
        }
        match result {
            TcpCarrierValidationResult::Retain => {
                active.lease.commit_retain()?;
                active.output.promote()?;
                let _ = active.events.send(ServerTcpValidationEvent::Retained).await;
                self.lifecycle = ServerTcpValidationLifecycle::ServerToClientRetained {
                    stream_id: active.stream_id,
                    output: active.output,
                };
                self.validation_deadline = None;
            }
            TcpCarrierValidationResult::NoGain | TcpCarrierValidationResult::Withdrawn => {
                active.lease.settle_without_retain()?;
                if !active.output.settle() {
                    return Ok(false);
                }
                let _ = active
                    .events
                    .send(ServerTcpValidationEvent::ResultAcknowledged(result))
                    .await;
                self.commands_rx.close_for_path_drain();
                self.lifecycle = ServerTcpValidationLifecycle::ServerToClientSettling(
                    SettlingServerToClientValidation {
                        stream_id: active.stream_id,
                        output: active.output,
                    },
                );
            }
        }
        Ok(true)
    }

    async fn write_server_to_client_command_run(
        &mut self,
        first: ReliablePathCommand,
    ) -> Result<bool, RuntimeError> {
        let mode = match &self.lifecycle {
            ServerTcpValidationLifecycle::ServerToClientActive(active)
                if active.immutable_result.is_none() =>
            {
                ServerToClientCommandMode::Validation {
                    validation_id: active.validation_id,
                    stream_id: active.stream_id,
                }
            }
            ServerTcpValidationLifecycle::ServerToClientRetained { stream_id, .. } => {
                ServerToClientCommandMode::Retained {
                    stream_id: *stream_id,
                }
            }
            _ => {
                return Err(RuntimeError::Protocol(
                    "S2C TCP carrier command has no active output",
                ));
            }
        };
        let byte_budget = reliable_path_command_writer_run_budget_bytes(self.context.mux_limits);
        let item_budget = reliable_path_command_writer_run_budget_items(self.context.mux_limits);
        let mut next = Some(first);
        let mut pending_bytes = 0usize;
        let mut written_bytes = 0usize;
        let mut written_items = 0usize;
        let mut writer_boundary = None;
        let mut detach_stream = None;
        self.writer.clear_batch();
        loop {
            let Some(command) = next.take() else {
                break;
            };
            let charge = reliable_path_command_pending_bytes(&command);
            let writer_bytes = reliable_path_command_writer_run_bytes(&command);
            match (mode, command) {
                (
                    ServerToClientCommandMode::Validation {
                        validation_id,
                        stream_id,
                    },
                    ReliablePathCommand::SendTcpCarrierValidationData {
                        validation_id: command_validation_id,
                        frame:
                            frame @ Frame::StreamData {
                                stream_id: frame_stream_id,
                                ..
                            },
                    },
                ) if command_validation_id == validation_id && frame_stream_id == stream_id => {
                    self.writer.push_frame(frame);
                    pending_bytes = pending_bytes.saturating_add(charge);
                }
                (
                    ServerToClientCommandMode::Validation { validation_id, .. },
                    ReliablePathCommand::TcpCarrierValidationWriterBoundary {
                        validation_id: command_validation_id,
                        completion,
                    },
                ) if command_validation_id == validation_id => {
                    writer_boundary = Some(completion);
                }
                (
                    ServerToClientCommandMode::Retained { stream_id },
                    ReliablePathCommand::SendFrame(frame),
                ) if frame_belongs_to_stream(&frame, stream_id) => {
                    self.writer.push_frame(frame);
                    pending_bytes = pending_bytes.saturating_add(charge);
                }
                (
                    ServerToClientCommandMode::Retained { stream_id },
                    ReliablePathCommand::ResetAndCloseStream {
                        stream_id: command_stream_id,
                        reason,
                    },
                ) if command_stream_id == stream_id => {
                    self.writer
                        .push_frame(Frame::StreamReset { stream_id, reason });
                    pending_bytes = pending_bytes.saturating_add(charge);
                    detach_stream = Some(stream_id);
                }
                (
                    ServerToClientCommandMode::Retained { stream_id },
                    ReliablePathCommand::CloseStream(command_stream_id),
                ) if command_stream_id == stream_id => {
                    self.commands_rx.release_pending_command_bytes(charge);
                    detach_stream = Some(stream_id);
                }
                (_, _) => {
                    self.commands_rx.release_pending_command_bytes(charge);
                    return Err(RuntimeError::Protocol(
                        "S2C TCP carrier received unauthorized writer command",
                    ));
                }
            }
            written_bytes = written_bytes.saturating_add(writer_bytes);
            written_items = written_items.saturating_add(1);
            if writer_boundary.is_some()
                || detach_stream.is_some()
                || written_bytes >= byte_budget
                || written_items >= item_budget
            {
                break;
            }
            next = try_recv_reliable_path_command(&mut self.commands_rx);
        }
        if !self.writer.write_batch(&mut self.evidence).await? {
            return Ok(false);
        }
        if pending_bytes > 0 {
            self.commands_rx
                .release_pending_command_bytes(pending_bytes);
            self.evidence
                .observe_after_write(&self.context, &self.path_registration, self.path_id);
        }
        if writer_boundary.is_some() && !self.writer.flush().await? {
            return Ok(false);
        }
        if let Some(completion) = writer_boundary {
            let _ = completion.send(Instant::now());
        }
        if let Some(stream_id) = detach_stream {
            let _ = self
                .context
                .reliable_streams
                .detach_path(&self.path_registration, stream_id);
        }
        Ok(true)
    }

    async fn complete_path_drain(&mut self) -> Result<(), RuntimeError> {
        self.path_registration.set_state(PeerPathState::Draining);
        self.evidence.cancel_for_path_drain();
        self.lifecycle = ServerTcpValidationLifecycle::Draining;

        let drain_deadline = tokio::time::Instant::now() + self.context.session_retention_timeout;
        let deadline = self
            .validation_deadline
            .map_or(drain_deadline, |validation| validation.min(drain_deadline));
        let retirement = self.path_registration.begin_retirement().wait();
        if tokio::time::timeout_at(deadline, retirement).await.is_err() {
            return Ok(());
        }
        if self
            .writer
            .write_frame_unflushed(&Frame::PathClose {
                path_id: self.path_id,
                reason: CloseReason::Normal,
            })
            .await?
        {
            let _ = self.writer.flush().await?;
        }
        Ok(())
    }

    fn expire_validation(&mut self) {
        let can_serialize_withdrawn = matches!(
            &self.lifecycle,
            ServerTcpValidationLifecycle::ServerToClientActive(active)
                if active.immutable_result.is_none()
        );
        if !can_serialize_withdrawn {
            return;
        }
        self.commands_rx.close_for_path_drain();
        match self
            .serialize_expired_server_to_client_withdrawal()
            .now_or_never()
        {
            Some(Ok(true)) | None => {}
            Some(Ok(false) | Err(_)) => {}
        }
    }

    async fn serialize_expired_server_to_client_withdrawal(
        &mut self,
    ) -> Result<bool, RuntimeError> {
        while let Some(command) =
            recv_reliable_path_command_during_drain(&mut self.commands_rx).await
        {
            if !self.write_server_to_client_command_run(command).await? {
                return Ok(false);
            }
        }
        let validation_id = match &self.lifecycle {
            ServerTcpValidationLifecycle::ServerToClientActive(active)
                if active.immutable_result.is_none() =>
            {
                active.validation_id
            }
            _ => return Ok(false),
        };
        if !self
            .write_frame(&Frame::TcpCarrierResult {
                validation_id: validation_id.get(),
                direction: PathMetricDirection::ServerToClient,
                result: TcpCarrierValidationResult::Withdrawn,
            })
            .await?
        {
            return Ok(false);
        }
        if let ServerTcpValidationLifecycle::ServerToClientActive(active) = &mut self.lifecycle {
            active.immutable_result = Some(TcpCarrierValidationResult::Withdrawn);
        }
        Ok(true)
    }

    async fn write_frame(&mut self, frame: &Frame) -> Result<bool, RuntimeError> {
        if !self.writer.write_frame(frame).await? {
            return Ok(false);
        }
        self.evidence
            .observe_after_write(&self.context, &self.path_registration, self.path_id);
        Ok(true)
    }
}

impl Drop for ServerTcpValidationSession {
    fn drop(&mut self) {
        let output = match &self.lifecycle {
            ServerTcpValidationLifecycle::ServerToClientActive(active) => Some(&active.output),
            ServerTcpValidationLifecycle::ServerToClientSettling(settling) => {
                Some(&settling.output)
            }
            ServerTcpValidationLifecycle::ServerToClientRetained { output, .. } => Some(output),
            _ => None,
        };
        if let Some(output) = output {
            let _ = output.settle();
        }
    }
}

enum ValidationEvent {
    Frame(Frame),
    Command(Option<ReliablePathCommand>),
    Control(Option<ServerTcpValidationControl>),
    PeerStatusRequest(u64),
    SenderObservationDue,
    CarrierDemand(ServerTcpCarrierDemand),
}

#[derive(Clone, Copy)]
enum ServerToClientCommandMode {
    Validation {
        validation_id: NonZeroU64,
        stream_id: StreamId,
    },
    Retained {
        stream_id: StreamId,
    },
}

fn frame_belongs_to_stream(frame: &Frame, expected: StreamId) -> bool {
    matches!(
        frame,
        Frame::StreamData { stream_id, .. }
            | Frame::StreamAck { stream_id, .. }
            | Frame::StreamMaxData { stream_id, .. }
            | Frame::StreamFin { stream_id, .. }
            | Frame::StreamReset { stream_id, .. }
            | Frame::StreamDetach { stream_id }
            if *stream_id == expected
    )
}

async fn recv_server_to_client_control(
    lifecycle: &mut ServerTcpValidationLifecycle,
) -> Option<ServerTcpValidationControl> {
    match lifecycle {
        ServerTcpValidationLifecycle::ServerToClientActive(active)
            if active.immutable_result.is_none() =>
        {
            active.controls.recv().await
        }
        _ => std::future::pending().await,
    }
}

async fn sleep_until_optional_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

async fn complete_before_optional_deadline<F>(
    deadline: Option<tokio::time::Instant>,
    future: F,
) -> Option<F::Output>
where
    F: Future,
{
    if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
        return None;
    }
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, future).await.ok(),
        None => Some(future.await),
    }
}

async fn wait_for_optional_std_deadline(deadline: Option<std::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::complete_before_optional_deadline;

    #[tokio::test]
    async fn absolute_validation_ceiling_preempts_ready_and_blocked_work() {
        let expired = tokio::time::Instant::now();
        assert!(
            complete_before_optional_deadline(expired.into(), std::future::pending::<()>())
                .await
                .is_none(),
            "an in-progress actor operation cannot outlive the absolute validation ceiling",
        );
        assert!(
            complete_before_optional_deadline(expired.into(), std::future::ready(()))
                .await
                .is_none(),
            "already-ready work cannot starve an expired absolute validation ceiling",
        );
    }
}
