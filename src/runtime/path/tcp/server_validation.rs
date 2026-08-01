//! Server ownership of one validation-purpose TCP carrier.
//!
//! A validation carrier is deliberately kept out of the ordinary TCP actor:
//! before an acknowledged directional retain it may route only the finite
//! Product work named by the exact validation transaction.  This actor owns
//! the wire ordering, absolute validation lifetime, and ordered carrier drain;
//! the existing Product stream remains owned by the stream registry.

use super::io::encrypted_framed_peer_closed;
use super::server_evidence::ServerTcpEvidenceState;
use super::server_writer::ServerTcpWriter;
use crate::protocol::{
    CloseReason, Frame, PathId, PathMetricDirection, PeerPathState, SessionId, StreamId,
    TcpCarrierValidationResult,
};
use crate::runtime::RuntimeError;
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    ServerCarrierPathRegistration, ServerStreamFrameRoute, ServerTcpCarrierValidationLease,
    ServerValidationStreamBinding,
};
use crate::runtime::peer_status::PeerStatusCarrier;
use crate::transport::encrypted::EncryptedFramedTransportError;
use std::future::Future;
use std::num::NonZeroU64;
use tokio::sync::mpsc;

pub(super) struct ServerTcpValidationAdmission {
    pub(super) context: ServerPathContext,
    pub(super) session_id: SessionId,
    pub(super) path_id: PathId,
    pub(super) path_registration: ServerCarrierPathRegistration,
    pub(super) writer: ServerTcpWriter,
    pub(super) path_frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    pub(super) evidence: ServerTcpEvidenceState,
    pub(super) peer_status: PeerStatusCarrier,
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

enum ServerTcpValidationLifecycle {
    AwaitingValidation,
    ClientToServerActive(ActiveClientToServerValidation),
    ClientToServerSettling(SettlingClientToServerValidation),
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
    evidence: ServerTcpEvidenceState,
    peer_status: PeerStatusCarrier,
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
            evidence: admission.evidence,
            peer_status: admission.peer_status,
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

        loop {
            if self
                .validation_deadline
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            {
                return Ok(());
            }
            self.evidence
                .observe_periodic(&self.context, &self.path_registration, self.path_id);
            let sender_observation_at = self.evidence.next_sender_observation_at();
            let validation_deadline = self.validation_deadline;
            let event = tokio::select! {
                biased;
                () = &mut retirement => return Ok(()),
                () = sleep_until_optional_deadline(validation_deadline) => {
                    // A server is the receiver for C2S validation and cannot
                    // synthesize the sender-owned result or client-owned
                    // PATH_DRAIN. Exact native close is the RFC terminal.
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
            Frame::TcpCarrierValidate { .. } => Err(RuntimeError::Protocol(
                "invalid TCP carrier validation request",
            )),
            frame @ (Frame::StreamData { stream_id, .. }
            | Frame::StreamAck { stream_id, .. }
            | Frame::StreamMaxData { stream_id, .. }
            | Frame::StreamFin { stream_id, .. }
            | Frame::StreamReset { stream_id, .. }) => {
                self.route_client_to_server_stream_frame(stream_id, frame)
                    .await
            }
            Frame::TcpCarrierResult {
                validation_id,
                direction: PathMetricDirection::ClientToServer,
                result,
            } => {
                self.settle_client_to_server_validation(validation_id, result)
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
                self.detach_client_to_server_validation_attachment(stream_id)
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

    async fn write_frame(&mut self, frame: &Frame) -> Result<bool, RuntimeError> {
        if !self.writer.write_frame(frame).await? {
            return Ok(false);
        }
        self.evidence
            .observe_after_write(&self.context, &self.path_registration, self.path_id);
        Ok(true)
    }
}

enum ValidationEvent {
    Frame(Frame),
    PeerStatusRequest(u64),
    SenderObservationDue,
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
