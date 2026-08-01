//! Client ownership of one client-to-server validation-purpose TCP carrier.
//!
//! This actor is intentionally separate from the ordinary TCP path actor. It
//! owns the exact group reservation, transport, validation command queue, wire
//! ordering, heartbeat, immutable result transaction, and ordered retirement.
//! It does not publish ordinary membership, path health, or authenticated
//! carrier availability before an exact acknowledged `RETAIN`.

use super::client_connection::{
    ClientTcpCarrierConnect, ClientTcpCarrierConnection, connect_client_tcp_carrier,
};
use super::client_receive::apply_client_tcp_carrier_demand;
use super::client_state::ClientTcpPathSessionRuntime;
use super::group::ClientTcpCarrierReservation;
use super::retained::ClientTcpRetainedCarrierPublicationReservation;
use super::service::{ClientTcpCarrierAdmissionLease, ClientTcpServerToClientAdmissionLease};
use crate::model::path::RelayPathInstance;
use crate::protocol::{
    CloseReason, Frame, PathId, PathMetricDirection, PathPurpose, PathUsage, StreamId,
    TcpCarrierValidationResult,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    recv_reliable_path_command, recv_reliable_path_command_during_drain,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    reliable_path_command_writer_run_budget_bytes, reliable_path_command_writer_run_budget_items,
    reliable_path_command_writer_run_bytes, try_recv_reliable_path_command,
};
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame};
use crate::runtime::peer_status::PeerStatusCarrier;
use futures::FutureExt;
use std::num::NonZeroU64;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

/// Exact local admission for one C2S validation transaction.
pub(in crate::runtime) struct ClientTcpValidationAdmission {
    pub(super) runtime: ClientTcpPathSessionRuntime,
    pub(super) service_admission: Option<ClientTcpCarrierAdmissionLease>,
    pub(super) server_to_client_admission: Option<ClientTcpServerToClientAdmissionLease>,
    pub(super) reservation: ClientTcpCarrierReservation,
    pub(super) endpoint_generation: u64,
    pub(super) validation_id: NonZeroU64,
    pub(super) request_id: Option<NonZeroU64>,
    pub(super) direction: PathMetricDirection,
    pub(super) stream_id: StreamId,
    pub(super) instance: RelayPathInstance,
    /// Existing configured path-probe ceiling; validation defines no new
    /// connection timer.
    pub(super) open_deadline: tokio::time::Instant,
}

impl ClientTcpValidationAdmission {
    pub(in crate::runtime) fn instance(&self) -> RelayPathInstance {
        self.instance
    }
}

/// Stable identity shared by all actor events for one candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpValidationCandidate {
    pub(in crate::runtime) validation_id: NonZeroU64,
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) remote_port: u16,
}

/// Transport ownership yielded only after the exact `RETAIN` acknowledgment.
///
/// The receiving integration is responsible for committing directional
/// authority and publishing the resulting ordinary actor. Until it does so,
/// merely holding this value publishes nothing.
pub(in crate::runtime) struct ClientTcpValidationHandoff {
    pub(in crate::runtime) candidate: ClientTcpValidationCandidate,
    pub(in crate::runtime) endpoint_generation: u64,
    pub(in crate::runtime) runtime: ClientTcpElasticCarrierRuntime,
    pub(in crate::runtime) connection: ClientTcpCarrierConnection,
    pub(in crate::runtime) peer_status: PeerStatusCarrier,
    pub(in crate::runtime) path_proofs: PathProofTracker,
    pub(in crate::runtime) reservation: ClientTcpCarrierReservation,
    pub(in crate::runtime) server_to_client: Option<ClientTcpServerToClientRetainedPreparation>,
}

/// Resources whose exact S2C registry authority was committed before the
/// matching wire acknowledgment. They remain rollback-owned until adoption
/// publishes path health and transfers cleanup to the retained actor.
pub(in crate::runtime) struct ClientTcpServerToClientRetainedPreparation {
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) command_receivers: ReliablePathCommandReceivers,
    pub(in crate::runtime) publication: ClientTcpRetainedCarrierPublicationReservation,
}

/// Configuration ownership for an acknowledged elastic instance.
///
/// This typed wrapper deliberately cannot be passed to the configured-minimum
/// reconnect actor, whose input is `ClientTcpPathSessionRuntime`. A later
/// directional-authority integration may consume it only through this module's
/// dedicated elastic-carrier publication path.
#[derive(Clone)]
pub(in crate::runtime) struct ClientTcpElasticCarrierRuntime {
    runtime: ClientTcpPathSessionRuntime,
}

impl ClientTcpElasticCarrierRuntime {
    pub(super) fn into_runtime(self) -> ClientTcpPathSessionRuntime {
        self.runtime
    }
}

/// Bounded outputs from the carrier owner to the sender/admission owner.
pub(in crate::runtime) enum ClientTcpValidationEvent {
    /// `TCP_CARRIER_VALIDATE` is already serialized when this is emitted, so
    /// the enclosed queue cannot create pre-validation Product traffic.
    Admitted {
        candidate: ClientTcpValidationCandidate,
        validation_data: ReliablePathCommandSender,
    },
    /// Receiver-side admission for S2C. No Product writer is exposed because
    /// candidate placement belongs to the server sender.
    ReceiverAdmitted {
        candidate: ClientTcpValidationCandidate,
    },
    /// Exact target/session control that remains owned by the existing Product
    /// stream and its sender evidence model.
    Control {
        candidate: ClientTcpValidationCandidate,
        frame: Frame,
    },
    ResultAcknowledged {
        candidate: ClientTcpValidationCandidate,
        result: TcpCarrierValidationResult,
    },
    /// One immutable server-owned S2C verdict, delivered only after all prior
    /// candidate Product frames have entered this same FIFO.
    ResultReceived {
        candidate: ClientTcpValidationCandidate,
        result: TcpCarrierValidationResult,
    },
    Retained(Box<ClientTcpValidationHandoff>),
    Drained {
        candidate: ClientTcpValidationCandidate,
    },
}

enum ClientTcpValidationControl {
    SerializeResult {
        result: TcpCarrierValidationResult,
        response: oneshot::Sender<Result<(), RuntimeError>>,
    },
    ConfirmCandidateWorkZero {
        response: oneshot::Sender<Result<(), RuntimeError>>,
    },
    AcknowledgeServerToClientResult {
        result: TcpCarrierValidationResult,
        response: oneshot::Sender<Result<(), RuntimeError>>,
    },
}

/// Single-owner result and zero-work authority for a validation actor.
#[derive(Clone)]
pub(in crate::runtime) struct ClientTcpValidationController {
    controls: mpsc::Sender<ClientTcpValidationControl>,
    validation_data: ReliablePathCommandSender,
    validation_id: NonZeroU64,
}

impl ClientTcpValidationController {
    /// Returns the transport-writer instant that separates preceding Product
    /// placement from subsequent validation evidence. A closed carrier or a
    /// write failure drops the receipt and therefore withdraws the caller's
    /// comparison rather than manufacturing a boundary.
    pub(in crate::runtime) async fn writer_boundary(&self) -> Result<Instant, RuntimeError> {
        self.validation_data
            .tcp_carrier_validation_writer_boundary(self.validation_id)
            .await
    }

    /// Stops fresh candidate placement at the exact carrier boundary, then
    /// asks the actor to serialize one immutable sender-owned result after all
    /// work that crossed that boundary has reached the ordered writer.
    pub(in crate::runtime) async fn serialize_result(
        &self,
        result: TcpCarrierValidationResult,
    ) -> Result<(), RuntimeError> {
        self.validation_data.begin_path_drain();
        let (response, receipt) = oneshot::channel();
        self.controls
            .send(ClientTcpValidationControl::SerializeResult { result, response })
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
        receipt
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?
    }

    /// Confirms the sender model's exact validation queue, original flight,
    /// recovery work, and reorder debt are all zero. The actor independently
    /// requires its bounded command writer to be empty and a matching negative
    /// acknowledgment before it can serialize `PATH_DRAIN`.
    pub(in crate::runtime) async fn confirm_candidate_work_zero(&self) -> Result<(), RuntimeError> {
        let (response, receipt) = oneshot::channel();
        self.controls
            .send(ClientTcpValidationControl::ConfirmCandidateWorkZero { response })
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
        receipt
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?
    }

    /// Commits the exact receiver-side S2C result after the target relay has
    /// applied every preceding candidate frame. The carrier actor owns both
    /// acknowledgment serialization and local admission settlement.
    pub(in crate::runtime) async fn acknowledge_server_to_client_result(
        &self,
        result: TcpCarrierValidationResult,
    ) -> Result<(), RuntimeError> {
        let (response, receipt) = oneshot::channel();
        self.controls
            .send(ClientTcpValidationControl::AcknowledgeServerToClientResult { result, response })
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
        receipt
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?
    }
}

enum ClientTcpValidationLifecycle {
    Active,
    AwaitingServerToClientResultCommit {
        result: TcpCarrierValidationResult,
    },
    DrainingCommands {
        result: TcpCarrierValidationResult,
        response: Option<oneshot::Sender<Result<(), RuntimeError>>>,
        work_zero: bool,
    },
    AwaitingResultAck {
        result: TcpCarrierValidationResult,
        work_zero: bool,
    },
    NegativeAcknowledged {
        result: TcpCarrierValidationResult,
        work_zero: bool,
    },
    Draining,
}

#[derive(Clone, Copy)]
enum ValidationCommandReceiveMode {
    Live,
    Drain,
    None,
}

#[derive(Clone, Copy)]
enum ValidationWriteContext {
    RetentionBounded,
    ExpiryReadyOnly,
}

/// Exact transport and protocol owner for one C2S validation transaction.
pub(in crate::runtime) struct ClientTcpValidationSession {
    runtime: ClientTcpPathSessionRuntime,
    reservation: Option<ClientTcpCarrierReservation>,
    service_admission: Option<ClientTcpCarrierAdmissionLease>,
    server_to_client_admission: Option<ClientTcpServerToClientAdmissionLease>,
    endpoint_generation: u64,
    validation_id: NonZeroU64,
    request_id: Option<NonZeroU64>,
    direction: PathMetricDirection,
    stream_id: StreamId,
    instance: RelayPathInstance,
    open_deadline: tokio::time::Instant,
    candidate: Option<ClientTcpValidationCandidate>,
    connection: Option<ClientTcpCarrierConnection>,
    peer_status: Option<PeerStatusCarrier>,
    path_proofs: PathProofTracker,
    validation_data: ReliablePathCommandSender,
    commands: Option<ReliablePathCommandReceivers>,
    controls: mpsc::Receiver<ClientTcpValidationControl>,
    controls_open: bool,
    pending_server_to_client_ack: Option<(
        TcpCarrierValidationResult,
        oneshot::Sender<Result<(), RuntimeError>>,
    )>,
    events: mpsc::Sender<ClientTcpValidationEvent>,
    lifecycle: ClientTcpValidationLifecycle,
    retention_deadline: Option<tokio::time::Instant>,
    terminal: bool,
}

impl ClientTcpValidationSession {
    pub(in crate::runtime) fn new(
        admission: ClientTcpValidationAdmission,
    ) -> (
        Self,
        ClientTcpValidationController,
        mpsc::Receiver<ClientTcpValidationEvent>,
    ) {
        let ClientTcpValidationAdmission {
            mut runtime,
            service_admission,
            server_to_client_admission,
            reservation,
            endpoint_generation,
            validation_id,
            request_id,
            direction,
            stream_id,
            instance,
            open_deadline,
        } = admission;
        runtime.path_id = Some(reservation.path_id());
        runtime.purpose = PathPurpose::Validation;
        let queue = runtime.command_queue.max(1);
        let (validation_data, commands) = reliable_path_command_channels(queue);
        let (control_tx, controls) = mpsc::channel(queue);
        let (events, event_rx) = mpsc::channel(queue);
        let controller = ClientTcpValidationController {
            controls: control_tx,
            validation_data: validation_data.clone(),
            validation_id,
        };
        let session = Self {
            path_proofs: PathProofTracker::from_limits(runtime.mux_limits),
            runtime,
            service_admission,
            server_to_client_admission,
            reservation: Some(reservation),
            endpoint_generation,
            validation_id,
            request_id,
            direction,
            stream_id,
            instance,
            open_deadline,
            candidate: None,
            connection: None,
            peer_status: None,
            validation_data,
            commands: Some(commands),
            controls,
            controls_open: true,
            pending_server_to_client_ack: None,
            events,
            lifecycle: ClientTcpValidationLifecycle::Active,
            retention_deadline: None,
            terminal: false,
        };
        (session, controller, event_rx)
    }

    pub(in crate::runtime) async fn run(mut self) -> Result<(), RuntimeError> {
        self.connect().await?;
        self.admit_validation().await?;

        loop {
            if self.advance_lifecycle().await? {
                return Ok(());
            }

            let retention_deadline = self
                .retention_deadline
                .expect("validation admission owns one absolute retention deadline");
            let heartbeat_deadline = self
                .connection
                .as_ref()
                .expect("validation actor owns its connected carrier")
                .heartbeat_deadline();
            let receive_mode = match self.lifecycle {
                ClientTcpValidationLifecycle::Active
                    if self.direction == PathMetricDirection::ClientToServer =>
                {
                    ValidationCommandReceiveMode::Live
                }
                ClientTcpValidationLifecycle::Active
                | ClientTcpValidationLifecycle::AwaitingServerToClientResultCommit { .. } => {
                    ValidationCommandReceiveMode::None
                }
                ClientTcpValidationLifecycle::DrainingCommands { .. } => {
                    ValidationCommandReceiveMode::Drain
                }
                ClientTcpValidationLifecycle::AwaitingResultAck { .. }
                | ClientTcpValidationLifecycle::NegativeAcknowledged { .. }
                | ClientTcpValidationLifecycle::Draining => ValidationCommandReceiveMode::None,
            };
            let controls_open = self.controls_open;

            enum ActorEvent {
                Frame(
                    Option<
                        Result<Frame, crate::transport::encrypted::EncryptedFramedTransportError>,
                    >,
                ),
                Command(Option<ReliablePathCommand>),
                Control(Option<ClientTcpValidationControl>),
                PeerStatus(Option<u64>),
                Heartbeat,
                RetentionExpired,
            }

            let event = {
                let connection = self
                    .connection
                    .as_mut()
                    .expect("validation actor owns its connected carrier");
                let commands = self.commands.as_mut();
                let peer_status = self
                    .peer_status
                    .as_mut()
                    .expect("connected validation carrier owns peer-status registration");
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(retention_deadline) => {
                        ActorEvent::RetentionExpired
                    }
                    frame = connection.frames.recv() => ActorEvent::Frame(frame),
                    command = receive_validation_command(commands, receive_mode) => {
                        ActorEvent::Command(command)
                    }
                    control = self.controls.recv(), if controls_open => {
                        ActorEvent::Control(control)
                    }
                    request_id = peer_status.recv_request() => {
                        ActorEvent::PeerStatus(request_id)
                    }
                    _ = tokio::time::sleep_until(heartbeat_deadline) => ActorEvent::Heartbeat,
                }
            };

            match event {
                ActorEvent::Frame(Some(Ok(frame))) => self.handle_frame(frame).await?,
                ActorEvent::Frame(Some(Err(error))) => {
                    return Err(RuntimeError::Encrypted(error));
                }
                ActorEvent::Frame(None) => return Err(RuntimeError::ReliablePathSessionClosed),
                ActorEvent::Command(Some(command)) => {
                    self.write_validation_command_run(
                        command,
                        ValidationWriteContext::RetentionBounded,
                    )
                    .await?;
                }
                ActorEvent::Command(None) => match receive_mode {
                    ValidationCommandReceiveMode::Live => {
                        self.begin_result(TcpCarrierValidationResult::Withdrawn, None, false)?;
                    }
                    ValidationCommandReceiveMode::Drain => {
                        self.commands = None;
                    }
                    ValidationCommandReceiveMode::None => unreachable!(),
                },
                ActorEvent::Control(Some(control)) => self.handle_control(control)?,
                ActorEvent::Control(None) => {
                    self.controls_open = false;
                    if matches!(self.lifecycle, ClientTcpValidationLifecycle::Active)
                        && self.direction == PathMetricDirection::ClientToServer
                    {
                        self.begin_result(TcpCarrierValidationResult::Withdrawn, None, false)?;
                    } else if self.direction == PathMetricDirection::ServerToClient
                        && self.prepare_server_to_client_drain()
                    {
                        self.serialize_server_to_client_drain().await?;
                    }
                }
                ActorEvent::PeerStatus(Some(request_id)) => {
                    self.write_frames(&[Frame::PeerStatusRequest { request_id }])
                        .await?;
                }
                ActorEvent::PeerStatus(None) => {}
                ActorEvent::Heartbeat => match self.tick_heartbeat().await {
                    Ok(()) => {}
                    Err(RuntimeError::SessionRetentionTimeout) => {
                        return self.expire_validation().await;
                    }
                    Err(error) => return Err(error),
                },
                ActorEvent::RetentionExpired => {
                    return self.expire_validation().await;
                }
            }
        }
    }

    async fn connect(&mut self) -> Result<(), RuntimeError> {
        if !self
            .runtime
            .endpoint_policy
            .allows(self.endpoint_generation)
        {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let path_id = self
            .reservation
            .as_ref()
            .expect("validation actor owns exact carrier reservation")
            .path_id();
        let connection = {
            let connect = connect_client_tcp_carrier(
                ClientTcpCarrierConnect {
                    path: self.runtime.path(),
                    path_id,
                    purpose: PathPurpose::Validation,
                    carrier_identity: self.runtime.carrier_identity,
                    session_id: self.runtime.session_id,
                    security: self.runtime.security(),
                    tls: self.runtime.tls(),
                    codec_limits: self.runtime.codec_limits,
                    mux_limits: self.runtime.mux_limits,
                    carrier_network: self.runtime.carrier_network.as_ref(),
                    remote_port: self.runtime.remote_port,
                },
                self.open_deadline,
            );
            tokio::pin!(connect);
            let mut policy = self.runtime.endpoint_policy.subscribe();
            let policy_change =
                wait_for_endpoint_policy_change(&mut policy, self.endpoint_generation);
            tokio::pin!(policy_change);
            tokio::select! {
                biased;
                _ = &mut policy_change => return Err(RuntimeError::NoSchedulableTcpPath),
                result = &mut connect => result?,
            }
        };
        debug_assert_eq!(connection.path_id, path_id);
        debug_assert_eq!(connection.purpose, PathPurpose::Validation);
        let candidate = ClientTcpValidationCandidate {
            validation_id: self.validation_id,
            stream_id: self.stream_id,
            path_id,
            instance: self.instance,
            remote_port: connection.remote_port,
        };
        self.runtime.remote_port = Some(connection.remote_port);
        self.peer_status = Some(self.runtime.peer_status.register(self.runtime.session_id));
        self.candidate = Some(candidate);
        self.connection = Some(connection);
        Ok(())
    }

    async fn admit_validation(&mut self) -> Result<(), RuntimeError> {
        if !self
            .runtime
            .endpoint_policy
            .allows(self.endpoint_generation)
        {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        self.retention_deadline =
            Some(tokio::time::Instant::now() + self.runtime.session_retention_timeout);
        let admitted = match self.direction {
            PathMetricDirection::ClientToServer => self
                .service_admission
                .as_mut()
                .is_none_or(ClientTcpCarrierAdmissionLease::begin_validation),
            PathMetricDirection::ServerToClient => self
                .server_to_client_admission
                .as_mut()
                .is_some_and(ClientTcpServerToClientAdmissionLease::begin_validation),
        };
        if !admitted {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        self.write_frames(&[Frame::TcpCarrierValidate {
            validation_id: self.validation_id.get(),
            request_id: self.request_id.map_or(0, NonZeroU64::get),
            direction: self.direction,
            stream_id: self.stream_id,
        }])
        .await?;

        let candidate = self.candidate();
        let peer_available = self.direction == PathMetricDirection::ServerToClient
            || self
                .connection
                .as_ref()
                .expect("validation actor owns its connected carrier")
                .peer_usage
                == PathUsage::Available;
        if !peer_available {
            // BACKUP preference is never bypassed to create favorable Product
            // evidence. No data sender escaped, so the candidate is already at
            // the local zero-work boundary.
            self.begin_result(TcpCarrierValidationResult::Withdrawn, None, true)?;
            return Ok(());
        }
        let event = match self.direction {
            PathMetricDirection::ClientToServer => ClientTcpValidationEvent::Admitted {
                candidate,
                validation_data: self.validation_data.clone(),
            },
            PathMetricDirection::ServerToClient => {
                ClientTcpValidationEvent::ReceiverAdmitted { candidate }
            }
        };
        if self.send_event(event).await.is_err() {
            // No sender/admission owner can consume the candidate. Because the
            // data queue has not escaped, ordered withdrawal is zero-work.
            self.begin_result(TcpCarrierValidationResult::Withdrawn, None, true)?;
        }
        Ok(())
    }

    fn begin_result(
        &mut self,
        result: TcpCarrierValidationResult,
        response: Option<oneshot::Sender<Result<(), RuntimeError>>>,
        work_zero: bool,
    ) -> Result<(), RuntimeError> {
        if self.direction != PathMetricDirection::ClientToServer {
            if let Some(response) = response {
                let _ = response.send(Err(RuntimeError::Protocol(
                    "S2C validation result is sender-owned by the server",
                )));
            }
            return Err(RuntimeError::Protocol(
                "S2C validation result is sender-owned by the server",
            ));
        }
        if !matches!(self.lifecycle, ClientTcpValidationLifecycle::Active) {
            if let Some(response) = response {
                let _ = response.send(Err(RuntimeError::Protocol(
                    "TCP carrier validation result is already committed",
                )));
            }
            return Ok(());
        }
        self.validation_data.begin_path_drain();
        self.commands
            .as_mut()
            .expect("active validation owns its bounded command receiver")
            .close_for_path_drain();
        self.lifecycle = ClientTcpValidationLifecycle::DrainingCommands {
            result,
            response,
            work_zero,
        };
        Ok(())
    }

    fn handle_control(&mut self, control: ClientTcpValidationControl) -> Result<(), RuntimeError> {
        match control {
            ClientTcpValidationControl::SerializeResult { result, response } => {
                self.begin_result(result, Some(response), false)
            }
            ClientTcpValidationControl::ConfirmCandidateWorkZero { response } => {
                let accepted = match &mut self.lifecycle {
                    ClientTcpValidationLifecycle::AwaitingResultAck {
                        result:
                            TcpCarrierValidationResult::NoGain | TcpCarrierValidationResult::Withdrawn,
                        work_zero,
                    }
                    | ClientTcpValidationLifecycle::NegativeAcknowledged {
                        result:
                            TcpCarrierValidationResult::NoGain | TcpCarrierValidationResult::Withdrawn,
                        work_zero,
                    } => {
                        *work_zero = true;
                        true
                    }
                    _ => false,
                };
                let result = if accepted {
                    Ok(())
                } else {
                    Err(RuntimeError::Protocol(
                        "TCP carrier zero-work confirmation has no serialized negative result",
                    ))
                };
                let _ = response.send(result);
                Ok(())
            }
            ClientTcpValidationControl::AcknowledgeServerToClientResult { result, response } => {
                let expected = match self.lifecycle {
                    ClientTcpValidationLifecycle::AwaitingServerToClientResultCommit { result } => {
                        Some(result)
                    }
                    _ => None,
                };
                if self.direction != PathMetricDirection::ServerToClient || expected != Some(result)
                {
                    let _ = response.send(Err(RuntimeError::Protocol(
                        "S2C result acknowledgment has no exact current result",
                    )));
                    return Ok(());
                }
                // The async write and exact local settlement are performed by
                // the actor loop so no other wire event can interleave them.
                self.pending_server_to_client_ack = Some((result, response));
                Ok(())
            }
        }
    }

    async fn advance_lifecycle(&mut self) -> Result<bool, RuntimeError> {
        if self.terminal {
            return Ok(true);
        }
        if let Some((result, response)) = self.pending_server_to_client_ack.take() {
            let current = self
                .server_to_client_admission
                .as_ref()
                .is_some_and(ClientTcpServerToClientAdmissionLease::is_current);
            if !current {
                let _ = response.send(Err(RuntimeError::ReliablePathRetired));
                return Err(RuntimeError::ReliablePathRetired);
            }
            let mut retained_preparation = if result == TcpCarrierValidationResult::Retain {
                let commands = self.validation_data.clone();
                let command_receivers = self
                    .commands
                    .take()
                    .ok_or(RuntimeError::ReliablePathRetired)?;
                let publication = self
                    .runtime
                    .retained_carriers
                    .reserve_publication(
                        self.runtime.config_index,
                        self.instance.key,
                        self.instance.path_instance_id,
                        commands.clone(),
                    )
                    .ok_or(RuntimeError::ReliablePathRetired)?;
                Some(ClientTcpServerToClientRetainedPreparation {
                    commands,
                    command_receivers,
                    publication,
                })
            } else {
                None
            };
            if self
                .server_to_client_admission
                .take()
                .is_none_or(|admission| !admission.finish())
            {
                let _ = response.send(Err(RuntimeError::ReliablePathRetired));
                return Err(RuntimeError::ReliablePathRetired);
            }
            if let Some(preparation) = retained_preparation.as_mut()
                && !preparation
                    .publication
                    .commit_direction(PathMetricDirection::ServerToClient)
            {
                let _ = response.send(Err(RuntimeError::ReliablePathRetired));
                return Err(RuntimeError::ReliablePathRetired);
            }
            self.write_frames(&[Frame::TcpCarrierResultAck {
                validation_id: self.validation_id.get(),
                direction: PathMetricDirection::ServerToClient,
                result,
            }])
            .await?;
            match result {
                TcpCarrierValidationResult::Retain => {
                    let handoff = self.take_retained_handoff(retained_preparation);
                    self.send_event(ClientTcpValidationEvent::Retained(Box::new(handoff)))
                        .await?;
                    self.retention_deadline = None;
                    self.terminal = true;
                }
                TcpCarrierValidationResult::NoGain | TcpCarrierValidationResult::Withdrawn => {
                    self.path_proofs.cancel_for_path_drain();
                    self.write_frames(&[
                        Frame::StreamDetach {
                            stream_id: self.stream_id,
                        },
                        Frame::PathDrain {
                            path_id: self.candidate().path_id,
                        },
                    ])
                    .await?;
                    self.lifecycle = ClientTcpValidationLifecycle::Draining;
                }
            }
            let _ = response.send(Ok(()));
        }
        let ready_result = match &mut self.lifecycle {
            ClientTcpValidationLifecycle::DrainingCommands {
                result,
                response,
                work_zero,
            } if self.commands.is_none() => Some((*result, response.take(), *work_zero)),
            _ => None,
        };
        if let Some((result, response, work_zero)) = ready_result {
            if !self
                .runtime
                .endpoint_policy
                .allows(self.endpoint_generation)
            {
                if let Some(response) = response {
                    let _ = response.send(Err(RuntimeError::ReliablePathRetired));
                }
                return Err(RuntimeError::ReliablePathRetired);
            }
            if result == TcpCarrierValidationResult::Retain
                && self
                    .connection
                    .as_ref()
                    .expect("validation actor owns its connected carrier")
                    .peer_usage
                    != PathUsage::Available
            {
                if let Some(response) = response {
                    let _ = response.send(Err(RuntimeError::ReliablePathRetired));
                }
                return Err(RuntimeError::ReliablePathRetired);
            }
            self.write_frames(&[Frame::TcpCarrierResult {
                validation_id: self.validation_id.get(),
                direction: PathMetricDirection::ClientToServer,
                result,
            }])
            .await?;
            if let Some(response) = response {
                let _ = response.send(Ok(()));
            }
            self.lifecycle = ClientTcpValidationLifecycle::AwaitingResultAck { result, work_zero };
        }

        let begin_drain = matches!(
            self.lifecycle,
            ClientTcpValidationLifecycle::NegativeAcknowledged {
                work_zero: true,
                ..
            }
        );
        if begin_drain {
            self.path_proofs.cancel_for_path_drain();
            self.write_frames(&[
                Frame::StreamDetach {
                    stream_id: self.stream_id,
                },
                Frame::PathDrain {
                    path_id: self.candidate().path_id,
                },
            ])
            .await?;
            self.lifecycle = ClientTcpValidationLifecycle::Draining;
        }
        Ok(self.terminal)
    }

    async fn handle_frame(&mut self, frame: Frame) -> Result<(), RuntimeError> {
        self.connection
            .as_mut()
            .expect("validation actor owns its connected carrier")
            .refresh_liveness();
        match frame {
            Frame::TcpCarrierResultAck {
                validation_id,
                direction: PathMetricDirection::ClientToServer,
                result,
            } if self.direction == PathMetricDirection::ClientToServer => {
                self.handle_result_ack(validation_id, result).await
            }
            Frame::TcpCarrierResult {
                validation_id,
                direction: PathMetricDirection::ServerToClient,
                result,
            } if self.direction == PathMetricDirection::ServerToClient => {
                self.handle_server_to_client_result(validation_id, result)
                    .await
            }
            Frame::TcpCarrierResultAck { .. }
            | Frame::TcpCarrierResult { .. }
            | Frame::TcpCarrierValidate { .. } => Err(RuntimeError::Protocol(
                "invalid TCP carrier validation result transaction",
            )),
            frame @ Frame::StreamData { stream_id, .. }
                if self.direction == PathMetricDirection::ServerToClient
                    && stream_id == self.stream_id
                    && matches!(self.lifecycle, ClientTcpValidationLifecycle::Active) =>
            {
                self.send_event(ClientTcpValidationEvent::Control {
                    candidate: self.candidate(),
                    frame,
                })
                .await
            }
            frame @ (Frame::StreamAck { stream_id, .. }
            | Frame::StreamMaxData { stream_id, .. }
            | Frame::StreamFin { stream_id, .. }
            | Frame::StreamReset { stream_id, .. }
            | Frame::StreamDetach { stream_id })
                if self.direction == PathMetricDirection::ClientToServer
                    && stream_id == self.stream_id =>
            {
                self.send_event(ClientTcpValidationEvent::Control {
                    candidate: self.candidate(),
                    frame,
                })
                .await
            }
            Frame::StreamData { .. }
            | Frame::StreamAck { .. }
            | Frame::StreamMaxData { .. }
            | Frame::StreamFin { .. }
            | Frame::StreamReset { .. }
            | Frame::StreamDetach { .. } => Err(RuntimeError::Protocol(
                "TCP carrier validation control references another stream",
            )),
            Frame::PathMetrics { metrics } if metrics.path_id == self.candidate().path_id => Ok(()),
            Frame::PathMetrics { .. } => Err(RuntimeError::Protocol(
                "TCP carrier validation metrics path mismatch",
            )),
            frame @ Frame::PathStatus {
                path_id,
                sequence,
                usage,
            } if path_id == self.candidate().path_id => {
                let connection = self
                    .connection
                    .as_mut()
                    .expect("validation actor owns its connected carrier");
                if sequence > connection.peer_usage_sequence {
                    connection.peer_usage_sequence = sequence;
                    connection.peer_usage = usage;
                    self.send_event(ClientTcpValidationEvent::Control {
                        candidate: self.candidate(),
                        frame,
                    })
                    .await?;
                }
                Ok(())
            }
            Frame::PathStatus { .. } => Err(RuntimeError::Protocol(
                "TCP carrier validation status path mismatch",
            )),
            Frame::PathProofData {
                path_id,
                proof_id,
                payload,
            } if path_id == self.candidate().path_id => {
                self.write_frames(&[path_proof_ack_frame(path_id, proof_id, payload.len())])
                    .await
            }
            Frame::PathProofAck {
                path_id,
                proof_id,
                payload_bytes,
            } if path_id == self.candidate().path_id => {
                let _ = self
                    .path_proofs
                    .acknowledge(path_id, proof_id, payload_bytes);
                Ok(())
            }
            Frame::PathProofData { .. } | Frame::PathProofAck { .. } => Err(
                RuntimeError::Protocol("TCP carrier validation path proof mismatch"),
            ),
            Frame::Ping { nonce } => self.write_frames(&[Frame::Pong { nonce }]).await,
            Frame::Pong { nonce } => self
                .connection
                .as_mut()
                .expect("validation actor owns its connected carrier")
                .complete_expected_heartbeat(nonce),
            Frame::PeerStatusRequest { request_id } => {
                let response = self
                    .peer_status
                    .as_ref()
                    .expect("validation carrier owns peer-status registration")
                    .response_frame(request_id, self.runtime.codec_limits, || {
                        self.runtime.peer_status_snapshot.snapshot()
                    });
                self.write_frames(&[response]).await
            }
            Frame::PeerStatusResponse {
                request_id,
                code,
                paths,
            } => {
                let _ = self
                    .peer_status
                    .as_ref()
                    .expect("validation carrier owns peer-status registration")
                    .receive_response(request_id, code, paths);
                Ok(())
            }
            Frame::TcpCarrierDemand {
                request_id,
                stream_id,
            } => apply_client_tcp_carrier_demand(&self.runtime, request_id, stream_id),
            Frame::PathClose {
                path_id,
                reason: CloseReason::Normal,
            } if path_id == self.candidate().path_id
                && matches!(self.lifecycle, ClientTcpValidationLifecycle::Draining) =>
            {
                if self.controls_open {
                    self.send_event(ClientTcpValidationEvent::Drained {
                        candidate: self.candidate(),
                    })
                    .await?;
                }
                self.retention_deadline = None;
                self.terminal = true;
                Ok(())
            }
            Frame::PathClose { path_id, .. } if path_id != self.candidate().path_id => Err(
                RuntimeError::Protocol("TCP carrier path close acknowledgment mismatch"),
            ),
            Frame::PathClose {
                reason: CloseReason::Normal,
                ..
            } => Err(RuntimeError::Protocol(
                "TCP carrier path close preceded client drain",
            )),
            Frame::PathClose { reason, .. } => Err(RuntimeError::RemoteClosed(reason)),
            Frame::PathDrain { .. } => Err(RuntimeError::Protocol(
                "TCP client received peer path drain request",
            )),
            Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
            Frame::OpenStream { .. }
            | Frame::OpenDatagramFlow { .. }
            | Frame::DatagramData { .. }
            | Frame::DatagramFeedback { .. }
            | Frame::DatagramClose { .. }
            | Frame::PathCapacityData { .. }
            | Frame::PathCapacityFinish { .. }
            | Frame::PathCapacityReceipt { .. }
            | Frame::SessionHello { .. }
            | Frame::SessionAuth { .. }
            | Frame::SessionReady
            | Frame::PathJoin { .. } => Err(RuntimeError::Protocol(
                "unexpected validation-purpose TCP path frame",
            )),
        }
    }

    async fn handle_result_ack(
        &mut self,
        validation_id: u64,
        result: TcpCarrierValidationResult,
    ) -> Result<(), RuntimeError> {
        let (expected_result, work_zero) = match self.lifecycle {
            ClientTcpValidationLifecycle::AwaitingResultAck { result, work_zero } => {
                (result, work_zero)
            }
            _ => {
                return Err(RuntimeError::Protocol(
                    "TCP carrier result acknowledgment has no serialized result",
                ));
            }
        };
        if validation_id != self.validation_id.get() || result != expected_result {
            return Err(RuntimeError::Protocol(
                "TCP carrier result acknowledgment does not match immutable result",
            ));
        }
        if !self
            .runtime
            .endpoint_policy
            .allows(self.endpoint_generation)
            || (result == TcpCarrierValidationResult::Retain
                && self
                    .connection
                    .as_ref()
                    .expect("validation actor owns its connected carrier")
                    .peer_usage
                    != PathUsage::Available)
        {
            return Err(RuntimeError::ReliablePathRetired);
        }

        match result {
            TcpCarrierValidationResult::Retain => {
                if self
                    .service_admission
                    .take()
                    .is_some_and(|admission| !admission.commit_retained())
                {
                    return Err(RuntimeError::ReliablePathRetired);
                }
                let handoff = self.take_retained_handoff(None);
                self.send_event(ClientTcpValidationEvent::Retained(Box::new(handoff)))
                    .await?;
                self.retention_deadline = None;
                self.terminal = true;
                Ok(())
            }
            TcpCarrierValidationResult::NoGain | TcpCarrierValidationResult::Withdrawn => {
                self.send_event(ClientTcpValidationEvent::ResultAcknowledged {
                    candidate: self.candidate(),
                    result,
                })
                .await?;
                self.lifecycle =
                    ClientTcpValidationLifecycle::NegativeAcknowledged { result, work_zero };
                Ok(())
            }
        }
    }

    async fn handle_server_to_client_result(
        &mut self,
        validation_id: u64,
        result: TcpCarrierValidationResult,
    ) -> Result<(), RuntimeError> {
        if validation_id != self.validation_id.get()
            || self.direction != PathMetricDirection::ServerToClient
            || !matches!(self.lifecycle, ClientTcpValidationLifecycle::Active)
        {
            return Err(RuntimeError::Protocol(
                "S2C TCP carrier result has no exact active validation",
            ));
        }
        if !self
            .server_to_client_admission
            .as_ref()
            .is_some_and(ClientTcpServerToClientAdmissionLease::is_current)
        {
            return Err(RuntimeError::ReliablePathRetired);
        }
        self.lifecycle =
            ClientTcpValidationLifecycle::AwaitingServerToClientResultCommit { result };
        self.send_event(ClientTcpValidationEvent::ResultReceived {
            candidate: self.candidate(),
            result,
        })
        .await
    }

    fn take_retained_handoff(
        &mut self,
        server_to_client: Option<ClientTcpServerToClientRetainedPreparation>,
    ) -> ClientTcpValidationHandoff {
        ClientTcpValidationHandoff {
            candidate: self.candidate(),
            endpoint_generation: self.endpoint_generation,
            runtime: ClientTcpElasticCarrierRuntime {
                runtime: self.runtime.clone(),
            },
            connection: self
                .connection
                .take()
                .expect("retained validation hands off its exact connection"),
            peer_status: self
                .peer_status
                .take()
                .expect("retained validation hands off peer-status ownership"),
            path_proofs: std::mem::replace(
                &mut self.path_proofs,
                PathProofTracker::from_limits(self.runtime.mux_limits),
            ),
            reservation: self
                .reservation
                .take()
                .expect("retained validation hands off its exact reservation"),
            server_to_client,
        }
    }

    async fn write_validation_command_run(
        &mut self,
        first: ReliablePathCommand,
        write_context: ValidationWriteContext,
    ) -> Result<(), RuntimeError> {
        let byte_budget = reliable_path_command_writer_run_budget_bytes(self.runtime.mux_limits);
        let item_budget = reliable_path_command_writer_run_budget_items(self.runtime.mux_limits);
        let mut next = Some(first);
        let mut frames = Vec::new();
        let mut charges = Vec::new();
        let mut run_bytes = 0usize;
        let mut run_items = 0usize;
        let mut writer_boundary = None;

        loop {
            let Some(command) = next.take() else {
                break;
            };
            let pending_bytes = reliable_path_command_pending_bytes(&command);
            let writer_bytes = reliable_path_command_writer_run_bytes(&command);
            match command {
                ReliablePathCommand::SendTcpCarrierValidationData {
                    validation_id,
                    frame: frame @ Frame::StreamData { stream_id, .. },
                } if validation_id == self.validation_id && stream_id == self.stream_id => {
                    frames.push(frame);
                    charges.push(pending_bytes);
                }
                ReliablePathCommand::TcpCarrierValidationWriterBoundary {
                    validation_id,
                    completion,
                } if validation_id == self.validation_id => {
                    writer_boundary = Some(completion);
                }
                _ => {
                    self.commands
                        .as_ref()
                        .expect("validation command writer owns receiver accounting")
                        .release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "TCP carrier validation actor received unauthorized work",
                    ));
                }
            }
            run_bytes = run_bytes.saturating_add(writer_bytes);
            run_items = run_items.saturating_add(1);
            if writer_boundary.is_some() || run_bytes >= byte_budget || run_items >= item_budget {
                break;
            }
            next = self
                .commands
                .as_mut()
                .and_then(try_recv_reliable_path_command);
        }

        let boundary_instant = match (writer_boundary.is_some(), write_context) {
            (true, ValidationWriteContext::RetentionBounded) => {
                Some(self.write_frames_at_boundary(&frames).await?)
            }
            (false, ValidationWriteContext::RetentionBounded) => {
                self.write_frames(&frames).await?;
                None
            }
            (force_flush, ValidationWriteContext::ExpiryReadyOnly) => {
                self.write_frames_flushed_io(&frames, force_flush).await?
            }
        };
        let commands = self
            .commands
            .as_ref()
            .expect("validation command writer owns receiver accounting");
        for pending_bytes in charges {
            commands.release_pending_command_bytes(pending_bytes);
        }
        if let (Some(completion), Some(boundary_instant)) = (writer_boundary, boundary_instant) {
            let _ = completion.send(boundary_instant);
        }
        Ok(())
    }

    fn begin_expired_withdrawal(&mut self) -> Result<bool, RuntimeError> {
        match &mut self.lifecycle {
            ClientTcpValidationLifecycle::Active => {
                self.begin_result(TcpCarrierValidationResult::Withdrawn, None, false)?;
                Ok(true)
            }
            ClientTcpValidationLifecycle::DrainingCommands {
                result,
                response,
                work_zero,
            } => {
                *result = TcpCarrierValidationResult::Withdrawn;
                *work_zero = false;
                if let Some(response) = response.take() {
                    let _ = response.send(Err(RuntimeError::SessionRetentionTimeout));
                }
                Ok(true)
            }
            ClientTcpValidationLifecycle::AwaitingResultAck { .. }
            | ClientTcpValidationLifecycle::AwaitingServerToClientResultCommit { .. }
            | ClientTcpValidationLifecycle::NegativeAcknowledged { .. }
            | ClientTcpValidationLifecycle::Draining => Ok(false),
        }
    }

    async fn expire_validation(&mut self) -> Result<(), RuntimeError> {
        if self.direction == PathMetricDirection::ServerToClient {
            if matches!(
                self.lifecycle,
                ClientTcpValidationLifecycle::Active
                    | ClientTcpValidationLifecycle::AwaitingServerToClientResultCommit { .. }
            ) && self.prepare_server_to_client_drain()
            {
                match self.serialize_server_to_client_drain().now_or_never() {
                    Some(Ok(())) | None => {}
                    Some(Err(error)) => return Err(error),
                }
            }
            return Err(RuntimeError::SessionRetentionTimeout);
        }
        if self.begin_expired_withdrawal()? {
            match self.serialize_expired_withdrawal().now_or_never() {
                Some(Ok(())) | None => {}
                Some(Err(error)) => return Err(error),
            }
        }
        // The absolute retention ceiling is never extended. A result that was
        // already immutable, or a WITHDRAWN result serialized while the
        // carrier was immediately writable, is retired by exact native
        // failure without waiting for settlement beyond that ceiling.
        Err(RuntimeError::SessionRetentionTimeout)
    }

    fn prepare_server_to_client_drain(&mut self) -> bool {
        debug_assert_eq!(self.direction, PathMetricDirection::ServerToClient);
        if !matches!(
            self.lifecycle,
            ClientTcpValidationLifecycle::Active
                | ClientTcpValidationLifecycle::AwaitingServerToClientResultCommit { .. }
        ) {
            return false;
        }
        if let Some((_, response)) = self.pending_server_to_client_ack.take() {
            let _ = response.send(Err(RuntimeError::ReliablePathRetired));
        }
        drop(self.server_to_client_admission.take());
        self.validation_data.begin_path_drain();
        if let Some(commands) = self.commands.as_mut() {
            commands.close_for_path_drain();
        }
        self.path_proofs.cancel_for_path_drain();
        self.lifecycle = ClientTcpValidationLifecycle::Draining;
        true
    }

    async fn serialize_server_to_client_drain(&mut self) -> Result<(), RuntimeError> {
        debug_assert_eq!(self.direction, PathMetricDirection::ServerToClient);
        self.write_frames(&[
            Frame::StreamDetach {
                stream_id: self.stream_id,
            },
            Frame::PathDrain {
                path_id: self.candidate().path_id,
            },
        ])
        .await
    }

    async fn serialize_expired_withdrawal(&mut self) -> Result<(), RuntimeError> {
        loop {
            let command = recv_reliable_path_command_during_drain(
                self.commands
                    .as_mut()
                    .expect("expired validation still owns its command drain"),
            )
            .await;
            let Some(command) = command else {
                self.commands = None;
                break;
            };
            self.write_validation_command_run(command, ValidationWriteContext::ExpiryReadyOnly)
                .await?;
        }
        self.write_frames_flushed_io(
            &[Frame::TcpCarrierResult {
                validation_id: self.validation_id.get(),
                direction: PathMetricDirection::ClientToServer,
                result: TcpCarrierValidationResult::Withdrawn,
            }],
            false,
        )
        .await?;
        self.lifecycle = ClientTcpValidationLifecycle::AwaitingResultAck {
            result: TcpCarrierValidationResult::Withdrawn,
            work_zero: false,
        };
        Ok(())
    }

    async fn tick_heartbeat(&mut self) -> Result<(), RuntimeError> {
        let deadline = self.retention_deadline();
        tokio::time::timeout_at(
            deadline,
            self.connection
                .as_mut()
                .expect("validation actor owns its connected carrier")
                .tick_heartbeat(),
        )
        .await
        .map_err(|_| RuntimeError::SessionRetentionTimeout)??;
        Ok(())
    }

    async fn write_frames(&mut self, frames: &[Frame]) -> Result<(), RuntimeError> {
        let _ = self.write_frames_flushed(frames, false).await?;
        Ok(())
    }

    async fn write_frames_at_boundary(
        &mut self,
        frames: &[Frame],
    ) -> Result<Instant, RuntimeError> {
        self.write_frames_flushed(frames, true)
            .await
            .map(|instant| instant.expect("writer boundary always flushes"))
    }

    async fn write_frames_flushed(
        &mut self,
        frames: &[Frame],
        force_flush: bool,
    ) -> Result<Option<Instant>, RuntimeError> {
        let deadline = self.retention_deadline();
        tokio::time::timeout_at(deadline, self.write_frames_flushed_io(frames, force_flush))
            .await
            .map_err(|_| RuntimeError::SessionRetentionTimeout)?
    }

    async fn write_frames_flushed_io(
        &mut self,
        frames: &[Frame],
        force_flush: bool,
    ) -> Result<Option<Instant>, RuntimeError> {
        if frames.is_empty() && !force_flush {
            return Ok(None);
        }
        let connection = self
            .connection
            .as_mut()
            .expect("validation actor owns its connected carrier");
        if !frames.is_empty() {
            connection.writer.write_frames(frames).await?;
        }
        connection.writer.flush().await?;
        let completed_at = Instant::now();
        connection.refresh_liveness();
        for frame in frames {
            self.path_proofs.record_sent_frame(frame);
        }
        Ok(Some(completed_at))
    }

    async fn send_event(&self, event: ClientTcpValidationEvent) -> Result<(), RuntimeError> {
        tokio::time::timeout_at(self.retention_deadline(), self.events.send(event))
            .await
            .map_err(|_| RuntimeError::SessionRetentionTimeout)?
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)
    }

    fn candidate(&self) -> ClientTcpValidationCandidate {
        self.candidate
            .expect("connected validation actor owns one candidate identity")
    }

    fn retention_deadline(&self) -> tokio::time::Instant {
        self.retention_deadline
            .expect("validation admission owns one absolute retention deadline")
    }
}

async fn receive_validation_command(
    commands: Option<&mut ReliablePathCommandReceivers>,
    mode: ValidationCommandReceiveMode,
) -> Option<ReliablePathCommand> {
    match (commands, mode) {
        (Some(commands), ValidationCommandReceiveMode::Live) => {
            recv_reliable_path_command(commands).await
        }
        (Some(commands), ValidationCommandReceiveMode::Drain) => {
            recv_reliable_path_command_during_drain(commands).await
        }
        (_, ValidationCommandReceiveMode::None) | (None, _) => {
            std::future::pending::<Option<ReliablePathCommand>>().await
        }
    }
}

async fn wait_for_endpoint_policy_change(
    policy: &mut tokio::sync::watch::Receiver<super::group::ClientTcpEndpointPolicySnapshot>,
    endpoint_generation: u64,
) {
    loop {
        let snapshot = *policy.borrow_and_update();
        if !snapshot.enabled || snapshot.generation != endpoint_generation {
            return;
        }
        if policy.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
#[path = "client_validation_test.rs"]
mod tests;
