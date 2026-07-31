//! Authenticated server TCP actor and exact carrier lifetime.
//!
//! The registration guard, substates, and biased input loop live together so
//! no stream, datagram, or proof can outlive the TCP carrier that owns it.

use super::io::encrypted_framed_peer_closed;
use super::server_datagram::{ServerTcpDatagramEffect, ServerTcpDatagramState};
use super::server_evidence::ServerTcpEvidenceState;
use super::server_stream::ServerTcpStreamState;
use super::server_writer::ServerTcpWriter;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::protocol::{CloseReason, Frame, PathId, PeerPathState, ResetReason, SessionId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    recv_reliable_path_command, recv_reliable_path_command_during_drain,
    reliable_path_command_pending_bytes, reliable_path_command_writer_run_budget_bytes,
    reliable_path_command_writer_run_budget_items, reliable_path_command_writer_run_bytes,
    reliable_path_frame_requires_capacity_command, reliable_path_receivers_closed,
    try_coalesce_reliable_path_writer_run, try_recv_reliable_path_command,
};
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{ServerCarrierPathRegistration, ServerStreamFrameRoute};
use crate::runtime::peer_status::PeerStatusCarrier;
use crate::transport::encrypted::EncryptedFramedTransportError;
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
use tokio::sync::mpsc;

enum ServerTcpSessionDisposition {
    Continue,
    Stop,
}

enum ServerTcpFrameDisposition {
    Continue,
    BeginPathDrain,
    SkipCommandPoll,
    Stop,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ServerTcpCarrierState {
    Active,
    Draining,
    Terminal,
}

enum ServerTcpPathEvent {
    Frame(Frame),
    Command(ReliablePathCommand),
    PeerStatusRequest(u64),
    SenderObservationDue,
}

enum ServerTcpPathDrainEvent {
    Incoming(Option<Result<Frame, EncryptedFramedTransportError>>),
    Command(Option<ReliablePathCommand>),
}

/// Owned state transferred from authenticated admission into the long-lived actor.
pub(super) struct ServerTcpPathAdmission {
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
}

pub(super) struct ServerTcpPathSession {
    session_id: SessionId,
    path_id: PathId,
    state: ServerTcpCarrierState,
    // Field order releases probe/flow RAII before queues and registration.
    evidence: ServerTcpEvidenceState,
    datagrams: ServerTcpDatagramState,
    streams: ServerTcpStreamState,
    commands_rx: ReliablePathCommandReceivers,
    commands_tx: ReliablePathCommandSender,
    path_frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    deferred_input: Option<Frame>,
    writer: ServerTcpWriter,
    peer_status: PeerStatusCarrier,
    path_registration: ServerCarrierPathRegistration,
    context: ServerPathContext,
}

impl ServerTcpPathSession {
    pub(super) fn new(admission: ServerTcpPathAdmission) -> Self {
        Self {
            session_id: admission.session_id,
            path_id: admission.path_id,
            state: ServerTcpCarrierState::Active,
            evidence: admission.evidence,
            datagrams: ServerTcpDatagramState::new(),
            streams: ServerTcpStreamState::new(),
            commands_rx: admission.commands_rx,
            commands_tx: admission.commands_tx,
            path_frames: admission.path_frames,
            deferred_input: None,
            writer: admission.writer,
            peer_status: admission.peer_status,
            path_registration: admission.path_registration,
            context: admission.context,
        }
    }

    pub(super) async fn run(self) -> Result<(), RuntimeError> {
        let retirement = self
            .context
            .wait_for_credential_retirement(self.path_registration.principal_permit().clone());
        tokio::pin!(retirement);
        tokio::select! {
            result = self.run_active() => result,
            () = &mut retirement => Ok(()),
        }
    }

    async fn run_active(mut self) -> Result<(), RuntimeError> {
        loop {
            let event = if let Some(frame) = self.deferred_input.take() {
                Some(ServerTcpPathEvent::Frame(frame))
            } else {
                recv_server_tcp_path_event(
                    &mut self.path_frames,
                    &mut self.commands_rx,
                    &mut self.peer_status,
                    self.evidence.next_sender_observation_at(),
                )
                .await?
            };
            let Some(event) = event else {
                return Ok(());
            };
            self.evidence
                .observe_periodic(&self.context, &self.path_registration, self.path_id);
            match event {
                ServerTcpPathEvent::Command(command) => {
                    if matches!(
                        self.drain_commands(command).await?,
                        ServerTcpSessionDisposition::Stop
                    ) {
                        return Ok(());
                    }
                }
                ServerTcpPathEvent::Frame(frame) => {
                    match self.handle_frame(frame).await? {
                        ServerTcpFrameDisposition::Continue => {}
                        ServerTcpFrameDisposition::BeginPathDrain => {
                            return self.run_path_drain().await;
                        }
                        ServerTcpFrameDisposition::SkipCommandPoll => continue,
                        ServerTcpFrameDisposition::Stop => return Ok(()),
                    }
                    if let Some(command) = try_recv_reliable_path_command(&mut self.commands_rx)
                        && matches!(
                            self.drain_commands(command).await?,
                            ServerTcpSessionDisposition::Stop
                        )
                    {
                        return Ok(());
                    }
                }
                ServerTcpPathEvent::PeerStatusRequest(request_id) => {
                    if matches!(
                        self.write_reply(Frame::PeerStatusRequest { request_id })
                            .await?,
                        ServerTcpFrameDisposition::Stop
                    ) {
                        return Ok(());
                    }
                }
                ServerTcpPathEvent::SenderObservationDue => {}
            }
        }
    }

    async fn run_path_drain(&mut self) -> Result<(), RuntimeError> {
        let deadline = tokio::time::Instant::now() + self.context.session_retention_timeout;
        tokio::time::timeout_at(deadline, self.complete_path_drain())
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)?
    }

    async fn complete_path_drain(&mut self) -> Result<(), RuntimeError> {
        debug_assert!(self.state == ServerTcpCarrierState::Draining);
        let retirement = self.path_registration.begin_retirement().wait();
        tokio::pin!(retirement);

        loop {
            let event = if let Some(frame) = self.deferred_input.take() {
                Some(ServerTcpPathEvent::Frame(frame))
            } else {
                tokio::select! {
                    biased;
                    () = &mut retirement => break,
                    event = recv_server_tcp_path_event(
                        &mut self.path_frames,
                        &mut self.commands_rx,
                        &mut self.peer_status,
                        self.evidence.next_sender_observation_at(),
                    ) => event?,
                }
            };
            let Some(event) = event else {
                return Ok(());
            };
            match event {
                ServerTcpPathEvent::Command(command) => {
                    if matches!(
                        self.drain_path_command(command).await?,
                        ServerTcpSessionDisposition::Stop
                    ) {
                        return Ok(());
                    }
                }
                ServerTcpPathEvent::Frame(frame) => match self.handle_frame(frame).await? {
                    ServerTcpFrameDisposition::Continue
                    | ServerTcpFrameDisposition::BeginPathDrain => {}
                    ServerTcpFrameDisposition::SkipCommandPoll => continue,
                    ServerTcpFrameDisposition::Stop => return Ok(()),
                },
                ServerTcpPathEvent::PeerStatusRequest(request_id) => {
                    if matches!(
                        self.write_reply(Frame::PeerStatusRequest { request_id })
                            .await?,
                        ServerTcpFrameDisposition::Stop
                    ) {
                        return Ok(());
                    }
                }
                ServerTcpPathEvent::SenderObservationDue => {}
            }
        }

        self.commands_rx.close_for_path_drain();

        loop {
            if let Some(frame) = self.deferred_input.take() {
                match self.handle_frame(frame).await? {
                    ServerTcpFrameDisposition::Continue
                    | ServerTcpFrameDisposition::BeginPathDrain
                    | ServerTcpFrameDisposition::SkipCommandPoll => continue,
                    ServerTcpFrameDisposition::Stop => return Ok(()),
                }
            }

            let event = {
                let command = recv_reliable_path_command_during_drain(&mut self.commands_rx);
                tokio::pin!(command);
                tokio::select! {
                    biased;
                    incoming = self.path_frames.recv() => {
                        ServerTcpPathDrainEvent::Incoming(incoming)
                    }
                    command = &mut command => ServerTcpPathDrainEvent::Command(command),
                }
            };
            match event {
                ServerTcpPathDrainEvent::Incoming(incoming) => {
                    let frame = match incoming {
                        Some(Ok(frame)) => frame,
                        Some(Err(err)) if encrypted_framed_peer_closed(&err) => return Ok(()),
                        Some(Err(err)) => return Err(RuntimeError::Encrypted(err)),
                        None => return Ok(()),
                    };
                    match self.handle_frame(frame).await? {
                        ServerTcpFrameDisposition::Continue
                        | ServerTcpFrameDisposition::BeginPathDrain
                        | ServerTcpFrameDisposition::SkipCommandPoll => {}
                        ServerTcpFrameDisposition::Stop => return Ok(()),
                    }
                }
                ServerTcpPathDrainEvent::Command(command) => {
                    let Some(command) = command else {
                        break;
                    };
                    if matches!(
                        self.drain_path_command(command).await?,
                        ServerTcpSessionDisposition::Stop
                    ) {
                        return Ok(());
                    }
                }
            }
        }

        if !self.streams.is_empty() || !self.datagrams.is_empty() {
            return Err(RuntimeError::Protocol(
                "TCP path drain preceded product attachment retirement",
            ));
        }
        debug_assert!(self.evidence.is_idle());
        self.state = ServerTcpCarrierState::Terminal;
        if !self
            .writer
            .write_frame_unflushed(&Frame::PathClose {
                path_id: self.path_id,
                reason: CloseReason::Normal,
            })
            .await?
        {
            return Ok(());
        }
        let _ = self.writer.flush().await?;
        Ok(())
    }

    async fn handle_frame(
        &mut self,
        frame: Frame,
    ) -> Result<ServerTcpFrameDisposition, RuntimeError> {
        match frame {
            Frame::OpenStream {
                stream_id,
                target,
                demand,
                ..
            } if self.state == ServerTcpCarrierState::Active => {
                let reply = self
                    .streams
                    .open(
                        &self.context,
                        &self.path_registration,
                        &self.commands_tx,
                        self.session_id,
                        stream_id,
                        target,
                        demand,
                    )
                    .await?;
                let accepted = matches!(&reply, Some(Frame::StreamMaxData { .. }));
                let disposition = self.write_optional_reply(reply).await?;
                if !accepted || matches!(disposition, ServerTcpFrameDisposition::Stop) {
                    return Ok(disposition);
                }
                self.write_path_validation().await
            }
            Frame::OpenStream { stream_id, .. } => {
                self.write_reply(Frame::StreamReset {
                    stream_id,
                    reason: ResetReason::Refused,
                })
                .await
            }
            Frame::OpenDatagramFlow {
                flow_id, target, ..
            } if self.state == ServerTcpCarrierState::Active => {
                let effect = self
                    .datagrams
                    .open(
                        &self.context,
                        &self.commands_tx,
                        self.session_id,
                        self.path_registration.principal_permit().clone(),
                        flow_id,
                        target,
                    )
                    .await?;
                self.apply_datagram_effect(effect).await
            }
            Frame::OpenDatagramFlow { flow_id, .. } => {
                self.write_reply(Frame::DatagramClose { flow_id }).await
            }
            Frame::DatagramData {
                flow_id,
                datagram_id,
                ttl_ms,
                payload,
            } => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_tcp_datagram_request_received",
                    format_args!(
                        "session_id={} path_id={} flow_id={} datagram_id={} payload_bytes={} ttl_ms={}",
                        self.session_id.0,
                        self.path_id.0,
                        flow_id.0,
                        datagram_id.0,
                        payload.len(),
                        ttl_ms,
                    ),
                );
                let effect = self
                    .datagrams
                    .handle_data(flow_id, datagram_id, ttl_ms, payload)
                    .await?;
                self.apply_datagram_effect(effect).await
            }
            Frame::DatagramFeedback { flow_id, received } => {
                self.datagrams.handle_feedback(flow_id, received)?;
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::DatagramClose { flow_id } => {
                self.datagrams.remove(flow_id);
                Ok(ServerTcpFrameDisposition::Continue)
            }
            frame @ (Frame::StreamData { stream_id, .. }
            | Frame::StreamAck { stream_id, .. }
            | Frame::StreamMaxData { stream_id, .. }
            | Frame::StreamFin { stream_id, .. }
            | Frame::StreamReset { stream_id, .. }) => {
                self.streams
                    .route_frame(
                        &self.context,
                        &self.path_registration,
                        self.path_id,
                        stream_id,
                        frame,
                    )
                    .await?;
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::StreamDetach { stream_id } => {
                self.streams
                    .detach(&self.context, &self.path_registration, stream_id)?;
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::Ping { nonce } => self.write_reply(Frame::Pong { nonce }).await,
            Frame::PathProofData {
                path_id: proof_path_id,
                proof_id,
                payload,
            } if proof_path_id == self.path_id && self.state == ServerTcpCarrierState::Active => {
                let reply =
                    self.evidence
                        .handle_path_proof_data(self.path_id, proof_id, payload.len());
                self.write_reply(reply).await
            }
            Frame::PathProofAck {
                path_id: proof_path_id,
                proof_id,
                payload_bytes,
            } if proof_path_id == self.path_id && self.state == ServerTcpCarrierState::Active => {
                self.evidence.handle_path_proof_ack(
                    &self.context,
                    &self.path_registration,
                    self.path_id,
                    proof_id,
                    payload_bytes,
                );
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathCapacityData {
                path_id: capacity_path_id,
                measurement_id,
                payload,
            } if capacity_path_id == self.path_id
                && self.state == ServerTcpCarrierState::Active =>
            {
                self.evidence
                    .handle_request_capacity_data(measurement_id, payload.len())?;
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathCapacityFinish {
                path_id: capacity_path_id,
                measurement_id,
                payload_bytes,
            } if capacity_path_id == self.path_id
                && self.state == ServerTcpCarrierState::Active =>
            {
                let reply = self.evidence.handle_request_capacity_finish(
                    self.path_id,
                    measurement_id,
                    payload_bytes,
                )?;
                self.write_reply(reply).await
            }
            Frame::PathProofData {
                path_id: measurement_path_id,
                ..
            }
            | Frame::PathProofAck {
                path_id: measurement_path_id,
                ..
            }
            | Frame::PathCapacityData {
                path_id: measurement_path_id,
                ..
            }
            | Frame::PathCapacityFinish {
                path_id: measurement_path_id,
                ..
            }
            | Frame::PathCapacityReceipt {
                path_id: measurement_path_id,
                ..
            } if measurement_path_id == self.path_id
                && self.state == ServerTcpCarrierState::Draining =>
            {
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                self.evidence
                    .record_peer_metrics(&self.context, &self.path_registration, metrics);
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathStatus {
                path_id: status_path_id,
                sequence,
                usage,
            } if status_path_id == self.path_id => {
                self.context.reliable_streams.record_peer_path_usage(
                    &self.path_registration,
                    sequence,
                    usage,
                );
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathStatus { .. } => Err(RuntimeError::Protocol(
                "TCP path usage advertisement path mismatch",
            )),
            Frame::PeerStatusRequest { request_id } => {
                let response =
                    self.peer_status
                        .response_frame(request_id, self.context.codec_limits, || {
                            Some(self.context.peer_status_snapshot(self.session_id))
                        });
                self.write_reply(response).await
            }
            Frame::PeerStatusResponse {
                request_id,
                code,
                paths,
            } => {
                let _ = self.peer_status.receive_response(request_id, code, paths);
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathDrain {
                path_id: drain_path_id,
            } if drain_path_id == self.path_id => match self.state {
                ServerTcpCarrierState::Active => {
                    self.commands_tx.begin_path_drain();
                    self.state = ServerTcpCarrierState::Draining;
                    self.path_registration.set_state(PeerPathState::Draining);
                    self.evidence.cancel_for_path_drain();
                    Ok(ServerTcpFrameDisposition::BeginPathDrain)
                }
                ServerTcpCarrierState::Draining | ServerTcpCarrierState::Terminal => {
                    Err(RuntimeError::Protocol("duplicate TCP path drain request"))
                }
            },
            Frame::PathDrain { .. } => Err(RuntimeError::Protocol(
                "TCP path drain request path mismatch",
            )),
            Frame::PathClose { .. } => Err(RuntimeError::Protocol(
                "TCP server received peer path close",
            )),
            Frame::SessionClose { .. } => Ok(ServerTcpFrameDisposition::Stop),
            _ => Err(RuntimeError::Protocol("unexpected TCP path session frame")),
        }
    }

    async fn drain_commands(
        &mut self,
        first_command: ReliablePathCommand,
    ) -> Result<ServerTcpSessionDisposition, RuntimeError> {
        self.write_command_run::<true>(first_command).await
    }

    async fn drain_path_command(
        &mut self,
        command: ReliablePathCommand,
    ) -> Result<ServerTcpSessionDisposition, RuntimeError> {
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        match command {
            ReliablePathCommand::SendFrame(frame)
                if server_tcp_frame_is_measurement_only(&frame) =>
            {
                self.commands_rx
                    .release_pending_command_bytes(pending_bytes);
                Ok(ServerTcpSessionDisposition::Continue)
            }
            ReliablePathCommand::SendTcpCapacityProbe(probe) => {
                drop(probe);
                self.commands_rx
                    .release_pending_command_bytes(pending_bytes);
                Ok(ServerTcpSessionDisposition::Continue)
            }
            command => self.write_command_run::<false>(command).await,
        }
    }

    async fn write_command_run<const POLL_READY: bool>(
        &mut self,
        first_command: ReliablePathCommand,
    ) -> Result<ServerTcpSessionDisposition, RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let drain_started = Instant::now();
        let byte_budget = reliable_path_command_writer_run_budget_bytes(self.context.mux_limits);
        let item_budget = reliable_path_command_writer_run_budget_items(self.context.mux_limits);
        let mut next_command = Some(first_command);
        self.writer.clear_batch();
        let mut sent_bytes = 0usize;
        let mut sent_items = 0usize;
        let mut wrote_frame = false;
        let mut writer_pending_bytes = 0usize;

        loop {
            let Some(command) = next_command.take().or_else(|| {
                POLL_READY
                    .then(|| try_recv_reliable_path_command(&mut self.commands_rx))
                    .flatten()
            }) else {
                if POLL_READY
                    && try_coalesce_reliable_path_writer_run(
                        &mut self.commands_rx,
                        &mut next_command,
                        sent_items,
                        sent_bytes,
                        byte_budget,
                        item_budget,
                    )
                    .await
                {
                    continue;
                }
                break;
            };
            let pending_bytes = reliable_path_command_pending_bytes(&command);
            let writer_run_bytes = reliable_path_command_writer_run_bytes(&command);
            if let ReliablePathCommand::SendFrame(Frame::DatagramClose { flow_id }) = &command {
                self.datagrams.remove(*flow_id);
            }
            match command {
                ReliablePathCommand::SendFrame(frame)
                    if reliable_path_frame_requires_capacity_command(&frame) =>
                {
                    self.commands_rx
                        .release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server TCP path received an untyped capacity frame",
                    ));
                }
                ReliablePathCommand::SendFrame(frame) => {
                    let is_stream_detach = matches!(&frame, Frame::StreamDetach { .. });
                    self.writer.push_frame(frame);
                    writer_pending_bytes = writer_pending_bytes.saturating_add(pending_bytes);
                    wrote_frame = true;
                    sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                    sent_items = sent_items.saturating_add(1);
                    if is_stream_detach || sent_bytes >= byte_budget || sent_items >= item_budget {
                        break;
                    }
                    continue;
                }
                ReliablePathCommand::SendTcpCapacityProbe(probe) => {
                    drop(probe);
                    self.commands_rx
                        .release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server TCP path received request capacity command",
                    ));
                }
                ReliablePathCommand::ResetAndCloseStream { stream_id, reason } => {
                    // A TCP session is shared, so retire only this attachment
                    // after its reset has entered the ordered carrier writer.
                    self.writer
                        .push_frame(Frame::StreamReset { stream_id, reason });
                    writer_pending_bytes = writer_pending_bytes.saturating_add(pending_bytes);
                    wrote_frame = true;
                    sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                    sent_items = sent_items.saturating_add(1);
                    if matches!(
                        self.write_batch_interlocked(&mut writer_pending_bytes)
                            .await?,
                        ServerTcpSessionDisposition::Stop
                    ) {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    self.streams
                        .detach(&self.context, &self.path_registration, stream_id)?;
                    if self.deferred_input.is_some() {
                        break;
                    }
                }
                ReliablePathCommand::CloseStream(stream_id) => {
                    if matches!(
                        self.write_batch_interlocked(&mut writer_pending_bytes)
                            .await?,
                        ServerTcpSessionDisposition::Stop
                    ) {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    self.streams
                        .detach(&self.context, &self.path_registration, stream_id)?;
                    self.commands_rx
                        .release_pending_command_bytes(pending_bytes);
                    sent_items = sent_items.saturating_add(1);
                    if self.deferred_input.is_some() {
                        break;
                    }
                }
                ReliablePathCommand::PrepareConnection { .. }
                | ReliablePathCommand::OpenStream { .. }
                | ReliablePathCommand::OpenDatagramAttachment { .. }
                | ReliablePathCommand::OpenDatagramFlow { .. }
                | ReliablePathCommand::SendDatagramFrame { .. }
                | ReliablePathCommand::CloseDatagramAttachment { .. } => {
                    if matches!(
                        self.write_batch_interlocked(&mut writer_pending_bytes)
                            .await?,
                        ServerTcpSessionDisposition::Stop
                    ) {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    return Err(RuntimeError::Protocol(
                        "server TCP path received client session command",
                    ));
                }
                ReliablePathCommand::CancelTcpOpen { .. } => {
                    if matches!(
                        self.write_batch_interlocked(&mut writer_pending_bytes)
                            .await?,
                        ServerTcpSessionDisposition::Stop
                    ) {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    return Err(RuntimeError::Protocol(
                        "server TCP path received client open cancellation",
                    ));
                }
            }
            if sent_bytes >= byte_budget || sent_items >= item_budget {
                break;
            }
        }

        if self.deferred_input.is_none()
            && matches!(
                self.write_batch_interlocked(&mut writer_pending_bytes)
                    .await?,
                ServerTcpSessionDisposition::Stop
            )
        {
            return Ok(ServerTcpSessionDisposition::Stop);
        }

        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "path_writer_drain",
            format_args!(
                "role=server underlay=Tcp path_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                self.path_id.0,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
                self.commands_rx.pending_bytes(),
                drain_started.elapsed().as_micros(),
                sent_bytes >= byte_budget,
                sent_items >= item_budget,
            ),
        );
        if wrote_frame && !self.writer.flush().await? {
            return Ok(ServerTcpSessionDisposition::Stop);
        }
        Ok(ServerTcpSessionDisposition::Continue)
    }

    async fn write_batch_interlocked(
        &mut self,
        writer_pending_bytes: &mut usize,
    ) -> Result<ServerTcpSessionDisposition, RuntimeError> {
        debug_assert!(self.deferred_input.is_none());
        let mut routed_frames = 0usize;
        let write_result = {
            let write = self.writer.write_batch(&mut self.evidence);
            tokio::pin!(write);
            loop {
                tokio::select! {
                    biased;
                    result = &mut write => break result?,
                    incoming = self.path_frames.recv(), if self.deferred_input.is_none() => {
                        let frame = match incoming {
                            Some(Ok(frame)) => frame,
                            Some(Err(err)) if encrypted_framed_peer_closed(&err) => {
                                return Ok(ServerTcpSessionDisposition::Stop);
                            }
                            Some(Err(err)) => return Err(RuntimeError::Encrypted(err)),
                            None => return Ok(ServerTcpSessionDisposition::Stop),
                        };
                        let stream_id = match &frame {
                            Frame::StreamData { stream_id, .. }
                            | Frame::StreamAck { stream_id, .. }
                            | Frame::StreamMaxData { stream_id, .. }
                            | Frame::StreamFin { stream_id, .. }
                            | Frame::StreamReset { stream_id, .. } => Some(*stream_id),
                            _ => None,
                        };
                        match stream_id {
                            Some(stream_id) => match self.streams.try_route_frame(
                                &self.context,
                                &self.path_registration,
                                self.path_id,
                                stream_id,
                                frame,
                            )? {
                                ServerStreamFrameRoute::Routed => {
                                    routed_frames = routed_frames.saturating_add(1);
                                }
                                ServerStreamFrameRoute::Backpressured(frame) => {
                                    self.deferred_input = Some(frame);
                                }
                            },
                            None => self.deferred_input = Some(frame),
                        }
                    }
                }
            }
        };
        if write_result && *writer_pending_bytes > 0 {
            self.evidence
                .observe_after_write(&self.context, &self.path_registration, self.path_id);
            self.commands_rx
                .release_pending_command_bytes(std::mem::take(writer_pending_bytes));
        }
        #[cfg(feature = "lab-diagnostics")]
        if routed_frames > 0 || self.deferred_input.is_some() {
            lab_diagnostic(
                "server_tcp_write_feedback_interlock",
                format_args!(
                    "path_id={} routed_frames={} deferred_frames={}",
                    self.path_id.0,
                    routed_frames,
                    usize::from(self.deferred_input.is_some()),
                ),
            );
        }
        if write_result {
            Ok(ServerTcpSessionDisposition::Continue)
        } else {
            Ok(ServerTcpSessionDisposition::Stop)
        }
    }

    async fn apply_datagram_effect(
        &mut self,
        effect: ServerTcpDatagramEffect,
    ) -> Result<ServerTcpFrameDisposition, RuntimeError> {
        match effect {
            ServerTcpDatagramEffect::None => Ok(ServerTcpFrameDisposition::Continue),
            ServerTcpDatagramEffect::Reply(frame) => self.write_reply(frame).await,
            ServerTcpDatagramEffect::ReplyAndSkipCommandPoll(frame) => {
                if self.writer.write_frame(&frame).await? {
                    Ok(ServerTcpFrameDisposition::SkipCommandPoll)
                } else {
                    Ok(ServerTcpFrameDisposition::Stop)
                }
            }
        }
    }

    async fn write_optional_reply(
        &mut self,
        frame: Option<Frame>,
    ) -> Result<ServerTcpFrameDisposition, RuntimeError> {
        match frame {
            Some(frame) => self.write_reply(frame).await,
            None => Ok(ServerTcpFrameDisposition::Continue),
        }
    }

    async fn write_path_validation(&mut self) -> Result<ServerTcpFrameDisposition, RuntimeError> {
        let Some(challenge) = self
            .path_registration
            .path_validation_challenge(self.context.mux_limits)
        else {
            return Ok(ServerTcpFrameDisposition::Continue);
        };
        if !self.writer.write_frame(&challenge).await? {
            return Ok(ServerTcpFrameDisposition::Stop);
        }
        self.evidence.record_sent_frame(&challenge);
        Ok(ServerTcpFrameDisposition::Continue)
    }

    async fn write_reply(
        &mut self,
        frame: Frame,
    ) -> Result<ServerTcpFrameDisposition, RuntimeError> {
        if self.writer.write_frame(&frame).await? {
            Ok(ServerTcpFrameDisposition::Continue)
        } else {
            Ok(ServerTcpFrameDisposition::Stop)
        }
    }
}

fn server_tcp_frame_is_measurement_only(frame: &Frame) -> bool {
    matches!(
        frame,
        Frame::PathProofData { .. }
            | Frame::PathProofAck { .. }
            | Frame::PathCapacityData { .. }
            | Frame::PathCapacityFinish { .. }
            | Frame::PathCapacityReceipt { .. }
    )
}

async fn recv_server_tcp_path_event(
    path_frames: &mut mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    commands_rx: &mut ReliablePathCommandReceivers,
    peer_status: &mut PeerStatusCarrier,
    sender_observation_at: Option<std::time::Instant>,
) -> Result<Option<ServerTcpPathEvent>, RuntimeError> {
    let sender_observation_timer = async move {
        match sender_observation_at {
            Some(deadline) => {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            }
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(sender_observation_timer);
    loop {
        let command_may_recv = !reliable_path_receivers_closed(commands_rx);
        tokio::select! {
            biased;
            request_id = peer_status.recv_request() => {
                if let Some(request_id) = request_id {
                    return Ok(Some(ServerTcpPathEvent::PeerStatusRequest(request_id)));
                }
            }
            frame = path_frames.recv() => {
                return match frame {
                    Some(Ok(frame)) => Ok(Some(ServerTcpPathEvent::Frame(frame))),
                    Some(Err(err)) if encrypted_framed_peer_closed(&err) => Ok(None),
                    Some(Err(err)) => Err(RuntimeError::Encrypted(err)),
                    // The reader task is the sole producer. Exhaustion means
                    // the carrier is gone; actor drop guards retire its path
                    // and streams regardless of transport close ordering.
                    None => Ok(None),
                };
            }
            command = recv_reliable_path_command(commands_rx), if command_may_recv => {
                match command {
                    Some(command) => return Ok(Some(ServerTcpPathEvent::Command(command))),
                    None if reliable_path_receivers_closed(commands_rx) => return Ok(None),
                    None => continue,
                }
            }
            _ = &mut sender_observation_timer => {
                return Ok(Some(ServerTcpPathEvent::SenderObservationDue));
            }
        }
    }
}

#[cfg(test)]
#[path = "server_session_test.rs"]
mod tests;
