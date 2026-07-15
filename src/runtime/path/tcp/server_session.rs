//! Authenticated server TCP actor and exact carrier lifetime.
//!
//! The registration guard, substates, and biased input loop live together so
//! no stream, datagram, or proof can outlive the TCP carrier that owns it.

use super::io::encrypted_framed_peer_closed;
use super::server_datagram::{ServerTcpDatagramEffect, ServerTcpDatagramState};
use super::server_evidence::{ServerTcpEvidenceOutcome, ServerTcpEvidenceState};
use super::server_stream::ServerTcpStreamState;
use super::server_writer::ServerTcpWriter;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::protocol::{CloseReason, Frame, PathId, ResetReason, SessionId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    TcpCapacityProbeOwner, recv_reliable_path_command,
    reliable_noninterlocked_tcp_writer_run_budget_bytes, reliable_path_command_pending_bytes,
    reliable_path_command_writer_run_budget_items, reliable_path_command_writer_run_bytes,
    reliable_path_frame_requires_capacity_command, reliable_path_receivers_closed,
    try_coalesce_reliable_path_writer_run, try_recv_reliable_path_command,
};
use crate::runtime::path::server_context::ServerPathContext;
use crate::transport::encrypted::EncryptedFramedTransportError;
use std::time::Instant;
use tokio::sync::mpsc;

enum ServerTcpSessionDisposition {
    Continue,
    Stop,
}

enum ServerTcpFrameDisposition {
    Continue,
    SkipCommandPoll,
    Stop,
}

enum ServerTcpPathEvent {
    Frame(Frame),
    Command(ReliablePathCommand),
}

/// Owned handoff from authenticated admission into the long-lived actor.
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
}

pub(super) struct ServerTcpPathSession {
    session_id: SessionId,
    path_id: PathId,
    draining: bool,
    // Field order releases probe/flow RAII before queues and registration.
    evidence: ServerTcpEvidenceState,
    datagrams: ServerTcpDatagramState,
    streams: ServerTcpStreamState,
    commands_rx: ReliablePathCommandReceivers,
    commands_tx: ReliablePathCommandSender,
    path_frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    writer: ServerTcpWriter,
    path_registration: ServerCarrierPathRegistration,
    context: ServerPathContext,
}

impl ServerTcpPathSession {
    pub(super) fn new(admission: ServerTcpPathAdmission) -> Self {
        Self {
            session_id: admission.session_id,
            path_id: admission.path_id,
            draining: false,
            evidence: admission.evidence,
            datagrams: ServerTcpDatagramState::new(),
            streams: ServerTcpStreamState::new(),
            commands_rx: admission.commands_rx,
            commands_tx: admission.commands_tx,
            path_frames: admission.path_frames,
            writer: admission.writer,
            path_registration: admission.path_registration,
            context: admission.context,
        }
    }

    pub(super) async fn run(mut self) -> Result<(), RuntimeError> {
        loop {
            let event = if let Some(deadline) = self.evidence.response_probe_deadline() {
                match tokio::time::timeout_at(
                    deadline,
                    recv_server_tcp_path_event(&mut self.path_frames, &mut self.commands_rx),
                )
                .await
                {
                    Ok(event) => event?,
                    Err(_) => {
                        self.evidence.log_response_probe_timeout(
                            self.session_id,
                            self.path_id,
                            self.path_registration.path_instance_id(),
                        );
                        // A late receipt cannot be attributed after the lease is
                        // released, so fail-close this exact carrier.
                        return Ok(());
                    }
                }
            } else {
                recv_server_tcp_path_event(&mut self.path_frames, &mut self.commands_rx).await?
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
            }
        }
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
                role,
                ..
            } if !self.draining => {
                let reply = self
                    .streams
                    .open(
                        &self.context,
                        &self.path_registration,
                        &self.commands_tx,
                        self.session_id,
                        self.path_id,
                        self.evidence.startup_metrics(),
                        stream_id,
                        target,
                        demand,
                        role,
                    )
                    .await?;
                self.write_optional_reply(reply).await
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
            } if !self.draining => {
                let effect = self
                    .datagrams
                    .open(
                        &self.context,
                        &self.commands_tx,
                        self.session_id,
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
                let effect = self
                    .datagrams
                    .handle_data(flow_id, datagram_id, ttl_ms, payload)?;
                self.apply_datagram_effect(effect).await
            }
            Frame::DatagramFeedback { .. } => Ok(ServerTcpFrameDisposition::Continue),
            Frame::DatagramClose { flow_id } => {
                self.datagrams.remove(flow_id);
                if self.draining && self.streams.is_empty() && self.datagrams.is_empty() {
                    self.write_stop_reply(Frame::PathClose {
                        path_id: self.path_id,
                        reason: CloseReason::Normal,
                    })
                    .await
                } else {
                    Ok(ServerTcpFrameDisposition::Continue)
                }
            }
            frame @ (Frame::StreamData { stream_id, .. }
            | Frame::StreamAck { stream_id, .. }
            | Frame::StreamMaxData { stream_id, .. }
            | Frame::StreamFin { stream_id, .. }
            | Frame::StreamReset { stream_id, .. }) => {
                self.streams
                    .route_frame(
                        &self.context,
                        self.session_id,
                        self.path_id,
                        stream_id,
                        frame,
                    )
                    .await?;
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::StreamDetach { stream_id } => {
                self.streams.detach(
                    &self.context,
                    &self.commands_tx,
                    self.session_id,
                    self.path_id,
                    stream_id,
                );
                if self.draining && self.streams.is_empty() && self.datagrams.is_empty() {
                    self.write_stop_reply(Frame::PathClose {
                        path_id: self.path_id,
                        reason: CloseReason::Normal,
                    })
                    .await
                } else {
                    Ok(ServerTcpFrameDisposition::Continue)
                }
            }
            Frame::Ping { nonce } => self.write_reply(Frame::Pong { nonce }).await,
            Frame::PathProofData {
                path_id: proof_path_id,
                proof_id,
                payload,
            } if proof_path_id == self.path_id => {
                let reply =
                    self.evidence
                        .handle_path_proof_data(self.path_id, proof_id, payload.len());
                self.write_reply(reply).await
            }
            Frame::PathProofAck {
                path_id: proof_path_id,
                proof_id,
                payload_bytes,
            } if proof_path_id == self.path_id => {
                self.evidence.handle_path_proof_ack(
                    &self.context,
                    &self.path_registration,
                    self.path_id,
                    proof_id,
                    payload_bytes,
                );
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathCapacityReceipt {
                path_id: receipt_path_id,
                calibration_id,
                received_payload_bytes,
            } if receipt_path_id == self.path_id => {
                match self.evidence.handle_response_capacity_receipt(
                    &self.context,
                    &self.path_registration,
                    self.session_id,
                    self.path_id,
                    calibration_id,
                    received_payload_bytes,
                )? {
                    ServerTcpEvidenceOutcome::Handled => Ok(ServerTcpFrameDisposition::Continue),
                    ServerTcpEvidenceOutcome::SkipCommandPoll => {
                        Ok(ServerTcpFrameDisposition::SkipCommandPoll)
                    }
                }
            }
            Frame::PathCapacityData {
                path_id: capacity_path_id,
                calibration_id,
                payload,
            } if capacity_path_id == self.path_id => {
                self.evidence
                    .handle_request_capacity_data(calibration_id, payload.len())?;
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathCapacityFinish {
                path_id: capacity_path_id,
                calibration_id,
                payload_bytes,
            } if capacity_path_id == self.path_id => {
                let reply = self.evidence.handle_request_capacity_finish(
                    self.path_id,
                    calibration_id,
                    payload_bytes,
                )?;
                self.write_reply(reply).await
            }
            Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                self.evidence
                    .record_peer_metrics(&self.context, &self.path_registration, metrics);
                Ok(ServerTcpFrameDisposition::Continue)
            }
            Frame::PathDrain {
                path_id: drain_path_id,
            } if drain_path_id == self.path_id => {
                self.draining = true;
                if !self
                    .writer
                    .write_frame(&Frame::PathStatus {
                        path_id: self.path_id,
                        status: crate::protocol::PathStatus::Draining,
                    })
                    .await?
                {
                    return Ok(ServerTcpFrameDisposition::Stop);
                }
                if self.streams.is_empty() && self.datagrams.is_empty() {
                    Ok(ServerTcpFrameDisposition::Stop)
                } else {
                    Ok(ServerTcpFrameDisposition::Continue)
                }
            }
            Frame::PathClose {
                path_id: close_path_id,
                ..
            } if close_path_id == self.path_id => Ok(ServerTcpFrameDisposition::Stop),
            Frame::SessionClose { .. } => Ok(ServerTcpFrameDisposition::Stop),
            _ => Err(RuntimeError::Protocol("unexpected TCP path session frame")),
        }
    }

    async fn drain_commands(
        &mut self,
        first_command: ReliablePathCommand,
    ) -> Result<ServerTcpSessionDisposition, RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let drain_started = Instant::now();
        let byte_budget =
            reliable_noninterlocked_tcp_writer_run_budget_bytes(self.context.mux_limits);
        let item_budget = reliable_path_command_writer_run_budget_items(self.context.mux_limits);
        let mut next_command = Some(first_command);
        self.writer.clear_batch();
        let mut sent_bytes = 0usize;
        let mut sent_items = 0usize;
        let mut wrote_frame = false;

        loop {
            let Some(command) = next_command
                .take()
                .or_else(|| try_recv_reliable_path_command(&mut self.commands_rx))
            else {
                if try_coalesce_reliable_path_writer_run(
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
                    self.commands_rx
                        .release_pending_command_bytes(pending_bytes);
                    wrote_frame = true;
                    sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                    sent_items = sent_items.saturating_add(1);
                    if is_stream_detach || sent_bytes >= byte_budget || sent_items >= item_budget {
                        break;
                    }
                    continue;
                }
                ReliablePathCommand::SendQuicCapacityProbe(probe) => {
                    probe.ticket.cancel();
                    self.commands_rx
                        .release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server TCP path received QUIC capacity command",
                    ));
                }
                ReliablePathCommand::SendTcpCapacityProbe(probe) => {
                    let TcpCapacityProbeOwner::Response { path_instance_id } = probe.owner else {
                        self.commands_rx
                            .release_pending_command_bytes(pending_bytes);
                        return Err(RuntimeError::Protocol(
                            "server TCP path received request capacity command",
                        ));
                    };
                    if probe.path_id != self.path_id
                        || path_instance_id != self.path_registration.path_instance_id()
                        || probe.train_payload_bytes < probe.sample_floor_bytes
                        || self.evidence.has_response_probe()
                    {
                        self.commands_rx
                            .release_pending_command_bytes(pending_bytes);
                        return Err(RuntimeError::Protocol(
                            "server TCP capacity command does not match idle writer",
                        ));
                    }
                    if !self.writer.write_batch(&mut self.evidence).await? {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    if Instant::now() >= probe.expires_at {
                        self.commands_rx
                            .release_pending_command_bytes(pending_bytes);
                        return Ok(ServerTcpSessionDisposition::Continue);
                    }
                    let started_at = Instant::now();
                    let wrote = match tokio::time::timeout_at(
                        tokio::time::Instant::from_std(probe.expires_at),
                        self.writer.write_capacity_probe(
                            &probe,
                            self.context.mux_limits.max_payload_bytes,
                        ),
                    )
                    .await
                    {
                        Ok(result) => result?,
                        Err(_) => {
                            self.commands_rx
                                .release_pending_command_bytes(pending_bytes);
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "response_tcp_capacity_probe",
                                format_args!(
                                    "phase=rejected reason=send_timeout session_id={} path_id={} path_instance_id={} calibration_id={}",
                                    self.session_id.0,
                                    self.path_id.0,
                                    path_instance_id.as_u64(),
                                    probe.calibration_id,
                                ),
                            );
                            // Receipt cannot identify a partial train, so close
                            // the exact carrier instead of releasing it.
                            return Ok(ServerTcpSessionDisposition::Stop);
                        }
                    };
                    self.commands_rx
                        .release_pending_command_bytes(pending_bytes);
                    if !wrote {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_tcp_capacity_probe",
                        format_args!(
                            "phase=sent session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} sample_floor_bytes={}",
                            self.session_id.0,
                            self.path_id.0,
                            path_instance_id.as_u64(),
                            probe.calibration_id,
                            probe.train_payload_bytes,
                            probe.sample_floor_bytes,
                        ),
                    );
                    self.evidence.publish_response_probe(probe, started_at);
                    return Ok(ServerTcpSessionDisposition::Continue);
                }
                ReliablePathCommand::ResetAndCloseStream { stream_id, reason } => {
                    // A TCP session is shared, so retire only this attachment
                    // after its reset has entered the ordered carrier writer.
                    self.writer
                        .push_frame(Frame::StreamReset { stream_id, reason });
                    self.commands_rx
                        .release_pending_command_bytes(pending_bytes);
                    wrote_frame = true;
                    sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                    sent_items = sent_items.saturating_add(1);
                    if !self.writer.write_batch(&mut self.evidence).await? {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    self.streams.detach(
                        &self.context,
                        &self.commands_tx,
                        self.session_id,
                        self.path_id,
                        stream_id,
                    );
                    if self.draining && self.streams.is_empty() && self.datagrams.is_empty() {
                        let _ = self
                            .writer
                            .write_frame(&Frame::PathClose {
                                path_id: self.path_id,
                                reason: CloseReason::Normal,
                            })
                            .await?;
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                }
                ReliablePathCommand::CloseStream(stream_id) => {
                    if !self.writer.write_batch(&mut self.evidence).await? {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    self.streams.detach(
                        &self.context,
                        &self.commands_tx,
                        self.session_id,
                        self.path_id,
                        stream_id,
                    );
                    if self.draining && self.streams.is_empty() && self.datagrams.is_empty() {
                        let _ = self
                            .writer
                            .write_frame(&Frame::PathClose {
                                path_id: self.path_id,
                                reason: CloseReason::Normal,
                            })
                            .await?;
                        self.commands_rx
                            .release_pending_command_bytes(pending_bytes);
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    self.commands_rx
                        .release_pending_command_bytes(pending_bytes);
                    sent_items = sent_items.saturating_add(1);
                }
                ReliablePathCommand::OpenStream { .. } => {
                    if !self.writer.write_batch(&mut self.evidence).await? {
                        return Ok(ServerTcpSessionDisposition::Stop);
                    }
                    return Err(RuntimeError::Protocol(
                        "server TCP path received client open command",
                    ));
                }
                ReliablePathCommand::CancelTcpOpen { .. } => {
                    if !self.writer.write_batch(&mut self.evidence).await? {
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

        if !self.writer.write_batch(&mut self.evidence).await? {
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
            ServerTcpDatagramEffect::ReplyThenError { frame, failure } => {
                let wrote = self.writer.write_frame(&frame).await?;
                if wrote {
                    Err(failure.into_error())
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

    async fn write_stop_reply(
        &mut self,
        frame: Frame,
    ) -> Result<ServerTcpFrameDisposition, RuntimeError> {
        let _ = self.writer.write_frame(&frame).await?;
        Ok(ServerTcpFrameDisposition::Stop)
    }
}

async fn recv_server_tcp_path_event(
    path_frames: &mut mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    commands_rx: &mut ReliablePathCommandReceivers,
) -> Result<Option<ServerTcpPathEvent>, RuntimeError> {
    loop {
        let command_may_recv = !reliable_path_receivers_closed(commands_rx);
        tokio::select! {
            biased;
            frame = path_frames.recv() => {
                return match frame {
                    Some(Ok(frame)) => Ok(Some(ServerTcpPathEvent::Frame(frame))),
                    Some(Err(err)) if encrypted_framed_peer_closed(&err) => Ok(None),
                    Some(Err(err)) => Err(RuntimeError::Encrypted(err)),
                    None => Err(RuntimeError::ReliablePathSessionClosed),
                };
            }
            command = recv_reliable_path_command(commands_rx), if command_may_recv => {
                match command {
                    Some(command) => return Ok(Some(ServerTcpPathEvent::Command(command))),
                    None if reliable_path_receivers_closed(commands_rx) => return Ok(None),
                    None => continue,
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "server_session_test.rs"]
mod tests;
