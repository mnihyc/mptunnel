//! Public handle for the reliable TCP path actor.
//!
//! Callers provide the work's traffic class and enqueue typed commands here;
//! connection, stream, receive, capacity, and writer state remain with their
//! runtime owners.

use super::client_datagram::ClientTcpDatagramOpenCancellation;
pub(in crate::runtime) use super::client_datagram::{
    ClientTcpDatagramAttachment, ClientTcpDatagramInbound,
};
use super::client_session::run_client_tcp_path_session;
pub(in crate::runtime) use super::client_state::ClientTcpPathSessionRuntime;
use super::client_stream::{ClientTcpOpenCancellation, next_client_tcp_open_attempt_id};
use crate::model::path::CarrierPathInstanceId;
use crate::performance::ResourceLimits;
use crate::protocol::{StreamId, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ClientTcpOpenDeadlines, ClientTcpOpenResponse, ClientTcpOpenedStream, ReliablePathCommand,
    ReliablePathCommandSender, reliable_path_command_channels, reliable_path_command_queue,
};
use crate::scheduler::TrafficClass;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

pub(in crate::runtime) struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    ready_carrier_instance: Arc<AtomicU64>,
    // Traffic class changes command priority, not physical path identity. One
    // configured TCP path therefore owns one live carrier actor at a time.
    commands: Arc<Mutex<Option<ClientTcpPathSessionSlot>>>,
}

#[derive(Clone)]
struct ClientTcpPathSessionSlot {
    commands: ReliablePathCommandSender,
    terminal: Arc<AtomicBool>,
}

impl std::fmt::Debug for ClientTcpPathSessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTcpPathSessionHandle")
            .finish_non_exhaustive()
    }
}

impl Clone for ClientTcpPathSessionHandle {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            ready_carrier_instance: self.ready_carrier_instance.clone(),
            commands: self.commands.clone(),
        }
    }
}

impl ClientTcpPathSessionHandle {
    pub(in crate::runtime) fn new(runtime: ClientTcpPathSessionRuntime) -> Self {
        Self {
            runtime,
            ready_carrier_instance: Arc::new(AtomicU64::new(0)),
            commands: Arc::new(Mutex::new(None)),
        }
    }

    pub(in crate::runtime) async fn open_stream_with_deadlines(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        lane: TrafficClass,
        open_deadlines: ClientTcpOpenDeadlines,
        advertised_recv_max_offset: u64,
    ) -> Result<ClientTcpOpenedStream, RuntimeError> {
        let session = self.ensure_session_slot()?;
        let commands = session.commands.clone();
        let observed_carrier_instance = self.ready_carrier_instance.load(Ordering::Acquire);
        let (response_tx, response_rx) = oneshot::channel();
        let attempt_id = next_client_tcp_open_attempt_id();
        let wait_for_deadline = || async {
            let first_deadline = if observed_carrier_instance == 0 {
                open_deadlines.setup
            } else {
                open_deadlines.live
            };
            tokio::time::sleep_until(first_deadline).await;
            if observed_carrier_instance != 0
                && self.ready_carrier_instance.load(Ordering::Acquire) != observed_carrier_instance
            {
                tokio::time::sleep_until(open_deadlines.setup).await;
            }
        };
        tokio::select! {
            biased;
            result = commands.send_control(ReliablePathCommand::OpenStream {
                stream_id,
                attempt_id,
                observed_carrier_instance,
                target,
                lane,
                advertised_recv_max_offset,
                open_deadlines,
                session_commands: commands.clone(),
                response: response_tx,
            }) => result.map_err(|_| RuntimeError::ReliablePathSessionClosed)?,
            _ = wait_for_deadline() => return Err(RuntimeError::PathOpenTimedOut),
        }
        // Tokio mpsc send is cancellation-safe. Arm cleanup only after the
        // actor owns this exact generation, never for an unqueued attempt.
        let mut cancellation =
            ClientTcpOpenCancellation::new(commands.clone(), stream_id, attempt_id);
        let response = tokio::select! {
            biased;
            response = response_rx => response.map_err(|_| RuntimeError::ReliablePathSessionClosed)?,
            _ = wait_for_deadline() => return Err(RuntimeError::PathOpenTimedOut),
        };
        match response {
            ClientTcpOpenResponse::Opened(opened) => {
                cancellation.disarm();
                Ok(opened)
            }
            ClientTcpOpenResponse::RejectedWithoutOpen(err) => {
                cancellation.disarm();
                Err(err)
            }
            ClientTcpOpenResponse::FailedAfterOpen(err) => Err(err),
        }
    }

    /// Registers a product-datagram route on this path's existing TCP actor.
    pub(in crate::runtime) async fn open_datagram_attachment(
        &self,
        open_deadline: tokio::time::Instant,
        frame_queue: usize,
    ) -> Result<ClientTcpDatagramAttachment, RuntimeError> {
        let session = self.ensure_session_slot()?;
        let commands = session.commands.clone();
        let channels = ClientTcpDatagramAttachment::channel(frame_queue);
        let attachment_id = channels.attachment_id;
        let (response_tx, response_rx) = oneshot::channel();
        tokio::select! {
            biased;
            result = commands.send_control(ReliablePathCommand::OpenDatagramAttachment {
                attachment_id,
                frames: channels.frames_tx,
                failure: channels.failure_tx,
                open_deadline,
                response: response_tx,
            }) => result.map_err(|_| RuntimeError::ReliablePathSessionClosed)?,
            _ = tokio::time::sleep_until(open_deadline) => {
                return Err(RuntimeError::PathOpenTimedOut);
            }
        }
        // The close uses the same FIFO control queue as the open. If this
        // future is cancelled after admission, the actor still retires the
        // exact attachment after processing its open command.
        let mut cancellation =
            ClientTcpDatagramOpenCancellation::new(commands.clone(), attachment_id);
        let path_instance_id = tokio::select! {
            biased;
            response = response_rx => {
                response.map_err(|_| RuntimeError::ReliablePathSessionClosed)??
            }
            _ = tokio::time::sleep_until(open_deadline) => {
                return Err(RuntimeError::PathOpenTimedOut);
            }
        };
        let attachment = ClientTcpDatagramAttachment::new(
            attachment_id,
            path_instance_id,
            commands,
            channels.frames_rx,
            channels.failure_rx,
        );
        cancellation.disarm();
        Ok(attachment)
    }

    /// Establishes the durable carrier without creating a product stream.
    #[cfg(test)]
    pub(in crate::runtime) async fn prepare_connection(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<Option<Duration>, RuntimeError> {
        let policy = self.runtime.endpoint_policy.snapshot();
        if !policy.enabled {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        self.prepare_connection_for_endpoint_generation(open_deadline, policy.generation)
            .await
    }

    pub(in crate::runtime) async fn prepare_connection_for_endpoint_generation(
        &self,
        open_deadline: tokio::time::Instant,
        endpoint_generation: u64,
    ) -> Result<Option<Duration>, RuntimeError> {
        if !self.runtime.endpoint_policy.allows(endpoint_generation) {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let session = self.ensure_session_slot_for_endpoint_generation(endpoint_generation)?;
        let commands = session.commands.clone();
        let (response_tx, response_rx) = oneshot::channel();
        tokio::select! {
            biased;
            result = commands.send_control(ReliablePathCommand::PrepareConnection {
                open_deadline,
                endpoint_generation,
                response: response_tx,
            }) => result.map_err(|_| RuntimeError::ReliablePathSessionClosed)?,
            _ = tokio::time::sleep_until(open_deadline) => {
                return Err(RuntimeError::PathOpenTimedOut);
            }
        }
        tokio::select! {
            biased;
            response = response_rx => {
                response.map_err(|_| RuntimeError::ReliablePathSessionClosed)?
            }
            _ = tokio::time::sleep_until(open_deadline) => {
                Err(RuntimeError::PathOpenTimedOut)
            }
        }
    }

    pub(in crate::runtime) fn is_connection_ready(&self) -> bool {
        self.connection_instance_id().is_some()
    }

    pub(in crate::runtime) fn connection_instance_id(&self) -> Option<CarrierPathInstanceId> {
        match self.ready_carrier_instance.load(Ordering::Acquire) {
            0 => None,
            instance_id => Some(CarrierPathInstanceId::from_raw(instance_id)),
        }
    }

    pub(in crate::runtime) fn begin_path_drain(&self) {
        self.ready_carrier_instance.store(0, Ordering::Release);
        let current = self.commands.lock().expect("TCP path session lock");
        if let Some(session) = current.as_ref()
            && !session.terminal.load(Ordering::Acquire)
        {
            session.commands.begin_path_drain();
        }
    }

    fn ensure_session_slot(&self) -> Result<ClientTcpPathSessionSlot, RuntimeError> {
        let endpoint_generation = self.runtime.endpoint_policy.snapshot().generation;
        self.ensure_session_slot_for_endpoint_generation(endpoint_generation)
    }

    fn ensure_session_slot_for_endpoint_generation(
        &self,
        endpoint_generation: u64,
    ) -> Result<ClientTcpPathSessionSlot, RuntimeError> {
        self.runtime
            .endpoint_policy
            .with_current(endpoint_generation, || {
                let mut current = self.commands.lock().expect("TCP path session lock");
                if let Some(session) = current.as_ref()
                    && !session.terminal.load(Ordering::Acquire)
                {
                    return session.clone();
                }

                let (commands, receivers) =
                    reliable_path_command_channels(self.runtime.command_queue);
                let terminal = Arc::new(AtomicBool::new(false));
                tokio::spawn(run_client_tcp_path_session(
                    self.runtime.clone(),
                    receivers,
                    self.ready_carrier_instance.clone(),
                    terminal.clone(),
                ));
                let session = ClientTcpPathSessionSlot { commands, terminal };
                *current = Some(session.clone());
                session
            })
            .ok_or(RuntimeError::NoSchedulableTcpPath)
    }
}

pub(in crate::runtime) fn tcp_session_command_queue(resources: ResourceLimits) -> usize {
    reliable_path_command_queue(resources.into())
}
