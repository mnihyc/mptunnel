//! Public handle for the reliable TCP path actor.
//!
//! Callers provide the work's traffic class and enqueue typed commands here;
//! connection, stream, receive, capacity, and writer state remain with their
//! runtime owners.

use super::client_session::run_client_tcp_path_session;
pub(in crate::runtime) use super::client_state::ClientTcpPathSessionRuntime;
use super::client_stream::{ClientTcpOpenCancellation, next_client_tcp_open_attempt_id};
use crate::config::ResourceLimits;
use crate::protocol::{StreamId, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ClientTcpOpenDeadlines, ClientTcpOpenResponse, ClientTcpOpenedStream, ReliablePathCommand,
    ReliablePathCommandSender, reliable_path_command_channels, reliable_path_command_queue,
};
use crate::scheduler::TrafficClass;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

pub(in crate::runtime) struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    // Traffic class changes command priority, not physical path identity. One
    // configured TCP path therefore owns one live carrier actor at a time.
    commands: Arc<Mutex<Option<ClientTcpPathSessionSlot>>>,
}

#[derive(Clone)]
struct ClientTcpPathSessionSlot {
    commands: ReliablePathCommandSender,
    carrier_generation: Arc<AtomicU64>,
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
            commands: self.commands.clone(),
        }
    }
}

impl ClientTcpPathSessionHandle {
    pub(in crate::runtime) fn new(runtime: ClientTcpPathSessionRuntime) -> Self {
        Self {
            runtime,
            commands: Arc::new(Mutex::new(None)),
        }
    }

    pub(in crate::runtime) async fn open_stream_with_deadlines(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        lane: TrafficClass,
        open_deadlines: ClientTcpOpenDeadlines,
    ) -> Result<ClientTcpOpenedStream, RuntimeError> {
        let session = self.ensure_session_slot();
        let commands = session.commands.clone();
        let observed_carrier_generation = session.carrier_generation.load(Ordering::Acquire);
        let (response_tx, response_rx) = oneshot::channel();
        let attempt_id = next_client_tcp_open_attempt_id();
        let wait_for_deadline = || async {
            let first_deadline = if observed_carrier_generation == 0 {
                open_deadlines.setup
            } else {
                open_deadlines.live
            };
            tokio::time::sleep_until(first_deadline).await;
            if observed_carrier_generation != 0
                && session.carrier_generation.load(Ordering::Acquire) != observed_carrier_generation
            {
                tokio::time::sleep_until(open_deadlines.setup).await;
            }
        };
        tokio::select! {
            biased;
            result = commands.send_control(ReliablePathCommand::OpenStream {
                stream_id,
                attempt_id,
                observed_carrier_generation,
                target,
                lane,
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

    /// Establishes the durable carrier without creating a product stream.
    pub(in crate::runtime) async fn prepare_connection(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<Option<Duration>, RuntimeError> {
        let session = self.ensure_session_slot();
        let commands = session.commands.clone();
        let (response_tx, response_rx) = oneshot::channel();
        tokio::select! {
            biased;
            result = commands.send_control(ReliablePathCommand::PrepareConnection {
                open_deadline,
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

    fn ensure_session_slot(&self) -> ClientTcpPathSessionSlot {
        let mut current = self.commands.lock().expect("TCP path session lock");
        if let Some(session) = current.as_ref()
            && !session.commands.is_closed()
        {
            return session.clone();
        }

        let (commands, receivers) = reliable_path_command_channels(self.runtime.command_queue);
        let carrier_generation = Arc::new(AtomicU64::new(0));
        tokio::spawn(run_client_tcp_path_session(
            self.runtime.clone(),
            receivers,
            carrier_generation.clone(),
        ));
        let session = ClientTcpPathSessionSlot {
            commands,
            carrier_generation,
        };
        *current = Some(session.clone());
        session
    }
}

pub(in crate::runtime) fn tcp_session_command_queue(resources: ResourceLimits) -> usize {
    reliable_path_command_queue(resources.into())
}
