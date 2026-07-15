//! Public handle for the reliable TCP path actor.
//!
//! Callers choose a lane and enqueue typed commands here; connection, stream,
//! receive, capacity, and writer state remain with their runtime owners.

use super::client_session::run_client_tcp_path_session;
pub(in crate::runtime) use super::client_state::ClientTcpPathSessionRuntime;
use super::client_stream::{ClientTcpOpenCancellation, next_client_tcp_open_attempt_id};
use crate::config::ResourceLimits;
#[cfg(test)]
use crate::protocol::SessionId;
use crate::protocol::{StreamId, StreamOpenRole, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ClientTcpOpenDeadlines, ClientTcpOpenResponse, ClientTcpOpenedStream, ReliablePathCommand,
    ReliablePathCommandSender, reliable_path_command_channels, reliable_path_command_queue,
};
#[cfg(test)]
use crate::runtime::stream::ReliablePathStream;
use crate::scheduler::FlowLane;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

pub(in crate::runtime) struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    commands: Arc<Mutex<Option<ClientTcpPathSessionSlot>>>,
    latency_commands: Arc<Mutex<Option<ClientTcpPathSessionSlot>>>,
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
            latency_commands: self.latency_commands.clone(),
        }
    }
}

impl ClientTcpPathSessionHandle {
    pub(in crate::runtime) fn new(runtime: ClientTcpPathSessionRuntime) -> Self {
        Self {
            runtime,
            commands: Arc::new(Mutex::new(None)),
            latency_commands: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.runtime.session_id
    }

    #[cfg(test)]
    pub(in crate::runtime) fn carrier_generation(&self, lane: FlowLane) -> u64 {
        let lane = if tcp_path_lane_uses_latency_session(lane) {
            &self.latency_commands
        } else {
            &self.commands
        };
        lane.lock()
            .expect("TCP path session lock")
            .as_ref()
            .map_or(0, |session| {
                session.carrier_generation.load(Ordering::Acquire)
            })
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        lane: FlowLane,
        role: StreamOpenRole,
        open_deadline: tokio::time::Instant,
    ) -> Result<ReliablePathStream, RuntimeError> {
        self.open_stream_with_deadlines(
            stream_id,
            target,
            lane,
            role,
            ClientTcpOpenDeadlines::fixed(open_deadline),
        )
        .await
        .map(|opened| ReliablePathStream::from_opened_carrier(opened.carrier))
    }

    pub(in crate::runtime) async fn open_stream_with_deadlines(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        lane: FlowLane,
        role: StreamOpenRole,
        open_deadlines: ClientTcpOpenDeadlines,
    ) -> Result<ClientTcpOpenedStream, RuntimeError> {
        let session = self.ensure_session_slot(lane);
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
                role,
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

    #[cfg(test)]
    pub(in crate::runtime) fn ensure_session(&self, lane: FlowLane) -> ReliablePathCommandSender {
        self.ensure_session_slot(lane).commands
    }

    fn ensure_session_slot(&self, lane: FlowLane) -> ClientTcpPathSessionSlot {
        let lane = if tcp_path_lane_uses_latency_session(lane) {
            &self.latency_commands
        } else {
            &self.commands
        };
        let mut current = lane.lock().expect("TCP path session lock");
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

pub(in crate::runtime) fn tcp_path_lane_uses_latency_session(lane: FlowLane) -> bool {
    lane.is_latency_sensitive()
}

pub(in crate::runtime) fn tcp_session_command_queue(resources: ResourceLimits) -> usize {
    reliable_path_command_queue(resources.into())
}
