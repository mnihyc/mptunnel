//! Public handle for the reliable TCP path actor.
//!
//! Callers provide the work's traffic class and enqueue typed commands here;
//! connection, stream, receive, capacity, and writer state remain with their
//! runtime owners.

mod capacity;
pub(in crate::runtime) mod connection;
mod datagram;
mod receive;
pub(super) mod session;
pub(super) mod state;
mod stream;
#[cfg(test)]
#[path = "client/tests_connection.rs"]
mod tests_connection;
mod writer;

use self::datagram::ClientTcpDatagramOpenCancellation;
pub(in crate::runtime) use self::datagram::{
    ClientTcpDatagramAttachment, ClientTcpDatagramInbound,
};
use self::session::{
    connect_client_tcp_path, publish_client_tcp_replacement_connection_committed,
    run_client_tcp_path_session, run_client_tcp_path_session_with_connection,
};
pub(in crate::runtime) use self::state::ClientTcpPathSessionRuntime;
use self::stream::{ClientTcpOpenCancellation, next_client_tcp_open_attempt_id};
use crate::model::path::CarrierPathInstanceId;
use crate::performance::ResourceLimits;
use crate::protocol::{
    CloseReason, Frame, IpPacketId, IpTunnelId, PathId, StreamDemandHint, StreamId, TargetAddr,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ClientTcpOpenDeadlines, ClientTcpOpenResponse, ClientTcpOpenedStream, ReliablePathCommand,
    ReliablePathCommandSender, reliable_path_command_channels, reliable_path_command_queue,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

pub(in crate::runtime) struct ClientTcpIpTunnelAttachment {
    handle: ClientTcpPathSessionHandle,
    commands: ReliablePathCommandSender,
    tunnel_id: IpTunnelId,
    path_instance_id: CarrierPathInstanceId,
    opened: AtomicBool,
}

impl ClientTcpIpTunnelAttachment {
    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.path_instance_id
    }

    pub(in crate::runtime) fn is_current(&self) -> bool {
        self.handle.connection_instance_id() == Some(self.path_instance_id)
    }

    pub(in crate::runtime) async fn start(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        if !self.is_current() {
            return Err(RuntimeError::ReliablePathRetired);
        }
        self.opened.store(true, Ordering::Release);
        let queued = tokio::select! {
            result = self.commands.send_control(ReliablePathCommand::SendFrame(
                Frame::OpenIpTunnel { tunnel_id: self.tunnel_id },
            )) => result,
            _ = tokio::time::sleep_until(open_deadline) => {
                self.opened.store(false, Ordering::Release);
                return Err(RuntimeError::PathOpenTimedOut);
            }
        };
        if queued.is_err() {
            self.opened.store(false, Ordering::Release);
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        if !self.is_current() {
            return Err(RuntimeError::ReliablePathRetired);
        }
        Ok(())
    }

    pub(in crate::runtime) fn try_send(
        &self,
        packet_id: IpPacketId,
        payload: Bytes,
    ) -> Result<(), RuntimeError> {
        if !self.is_current() {
            return Err(RuntimeError::ReliablePathRetired);
        }
        self.commands.try_enqueue_admitted_frame(
            Frame::IpPacket {
                tunnel_id: self.tunnel_id,
                packet_id,
                payload,
            },
            TrafficClass::RealtimeDatagram,
        )
    }

    pub(in crate::runtime) async fn wait_retired(&self) {
        let mut changes = self.handle.runtime.carrier_groups.subscribe();
        while self.is_current() && changes.changed().await.is_ok() {}
    }
}

impl Drop for ClientTcpIpTunnelAttachment {
    fn drop(&mut self) {
        if self.opened.load(Ordering::Acquire) && self.is_current() {
            let _ = self.commands.try_enqueue_admitted_frame(
                Frame::IpTunnelClose {
                    tunnel_id: self.tunnel_id,
                    reason: CloseReason::Normal,
                },
                TrafficClass::Control,
            );
        }
    }
}

pub(in crate::runtime) struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    ready_carrier_instance: Arc<AtomicU64>,
    ready_remote_port: Arc<AtomicU32>,
    member: Arc<Mutex<ClientTcpCarrierMember>>,
}

#[derive(Clone)]
struct ClientTcpPathSessionSlot {
    commands: ReliablePathCommandSender,
    terminal: Arc<AtomicBool>,
    path_id: PathId,
}

struct ClientTcpCarrierMember {
    current: Option<ClientTcpPathSessionSlot>,
    successor_establishing: bool,
    retiring_predecessor: Option<ClientTcpPathSessionSlot>,
}

struct ClientTcpSuccessorClaim {
    member: Arc<Mutex<ClientTcpCarrierMember>>,
    carrier_groups: Arc<super::group::ClientTcpCarrierGroups>,
    predecessor_path_id: PathId,
    committed: bool,
}

impl ClientTcpSuccessorClaim {
    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for ClientTcpSuccessorClaim {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut member = self.member.lock().expect("TCP carrier member lock");
        if member.current.as_ref().map(|slot| slot.path_id) == Some(self.predecessor_path_id) {
            member.successor_establishing = false;
        }
        drop(member);
        self.carrier_groups.publish_change();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpCarrierReplacement {
    pub(in crate::runtime) predecessor_path_id: PathId,
    pub(in crate::runtime) successor_path_id: PathId,
    pub(in crate::runtime) readiness_rtt: Duration,
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
            ready_remote_port: self.ready_remote_port.clone(),
            member: self.member.clone(),
        }
    }
}

impl ClientTcpPathSessionHandle {
    pub(in crate::runtime) fn new(runtime: ClientTcpPathSessionRuntime) -> Self {
        Self {
            runtime,
            ready_carrier_instance: Arc::new(AtomicU64::new(0)),
            ready_remote_port: Arc::new(AtomicU32::new(0)),
            member: Arc::new(Mutex::new(ClientTcpCarrierMember {
                current: None,
                successor_establishing: false,
                retiring_predecessor: None,
            })),
        }
    }

    pub(in crate::runtime) async fn open_stream_with_deadlines(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        lane: TrafficClass,
        initial_demand: StreamDemandHint,
        open_deadlines: ClientTcpOpenDeadlines,
        advertised_recv_max_offset: u64,
    ) -> Result<ClientTcpOpenedStream, RuntimeError> {
        let mut changes = self.runtime.carrier_groups.subscribe();
        loop {
            let (session, observed_carrier_instance) = self
                .wait_for_ready_session_slot(&mut changes, open_deadlines.setup)
                .await?;
            let commands = session.commands.clone();
            let (response_tx, response_rx) = oneshot::channel();
            let attempt_id = next_client_tcp_open_attempt_id();
            let wait_for_deadline = || async {
                tokio::time::sleep_until(open_deadlines.live).await;
                if !self.session_slot_is_current(session.path_id) {
                    tokio::time::sleep_until(open_deadlines.setup).await;
                }
            };
            let queued = tokio::select! {
                biased;
                result = commands.send_control(ReliablePathCommand::OpenStream {
                    stream_id,
                    attempt_id,
                    observed_carrier_instance,
                    target: target.clone(),
                    lane,
                    initial_demand,
                    advertised_recv_max_offset,
                    open_deadlines,
                    session_commands: commands.clone(),
                    response: response_tx,
                }) => result,
                _ = wait_for_deadline() => return Err(RuntimeError::PathOpenTimedOut),
            };
            if queued.is_err() {
                if !self.session_slot_is_current(session.path_id) {
                    continue;
                }
                return Err(RuntimeError::ReliablePathSessionClosed);
            }

            // Tokio mpsc send is cancellation-safe. Arm cleanup only after the
            // exact actor owns the command, never for an unqueued attempt.
            let mut cancellation =
                ClientTcpOpenCancellation::new(commands.clone(), stream_id, attempt_id);
            let response = tokio::select! {
                biased;
                response = response_rx => match response {
                    Ok(response) => response,
                    Err(_) if !self.session_slot_is_current(session.path_id) => {
                        return Err(RuntimeError::ReliablePathRetired);
                    }
                    Err(_) => return Err(RuntimeError::ReliablePathSessionClosed),
                },
                _ = wait_for_deadline() => return Err(RuntimeError::PathOpenTimedOut),
            };
            match response {
                ClientTcpOpenResponse::Opened(opened) => {
                    cancellation.disarm();
                    return Ok(opened);
                }
                ClientTcpOpenResponse::RejectedWithoutOpen(_)
                    if !self.session_slot_is_current(session.path_id) =>
                {
                    cancellation.disarm();
                    continue;
                }
                ClientTcpOpenResponse::RejectedWithoutOpen(err) => {
                    cancellation.disarm();
                    return Err(err);
                }
                ClientTcpOpenResponse::FailedAfterOpen(_)
                    if !self.session_slot_is_current(session.path_id) =>
                {
                    return Err(RuntimeError::ReliablePathRetired);
                }
                ClientTcpOpenResponse::FailedAfterOpen(err) => return Err(err),
            }
        }
    }

    /// Registers a product-datagram route on this path's existing TCP actor.
    pub(in crate::runtime) async fn open_datagram_attachment(
        &self,
        open_deadline: tokio::time::Instant,
        frame_queue: usize,
    ) -> Result<ClientTcpDatagramAttachment, RuntimeError> {
        let mut changes = self.runtime.carrier_groups.subscribe();
        loop {
            let (session, _) = self
                .wait_for_ready_session_slot(&mut changes, open_deadline)
                .await?;
            let commands = session.commands.clone();
            let channels = ClientTcpDatagramAttachment::channel(frame_queue);
            let attachment_id = channels.attachment_id;
            let (response_tx, response_rx) = oneshot::channel();
            let queued = tokio::select! {
                biased;
                result = commands.send_control(ReliablePathCommand::OpenDatagramAttachment {
                    attachment_id,
                    frames: channels.frames_tx,
                    failure: channels.failure_tx,
                    open_deadline,
                    response: response_tx,
                }) => result,
                _ = tokio::time::sleep_until(open_deadline) => {
                    return Err(RuntimeError::PathOpenTimedOut);
                }
            };
            if queued.is_err() {
                if !self.session_slot_is_current(session.path_id) {
                    continue;
                }
                return Err(RuntimeError::ReliablePathSessionClosed);
            }

            // The close uses the same FIFO control queue as the open. If this
            // future is cancelled after admission, the actor still retires the
            // exact attachment after processing its open command.
            let mut cancellation =
                ClientTcpDatagramOpenCancellation::new(commands.clone(), attachment_id);
            let response = tokio::select! {
                biased;
                response = response_rx => response,
                _ = tokio::time::sleep_until(open_deadline) => {
                    return Err(RuntimeError::PathOpenTimedOut);
                }
            };
            let opened = match response {
                Ok(Ok(opened)) => opened,
                Ok(Err(_)) | Err(_) if !self.session_slot_is_current(session.path_id) => {
                    cancellation.disarm();
                    continue;
                }
                Ok(Err(error)) => {
                    cancellation.disarm();
                    return Err(error);
                }
                Err(_) => return Err(RuntimeError::ReliablePathSessionClosed),
            };
            let attachment = ClientTcpDatagramAttachment::new(
                attachment_id,
                opened.path_instance_id,
                opened.path_snapshot,
                commands,
                channels.frames_rx,
                channels.failure_rx,
            );
            cancellation.disarm();
            return Ok(attachment);
        }
    }

    pub(in crate::runtime) async fn prepare_ip_tunnel_attachment(
        &self,
        tunnel_id: IpTunnelId,
        open_deadline: tokio::time::Instant,
    ) -> Result<ClientTcpIpTunnelAttachment, RuntimeError> {
        let mut changes = self.runtime.carrier_groups.subscribe();
        loop {
            let (session, observed_instance) = self
                .wait_for_ready_session_slot(&mut changes, open_deadline)
                .await?;
            let commands = session.commands.clone();
            let path_instance_id = CarrierPathInstanceId::from_raw(observed_instance);
            if self.connection_instance_id() != Some(path_instance_id) {
                continue;
            }
            return Ok(ClientTcpIpTunnelAttachment {
                handle: self.clone(),
                commands,
                tunnel_id,
                path_instance_id,
                opened: AtomicBool::new(false),
            });
        }
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
        self.prepare_connection_for_endpoint_generation_on_port(
            open_deadline,
            policy.generation,
            None,
        )
        .await
    }

    pub(in crate::runtime) async fn prepare_connection_for_endpoint_generation_on_port(
        &self,
        open_deadline: tokio::time::Instant,
        endpoint_generation: u64,
        remote_port: Option<u16>,
    ) -> Result<Option<Duration>, RuntimeError> {
        if !self.runtime.endpoint_policy.allows(endpoint_generation) {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let session =
            self.ensure_session_slot_for_endpoint_generation(endpoint_generation, remote_port)?;
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

    /// Establishes and atomically publishes one planned carrier-member
    /// successor before ordering retirement of its predecessor.
    pub(in crate::runtime) async fn replace_connection_for_endpoint_generation(
        &self,
        open_deadline: tokio::time::Instant,
        endpoint_generation: u64,
        remote_port: u16,
    ) -> Result<ClientTcpCarrierReplacement, RuntimeError> {
        if !self.runtime.endpoint_policy.allows(endpoint_generation) {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let expected_instance = self
            .connection_instance_id()
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        if !self
            .runtime
            .state
            .tcp_path_is_product_quiescent_for_instance(self.runtime.path_index, expected_instance)
        {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let (predecessor, mut successor_claim) = {
            let mut member = self.member.lock().expect("TCP carrier member lock");
            clear_terminal_tcp_predecessor(&mut member);
            let current = member
                .current
                .as_ref()
                .filter(|slot| !slot.terminal.load(Ordering::Acquire))
                .cloned()
                .ok_or(RuntimeError::NoSchedulableTcpPath)?;
            if member.successor_establishing || member.retiring_predecessor.is_some() {
                return Err(RuntimeError::NoSchedulableTcpPath);
            }
            member.successor_establishing = true;
            let claim = ClientTcpSuccessorClaim {
                member: self.member.clone(),
                carrier_groups: self.runtime.carrier_groups.clone(),
                predecessor_path_id: current.path_id,
                committed: false,
            };
            (current, claim)
        };

        let Some(reservation) = self
            .runtime
            .carrier_groups
            .reserve(self.runtime.config_index)
        else {
            return Err(RuntimeError::NoSchedulableTcpPath);
        };
        let runtime = self
            .runtime
            .for_carrier(reservation.path_id(), Some(remote_port));
        let mut connection =
            match connect_client_tcp_path(&runtime, open_deadline, endpoint_generation).await {
                Ok(connection) => connection,
                Err(error) => return Err(error),
            };
        let readiness_rtt = connection.carrier.readiness_rtt;
        let (commands, receivers) = reliable_path_command_channels(runtime.command_queue);
        let terminal = Arc::new(AtomicBool::new(false));
        let successor = ClientTcpPathSessionSlot {
            commands,
            terminal: terminal.clone(),
            path_id: reservation.path_id(),
        };

        let promoted = self
            .runtime
            .endpoint_policy
            .with_current(endpoint_generation, || {
                let mut member = self.member.lock().expect("TCP carrier member lock");
                if !member.successor_establishing
                    || member.current.as_ref().map(|slot| slot.path_id) != Some(predecessor.path_id)
                {
                    return false;
                }
                if !publish_client_tcp_replacement_connection_committed(
                    &runtime,
                    &mut connection,
                    expected_instance,
                    Some(readiness_rtt),
                    |path_instance_id, selected_port| {
                        let current = self.ready_carrier_instance.load(Ordering::Acquire);
                        assert!(
                            current == 0 || current == expected_instance.as_u64(),
                            "carrier member cannot publish two concurrent successors"
                        );
                        self.ready_carrier_instance
                            .store(path_instance_id.as_u64(), Ordering::Release);
                        self.ready_remote_port
                            .store(u32::from(selected_port), Ordering::Release);
                        member.retiring_predecessor = Some(predecessor.clone());
                        member.current = Some(successor.clone());
                        member.successor_establishing = false;
                        self.runtime.carrier_groups.publish_change();
                    },
                ) {
                    return false;
                }
                true
            })
            .unwrap_or(false);
        if !promoted {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        successor_claim.commit();

        tokio::spawn(run_client_tcp_path_session_with_connection(
            runtime,
            receivers,
            self.ready_carrier_instance.clone(),
            self.ready_remote_port.clone(),
            terminal,
            reservation,
            connection,
        ));
        predecessor.commands.begin_path_drain();
        Ok(ClientTcpCarrierReplacement {
            predecessor_path_id: predecessor.path_id,
            successor_path_id: successor.path_id,
            readiness_rtt,
        })
    }

    pub(in crate::runtime) fn connection_instance_id(&self) -> Option<CarrierPathInstanceId> {
        match self.ready_carrier_instance.load(Ordering::Acquire) {
            0 => None,
            instance_id => Some(CarrierPathInstanceId::from_raw(instance_id)),
        }
    }

    pub(in crate::runtime) async fn wait_for_connection_instance_change(
        &self,
        previous: CarrierPathInstanceId,
    ) {
        let mut changes = self.runtime.carrier_groups.subscribe();
        while self.connection_instance_id() == Some(previous) && changes.changed().await.is_ok() {}
    }

    #[cfg(test)]
    pub(in crate::runtime) fn is_connection_ready(&self) -> bool {
        self.connection_instance_id().is_some()
    }

    pub(in crate::runtime) fn connection_remote_port(&self) -> Option<u16> {
        u16::try_from(self.ready_remote_port.load(Ordering::Acquire))
            .ok()
            .filter(|port| *port != 0)
    }

    pub(in crate::runtime) fn begin_path_drain(&self) {
        self.ready_carrier_instance.store(0, Ordering::Release);
        self.ready_remote_port.store(0, Ordering::Release);
        let mut member = self.member.lock().expect("TCP carrier member lock");
        clear_terminal_tcp_predecessor(&mut member);
        if let Some(session) = member.current.as_ref()
            && !session.terminal.load(Ordering::Acquire)
        {
            session.commands.begin_path_drain();
        }
        if let Some(session) = member.retiring_predecessor.as_ref()
            && !session.terminal.load(Ordering::Acquire)
        {
            session.commands.begin_path_drain();
        }
    }

    /// Terminates only the currently published failed physical instance.
    /// Stale failure reports cannot affect a replacement occupying this
    /// durable carrier member later.
    pub(in crate::runtime) fn terminate_failed_instance(
        &self,
        path_instance_id: CarrierPathInstanceId,
    ) -> bool {
        if self
            .ready_carrier_instance
            .compare_exchange(
                path_instance_id.as_u64(),
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        self.ready_remote_port.store(0, Ordering::Release);
        let mut member = self.member.lock().expect("TCP carrier member lock");
        clear_terminal_tcp_predecessor(&mut member);
        let Some(current) = member
            .current
            .as_ref()
            .filter(|session| !session.terminal.load(Ordering::Acquire))
        else {
            return false;
        };
        current.commands.terminate_failed_path();
        drop(member);
        self.runtime.carrier_groups.publish_change();
        true
    }

    pub(in crate::runtime) fn can_plan_replacement(&self) -> bool {
        let mut member = self.member.lock().expect("TCP carrier member lock");
        clear_terminal_tcp_predecessor(&mut member);
        !member.successor_establishing
            && member.retiring_predecessor.is_none()
            && member
                .current
                .as_ref()
                .is_some_and(|slot| !slot.terminal.load(Ordering::Acquire))
    }

    pub(in crate::runtime) fn can_establish(&self) -> bool {
        let member = self.member.lock().expect("TCP carrier member lock");
        member
            .current
            .as_ref()
            .is_none_or(|slot| slot.terminal.load(Ordering::Acquire))
            && !member.successor_establishing
    }

    pub(in crate::runtime) fn is_product_quiescent(&self) -> bool {
        self.connection_instance_id()
            .is_some_and(|path_instance_id| {
                self.runtime
                    .state
                    .tcp_path_is_product_quiescent_for_instance(
                        self.runtime.path_index,
                        path_instance_id,
                    )
            })
    }

    /// Starts a no-spare planned replacement only at the exact Product
    /// ownership boundary. The path-state transition fences later admission
    /// before the actor receives the ordered drain request.
    pub(in crate::runtime) fn begin_connection_replacement_if_product_quiescent(
        &self,
        endpoint_generation: u64,
    ) -> Result<bool, RuntimeError> {
        self.runtime
            .endpoint_policy
            .with_current(endpoint_generation, || {
                let mut member = self.member.lock().expect("TCP carrier member lock");
                clear_terminal_tcp_predecessor(&mut member);
                let Some(current) = member
                    .current
                    .as_ref()
                    .filter(|slot| !slot.terminal.load(Ordering::Acquire))
                    .cloned()
                else {
                    return false;
                };
                if member.successor_establishing || member.retiring_predecessor.is_some() {
                    return false;
                }
                let Some(path_instance_id) = self.connection_instance_id() else {
                    return false;
                };
                if !self
                    .runtime
                    .state
                    .begin_tcp_replacement_if_product_quiescent(
                        self.runtime.path_index,
                        path_instance_id,
                    )
                {
                    return false;
                }
                self.ready_carrier_instance.store(0, Ordering::Release);
                self.ready_remote_port.store(0, Ordering::Release);
                self.runtime.carrier_groups.publish_change();
                current.commands.begin_path_drain();
                true
            })
            .ok_or(RuntimeError::NoSchedulableTcpPath)
    }

    async fn wait_for_ready_session_slot(
        &self,
        changes: &mut tokio::sync::watch::Receiver<()>,
        deadline: tokio::time::Instant,
    ) -> Result<(ClientTcpPathSessionSlot, u64), RuntimeError> {
        loop {
            if !self.runtime.endpoint_policy.snapshot().enabled {
                return Err(RuntimeError::NoSchedulableTcpPath);
            }
            if let Some(slot) = self.current_ready_session_slot() {
                return Ok(slot);
            }
            tokio::select! {
                biased;
                changed = changes.changed() => {
                    changed.map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(RuntimeError::PathOpenTimedOut);
                }
            }
        }
    }

    fn current_ready_session_slot(&self) -> Option<(ClientTcpPathSessionSlot, u64)> {
        let member = self.member.lock().expect("TCP carrier member lock");
        let current = member
            .current
            .as_ref()
            .filter(|slot| !slot.terminal.load(Ordering::Acquire))?
            .clone();
        let instance = self.ready_carrier_instance.load(Ordering::Acquire);
        (instance != 0).then_some((current, instance))
    }

    fn session_slot_is_current(&self, path_id: PathId) -> bool {
        self.member
            .lock()
            .expect("TCP carrier member lock")
            .current
            .as_ref()
            .is_some_and(|slot| slot.path_id == path_id && !slot.terminal.load(Ordering::Acquire))
    }

    fn ensure_session_slot_for_endpoint_generation(
        &self,
        endpoint_generation: u64,
        remote_port: Option<u16>,
    ) -> Result<ClientTcpPathSessionSlot, RuntimeError> {
        self.runtime
            .endpoint_policy
            .with_current(endpoint_generation, || {
                let mut member = self.member.lock().expect("TCP carrier member lock");
                if let Some(session) = member.current.as_ref()
                    && !session.terminal.load(Ordering::Acquire)
                {
                    return Ok(session.clone());
                }
                if member.successor_establishing {
                    return Err(RuntimeError::NoSchedulableTcpPath);
                }

                let (commands, receivers) =
                    reliable_path_command_channels(self.runtime.command_queue);
                let terminal = Arc::new(AtomicBool::new(false));
                let reservation = self
                    .runtime
                    .carrier_groups
                    .reserve(self.runtime.config_index)
                    .ok_or(RuntimeError::NoSchedulableTcpPath)?;
                let path_id = reservation.path_id();
                let runtime = self.runtime.for_carrier(path_id, remote_port);
                tokio::spawn(run_client_tcp_path_session(
                    runtime,
                    receivers,
                    self.ready_carrier_instance.clone(),
                    self.ready_remote_port.clone(),
                    terminal.clone(),
                    reservation,
                ));
                let session = ClientTcpPathSessionSlot {
                    commands,
                    terminal,
                    path_id,
                };
                member.current = Some(session.clone());
                Ok(session)
            })
            .ok_or(RuntimeError::NoSchedulableTcpPath)?
    }
}

fn clear_terminal_tcp_predecessor(member: &mut ClientTcpCarrierMember) {
    if member
        .retiring_predecessor
        .as_ref()
        .is_some_and(|slot| slot.terminal.load(Ordering::Acquire))
    {
        member.retiring_predecessor = None;
    }
}

pub(in crate::runtime) fn tcp_session_command_queue(resources: ResourceLimits) -> usize {
    reliable_path_command_queue(resources.into())
}
