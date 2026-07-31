//! Datagram attachments multiplexed by the shared client TCP carrier.
//!
//! The path actor owns wire ordering and flow routes. Product associations keep
//! datagram identity, feedback, retry, and cross-path policy outside this layer.

use crate::model::path::CarrierPathInstanceId;
use crate::protocol::{DatagramFlowId, Frame, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{ReliablePathCommand, ReliablePathCommandSender};
use crate::runtime::recent_ids::RecentIdCache;
use crate::scheduler::PathSnapshot;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};

static NEXT_CLIENT_TCP_DATAGRAM_ATTACHMENT_ID: AtomicU64 = AtomicU64::new(1);

pub(in crate::runtime) struct ClientTcpDatagramInbound {
    pub(in crate::runtime) frame: Frame,
    pub(in crate::runtime) received_at: tokio::time::Instant,
}

pub(super) struct ClientTcpDatagramAttachmentChannels {
    pub(super) attachment_id: u64,
    pub(super) frames_tx: mpsc::Sender<Result<ClientTcpDatagramInbound, RuntimeError>>,
    pub(super) frames_rx: mpsc::Receiver<Result<ClientTcpDatagramInbound, RuntimeError>>,
    pub(super) failure_tx: oneshot::Sender<()>,
    pub(super) failure_rx: oneshot::Receiver<()>,
}

pub(super) struct ClientTcpDatagramOpenCancellation {
    commands: ReliablePathCommandSender,
    attachment_id: u64,
    armed: bool,
}

impl ClientTcpDatagramOpenCancellation {
    pub(super) fn new(commands: ReliablePathCommandSender, attachment_id: u64) -> Self {
        Self {
            commands,
            attachment_id,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ClientTcpDatagramOpenCancellation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let commands = self.commands.clone();
        let attachment_id = self.attachment_id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = commands
                    .send_control(ReliablePathCommand::CloseDatagramAttachment {
                        attachment_id,
                        response: None,
                    })
                    .await;
            });
        }
    }
}

fn next_client_tcp_datagram_attachment_id() -> u64 {
    let mut id = NEXT_CLIENT_TCP_DATAGRAM_ATTACHMENT_ID.fetch_add(1, Ordering::Relaxed);
    if id == 0 {
        id = NEXT_CLIENT_TCP_DATAGRAM_ATTACHMENT_ID.fetch_add(1, Ordering::Relaxed);
    }
    id
}

pub(in crate::runtime) struct ClientTcpDatagramAttachment {
    attachment_id: u64,
    path_instance_id: CarrierPathInstanceId,
    path_snapshot: PathSnapshot,
    commands: ReliablePathCommandSender,
    frames: mpsc::Receiver<Result<ClientTcpDatagramInbound, RuntimeError>>,
    failure: oneshot::Receiver<()>,
    retired: bool,
}

impl ClientTcpDatagramAttachment {
    pub(super) fn channel(frame_queue: usize) -> ClientTcpDatagramAttachmentChannels {
        let attachment_id = next_client_tcp_datagram_attachment_id();
        let (frames_tx, frames_rx) = mpsc::channel(frame_queue.max(1));
        let (failure_tx, failure_rx) = oneshot::channel();
        ClientTcpDatagramAttachmentChannels {
            attachment_id,
            frames_tx,
            frames_rx,
            failure_tx,
            failure_rx,
        }
    }

    pub(super) fn new(
        attachment_id: u64,
        path_instance_id: CarrierPathInstanceId,
        path_snapshot: PathSnapshot,
        commands: ReliablePathCommandSender,
        frames: mpsc::Receiver<Result<ClientTcpDatagramInbound, RuntimeError>>,
        failure: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            attachment_id,
            path_instance_id,
            path_snapshot,
            commands,
            frames,
            failure,
            retired: false,
        }
    }

    pub(in crate::runtime) fn id(&self) -> u64 {
        self.attachment_id
    }

    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.path_instance_id
    }

    pub(in crate::runtime) fn path_snapshot(&self) -> PathSnapshot {
        self.path_snapshot
    }

    pub(in crate::runtime) async fn open_flow(
        &self,
        flow_id: DatagramFlowId,
        target: TargetAddr,
        open_deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        let (response_tx, response_rx) = oneshot::channel();
        tokio::select! {
            biased;
            result = self.commands.send_control(ReliablePathCommand::OpenDatagramFlow {
                attachment_id: self.attachment_id,
                flow_id,
                target,
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

    pub(in crate::runtime) async fn send_frame(
        &self,
        frame: Frame,
        write_deadline: tokio::time::Instant,
        expires_at: Option<tokio::time::Instant>,
    ) -> Result<(), RuntimeError> {
        let (response_tx, response_rx) = oneshot::channel();
        tokio::select! {
            biased;
            result = self.commands.send_datagram_frame(
                self.attachment_id,
                frame,
                write_deadline,
                expires_at,
                response_tx,
            ) => result.map_err(|_| RuntimeError::ReliablePathSessionClosed)?,
            _ = tokio::time::sleep_until(write_deadline) => {
                return Err(RuntimeError::PathOpenTimedOut);
            }
        }
        tokio::select! {
            biased;
            response = response_rx => {
                response.map_err(|_| RuntimeError::ReliablePathSessionClosed)?
            }
            _ = tokio::time::sleep_until(write_deadline) => {
                Err(RuntimeError::PathOpenTimedOut)
            }
        }
    }

    pub(in crate::runtime) async fn next_frame(
        &mut self,
    ) -> Result<ClientTcpDatagramInbound, RuntimeError> {
        tokio::select! {
            biased;
            frame = self.frames.recv() => {
                frame.ok_or(RuntimeError::ReliablePathSessionClosed)?
            }
            _ = &mut self.failure => Err(RuntimeError::ReliablePathSessionClosed),
        }
    }

    pub(in crate::runtime) async fn close(
        &mut self,
        close_deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        if self.retired {
            return Ok(());
        }
        let (response_tx, response_rx) = oneshot::channel();
        tokio::select! {
            biased;
            result = self.commands.send_control(ReliablePathCommand::CloseDatagramAttachment {
                attachment_id: self.attachment_id,
                response: Some(response_tx),
            }) => result.map_err(|_| RuntimeError::ReliablePathSessionClosed)?,
            _ = tokio::time::sleep_until(close_deadline) => {
                return Err(RuntimeError::PathOpenTimedOut);
            }
        }
        let result = tokio::select! {
            biased;
            response = response_rx => {
                response.map_err(|_| RuntimeError::ReliablePathSessionClosed)?
            }
            _ = tokio::time::sleep_until(close_deadline) => {
                Err(RuntimeError::PathOpenTimedOut)
            }
        };
        if result.is_ok() {
            self.retired = true;
        }
        result
    }
}

impl Drop for ClientTcpDatagramAttachment {
    fn drop(&mut self) {
        if !self.retired {
            let _ = self.commands.retire_datagram_attachment(self.attachment_id);
        }
    }
}

struct ClientTcpDatagramAttachmentRoute {
    frames: mpsc::Sender<Result<ClientTcpDatagramInbound, RuntimeError>>,
    failure: Option<oneshot::Sender<()>>,
}

struct ClientTcpDatagramFlowRoute {
    attachment_id: u64,
    target: TargetAddr,
}

pub(super) struct ClientTcpDatagramState {
    attachments: HashMap<u64, ClientTcpDatagramAttachmentRoute>,
    flows: HashMap<DatagramFlowId, ClientTcpDatagramFlowRoute>,
    closed_flows: RecentIdCache<DatagramFlowId>,
    flow_limit: usize,
}

impl ClientTcpDatagramState {
    pub(super) fn new(flow_limit: usize, closed_flow_limit: usize) -> Self {
        Self {
            attachments: HashMap::new(),
            flows: HashMap::new(),
            closed_flows: RecentIdCache::new(closed_flow_limit),
            flow_limit: flow_limit.max(1),
        }
    }

    pub(super) fn attach(
        &mut self,
        attachment_id: u64,
        frames: mpsc::Sender<Result<ClientTcpDatagramInbound, RuntimeError>>,
        failure: oneshot::Sender<()>,
    ) -> Result<(), RuntimeError> {
        if frames.is_closed() {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        if self.attachments.contains_key(&attachment_id) {
            return Err(RuntimeError::Protocol("duplicate TCP datagram attachment"));
        }
        if self.attachments.len() >= self.flow_limit {
            return Err(RuntimeError::Datagram(
                crate::mux::datagram::DatagramError::FlowLimitExceeded {
                    limit: self.flow_limit,
                },
            ));
        }
        self.attachments.insert(
            attachment_id,
            ClientTcpDatagramAttachmentRoute {
                frames,
                failure: Some(failure),
            },
        );
        Ok(())
    }

    pub(super) fn validate_open_flow(
        &self,
        attachment_id: u64,
        flow_id: DatagramFlowId,
        target: &TargetAddr,
    ) -> Result<bool, RuntimeError> {
        if !self.attachments.contains_key(&attachment_id) {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        if let Some(flow) = self.flows.get(&flow_id) {
            return if flow.attachment_id == attachment_id && flow.target == *target {
                Ok(false)
            } else {
                Err(RuntimeError::Protocol("TCP datagram flow owner changed"))
            };
        }
        if self.flows.len() >= self.flow_limit {
            return Err(RuntimeError::Datagram(
                crate::mux::datagram::DatagramError::FlowLimitExceeded {
                    limit: self.flow_limit,
                },
            ));
        }
        Ok(true)
    }

    pub(super) fn commit_open_flow(
        &mut self,
        attachment_id: u64,
        flow_id: DatagramFlowId,
        target: TargetAddr,
    ) {
        self.flows.insert(
            flow_id,
            ClientTcpDatagramFlowRoute {
                attachment_id,
                target,
            },
        );
    }

    pub(super) fn validate_outbound(
        &self,
        attachment_id: u64,
        frame: &Frame,
    ) -> Result<(), RuntimeError> {
        let flow_id = match frame {
            Frame::DatagramData { flow_id, .. } | Frame::DatagramFeedback { flow_id, .. } => {
                *flow_id
            }
            _ => {
                return Err(RuntimeError::Protocol(
                    "TCP datagram attachment received a non-datagram frame",
                ));
            }
        };
        match self.flows.get(&flow_id) {
            Some(flow) if flow.attachment_id == attachment_id => Ok(()),
            _ => Err(RuntimeError::ReliablePathSessionClosed),
        }
    }

    pub(super) fn route_inbound(&mut self, frame: Frame) -> Result<(), RuntimeError> {
        let (flow_id, terminal) = match &frame {
            Frame::DatagramData { flow_id, .. } | Frame::DatagramFeedback { flow_id, .. } => {
                (*flow_id, false)
            }
            Frame::DatagramClose { flow_id } => (*flow_id, true),
            _ => {
                return Err(RuntimeError::Protocol(
                    "TCP datagram router received a non-datagram frame",
                ));
            }
        };
        let Some(route) = self.flows.get(&flow_id) else {
            return if self.closed_flows.contains(&flow_id) {
                Ok(())
            } else {
                Err(RuntimeError::Protocol("unknown TCP datagram flow"))
            };
        };
        let attachment_id = route.attachment_id;
        let Some(attachment) = self.attachments.get(&attachment_id) else {
            return Err(RuntimeError::Protocol(
                "TCP datagram flow lost its attachment",
            ));
        };
        let inbound = ClientTcpDatagramInbound {
            frame,
            received_at: tokio::time::Instant::now(),
        };
        match attachment.frames.try_send(Ok(inbound)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Product feedback/reinjection recovers a dropped local handoff.
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.remove_attachment(attachment_id);
                return Ok(());
            }
        }
        if terminal {
            self.flows.remove(&flow_id);
            self.closed_flows.insert(flow_id);
        }
        Ok(())
    }

    pub(super) fn attachment_flow_ids(&self, attachment_id: u64) -> Vec<DatagramFlowId> {
        self.flows
            .iter()
            .filter_map(|(flow_id, flow)| (flow.attachment_id == attachment_id).then_some(*flow_id))
            .collect()
    }

    pub(super) fn flow_ids(&self) -> Vec<DatagramFlowId> {
        self.flows.keys().copied().collect()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.attachments.is_empty() && self.flows.is_empty()
    }

    pub(super) fn remove_attachment(&mut self, attachment_id: u64) {
        self.attachments.remove(&attachment_id);
        let flow_ids = self.attachment_flow_ids(attachment_id);
        for flow_id in flow_ids {
            self.flows.remove(&flow_id);
            self.closed_flows.insert(flow_id);
        }
    }

    pub(super) fn clear(&mut self) {
        self.flows.clear();
        for (_, mut attachment) in self.attachments.drain() {
            if let Some(failure) = attachment.failure.take() {
                let _ = failure.send(());
            }
        }
    }
}

#[cfg(test)]
#[path = "client_datagram_test.rs"]
mod tests;
