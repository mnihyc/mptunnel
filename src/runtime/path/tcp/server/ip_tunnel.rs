//! TCP adapter for the transport-neutral layer-3 packet service.

use crate::protocol::{CloseReason, Frame, IpPacketId, IpTunnelId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{ReliablePathCommand, ReliablePathCommandSender};
use crate::runtime::path::{ServerCarrierPathRegistration, ServerPathContext};
use crate::runtime::tun_l3::{
    AcceptedServerIpTunnel, IpPacketQueueBudget, IpTunnelPacketSendOutcome, ServerIpTunnelCarrier,
    ServerIpTunnelOpenRequest,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::Arc;

pub(super) struct ServerTcpIpTunnelState {
    attachment: Option<ServerTcpIpTunnelAttachment>,
}

struct ServerTcpIpTunnelAttachment {
    tunnel_id: IpTunnelId,
    accepted: AcceptedServerIpTunnel,
}

struct ServerTcpIpTunnelCarrier {
    path: ReliablePathCommandSender,
}

impl std::fmt::Debug for ServerTcpIpTunnelCarrier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerTcpIpTunnelCarrier")
            .finish_non_exhaustive()
    }
}

impl ServerIpTunnelCarrier for ServerTcpIpTunnelCarrier {
    fn try_send_packet(
        &self,
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        payload: Bytes,
        _budget: &IpPacketQueueBudget,
    ) -> Result<IpTunnelPacketSendOutcome, RuntimeError> {
        match self.path.try_enqueue_admitted_frame(
            Frame::IpPacket {
                tunnel_id,
                packet_id,
                payload,
            },
            TrafficClass::RealtimeDatagram,
        ) {
            Ok(()) => Ok(IpTunnelPacketSendOutcome::Accepted),
            Err(RuntimeError::SenderServiceBlocked) => Ok(IpTunnelPacketSendOutcome::Full),
            Err(RuntimeError::ReliablePathRetired)
            | Err(RuntimeError::ReliablePathSessionClosed) => {
                Ok(IpTunnelPacketSendOutcome::Retired)
            }
            Err(error) => Err(error),
        }
    }

    fn close(&self, tunnel_id: IpTunnelId, reason: CloseReason) {
        let path = self.path.clone();
        tokio::spawn(async move {
            let _ = path
                .send_control(ReliablePathCommand::SendFrame(Frame::IpTunnelClose {
                    tunnel_id,
                    reason,
                }))
                .await;
        });
    }
}

impl ServerTcpIpTunnelState {
    pub(super) fn new() -> Self {
        Self { attachment: None }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.attachment.is_none()
    }

    pub(super) fn clear(&mut self) {
        self.attachment = None;
    }

    pub(super) fn open(
        &mut self,
        context: &ServerPathContext,
        path: &ServerCarrierPathRegistration,
        path_commands: &ReliablePathCommandSender,
        tunnel_id: IpTunnelId,
    ) -> Result<Frame, RuntimeError> {
        let Some(service) = context.ip_tunnels.as_ref() else {
            return Ok(Frame::IpTunnelClose {
                tunnel_id,
                reason: CloseReason::PolicyRejected,
            });
        };
        let accepted = match service.open(ServerIpTunnelOpenRequest {
            tunnel_id,
            path,
            carrier: Arc::new(ServerTcpIpTunnelCarrier {
                path: path_commands.clone(),
            }),
        }) {
            Ok(accepted) => accepted,
            Err(_) => {
                return Ok(Frame::IpTunnelClose {
                    tunnel_id,
                    reason: CloseReason::PolicyRejected,
                });
            }
        };
        let addresses = accepted.allocation().assigned_addresses().collect();
        self.attachment = Some(ServerTcpIpTunnelAttachment {
            tunnel_id,
            accepted,
        });
        Ok(Frame::IpTunnelReady {
            tunnel_id,
            mtu: service.plan().mtu(),
            addresses,
        })
    }

    pub(super) fn receive(
        &self,
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        payload: Bytes,
    ) -> Result<(), RuntimeError> {
        let Some(attachment) = self
            .attachment
            .as_ref()
            .filter(|attachment| attachment.tunnel_id == tunnel_id)
        else {
            return Err(RuntimeError::Protocol(
                "TCP IP packet preceded its tunnel attachment",
            ));
        };
        let _ = attachment.accepted.receive(packet_id, payload)?;
        Ok(())
    }

    pub(super) fn close(&mut self, tunnel_id: IpTunnelId) {
        if self
            .attachment
            .as_ref()
            .is_some_and(|attachment| attachment.tunnel_id == tunnel_id)
        {
            self.attachment = None;
        }
    }
}
