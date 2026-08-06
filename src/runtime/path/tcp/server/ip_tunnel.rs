//! TCP adapter for the transport-neutral layer-3 packet service.

use crate::protocol::{CloseReason, Frame, IpPacketId, IpTunnelId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandSender, reliable_stream_frame_queue,
};
use crate::runtime::path::{ServerCarrierPathRegistration, ServerPathContext};
use crate::runtime::tun_l3::{
    AcceptedServerIpTunnel, IpTunnelCarrierCommand, ServerIpTunnelOpenRequest,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use tokio::sync::mpsc;

pub(super) struct ServerTcpIpTunnelState {
    attachment: Option<ServerTcpIpTunnelAttachment>,
}

struct ServerTcpIpTunnelAttachment {
    tunnel_id: IpTunnelId,
    accepted: AcceptedServerIpTunnel,
    forwarding: tokio::task::JoinHandle<()>,
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
        let (commands, receiver) = mpsc::channel(reliable_stream_frame_queue(context.mux_limits));
        let accepted = match service.open(ServerIpTunnelOpenRequest {
            tunnel_id,
            path,
            commands,
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
        let forwarding = tokio::spawn(forward_ip_tunnel_commands(receiver, path_commands.clone()));
        self.attachment = Some(ServerTcpIpTunnelAttachment {
            tunnel_id,
            accepted,
            forwarding,
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

impl Drop for ServerTcpIpTunnelAttachment {
    fn drop(&mut self) {
        self.forwarding.abort();
    }
}

async fn forward_ip_tunnel_commands(
    mut commands: mpsc::Receiver<IpTunnelCarrierCommand>,
    path: ReliablePathCommandSender,
) {
    while let Some(command) = commands.recv().await {
        match command {
            IpTunnelCarrierCommand::Packet {
                tunnel_id,
                packet_id,
                payload,
            } => {
                let result = path.try_enqueue_admitted_frame(
                    Frame::IpPacket {
                        tunnel_id,
                        packet_id,
                        payload,
                    },
                    TrafficClass::RealtimeDatagram,
                );
                match result {
                    Ok(()) | Err(RuntimeError::SenderServiceBlocked) => {}
                    Err(_) => return,
                }
            }
            IpTunnelCarrierCommand::Close { tunnel_id, reason } => {
                let _ = path
                    .send_control(ReliablePathCommand::SendFrame(Frame::IpTunnelClose {
                        tunnel_id,
                        reason,
                    }))
                    .await;
                return;
            }
        }
    }
}
