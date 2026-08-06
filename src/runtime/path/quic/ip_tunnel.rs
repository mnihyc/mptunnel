//! QUIC request-stream adapter for the transport-neutral IP packet service.
//!
//! Lifecycle frames remain reliable on the request stream. Complete IP
//! packets are handed to QUIC's native datagram mapping by the transport layer.

use super::client::{ClientUdpCarrierInstance, ClientUdpPathSessionRuntime};
use super::io::{
    UdpPathRecvStream, UdpPathSendStream, udp_path_finish_stream, udp_path_input_finished,
    udp_path_read_frame, udp_path_write_frame,
};
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::protocol::{CloseReason, Frame, IpPacketId, IpTunnelId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::reliable_stream_frame_queue;
use crate::runtime::path::{ServerCarrierPathRegistration, ServerPathContext};
use crate::runtime::tun_l3::{IpTunnelCarrierCommand, ServerIpTunnelOpenRequest};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};

enum ClientUdpIpTunnelCommand {
    Packet {
        packet_id: IpPacketId,
        payload: Bytes,
    },
    Close,
}

pub(in crate::runtime) struct ClientUdpIpTunnelAttachment {
    commands: mpsc::Sender<ClientUdpIpTunnelCommand>,
    retired: watch::Receiver<bool>,
    path_instance_id: CarrierPathInstanceId,
}

pub(in crate::runtime) enum ClientUdpIpTunnelOpenOutcome {
    Attached {
        attachment: ClientUdpIpTunnelAttachment,
        start: oneshot::Sender<()>,
    },
    Rejected {
        path_instance_id: CarrierPathInstanceId,
    },
}

impl ClientUdpIpTunnelAttachment {
    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.path_instance_id
    }

    pub(in crate::runtime) fn try_send(
        &self,
        packet_id: IpPacketId,
        payload: Bytes,
    ) -> Result<(), RuntimeError> {
        match self
            .commands
            .try_send(ClientUdpIpTunnelCommand::Packet { packet_id, payload })
        {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(RuntimeError::SenderServiceBlocked),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(RuntimeError::ReliablePathRetired),
        }
    }

    pub(in crate::runtime) async fn wait_retired(&self) {
        let mut retired = self.retired.clone();
        while !*retired.borrow_and_update() && retired.changed().await.is_ok() {}
    }
}

impl Drop for ClientUdpIpTunnelAttachment {
    fn drop(&mut self) {
        let _ = self.commands.try_send(ClientUdpIpTunnelCommand::Close);
    }
}

struct ClientUdpIpTunnelLifetime(watch::Sender<bool>);

impl Drop for ClientUdpIpTunnelLifetime {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

struct ClientUdpIpTunnelTask {
    runtime: ClientUdpPathSessionRuntime,
    path_instance_id: CarrierPathInstanceId,
    tunnel_id: IpTunnelId,
    commands: mpsc::Receiver<ClientUdpIpTunnelCommand>,
    started: oneshot::Receiver<()>,
    _lifetime: ClientUdpIpTunnelLifetime,
}

pub(super) async fn open_client_udp_ip_tunnel(
    carrier: ClientUdpCarrierInstance,
    runtime: ClientUdpPathSessionRuntime,
    tunnel_id: IpTunnelId,
) -> Result<ClientUdpIpTunnelOpenOutcome, RuntimeError> {
    let (mut send, mut recv) = carrier.connection.open_bi().await?;
    send.set_traffic_class(TrafficClass::RealtimeDatagram)?;
    udp_path_write_frame(
        &mut send,
        &Frame::OpenIpTunnel { tunnel_id },
        runtime.codec_limits,
    )
    .await?;
    let ready = udp_path_read_frame(&mut recv, runtime.codec_limits).await?;
    match &ready {
        Frame::IpTunnelReady {
            tunnel_id: ready_id,
            ..
        } if *ready_id == tunnel_id => {}
        Frame::IpTunnelClose {
            tunnel_id: closed_id,
            reason,
        } if *closed_id == tunnel_id && *reason != CloseReason::Normal => {
            return Ok(ClientUdpIpTunnelOpenOutcome::Rejected {
                path_instance_id: carrier.path_instance_id,
            });
        }
        _ => {
            return Err(RuntimeError::Protocol(
                "unexpected QUIC IP tunnel open response",
            ));
        }
    }
    runtime
        .ip_tunnels
        .route(crate::runtime::tun_l3::ClientIpTunnelEvent {
            path: RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: runtime.path_index,
            },
            path_instance_id: carrier.path_instance_id,
            frame: ready,
        })?;
    let queue = runtime.stream_frame_queue.max(1);
    let (commands, receiver) = mpsc::channel(queue);
    let (retired, retired_rx) = watch::channel(false);
    let (start, started) = oneshot::channel();
    tokio::spawn(run_client_udp_ip_tunnel(
        send,
        recv,
        ClientUdpIpTunnelTask {
            runtime,
            path_instance_id: carrier.path_instance_id,
            tunnel_id,
            commands: receiver,
            started,
            _lifetime: ClientUdpIpTunnelLifetime(retired),
        },
    ));
    Ok(ClientUdpIpTunnelOpenOutcome::Attached {
        attachment: ClientUdpIpTunnelAttachment {
            commands,
            retired: retired_rx,
            path_instance_id: carrier.path_instance_id,
        },
        start,
    })
}

async fn run_client_udp_ip_tunnel(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    task: ClientUdpIpTunnelTask,
) {
    let ClientUdpIpTunnelTask {
        runtime,
        path_instance_id,
        tunnel_id,
        mut commands,
        started,
        _lifetime,
    } = task;
    if started.await.is_err() {
        return;
    }
    let result = async {
        loop {
            tokio::select! {
                frame = udp_path_read_frame(&mut recv, runtime.codec_limits) => {
                    match frame {
                        Ok(frame @ Frame::IpPacket { tunnel_id: packet_tunnel, .. })
                            if packet_tunnel == tunnel_id =>
                        {
                            let result = runtime.ip_tunnels.route(
                                crate::runtime::tun_l3::ClientIpTunnelEvent {
                                    path: RelayPathKey {
                                        underlay: UnderlayProtocol::Udp,
                                        index: runtime.path_index,
                                    },
                                    path_instance_id,
                                    frame,
                                },
                            );
                            match result {
                                Ok(()) | Err(RuntimeError::SenderServiceBlocked) => {}
                                Err(error) => return Err(error),
                            }
                        }
                        Ok(frame @ Frame::IpTunnelClose { tunnel_id: closed_tunnel, .. })
                            if closed_tunnel == tunnel_id =>
                        {
                            runtime.ip_tunnels.route(
                                crate::runtime::tun_l3::ClientIpTunnelEvent {
                                    path: RelayPathKey {
                                        underlay: UnderlayProtocol::Udp,
                                        index: runtime.path_index,
                                    },
                                    path_instance_id,
                                    frame,
                                },
                            )?;
                            return Ok(());
                        }
                        Ok(Frame::Ping { nonce }) => {
                            udp_path_write_frame(
                                &mut send,
                                &Frame::Pong { nonce },
                                runtime.codec_limits,
                            ).await?;
                        }
                        Err(error) if udp_path_input_finished(&error) => return Ok(()),
                        Err(error) => return Err(error),
                        Ok(_) => return Err(RuntimeError::Protocol(
                            "unexpected client QUIC IP tunnel frame",
                        )),
                    }
                }
                command = commands.recv() => {
                    match command {
                        Some(ClientUdpIpTunnelCommand::Packet { packet_id, payload }) => {
                            udp_path_write_frame(
                                &mut send,
                                &Frame::IpPacket { tunnel_id, packet_id, payload },
                                runtime.codec_limits,
                            ).await?;
                        }
                        Some(ClientUdpIpTunnelCommand::Close) | None => {
                            let _ = udp_path_write_frame(
                                &mut send,
                                &Frame::IpTunnelClose {
                                    tunnel_id,
                                    reason: CloseReason::Normal,
                                },
                                runtime.codec_limits,
                            ).await;
                            let _ = udp_path_finish_stream(&mut send).await;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    .await;
    if let Err(error) = result {
        super::io::warn_unexpected_udp_runtime_error(
            "client QUIC IP tunnel attachment failed",
            &error,
        );
    }
}

pub(super) async fn handle_server_udp_ip_tunnel(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    context: ServerPathContext,
    path_registration: ServerCarrierPathRegistration,
    tunnel_id: IpTunnelId,
) -> Result<(), RuntimeError> {
    send.set_traffic_class(TrafficClass::RealtimeDatagram)?;
    let Some(service) = context.ip_tunnels.as_ref() else {
        udp_path_write_frame(
            &mut send,
            &Frame::IpTunnelClose {
                tunnel_id,
                reason: CloseReason::PolicyRejected,
            },
            context.codec_limits,
        )
        .await?;
        udp_path_finish_stream(&mut send).await?;
        return Ok(());
    };
    let (commands, mut receiver) = mpsc::channel(reliable_stream_frame_queue(context.mux_limits));
    let accepted = match service.open(ServerIpTunnelOpenRequest {
        tunnel_id,
        path: &path_registration,
        commands,
    }) {
        Ok(accepted) => accepted,
        Err(_) => {
            udp_path_write_frame(
                &mut send,
                &Frame::IpTunnelClose {
                    tunnel_id,
                    reason: CloseReason::PolicyRejected,
                },
                context.codec_limits,
            )
            .await?;
            udp_path_finish_stream(&mut send).await?;
            return Ok(());
        }
    };
    udp_path_write_frame(
        &mut send,
        &Frame::IpTunnelReady {
            tunnel_id,
            mtu: service.plan().mtu(),
            addresses: accepted.allocation().assigned_addresses().collect(),
        },
        context.codec_limits,
    )
    .await?;
    loop {
        tokio::select! {
            frame = udp_path_read_frame(&mut recv, context.codec_limits) => {
                match frame {
                    Ok(Frame::IpPacket {
                        tunnel_id: packet_tunnel,
                        packet_id,
                        payload,
                    }) if packet_tunnel == tunnel_id => {
                        let _ = accepted.receive(packet_id, payload)?;
                    }
                    Ok(Frame::IpTunnelClose { tunnel_id: closed_tunnel, .. })
                        if closed_tunnel == tunnel_id =>
                    {
                        return Ok(());
                    }
                    Ok(Frame::Ping { nonce }) => {
                        udp_path_write_frame(
                            &mut send,
                            &Frame::Pong { nonce },
                            context.codec_limits,
                        ).await?;
                    }
                    Err(error) if udp_path_input_finished(&error) => return Ok(()),
                    Err(error) => return Err(error),
                    Ok(_) => return Err(RuntimeError::Protocol(
                        "unexpected server QUIC IP tunnel frame",
                    )),
                }
            }
            command = receiver.recv() => {
                match command {
                    Some(IpTunnelCarrierCommand::Packet {
                        tunnel_id: packet_tunnel,
                        packet_id,
                        payload,
                    }) if packet_tunnel == tunnel_id => {
                        udp_path_write_frame(
                            &mut send,
                            &Frame::IpPacket { tunnel_id, packet_id, payload },
                            context.codec_limits,
                        ).await?;
                    }
                    Some(IpTunnelCarrierCommand::Close {
                        tunnel_id: closed_tunnel,
                        reason,
                    }) if closed_tunnel == tunnel_id => {
                        udp_path_write_frame(
                            &mut send,
                            &Frame::IpTunnelClose { tunnel_id, reason },
                            context.codec_limits,
                        ).await?;
                        udp_path_finish_stream(&mut send).await?;
                        return Ok(());
                    }
                    None => return Ok(()),
                    Some(_) => return Err(RuntimeError::Protocol(
                        "QUIC IP tunnel command identity mismatch",
                    )),
                }
            }
        }
    }
}
