//! QUIC request-stream adapter for the transport-neutral IP packet service.
//!
//! Lifecycle frames remain reliable on the request stream. Complete IP
//! packets are handed to QUIC's native datagram mapping by the transport layer.

use super::client::{ClientUdpCarrierInstance, ClientUdpPathSessionRuntime};
use super::io::{
    UdpIpPacketSender, UdpPathRecvStream, UdpPathSendStream, udp_path_finish_stream,
    udp_path_input_finished, udp_path_read_frame, udp_path_write_frame,
};
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::protocol::{CloseReason, Frame, IpPacketId, IpTunnelId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::{ServerCarrierPathRegistration, ServerPathContext};
use crate::runtime::tun_l3::{
    IpPacketQueueBudget, IpPacketQueuePermit, IpTunnelPacketSendOutcome, ServerIpTunnelCarrier,
    ServerIpTunnelOpenRequest,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot, watch};

pub(in crate::runtime) struct ClientUdpIpTunnelAttachment {
    packets: mpsc::UnboundedSender<QueuedUdpIpPacket>,
    close: mpsc::UnboundedSender<()>,
    retired: watch::Receiver<bool>,
    tunnel_id: IpTunnelId,
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
        budget: IpPacketQueuePermit,
    ) -> Result<(), RuntimeError> {
        self.packets
            .send(QueuedUdpIpPacket {
                tunnel_id: self.tunnel_id,
                packet_id,
                payload,
                _budget: budget,
            })
            .map_err(|_| RuntimeError::ReliablePathRetired)
    }

    pub(in crate::runtime) async fn wait_retired(&self) {
        let mut retired = self.retired.clone();
        while !*retired.borrow_and_update() && retired.changed().await.is_ok() {}
    }
}

impl Drop for ClientUdpIpTunnelAttachment {
    fn drop(&mut self) {
        let _ = self.close.send(());
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
    close: mpsc::UnboundedReceiver<()>,
    packet_sender: UdpIpPacketSender,
    packets: mpsc::UnboundedReceiver<QueuedUdpIpPacket>,
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
        Frame::SessionClose { reason } => {
            let reason = runtime.state.session_lifecycle().retire(*reason);
            return Err(RuntimeError::RemoteClosed(reason));
        }
        _ => {
            return Err(RuntimeError::Protocol(
                "unexpected QUIC IP tunnel open response",
            ));
        }
    }
    recv.enable_ip_packets()?;
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
    let packet_sender = send.ip_packet_sender(runtime.codec_limits).await?;
    let (packets, packets_rx) = mpsc::unbounded_channel();
    let (close, close_rx) = mpsc::unbounded_channel();
    let (retired, retired_rx) = watch::channel(false);
    let (start, started) = oneshot::channel();
    tokio::spawn(run_client_udp_ip_tunnel(
        send,
        recv,
        ClientUdpIpTunnelTask {
            runtime,
            path_instance_id: carrier.path_instance_id,
            tunnel_id,
            close: close_rx,
            packet_sender,
            packets: packets_rx,
            started,
            _lifetime: ClientUdpIpTunnelLifetime(retired),
        },
    ));
    Ok(ClientUdpIpTunnelOpenOutcome::Attached {
        attachment: ClientUdpIpTunnelAttachment {
            packets,
            close,
            retired: retired_rx,
            tunnel_id,
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
        mut close,
        packet_sender,
        packets,
        started,
        _lifetime,
    } = task;
    if started.await.is_err() {
        return;
    }
    let mut packet_writer = tokio::task::JoinSet::new();
    packet_writer.spawn(run_udp_ip_packet_sender(packet_sender, packets));
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
                        Ok(Frame::SessionClose { reason }) => {
                            let reason = runtime.state.session_lifecycle().retire(reason);
                            return Err(RuntimeError::RemoteClosed(reason));
                        }
                        Err(error) if udp_path_input_finished(&error) => return Ok(()),
                        Err(error) => return Err(error),
                        Ok(_) => return Err(RuntimeError::Protocol(
                            "unexpected client QUIC IP tunnel frame",
                        )),
                    }
                }
                _ = close.recv() => {
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
                writer = packet_writer.join_next() => {
                    return match writer {
                        Some(Ok(result)) => result,
                        Some(Err(error)) => Err(RuntimeError::TaskJoin(error)),
                        None => Err(RuntimeError::Protocol(
                            "QUIC IP packet sender stopped",
                        )),
                    };
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

#[derive(Debug)]
struct QueuedUdpIpPacket {
    tunnel_id: IpTunnelId,
    packet_id: IpPacketId,
    payload: Bytes,
    _budget: IpPacketQueuePermit,
}

async fn run_udp_ip_packet_sender(
    sender: UdpIpPacketSender,
    mut packets: mpsc::UnboundedReceiver<QueuedUdpIpPacket>,
) -> Result<(), RuntimeError> {
    while let Some(packet) = packets.recv().await {
        sender
            .send(&Frame::IpPacket {
                tunnel_id: packet.tunnel_id,
                packet_id: packet.packet_id,
                payload: packet.payload,
            })
            .await?;
    }
    Ok(())
}

#[derive(Debug)]
struct ServerUdpIpTunnelClose {
    tunnel_id: IpTunnelId,
    reason: CloseReason,
}

#[derive(Debug)]
struct ServerUdpIpTunnelCarrier {
    packets: mpsc::UnboundedSender<QueuedUdpIpPacket>,
    close: mpsc::UnboundedSender<ServerUdpIpTunnelClose>,
    ready: AtomicBool,
}

impl ServerIpTunnelCarrier for ServerUdpIpTunnelCarrier {
    fn try_send_packet(
        &self,
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        payload: Bytes,
        budget: &IpPacketQueueBudget,
    ) -> Result<IpTunnelPacketSendOutcome, RuntimeError> {
        if !self.ready.load(Ordering::Acquire) {
            return Ok(IpTunnelPacketSendOutcome::Full);
        }
        let permit = match budget.try_reserve(payload.len()) {
            Ok(permit) => permit,
            Err(RuntimeError::SenderServiceBlocked) => {
                return Ok(IpTunnelPacketSendOutcome::Full);
            }
            Err(error) => return Err(error),
        };
        match self.packets.send(QueuedUdpIpPacket {
            tunnel_id,
            packet_id,
            payload,
            _budget: permit,
        }) {
            Ok(()) => Ok(IpTunnelPacketSendOutcome::Accepted),
            Err(_) => Ok(IpTunnelPacketSendOutcome::Retired),
        }
    }

    fn close(&self, tunnel_id: IpTunnelId, reason: CloseReason) {
        let _ = self
            .close
            .send(ServerUdpIpTunnelClose { tunnel_id, reason });
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
    let packet_sender = send.ip_packet_sender(context.codec_limits).await?;
    let (packets, packets_rx) = mpsc::unbounded_channel();
    let (close, mut close_rx) = mpsc::unbounded_channel();
    let carrier = Arc::new(ServerUdpIpTunnelCarrier {
        packets,
        close,
        ready: AtomicBool::new(false),
    });
    let accepted = match service.open(ServerIpTunnelOpenRequest {
        tunnel_id,
        path: &path_registration,
        carrier: carrier.clone(),
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
    recv.enable_ip_packets()?;
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
    carrier.ready.store(true, Ordering::Release);
    let mut packet_writer = tokio::task::JoinSet::new();
    packet_writer.spawn(run_udp_ip_packet_sender(packet_sender, packets_rx));
    async {
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
                    Ok(Frame::SessionClose { reason }) => {
                        context.retire_session(path_registration.session_id(), reason);
                        return Err(RuntimeError::RemoteClosed(reason));
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
            command = close_rx.recv() => {
                match command {
                    Some(ServerUdpIpTunnelClose {
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
            writer = packet_writer.join_next() => {
                return match writer {
                    Some(Ok(result)) => result,
                    Some(Err(error)) => Err(RuntimeError::TaskJoin(error)),
                    None => Err(RuntimeError::Protocol(
                        "QUIC IP packet sender stopped",
                    )),
                };
            }
            }
        }
    }
    .await
}
