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
    IpPacketQueuePermit, IpTunnelPacketSendOutcome, ServerIpTunnelCarrier,
    ServerIpTunnelOpenRequest,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot, watch};

pub(in crate::runtime) struct ClientUdpIpTunnelAttachment {
    packets: mpsc::UnboundedSender<QueuedUdpIpPacket>,
    close: mpsc::UnboundedSender<()>,
    retired: watch::Receiver<bool>,
    tunnel_id: IpTunnelId,
    path_instance_id: CarrierPathInstanceId,
    native_rate_authority: Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>,
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

    pub(in crate::runtime) fn native_rate_authority(
        &self,
    ) -> Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle> {
        self.native_rate_authority.clone()
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
                _native_pending: None,
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
    let native_rate_authority =
        carrier
            .connection
            .native_rate_authority()
            .ok_or(RuntimeError::Protocol(
                "client QUIC IP tunnel missing native rate authority",
            ))?;
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
            native_rate_authority,
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
        super::io::warn_unexpected_udp_operation_error(
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
    _native_pending: Option<ServerUdpPendingPacket>,
}

#[derive(Debug)]
struct ServerUdpPendingPacket {
    pending_bytes: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for ServerUdpPendingPacket {
    fn drop(&mut self) {
        self.pending_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

fn try_reserve_server_udp_pending(
    pending_bytes: &Arc<AtomicUsize>,
    bytes: usize,
    limit_bytes: u64,
) -> Option<ServerUdpPendingPacket> {
    let limit_bytes = usize::try_from(limit_bytes).unwrap_or(usize::MAX);
    let mut current = pending_bytes.load(Ordering::Acquire);
    loop {
        let next = current.checked_add(bytes)?;
        if next > limit_bytes {
            return None;
        }
        match pending_bytes.compare_exchange_weak(
            current,
            next,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                return Some(ServerUdpPendingPacket {
                    pending_bytes: pending_bytes.clone(),
                    bytes,
                });
            }
            Err(observed) => current = observed,
        }
    }
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
    pending_bytes: Arc<AtomicUsize>,
    native_rate_authority: Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>,
}

impl ServerIpTunnelCarrier for ServerUdpIpTunnelCarrier {
    fn try_send_packet(
        &self,
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        payload: Bytes,
        budget_permit: Option<IpPacketQueuePermit>,
        native_retention_limit_bytes: Option<u64>,
    ) -> Result<IpTunnelPacketSendOutcome, RuntimeError> {
        if !self.ready.load(Ordering::Acquire) {
            return Ok(IpTunnelPacketSendOutcome::Full);
        }
        let Some(permit) = budget_permit else {
            return Ok(IpTunnelPacketSendOutcome::Full);
        };
        let native_pending = match native_retention_limit_bytes {
            Some(limit) => {
                let Some(pending) =
                    try_reserve_server_udp_pending(&self.pending_bytes, payload.len(), limit)
                else {
                    return Ok(IpTunnelPacketSendOutcome::Full);
                };
                Some(pending)
            }
            None => None,
        };
        match self.packets.send(QueuedUdpIpPacket {
            tunnel_id,
            packet_id,
            payload,
            _budget: permit,
            _native_pending: native_pending,
        }) {
            Ok(()) => Ok(IpTunnelPacketSendOutcome::Accepted),
            Err(_) => Ok(IpTunnelPacketSendOutcome::Retired),
        }
    }

    fn native_rate_authority(
        &self,
    ) -> Option<Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>> {
        Some(self.native_rate_authority.clone())
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
    native_rate_authority: Arc<crate::runtime::path::authority::NativeCarrierRateAuthorityHandle>,
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
        pending_bytes: Arc::new(AtomicUsize::new(0)),
        native_rate_authority,
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
