#[cfg(test)]
use super::*;
use crate::config::MppPerformanceConfig;
use crate::ingress::tun::TunL4Config;
use crate::outbound;
use crate::protocol::TargetAddr;
use crate::runtime::datagram::{
    UdpEdgeCompletion, UdpEdgeLane, UdpEdgeRequest, close_udp_edge_lanes,
    dispatch_udp_edge_request, finish_udp_edge_completion, udp_edge_completion_queue,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::ingress_runtime::DEFAULT_SOCKS5_UDP_TTL_MS;
use crate::runtime::packet_device::PacketDevice;
use crate::runtime::path::ClientPathContext;
use crate::runtime::relay::control::relay_migrating_tcp_stream;
use crate::runtime::relay::open::{ReliableRelayOpenSpec, open_remote_stream};
use crate::scheduler::TrafficClass;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use netstack_smoltcp::{StackBuilder, TcpListener as TunTcpListener, UdpSocket as TunUdpSocket};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tun_rs::async_framed::{BytesCodec, DeviceFramed};

const TUN_UDP_FLOW_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) async fn run_tun_l4_client(
    tun: TunL4Config,
    context: ClientPathContext,
    performance: MppPerformanceConfig,
    device: PacketDevice,
) -> Result<(), RuntimeError> {
    let framed = DeviceFramed::new(device.into_inner(), BytesCodec::new());
    let (mut tun_sink, mut tun_stream) = framed.split();

    let (stack, runner, udp_socket, tcp_listener) = StackBuilder::default()
        .enable_tcp(true)
        .enable_udp(true)
        .enable_icmp(tun.enable_icmp)
        .mtu(usize::from(tun.mtu))
        .build()?;
    let runner = runner.ok_or(RuntimeError::Protocol("TUN stack runner is unavailable"))?;
    let udp_socket = udp_socket.ok_or(RuntimeError::Protocol("TUN UDP socket is unavailable"))?;
    let tcp_listener =
        tcp_listener.ok_or(RuntimeError::Protocol("TUN TCP listener is unavailable"))?;
    let (mut stack_sink, mut stack_stream) = stack.split();

    let stack_to_tun = async move {
        while let Some(packet) = stack_stream.next().await {
            let packet = packet?;
            tun_sink.send(BytesMut::from(packet.as_slice())).await?;
        }
        Ok::<(), RuntimeError>(())
    };
    let tun_to_stack = async move {
        while let Some(packet) = tun_stream.next().await {
            let packet = packet?;
            stack_sink.send(packet.to_vec()).await?;
        }
        Ok::<(), RuntimeError>(())
    };
    let stack_runner = async move { runner.await.map_err(RuntimeError::Io) };

    tokio::try_join!(
        stack_runner,
        stack_to_tun,
        tun_to_stack,
        run_tun_tcp_listener(tcp_listener, context.clone(), performance),
        run_tun_udp_socket(udp_socket, context, tun)
    )?;
    Ok(())
}

pub(super) async fn run_tun_tcp_listener(
    mut listener: TunTcpListener,
    context: ClientPathContext,
    performance: MppPerformanceConfig,
) -> Result<(), RuntimeError> {
    let mut flows = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.next() => {
                let Some((stream, local, remote)) = accepted else {
                    return Ok(());
                };
                let context = context.clone();
                flows.spawn(async move {
                    if let Err(err) =
                        handle_tun_tcp_stream(stream, local, remote, context, performance).await
                    {
                        eprintln!("warning: TUN TCP flow {local} -> {remote} failed: {err}");
                    }
                });
            }
            Some(result) = flows.join_next(), if !flows.is_empty() => {
                if let Err(err) = result {
                    eprintln!("warning: TUN TCP flow task failed: {err}");
                }
            }
        }
    }
}

pub(super) async fn handle_tun_tcp_stream<S>(
    stream: S,
    _local: SocketAddr,
    remote: SocketAddr,
    context: ClientPathContext,
    performance: MppPerformanceConfig,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let target = TargetAddr::Ip(remote);
    outbound::validate_target(&target)?;
    let remote = open_remote_stream(&context, target.clone(), TrafficClass::Latency).await?;
    relay_migrating_tcp_stream(
        stream,
        &context,
        performance,
        ReliableRelayOpenSpec { target },
        remote,
    )
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TunUdpFlowKey {
    local: SocketAddr,
    remote: SocketAddr,
}

pub(super) struct TunUdpResponse {
    payload: Bytes,
    source: SocketAddr,
    destination: SocketAddr,
}

pub(super) async fn run_tun_udp_socket(
    udp_socket: TunUdpSocket,
    context: ClientPathContext,
    tun: TunL4Config,
) -> Result<(), RuntimeError> {
    let (mut read_half, mut write_half) = udp_socket.split();
    let mut flows: HashMap<TunUdpFlowKey, mpsc::Sender<Vec<u8>>> = HashMap::new();
    let mut flow_tasks = tokio::task::JoinSet::new();
    let flow_limit = tun_udp_flow_limit(&context);
    let flow_queue = tun_udp_flow_queue(&context);
    let response_queue = tun_udp_response_queue(&context);
    let done_queue = flow_limit.max(1);
    let (response_tx, mut response_rx) = mpsc::channel::<TunUdpResponse>(response_queue);
    let (done_tx, mut done_rx) = mpsc::channel::<TunUdpFlowKey>(done_queue);

    loop {
        tokio::select! {
            received = read_half.next() => {
                let Some((payload, local, remote)) = received else {
                    return Ok(());
                };
                let key = TunUdpFlowKey { local, remote };
                if !flows.contains_key(&key) {
                    if flows.len() >= flow_limit {
                        eprintln!("warning: TUN UDP flow limit reached; dropping datagram from {local} to {remote}");
                        continue;
                    }
                    let (tx, rx) = mpsc::channel(flow_queue);
                    let flow_context = context.clone();
                    let flow_tun = tun.clone();
                    let flow_responses = response_tx.clone();
                    let flow_done = done_tx.clone();
                    flow_tasks.spawn(async move {
                        let result =
                            handle_tun_udp_flow(key, flow_context, flow_tun, rx, flow_responses)
                                .await;
                        let _ = flow_done.send(key).await;
                        if let Err(err) = result {
                            eprintln!(
                                "warning: TUN UDP flow {} -> {} failed: {err}",
                                key.local, key.remote
                            );
                        }
                    });
                    flows.insert(key, tx);
                }
                let send_result = flows
                    .get(&key)
                    .ok_or(RuntimeError::Protocol("missing TUN UDP flow"))?
                    .try_send(payload);
                match send_result {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        eprintln!("warning: TUN UDP flow queue full; dropping datagram from {local} to {remote}");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        flows.remove(&key);
                    }
                }
            }
            response = response_rx.recv() => {
                let Some(response) = response else {
                    return Ok(());
                };
                write_half
                    .send((
                        response.payload.to_vec(),
                        response.source,
                        response.destination,
                    ))
                    .await?;
            }
            done = done_rx.recv() => {
                if let Some(key) = done {
                    flows.remove(&key);
                }
            }
            Some(result) = flow_tasks.join_next(), if !flow_tasks.is_empty() => {
                if let Err(err) = result {
                    eprintln!("warning: TUN UDP flow task failed: {err}");
                }
            }
        }
    }
}

pub(super) async fn handle_tun_udp_flow(
    key: TunUdpFlowKey,
    context: ClientPathContext,
    tun: TunL4Config,
    mut datagrams: mpsc::Receiver<Vec<u8>>,
    responses: mpsc::Sender<TunUdpResponse>,
) -> Result<(), RuntimeError> {
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<TunUdpFlowKey>>(udp_edge_completion_queue(&context));
    let mut lanes = Vec::<UdpEdgeLane<TunUdpFlowKey>>::new();
    let mut next_lane_id = 0usize;
    let target = TargetAddr::Ip(tun_udp_target_for_remote(key.remote, &tun));
    let ttl_ms = tun_udp_ttl_ms(key.remote, &tun);
    let result = loop {
        tokio::select! {
            payload = tokio::time::timeout(TUN_UDP_FLOW_IDLE_TIMEOUT, datagrams.recv()) => {
                let payload = match payload {
                    Ok(Some(payload)) => payload,
                    Ok(None) | Err(_) => break Ok(()),
                };
                if dispatch_udp_edge_request(
                    &mut lanes,
                    &mut next_lane_id,
                    &context,
                    &completion_tx,
                    UdpEdgeRequest {
                        target: target.clone(),
                        payload: Bytes::from(payload),
                        ttl_ms,
                        metadata: key,
                        route_hint: None,
                    },
                )
                .is_err()
                {
                    eprintln!("warning: TUN UDP lane queue full; dropping datagram from {} to {}", key.local, key.remote);
                }
            }
            completion = completion_rx.recv() => {
                let Some(completion) = completion else {
                    break Err(RuntimeError::Protocol("TUN UDP completion channel closed"));
                };
                finish_udp_edge_completion(&mut lanes, &completion);
                match completion.result {
                    Ok(response) => {
                        responses
                            .send(TunUdpResponse {
                                payload: response,
                                source: completion.metadata.remote,
                                destination: completion.metadata.local,
                            })
                            .await
                            .map_err(|_| RuntimeError::Protocol("TUN UDP response channel closed"))?;
                    }
                    Err(err) => {
                        eprintln!(
                            "warning: TUN UDP datagram {} -> {} failed: {err}",
                            completion.metadata.local, completion.metadata.remote
                        );
                    }
                }
            }
            else => break Ok(()),
        }
    };
    drop(completion_tx);
    close_udp_edge_lanes(lanes).await;
    result
}

pub(super) fn tun_udp_target_for_remote(remote: SocketAddr, tun: &TunL4Config) -> SocketAddr {
    if remote.port() != 53 || tun.dns_resolvers.is_empty() {
        return remote;
    }
    tun.dns_resolvers
        .iter()
        .copied()
        .find(|resolver| resolver.ip().is_ipv4() == remote.ip().is_ipv4())
        .unwrap_or(tun.dns_resolvers[0])
}

pub(super) fn tun_udp_ttl_ms(remote: SocketAddr, tun: &TunL4Config) -> u32 {
    if remote.port() == 53 {
        tun.dns_ttl_ms
    } else {
        DEFAULT_SOCKS5_UDP_TTL_MS
    }
}

pub(super) fn tun_udp_flow_limit(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).max(1)
}

pub(super) fn tun_udp_flow_queue(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).max(1)
}

pub(super) fn tun_udp_response_queue(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).max(1)
}
