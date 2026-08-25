#[cfg(test)]
use super::*;
use crate::ingress::tun::TunL4Config;
use crate::platform::PacketDevice;
use crate::product::{InboundId, PrincipalId};
use crate::protocol::TargetAddr;
use crate::runtime::datagram::{
    UdpEdgeCompletion, UdpEdgeLane, UdpEdgeRequest, close_udp_edge_lanes,
    dispatch_udp_edge_request, finish_udp_edge_completion, reap_finished_udp_edge_lane_instance,
    udp_edge_completion_queue,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::ingress_runtime::{DEFAULT_SOCKS5_UDP_TTL_MS, hold_silent_route_drop};
use crate::runtime::outbound_registry::relay_opened_tcp;
use crate::runtime::product_policy::{ClientIngressRouter, ClientPolicyDisposition, ClientRoute};
use crate::runtime::readiness::RequiredServiceReadiness;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use netstack_smoltcp::{StackBuilder, TcpListener as TunTcpListener, UdpSocket as TunUdpSocket};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tun_rs::async_framed::{BytesCodec, DeviceFramed};

const TUN_UDP_FLOW_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const TUN_DNS_TCP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const TUN_DNS_TCP_MAX_QUERIES: usize = 64;
const TUN_DNS_UDP_RESPONSE_LIMIT: usize = 1_232;
const TUN_DNS_TCP_RESPONSE_LIMIT: usize = u16::MAX as usize;

pub(super) async fn run_tun_l4_client(
    tun: TunL4Config,
    mux_limits: crate::mux::MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    device: PacketDevice,
    readiness: RequiredServiceReadiness,
) -> Result<(), RuntimeError> {
    let tun = Arc::new(tun);
    let (device, mut managed) = device.into_parts();
    let framed = DeviceFramed::new(device, BytesCodec::new());
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
    let principal = PrincipalId::parse("anonymous")
        .map_err(|_| RuntimeError::Protocol("invalid TUN principal"))?;
    if let Some(managed) = managed.as_mut() {
        // Every packet-processing primitive and route dependency exists now.
        // Publication may proceed before the loops poll without losing data:
        // their bounded queues are already constructed.
        managed.signal_ready();
    }
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "inbound",
        "ready",
        format_args!(
            "{inbound}: TUN packet stack ready on {}",
            tun.interface_name
                .as_deref()
                .unwrap_or("host-selected interface")
        ),
    );
    readiness.ready();

    tokio::try_join!(
        stack_runner,
        stack_to_tun,
        tun_to_stack,
        run_tun_tcp_listener(
            tcp_listener,
            router.clone(),
            inbound.clone(),
            principal.clone(),
            tun.clone(),
            tun_tcp_flow_limit(mux_limits),
        ),
        run_tun_udp_socket(udp_socket, mux_limits, router, inbound, principal, tun)
    )?;
    Ok(())
}

pub(super) async fn run_tun_tcp_listener(
    mut listener: TunTcpListener,
    router: ClientIngressRouter,
    inbound: InboundId,
    principal: PrincipalId,
    tun: Arc<TunL4Config>,
    flow_limit: usize,
) -> Result<(), RuntimeError> {
    let flow_limit = flow_limit.max(1);
    let mut flows = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.next(), if flows.len() < flow_limit => {
                let Some((stream, local, remote)) = accepted else {
                    return Ok(());
                };
                let router = router.clone();
                let inbound = inbound.clone();
                let principal = principal.clone();
                let tun = tun.clone();
                flows.spawn(async move {
                    if let Err(err) =
                        handle_tun_tcp_stream(
                            stream, local, remote, router, inbound, principal, tun,
                        )
                        .await
                    {
                        crate::observability::process_event!(
                            Warn,
                            "tun",
                            "tcp_flow_failed",
                            "TUN TCP flow {local} -> {remote} failed: {err}"
                        );
                    }
                });
            }
            Some(result) = flows.join_next(), if !flows.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "tun",
                        "tcp_flow_task_failed",
                        "TUN TCP flow task failed: {err}"
                    );
                }
            }
        }
    }
}

pub(super) const fn tun_tcp_flow_limit(mux_limits: crate::mux::MuxLimits) -> usize {
    // The netstack owns its finite pending-connection backlog. Polling accepts
    // stops at this Core envelope so hostile SYN/accept churn cannot allocate
    // one Tokio task per peer ahead of Product routing and flow admission.
    mux_limits.max_streams
}

pub(super) async fn handle_tun_tcp_stream<S>(
    mut stream: S,
    local: SocketAddr,
    remote: SocketAddr,
    router: ClientIngressRouter,
    inbound: InboundId,
    principal: PrincipalId,
    tun: Arc<TunL4Config>,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if tun_dns_capture_target(remote, &tun) {
        return serve_tun_dns_tcp(
            stream,
            router,
            Duration::from_millis(u64::from(tun.dns_ttl_ms)),
        )
        .await;
    }
    let recovered = match router.recover_tun_target(remote) {
        Ok(target) => target,
        Err(RuntimeError::DestinationDenied(_)) => return Ok(()),
        Err(error) => return Err(error),
    };
    let route = match router.route_tun_tcp(&recovered, local, principal, inbound) {
        Ok(route) => route,
        Err(RuntimeError::DestinationDenied(_)) => return Ok(()),
        Err(err) => return Err(err),
    };
    let plan = match route {
        ClientRoute::Open(plan) => plan,
        ClientRoute::Deny(ClientPolicyDisposition::Reject) => return Ok(()),
        ClientRoute::Deny(ClientPolicyDisposition::Drop) => {
            hold_silent_route_drop(&mut stream).await;
            return Ok(());
        }
    };
    let opened = match plan.open_tcp(recovered.target()).await {
        Ok(opened) => opened,
        Err(RuntimeError::RouteRejected) => return Ok(()),
        Err(RuntimeError::RouteDropped) => {
            hold_silent_route_drop(&mut stream).await;
            return Ok(());
        }
        Err(RuntimeError::OutboundUnavailable(_)) => return Ok(()),
        Err(error) => return Err(error),
    };
    relay_opened_tcp(stream, opened).await?;
    Ok(())
}

async fn serve_tun_dns_tcp<S>(
    mut stream: S,
    router: ClientIngressRouter,
    answer_ttl: Duration,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    for _ in 0..TUN_DNS_TCP_MAX_QUERIES {
        let mut length = [0u8; 2];
        match tokio::time::timeout(TUN_DNS_TCP_IDLE_TIMEOUT, stream.read_exact(&mut length)).await {
            Err(_) => return Ok(()),
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Ok(Err(error)) => return Err(RuntimeError::Io(error)),
            Ok(Ok(_)) => {}
        }
        let length = usize::from(u16::from_be_bytes(length));
        if length == 0 {
            return Ok(());
        }
        let mut request = vec![0u8; length];
        match tokio::time::timeout(TUN_DNS_TCP_IDLE_TIMEOUT, stream.read_exact(&mut request)).await
        {
            Err(_) => return Ok(()),
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Ok(Err(error)) => return Err(RuntimeError::Io(error)),
            Ok(Ok(_)) => {}
        }
        let response = match router
            .answer_dns_wire_query(&request, answer_ttl, TUN_DNS_TCP_RESPONSE_LIMIT)
            .await
        {
            Ok(response) => response,
            Err(_) => return Ok(()),
        };
        let response_length = u16::try_from(response.len())
            .map_err(|_| RuntimeError::Protocol("captured DNS TCP response exceeds 65535 bytes"))?;
        let write = async {
            stream.write_all(&response_length.to_be_bytes()).await?;
            stream.write_all(&response).await
        };
        match tokio::time::timeout(TUN_DNS_TCP_IDLE_TIMEOUT, write).await {
            Err(_) => return Ok(()),
            Ok(Err(error)) => return Err(RuntimeError::Io(error)),
            Ok(Ok(())) => {}
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TunUdpFlowKey {
    local: SocketAddr,
    remote: SocketAddr,
}

enum TunUdpResponse {
    Flow {
        key: TunUdpFlowKey,
        generation: u64,
        payload: Bytes,
    },
    LocalDns {
        key: TunUdpFlowKey,
        payload: Bytes,
    },
}

enum TunUdpFlowState {
    Open {
        generation: u64,
        datagrams: mpsc::Sender<Vec<u8>>,
    },
    Denied {
        last_seen: tokio::time::Instant,
    },
    LocalDns {
        last_seen: tokio::time::Instant,
    },
}

struct TunUdpFlowBinding {
    target: TargetAddr,
    plan: crate::runtime::product_policy::ClientOutboundPlan,
}

struct TunUdpFlowTask {
    key: TunUdpFlowKey,
    generation: u64,
    binding: TunUdpFlowBinding,
    mux_limits: crate::mux::MuxLimits,
    ttl_ms: u32,
}

pub(super) async fn run_tun_udp_socket(
    udp_socket: TunUdpSocket,
    mux_limits: crate::mux::MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    principal: PrincipalId,
    tun: Arc<TunL4Config>,
) -> Result<(), RuntimeError> {
    let (mut read_half, mut write_half) = udp_socket.split();
    let mut flows = HashMap::<TunUdpFlowKey, TunUdpFlowState>::new();
    let mut flow_tasks = tokio::task::JoinSet::new();
    let mut dns_tasks = tokio::task::JoinSet::new();
    let flow_limit = tun_udp_flow_limit(mux_limits);
    let dns_task_limit = flow_limit.clamp(1, 64);
    let response_queue = tun_udp_response_queue(mux_limits);
    let done_queue = flow_limit.max(1);
    let (response_tx, mut response_rx) = mpsc::channel::<TunUdpResponse>(response_queue);
    let (done_tx, mut done_rx) = mpsc::channel::<(TunUdpFlowKey, u64)>(done_queue);
    let mut next_generation = 0u64;

    loop {
        tokio::select! {
            received = read_half.next() => {
                let Some((payload, local, remote)) = received else {
                    return Ok(());
                };
                let key = TunUdpFlowKey { local, remote };
                if !flows.contains_key(&key) {
                    evict_expired_inactive_tun_udp_flow(&mut flows, flow_limit);
                    if flows.len() >= flow_limit {
                        crate::observability::process_event!(
                            Warn,
                            "tun",
                            "udp_flow_limit",
                            "TUN UDP flow limit reached; dropping datagram from {local} to {remote}"
                        );
                        continue;
                    }
                    if tun_dns_capture_target(remote, &tun) {
                        flows.insert(
                            key,
                            TunUdpFlowState::LocalDns {
                                last_seen: tokio::time::Instant::now(),
                            },
                        );
                    } else {
                        let binding = match route_tun_udp_flow(
                            &router,
                            key,
                            &tun,
                            principal.clone(),
                            inbound.clone(),
                        ) {
                            Ok(Some(binding)) => binding,
                            Ok(None) => {
                                flows.insert(
                                    key,
                                    TunUdpFlowState::Denied {
                                        last_seen: tokio::time::Instant::now(),
                                    },
                                );
                                continue;
                            }
                            Err(err) => return Err(err),
                        };
                        let flow_queue = tun_udp_flow_queue(mux_limits);
                        let (tx, rx) = mpsc::channel(flow_queue);
                        let generation = next_generation;
                        next_generation = next_generation
                            .checked_add(1)
                            .ok_or(RuntimeError::Protocol("TUN UDP flow generation overflow"))?;
                        let ttl_ms = tun_udp_ttl_ms(key.remote, &tun);
                        let flow_responses = response_tx.clone();
                        let flow_done = done_tx.clone();
                        flow_tasks.spawn(async move {
                            let result = handle_tun_udp_flow(
                                TunUdpFlowTask {
                                    key,
                                    generation,
                                    binding,
                                    mux_limits,
                                    ttl_ms,
                                },
                                rx,
                                flow_responses,
                            )
                            .await;
                            let _ = flow_done.send((key, generation)).await;
                            if let Err(err) = result {
                                crate::observability::process_event!(
                                    Warn,
                                    "tun",
                                    "udp_flow_failed",
                                    "TUN UDP flow {} -> {} failed: {err}",
                                    key.local, key.remote
                                );
                            }
                        });
                        flows.insert(
                            key,
                            TunUdpFlowState::Open {
                                generation,
                                datagrams: tx,
                            },
                        );
                    }
                }
                let send_result = match flows
                    .get_mut(&key)
                    .ok_or(RuntimeError::Protocol("missing TUN UDP flow"))?
                {
                    TunUdpFlowState::Open { datagrams, .. } => datagrams.try_send(payload),
                    TunUdpFlowState::Denied { last_seen } => {
                        *last_seen = tokio::time::Instant::now();
                        continue;
                    }
                    TunUdpFlowState::LocalDns { last_seen } => {
                        *last_seen = tokio::time::Instant::now();
                        if dns_tasks.len() >= dns_task_limit {
                            continue;
                        }
                        let dns_router = router.clone();
                        let dns_responses = response_tx.clone();
                        let answer_ttl = Duration::from_millis(u64::from(tun.dns_ttl_ms));
                        dns_tasks.spawn(async move {
                            let response = dns_router
                                .answer_dns_wire_query(
                                    &payload,
                                    answer_ttl,
                                    TUN_DNS_UDP_RESPONSE_LIMIT,
                                )
                                .await;
                            if let Ok(payload) = response {
                                let _ = dns_responses
                                    .send(TunUdpResponse::LocalDns { key, payload })
                                    .await;
                            }
                        });
                        continue;
                    }
                };
                match send_result {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        crate::observability::process_event!(
                            Warn,
                            "tun",
                            "udp_flow_queue_full",
                            "TUN UDP flow queue full; dropping datagram from {local} to {remote}"
                        );
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
                let (key, payload) = match response {
                    TunUdpResponse::Flow {
                        key,
                        generation,
                        payload,
                    } => {
                        let current = matches!(
                            flows.get(&key),
                            Some(TunUdpFlowState::Open { generation: current, .. })
                                if *current == generation
                        );
                        if !current {
                            continue;
                        }
                        (key, payload)
                    }
                    TunUdpResponse::LocalDns { key, payload } => (key, payload),
                };
                write_half
                    .send((
                        payload.to_vec(),
                        key.remote,
                        key.local,
                    ))
                    .await?;
            }
            done = done_rx.recv() => {
                if let Some((key, generation)) = done
                    && matches!(
                        flows.get(&key),
                        Some(TunUdpFlowState::Open {
                            generation: current,
                            ..
                        }) if *current == generation
                    )
                {
                    flows.remove(&key);
                }
            }
            Some(result) = flow_tasks.join_next(), if !flow_tasks.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "tun",
                        "udp_flow_task_failed",
                        "TUN UDP flow task failed: {err}"
                    );
                }
            }
            Some(result) = dns_tasks.join_next(), if !dns_tasks.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "tun",
                        "dns_task_failed",
                        "TUN DNS query task failed: {err}"
                    );
                }
            }
        }
    }
}

async fn handle_tun_udp_flow(
    task: TunUdpFlowTask,
    mut datagrams: mpsc::Receiver<Vec<u8>>,
    responses: mpsc::Sender<TunUdpResponse>,
) -> Result<(), RuntimeError> {
    let TunUdpFlowTask {
        key,
        generation,
        binding: TunUdpFlowBinding { target, plan },
        mux_limits,
        ttl_ms,
    } = task;
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<TunUdpFlowKey>>(udp_edge_completion_queue(mux_limits));
    let mut lanes = Vec::<UdpEdgeLane<TunUdpFlowKey>>::new();
    let mut next_lane_id = 0usize;
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
                    &plan,
                    mux_limits,
                    &completion_tx,
                    UdpEdgeRequest {
                        target: target.clone(),
                        payload: Bytes::from(payload),
                        ttl_ms,
                        metadata: key,
                    },
                )
                .is_err()
                {
                    crate::observability::process_event!(
                        Warn,
                        "tun",
                        "udp_lane_queue_full",
                        "TUN UDP lane queue full; dropping datagram from {} to {}",
                        key.local,
                        key.remote
                    );
                }
            }
            completion = completion_rx.recv() => {
                let Some(completion) = completion else {
                    break Err(RuntimeError::Protocol("TUN UDP completion channel closed"));
                };
                finish_udp_edge_completion(&mut lanes, &completion);
                match completion {
                    UdpEdgeCompletion::Received { payload, .. } => {
                        responses
                            .send(TunUdpResponse::Flow {
                                key,
                                generation,
                                payload,
                            })
                            .await
                            .map_err(|_| RuntimeError::Protocol("TUN UDP response channel closed"))?;
                    }
                    UdpEdgeCompletion::Sent {
                        lane_id,
                        result: Err(error),
                        ..
                    } if matches!(error.as_ref(), RuntimeError::OutboundUnavailable(_)) => {
                        let _ = reap_finished_udp_edge_lane_instance(&mut lanes, lane_id);
                    }
                    UdpEdgeCompletion::Sent {
                        metadata,
                        result: Err(err),
                        ..
                    } => {
                        crate::observability::process_event!(
                            Warn,
                            "tun",
                            "udp_datagram_failed",
                            "TUN UDP datagram {} -> {} failed: {err}",
                            metadata.local, metadata.remote
                        );
                    }
                    UdpEdgeCompletion::Sent { result: Ok(()), .. } => {}
                    UdpEdgeCompletion::Discarded { .. } => {}
                }
            }
            else => break Ok(()),
        }
    };
    drop(completion_tx);
    close_udp_edge_lanes(lanes).await;
    result
}

fn route_tun_udp_flow(
    router: &ClientIngressRouter,
    key: TunUdpFlowKey,
    tun: &TunL4Config,
    principal: PrincipalId,
    inbound: InboundId,
) -> Result<Option<TunUdpFlowBinding>, RuntimeError> {
    let recovered = router.recover_tun_target(tun_udp_target_for_remote(key.remote, tun))?;
    match router.route_tun_udp(&recovered, key.local, principal, inbound) {
        Ok(ClientRoute::Open(plan)) => Ok(Some(TunUdpFlowBinding {
            target: recovered.target().clone(),
            plan,
        })),
        Ok(ClientRoute::Deny(_)) | Err(RuntimeError::DestinationDenied(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

fn evict_expired_inactive_tun_udp_flow(
    flows: &mut HashMap<TunUdpFlowKey, TunUdpFlowState>,
    flow_limit: usize,
) {
    if flows.len() < flow_limit {
        return;
    }
    let expired = flows.iter().find_map(|(key, state)| match state {
        TunUdpFlowState::Denied { last_seen } | TunUdpFlowState::LocalDns { last_seen }
            if last_seen.elapsed() >= TUN_UDP_FLOW_IDLE_TIMEOUT =>
        {
            Some(*key)
        }
        TunUdpFlowState::Open { .. }
        | TunUdpFlowState::Denied { .. }
        | TunUdpFlowState::LocalDns { .. } => None,
    });
    if let Some(key) = expired {
        flows.remove(&key);
    }
}

pub(super) fn tun_dns_capture_target(remote: SocketAddr, tun: &TunL4Config) -> bool {
    remote.port() == 53 && tun.managed_dns_capture_servers().contains(&remote.ip())
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

pub(super) fn tun_udp_flow_limit(mux_limits: crate::mux::MuxLimits) -> usize {
    let payload = mux_limits.max_payload_bytes.max(1);
    (mux_limits.max_datagram_queue_bytes / payload).max(1)
}

pub(super) fn tun_udp_flow_queue(mux_limits: crate::mux::MuxLimits) -> usize {
    let payload = mux_limits.max_payload_bytes.max(1);
    (mux_limits.max_datagram_queue_bytes / payload).max(1)
}

pub(super) fn tun_udp_response_queue(mux_limits: crate::mux::MuxLimits) -> usize {
    let payload = mux_limits.max_payload_bytes.max(1);
    (mux_limits.max_datagram_queue_bytes / payload).max(1)
}

#[cfg(test)]
#[path = "tests_tun_l4.rs"]
mod tests;
