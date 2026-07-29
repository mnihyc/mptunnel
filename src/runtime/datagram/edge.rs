//! Bounded UDP ingress association shared by SOCKS5 and packet-device ingress.
//!
//! Product routing and balancer selection occur once when a lane opens. The
//! actor then resolves the concrete MPP or native connector branch once, before
//! entering its payload loop.

use super::DatagramClientAssociation;
use super::association::DatagramClientReceive;
use crate::mux::MuxLimits;
use crate::outbound::Socks5UdpAssociation;
use crate::protocol::TargetAddr;
use crate::runtime::error::RuntimeError;
use crate::runtime::gateway::GatewayFlowLease;
use crate::runtime::outbound_registry::{OpenedProductFlow, OpenedUdpOutbound};
use crate::runtime::product_policy::ClientOutboundPlan;
use crate::runtime::telemetry::ProductFlowCounter;
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

const NATIVE_UDP_RECV_BUFFER_BYTES: usize = u16::MAX as usize;

pub(in crate::runtime) struct UdpEdgeRequest<M> {
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) payload: Bytes,
    pub(in crate::runtime) ttl_ms: u32,
    pub(in crate::runtime) metadata: M,
}

pub(in crate::runtime) enum UdpEdgeCompletion<M> {
    Sent {
        lane_id: usize,
        target: TargetAddr,
        metadata: M,
        result: Result<(), RuntimeError>,
    },
    Received {
        target: TargetAddr,
        metadata: M,
        payload: Bytes,
    },
}

pub(in crate::runtime) struct UdpEdgeLane<M> {
    lane_id: usize,
    metadata: M,
    pending: usize,
    requests: mpsc::Sender<UdpEdgeRequest<M>>,
    cancel: tokio::sync::watch::Sender<bool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl<M> Drop for UdpEdgeLane<M> {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pub(in crate::runtime) fn udp_edge_queue_slots(mux_limits: MuxLimits) -> usize {
    let payload = mux_limits.max_payload_bytes.max(1);
    (mux_limits.max_datagram_queue_bytes / payload).max(1)
}

pub(in crate::runtime) fn udp_edge_completion_queue(mux_limits: MuxLimits) -> usize {
    udp_edge_queue_slots(mux_limits)
}

fn spawn_udp_edge_lane<M>(
    lane_id: usize,
    metadata: M,
    plan: ClientOutboundPlan,
    mux_limits: MuxLimits,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) -> UdpEdgeLane<M>
where
    M: Clone + Eq + Send + Sync + 'static,
{
    let (requests, rx) = mpsc::channel(udp_edge_queue_slots(mux_limits));
    let (cancel, cancelled) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(run_udp_edge_lane(
        lane_id,
        metadata.clone(),
        plan,
        mux_limits,
        rx,
        completions,
        cancelled,
    ));
    UdpEdgeLane {
        lane_id,
        metadata,
        pending: 0,
        requests,
        cancel,
        handle: Some(handle),
    }
}

async fn run_udp_edge_lane<M>(
    lane_id: usize,
    local_metadata: M,
    plan: ClientOutboundPlan,
    mux_limits: MuxLimits,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
    mut cancelled: tokio::sync::watch::Receiver<bool>,
) where
    M: Clone + Eq + Send + Sync + 'static,
{
    let initial = tokio::select! {
        request = requests.recv() => request,
        result = cancelled.changed() => {
            if result.is_err() || *cancelled.borrow() {
                None
            } else {
                requests.recv().await
            }
        }
    };
    let Some(initial) = initial else {
        return;
    };
    debug_assert!(initial.metadata == local_metadata);
    let opened = match plan.open_udp(&initial.target).await {
        Ok(opened) => opened,
        Err(error) => {
            let _ = send_udp_edge_completion(
                &completions,
                &mut cancelled,
                UdpEdgeCompletion::Sent {
                    lane_id,
                    target: initial.target,
                    metadata: initial.metadata,
                    result: Err(error),
                },
            )
            .await;
            return;
        }
    };
    match opened {
        OpenedUdpOutbound::Mpp {
            context,
            target,
            traffic_class,
            gateway_lease,
            product_flow,
        } => {
            run_mpp_udp_edge_lane(
                lane_id,
                local_metadata,
                context,
                target,
                traffic_class,
                requests,
                completions,
                cancelled,
                gateway_lease,
                product_flow,
                initial,
            )
            .await;
        }
        OpenedUdpOutbound::Local {
            socket,
            _gateway_lease,
            _product_flow,
        } => match socket {
            crate::outbound::OutboundUdpSocket::Direct(socket) => {
                run_native_udp_edge_lane(
                    lane_id,
                    local_metadata,
                    socket,
                    mux_limits,
                    requests,
                    completions,
                    cancelled,
                    initial,
                    _gateway_lease,
                    _product_flow,
                )
                .await;
            }
            crate::outbound::OutboundUdpSocket::Socks5(socket) => {
                run_native_udp_edge_lane(
                    lane_id,
                    local_metadata,
                    socket,
                    mux_limits,
                    requests,
                    completions,
                    cancelled,
                    initial,
                    _gateway_lease,
                    _product_flow,
                )
                .await;
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_mpp_udp_edge_lane<M>(
    lane_id: usize,
    local_metadata: M,
    context: crate::runtime::path::ClientPathContext,
    routed_target: TargetAddr,
    routed_traffic_class: TrafficClass,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
    mut cancelled: tokio::sync::watch::Receiver<bool>,
    mut gateway_lease: Option<GatewayFlowLease>,
    product_flow: OpenedProductFlow,
    initial: UdpEdgeRequest<M>,
) where
    M: Clone + Eq + Send + Sync + 'static,
{
    let _product_flow = product_flow;
    let reported_target = initial.target.clone();
    let mut association = match DatagramClientAssociation::new(context).await {
        Ok(association) => association,
        Err(error) => {
            if let Some(lease) = gateway_lease.as_mut()
                && let Err(feedback) = lease.failed(error.to_string())
            {
                crate::observability::process_event!(
                    Warn,
                    "udp_balancer",
                    "open_feedback_failed",
                    "MPP balancer UDP open-failure feedback failed: {feedback}"
                );
            }
            let _ = send_udp_edge_completion(
                &completions,
                &mut cancelled,
                UdpEdgeCompletion::Sent {
                    lane_id,
                    target: initial.target,
                    metadata: initial.metadata,
                    result: Err(error),
                },
            )
            .await;
            return;
        }
    };

    if !send_mpp_request(
        &mut association,
        MppUdpSendContext {
            lane_id,
            local_metadata: &local_metadata,
            routed_target: &routed_target,
            routed_traffic_class,
            gateway_lease: &mut gateway_lease,
            completions: &completions,
            cancelled: &mut cancelled,
        },
        initial,
    )
    .await
    {
        return;
    }

    loop {
        let retry_deadline = association.next_retry_deadline();
        let has_retry = retry_deadline.is_some();
        let retry_deadline = retry_deadline.unwrap_or_else(tokio::time::Instant::now);
        tokio::select! {
            result = cancelled.changed() => {
                if result.is_err() || *cancelled.borrow() {
                    break;
                }
            }
            incoming = association.next_carrier_frame(), if association.can_receive() => {
                let event = match incoming {
                    Ok(event) => event,
                    Err(error) => {
                        crate::observability::process_event!(
                            Warn,
                            "udp_edge",
                            "carrier_receive_failed",
                            "UDP carrier receive failed: {error}"
                        );
                        continue;
                    }
                };
                match association.handle_carrier_frame(event).await {
                    Ok(DatagramClientReceive::Deliver { target, payload, receipt }) => {
                        debug_assert_eq!(target, routed_target);
                        if !send_udp_edge_completion(
                            &completions,
                            &mut cancelled,
                            UdpEdgeCompletion::Received {
                                target: reported_target.clone(),
                                metadata: local_metadata.clone(),
                                payload,
                            },
                        ).await {
                            break;
                        }
                        if let Err(error) = association.acknowledge_received(receipt).await {
                            crate::observability::process_event!(
                                Warn,
                                "udp_edge",
                                "response_feedback_failed",
                                "UDP response feedback failed: {error}"
                            );
                        }
                    }
                    Ok(DatagramClientReceive::Duplicate(receipt)) => {
                        if let Err(error) = association.acknowledge_received(receipt).await {
                            crate::observability::process_event!(
                                Warn,
                                "udp_edge",
                                "duplicate_feedback_failed",
                                "duplicate UDP response feedback failed: {error}"
                            );
                        }
                    }
                    Ok(DatagramClientReceive::Control) => {}
                    Err(error) => crate::observability::process_event!(
                        Warn,
                        "udp_edge",
                        "carrier_frame_failed",
                        "UDP carrier frame failed: {error}"
                    ),
                }
            }
            _ = tokio::time::sleep_until(retry_deadline), if has_retry => {
                if let Err(error) = association.retry_due_datagram().await {
                    crate::observability::process_event!(
                        Warn,
                        "udp_edge",
                        "reinjection_failed",
                        "UDP datagram reinjection failed: {error}"
                    );
                }
            }
            request = requests.recv() => {
                let Some(request) = request else {
                    break;
                };
                if !send_mpp_request(
                    &mut association,
                    MppUdpSendContext {
                        lane_id,
                        local_metadata: &local_metadata,
                        routed_target: &routed_target,
                        routed_traffic_class,
                        gateway_lease: &mut gateway_lease,
                        completions: &completions,
                        cancelled: &mut cancelled,
                    },
                    request,
                ).await {
                    break;
                }
            }
        }
    }
    let close_error = match association.close().await {
        Ok(()) => None,
        Err(error) => {
            crate::observability::process_event!(
                Warn,
                "udp_edge",
                "association_close_failed",
                "UDP edge association close failed: {error}"
            );
            Some(error.to_string())
        }
    };
    if let Some(lease) = gateway_lease.as_mut()
        && let Err(error) = lease.completed(close_error)
    {
        crate::observability::process_event!(
            Warn,
            "udp_balancer",
            "outcome_feedback_failed",
            "balancer UDP flow-outcome feedback failed: {error}"
        );
    }
}

struct MppUdpSendContext<'a, M> {
    lane_id: usize,
    local_metadata: &'a M,
    routed_target: &'a TargetAddr,
    routed_traffic_class: TrafficClass,
    gateway_lease: &'a mut Option<GatewayFlowLease>,
    completions: &'a mpsc::Sender<UdpEdgeCompletion<M>>,
    cancelled: &'a mut tokio::sync::watch::Receiver<bool>,
}

async fn send_mpp_request<M>(
    association: &mut DatagramClientAssociation,
    context: MppUdpSendContext<'_, M>,
    request: UdpEdgeRequest<M>,
) -> bool
where
    M: Clone + Eq + Send + Sync + 'static,
{
    let UdpEdgeRequest {
        target,
        payload,
        ttl_ms,
        metadata,
        ..
    } = request;
    debug_assert!(metadata == *context.local_metadata);
    let result = association
        .send_to_fresh_datagram_with_policy(
            context.routed_target.clone(),
            payload,
            ttl_ms,
            None,
            context.routed_traffic_class,
        )
        .await;
    if let Some(lease) = context.gateway_lease.as_mut()
        && lease.is_pending()
    {
        let feedback = if result.is_ok() {
            lease.opened()
        } else {
            lease.failed(
                result
                    .as_ref()
                    .expect_err("failed MPP UDP send has an error")
                    .to_string(),
            )
        };
        if let Err(error) = feedback {
            crate::observability::process_event!(
                Warn,
                "udp_balancer",
                "outcome_feedback_failed",
                "MPP balancer UDP outcome feedback failed: {error}"
            );
        }
    } else if let Some(lease) = context.gateway_lease.as_mut()
        && let Err(error) = result.as_ref()
        && let Err(feedback) = lease.completed(Some(error.to_string()))
    {
        crate::observability::process_event!(
            Warn,
            "udp_balancer",
            "flow_failure_feedback_failed",
            "MPP balancer UDP flow-failure feedback failed: {feedback}"
        );
    }
    send_udp_edge_completion(
        context.completions,
        context.cancelled,
        UdpEdgeCompletion::Sent {
            lane_id: context.lane_id,
            target,
            metadata,
            result,
        },
    )
    .await
}

trait NativeUdpIo {
    async fn send_payload(&mut self, payload: &[u8]) -> Result<usize, RuntimeError>;
    async fn recv_payload(&mut self, buffer: &mut [u8]) -> Result<usize, RuntimeError>;
}

impl NativeUdpIo for UdpSocket {
    async fn send_payload(&mut self, payload: &[u8]) -> Result<usize, RuntimeError> {
        Ok(self.send(payload).await?)
    }

    async fn recv_payload(&mut self, buffer: &mut [u8]) -> Result<usize, RuntimeError> {
        Ok(self.recv(buffer).await?)
    }
}

impl NativeUdpIo for Socks5UdpAssociation {
    async fn send_payload(&mut self, payload: &[u8]) -> Result<usize, RuntimeError> {
        Ok(self.send(payload).await?)
    }

    async fn recv_payload(&mut self, buffer: &mut [u8]) -> Result<usize, RuntimeError> {
        Ok(self.recv(buffer).await?)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_native_udp_edge_lane<M, S>(
    lane_id: usize,
    local_metadata: M,
    mut socket: S,
    mux_limits: MuxLimits,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
    mut cancelled: tokio::sync::watch::Receiver<bool>,
    initial: UdpEdgeRequest<M>,
    mut gateway_lease: Option<GatewayFlowLease>,
    mut product_flow: OpenedProductFlow,
) where
    M: Clone + Eq + Send + Sync + 'static,
    S: NativeUdpIo,
{
    let counter = product_flow
        .runtime_counter()
        .expect("client native UDP flow has one runtime observer");
    let mut runtime_failed = false;
    let target = initial.target.clone();
    if !send_native_request(
        lane_id,
        &target,
        &local_metadata,
        &mut socket,
        &completions,
        &mut cancelled,
        initial,
        &mut gateway_lease,
        &counter,
    )
    .await
    {
        complete_udp_gateway_flow(&mut gateway_lease, None);
        product_flow.complete_runtime();
        return;
    }
    let buffer_len = mux_limits
        .max_payload_bytes
        .clamp(1, NATIVE_UDP_RECV_BUFFER_BYTES);
    let mut buffer = vec![0; buffer_len];
    loop {
        tokio::select! {
            result = cancelled.changed() => {
                if result.is_err() || *cancelled.borrow() {
                    break;
                }
            }
            request = requests.recv() => {
                let Some(request) = request else {
                    break;
                };
                if !send_native_request(
                    lane_id,
                    &target,
                    &local_metadata,
                    &mut socket,
                    &completions,
                    &mut cancelled,
                    request,
                    &mut gateway_lease,
                    &counter,
                ).await {
                    break;
                }
            }
            received = socket.recv_payload(&mut buffer) => {
                match received {
                    Ok(len) => {
                        if !send_udp_edge_completion(
                            &completions,
                            &mut cancelled,
                            UdpEdgeCompletion::Received {
                                target: target.clone(),
                                metadata: local_metadata.clone(),
                                payload: Bytes::copy_from_slice(&buffer[..len]),
                            },
                        ).await {
                            break;
                        }
                        counter.record_datagram_from_peer(len as u64);
                    }
                    Err(error) => {
                        crate::observability::process_event!(
                            Warn,
                            "udp_outbound",
                            "receive_failed",
                            "native UDP outbound receive failed: {error}"
                        );
                        complete_udp_gateway_flow(
                            &mut gateway_lease,
                            Some(error.to_string()),
                        );
                        runtime_failed = true;
                        break;
                    }
                }
            }
        }
    }
    complete_udp_gateway_flow(&mut gateway_lease, None);
    if !runtime_failed {
        product_flow.complete_runtime();
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the datagram actor passes borrowed lane state explicitly to avoid per-packet allocation"
)]
async fn send_native_request<M, S>(
    lane_id: usize,
    lane_target: &TargetAddr,
    local_metadata: &M,
    socket: &mut S,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    cancelled: &mut tokio::sync::watch::Receiver<bool>,
    request: UdpEdgeRequest<M>,
    gateway_lease: &mut Option<GatewayFlowLease>,
    counter: &ProductFlowCounter,
) -> bool
where
    M: Clone + Eq + Send + Sync + 'static,
    S: NativeUdpIo,
{
    let UdpEdgeRequest {
        target,
        payload,
        metadata,
        ..
    } = request;
    debug_assert!(target == *lane_target);
    debug_assert!(metadata == *local_metadata);
    let result = socket.send_payload(&payload).await;
    if let Ok(bytes) = result {
        counter.record_datagram_to_peer(bytes as u64);
    }
    let result = result.map(|_| ());
    if let Err(error) = result.as_ref() {
        complete_udp_gateway_flow(gateway_lease, Some(error.to_string()));
    }
    send_udp_edge_completion(
        completions,
        cancelled,
        UdpEdgeCompletion::Sent {
            lane_id,
            target,
            metadata,
            result,
        },
    )
    .await
}

fn complete_udp_gateway_flow(gateway_lease: &mut Option<GatewayFlowLease>, error: Option<String>) {
    if let Some(lease) = gateway_lease.as_mut()
        && let Err(feedback) = lease.completed(error)
    {
        crate::observability::process_event!(
            Warn,
            "udp_balancer",
            "native_outcome_feedback_failed",
            "native balancer UDP flow-outcome feedback failed: {feedback}"
        );
    }
}

async fn send_udp_edge_completion<M>(
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    cancelled: &mut tokio::sync::watch::Receiver<bool>,
    completion: UdpEdgeCompletion<M>,
) -> bool {
    tokio::select! {
        result = completions.send(completion) => result.is_ok(),
        result = cancelled.changed() => result.is_ok() && !*cancelled.borrow(),
    }
}

pub(in crate::runtime) fn dispatch_udp_edge_request<M>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    next_lane_id: &mut usize,
    plan: &ClientOutboundPlan,
    mux_limits: MuxLimits,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    request: UdpEdgeRequest<M>,
) -> Result<(), UdpEdgeRequest<M>>
where
    M: Clone + Eq + Send + Sync + 'static,
{
    let queue_slots = udp_edge_queue_slots(mux_limits);
    let total_pending = lanes.iter().map(|lane| lane.pending).sum::<usize>();
    if total_pending >= queue_slots {
        return Err(request);
    }
    let mut position = lanes
        .iter()
        .position(|lane| lane.metadata == request.metadata);
    if position.is_none() {
        if lanes.len() >= queue_slots.min(mux_limits.max_streams) {
            return Err(request);
        }
        let lane_id = *next_lane_id;
        *next_lane_id = next_lane_id.saturating_add(1);
        lanes.push(spawn_udp_edge_lane(
            lane_id,
            request.metadata.clone(),
            plan.clone(),
            mux_limits,
            completions.clone(),
        ));
        position = Some(lanes.len() - 1);
    }

    let position = position.expect("UDP edge association exists");
    match lanes[position].requests.try_send(request) {
        Ok(()) => {
            lanes[position].pending = lanes[position].pending.saturating_add(1);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(request)) => Err(request),
        Err(mpsc::error::TrySendError::Closed(request)) => {
            lanes.swap_remove(position);
            Err(request)
        }
    }
}

pub(in crate::runtime) fn finish_udp_edge_completion<M>(
    lanes: &mut [UdpEdgeLane<M>],
    completion: &UdpEdgeCompletion<M>,
) {
    let UdpEdgeCompletion::Sent { lane_id, .. } = completion else {
        return;
    };
    if let Some(lane) = lanes.iter_mut().find(|lane| lane.lane_id == *lane_id) {
        lane.pending = lane.pending.saturating_sub(1);
    }
}

/// Cancel and remove exactly one association lane.
///
/// Callers that can recycle an external association key must include their
/// own generation in `metadata`; completions already queued by the removed
/// lane can otherwise be mistaken for the replacement association.
pub(in crate::runtime) fn remove_udp_edge_lane<M>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    metadata: &M,
) -> bool
where
    M: Eq,
{
    let Some(position) = lanes.iter().position(|lane| lane.metadata == *metadata) else {
        return false;
    };
    lanes.swap_remove(position);
    true
}

pub(in crate::runtime) async fn close_udp_edge_lanes<M>(mut lanes: Vec<UdpEdgeLane<M>>) {
    for lane in &lanes {
        let _ = lane.cancel.send(true);
    }
    let handles = lanes
        .iter_mut()
        .filter_map(|lane| lane.handle.take())
        .collect::<Vec<_>>();
    drop(lanes);
    for handle in handles {
        if let Err(error) = handle.await {
            crate::observability::process_event!(
                Warn,
                "udp_edge",
                "association_task_failed",
                "UDP edge association task failed: {error}"
            );
        }
    }
}
