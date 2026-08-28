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
use crate::runtime::product_lifecycle::ProductFlowActivity;
use crate::runtime::product_policy::ClientOutboundPlan;
use crate::runtime::telemetry::ProductFlowCounter;
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::future::Future;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

const NATIVE_UDP_RECV_BUFFER_BYTES: usize = u16::MAX as usize;

#[derive(Debug)]
enum IdleClosePublicationError {
    Failed(RuntimeError),
    TimedOut,
}

async fn bounded_idle_close_publication<F>(
    timeout: std::time::Duration,
    close: F,
) -> Result<(), IdleClosePublicationError>
where
    F: Future<Output = Result<(), RuntimeError>>,
{
    match tokio::time::timeout(timeout, close).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(IdleClosePublicationError::Failed(error)),
        Err(_) => Err(IdleClosePublicationError::TimedOut),
    }
}

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
        result: Result<(), Arc<RuntimeError>>,
    },
    Discarded {
        lane_id: usize,
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
    retirement: Arc<std::sync::Mutex<UdpEdgeRetirementGate>>,
    cancel: tokio::sync::watch::Sender<bool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
struct UdpEdgeRetirementGate {
    accepting: bool,
    activity: Arc<ProductFlowActivity>,
}

enum UdpEdgeIdleFence {
    Active,
    Retired,
}

fn try_send_udp_edge_request<M>(
    requests: &mpsc::Sender<UdpEdgeRequest<M>>,
    retirement: &std::sync::Mutex<UdpEdgeRetirementGate>,
    request: UdpEdgeRequest<M>,
) -> Result<(), mpsc::error::TrySendError<UdpEdgeRequest<M>>> {
    let gate = retirement
        .lock()
        .expect("UDP edge retirement gate poisoned");
    if !gate.accepting || requests.is_closed() {
        return Err(mpsc::error::TrySendError::Closed(request));
    }
    let result = requests.try_send(request);
    if result.is_ok() {
        // Admission and retirement share this gate. Therefore an accepted
        // datagram either refreshes activity before the retirement fence or
        // is rejected intact after it; actor scheduling cannot change which.
        let _ = gate.activity.record();
    }
    result
}

fn fence_udp_edge_idle<M>(
    idle_timeout: Option<std::time::Duration>,
    requests: &mut mpsc::Receiver<UdpEdgeRequest<M>>,
    retirement: &std::sync::Mutex<UdpEdgeRetirementGate>,
) -> UdpEdgeIdleFence {
    let mut gate = retirement
        .lock()
        .expect("UDP edge retirement gate poisoned");
    if !gate.activity.try_retire(idle_timeout) {
        return UdpEdgeIdleFence::Active;
    }
    gate.accepting = false;
    requests.close();
    UdpEdgeIdleFence::Retired
}

impl<M> Drop for UdpEdgeLane<M> {
    fn drop(&mut self) {
        if self.handle.take().is_some() {
            // Dropping a JoinHandle detaches rather than cancels the actor.
            // Signal its single retirement branch and leave the runtime-owned
            // actor to release Product state and settle transport close.
            let _ = self.cancel.send(true);
        }
    }
}

impl<M> UdpEdgeLane<M> {
    /// Fences new requests and transfers cleanup to the lane actor. Taking the
    /// handle prevents `Drop` from aborting the actor before it can release its
    /// Product owners and initiate transport-local close publication.
    fn begin_retirement(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        self.retirement
            .lock()
            .expect("UDP edge retirement gate poisoned")
            .accepting = false;
        let _ = self.cancel.send(true);
        self.handle.take()
    }
}

async fn observe_udp_edge_lane_retirement(handle: tokio::task::JoinHandle<()>) {
    if let Err(error) = handle.await {
        crate::observability::process_event!(
            Warn,
            "udp_edge",
            "association_task_failed",
            "UDP edge association task failed: {error}"
        );
    }
}

fn spawn_udp_edge_lane_retirement<M>(mut lane: UdpEdgeLane<M>) {
    let handle = lane.begin_retirement();
    drop(lane);
    if let Some(handle) = handle {
        tokio::spawn(observe_udp_edge_lane_retirement(handle));
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
    idle_timeout: Option<std::time::Duration>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) -> UdpEdgeLane<M>
where
    M: Clone + Eq + Send + Sync + 'static,
{
    let (requests, rx) = mpsc::channel(udp_edge_queue_slots(mux_limits));
    let activity = ProductFlowActivity::new();
    let retirement = Arc::new(std::sync::Mutex::new(UdpEdgeRetirementGate {
        accepting: true,
        activity: activity.clone(),
    }));
    let (cancel, cancelled) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(run_udp_edge_lane(
        lane_id,
        metadata.clone(),
        plan,
        mux_limits,
        idle_timeout,
        rx,
        activity,
        retirement.clone(),
        completions,
        cancelled,
    ));
    UdpEdgeLane {
        lane_id,
        metadata,
        pending: 0,
        requests,
        retirement,
        cancel,
        handle: Some(handle),
    }
}

async fn run_udp_edge_lane<M>(
    lane_id: usize,
    local_metadata: M,
    plan: ClientOutboundPlan,
    mux_limits: MuxLimits,
    idle_timeout: Option<std::time::Duration>,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    activity: Arc<ProductFlowActivity>,
    retirement: Arc<std::sync::Mutex<UdpEdgeRetirementGate>>,
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
        Err(RuntimeError::RouteRejected | RuntimeError::RouteDropped) => {
            run_silent_udp_denial_lane(
                lane_id,
                local_metadata,
                requests,
                completions,
                cancelled,
                initial,
                idle_timeout,
                activity,
                retirement,
            )
            .await;
            return;
        }
        Err(error) => {
            report_terminal_udp_edge_requests(
                lane_id,
                &mut requests,
                &completions,
                &mut cancelled,
                Some(initial),
                error,
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
                idle_timeout,
                activity,
                retirement,
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
                    idle_timeout,
                    activity,
                    retirement,
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
                    idle_timeout,
                    activity,
                    retirement,
                )
                .await;
            }
        },
    }
}

/// Retain a denied UDP association until its ingress flow expires.
///
/// UDP has no protocol-level rejection response. Completing each queued send
/// without opening an outbound preserves silent Reject/Drop behavior and
/// prevents later datagrams from repeating DNS and policy evaluation.
async fn run_silent_udp_denial_lane<M>(
    lane_id: usize,
    local_metadata: M,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
    mut cancelled: tokio::sync::watch::Receiver<bool>,
    initial: UdpEdgeRequest<M>,
    idle_timeout: Option<std::time::Duration>,
    activity: Arc<ProductFlowActivity>,
    retirement: Arc<std::sync::Mutex<UdpEdgeRetirementGate>>,
) where
    M: Eq + Send + Sync + 'static,
{
    let mut idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
    let mut current = initial;
    loop {
        if activity.is_idle(idle_timeout) {
            match fence_udp_edge_idle(idle_timeout, &mut requests, &retirement) {
                UdpEdgeIdleFence::Active => {
                    idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
                }
                UdpEdgeIdleFence::Retired => {
                    report_discarded_udp_edge_requests(
                        lane_id,
                        &mut requests,
                        &completions,
                        &mut cancelled,
                        Some(current),
                    )
                    .await;
                    return;
                }
            }
        }
        debug_assert!(current.metadata == local_metadata);
        if !send_udp_edge_completion(
            &completions,
            &mut cancelled,
            UdpEdgeCompletion::Discarded { lane_id },
        )
        .await
        {
            return;
        }
        current = match tokio::select! {
            request = requests.recv() => request,
            result = cancelled.changed() => {
                if result.is_err() || *cancelled.borrow() {
                    None
                } else {
                    requests.recv().await
                }
            }
            () = &mut idle => match fence_udp_edge_idle(
                idle_timeout,
                &mut requests,
                &retirement,
            ) {
                UdpEdgeIdleFence::Active => {
                    idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
                    continue;
                }
                UdpEdgeIdleFence::Retired => {
                    report_discarded_udp_edge_requests(
                        lane_id,
                        &mut requests,
                        &completions,
                        &mut cancelled,
                        None,
                    ).await;
                    return;
                }
            },
        } {
            Some(request) => request,
            None => return,
        };
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
    idle_timeout: Option<std::time::Duration>,
    activity: Arc<ProductFlowActivity>,
    retirement: Arc<std::sync::Mutex<UdpEdgeRetirementGate>>,
) where
    M: Clone + Eq + Send + Sync + 'static,
{
    let mut idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
    let reported_target = initial.target.clone();
    let session_retirement = context.session_retirement().wait();
    tokio::pin!(session_retirement);
    let mut association = match DatagramClientAssociation::new(context).await {
        Ok(association) => association,
        Err(error) => {
            // Once association construction is terminal, this actor must stop
            // accepting before feedback or completion reporting can block.
            requests.close();
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
            report_terminal_udp_edge_requests(
                lane_id,
                &mut requests,
                &completions,
                &mut cancelled,
                Some(initial),
                error,
            )
            .await;
            return;
        }
    };

    if activity.is_idle(idle_timeout)
        && matches!(
            fence_udp_edge_idle(idle_timeout, &mut requests, &retirement),
            UdpEdgeIdleFence::Retired
        )
    {
        report_terminal_udp_edge_requests(
            lane_id,
            &mut requests,
            &completions,
            &mut cancelled,
            Some(initial),
            RuntimeError::ProductIdleTimeout,
        )
        .await;
        retire_mpp_udp_product_lifetime(association, product_flow, &mut gateway_lease).await;
        return;
    }

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
        if *cancelled.borrow() {
            retire_mpp_udp_product_lifetime(association, product_flow, &mut gateway_lease).await;
        }
        return;
    }

    let mut terminal_session_reason = None;
    let mut product_retired = false;
    loop {
        if activity.is_idle(idle_timeout) {
            match fence_udp_edge_idle(idle_timeout, &mut requests, &retirement) {
                UdpEdgeIdleFence::Active => {
                    idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
                }
                UdpEdgeIdleFence::Retired => {
                    report_terminal_udp_edge_requests(
                        lane_id,
                        &mut requests,
                        &completions,
                        &mut cancelled,
                        None,
                        RuntimeError::ProductIdleTimeout,
                    )
                    .await;
                    product_retired = true;
                    break;
                }
            }
        }
        let retry_deadline = association.next_retry_deadline();
        let has_retry = retry_deadline.is_some();
        let retry_deadline = retry_deadline.unwrap_or_else(tokio::time::Instant::now);
        tokio::select! {
            biased;
            reason = &mut session_retirement => {
                terminal_session_reason = Some(reason);
                break;
            }
            result = cancelled.changed() => {
                if result.is_err() || *cancelled.borrow() {
                    product_retired = true;
                    break;
                }
            }
            incoming = association.next_carrier_frame(), if association.can_receive() => {
                match incoming {
                    Ok(event) => match association.handle_carrier_frame(event).await {
                        Ok(DatagramClientReceive::Deliver { target, payload, receipt }) => {
                            debug_assert_eq!(target, routed_target);
                            activity.record();
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
                    },
                    Err(error) => {
                        crate::observability::process_event!(
                            Warn,
                            "udp_edge",
                            "carrier_receive_failed",
                            "UDP carrier receive failed: {error}"
                        );
                    }
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
            () = &mut idle => {
                match fence_udp_edge_idle(
                    idle_timeout,
                    &mut requests,
                    &retirement,
                ) {
                    UdpEdgeIdleFence::Active => {
                        idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
                    }
                    UdpEdgeIdleFence::Retired => {
                        report_terminal_udp_edge_requests(
                            lane_id,
                            &mut requests,
                            &completions,
                            &mut cancelled,
                            None,
                            RuntimeError::ProductIdleTimeout,
                        )
                        .await;
                        product_retired = true;
                        break;
                    }
                }
            },
        }
    }
    // Completion publication also observes the lane cancellation. Preserve
    // that ownership handoff when it ends a send branch before the select can
    // take `cancelled.changed()` itself.
    product_retired |= *cancelled.borrow();
    if let Some(reason) = terminal_session_reason {
        report_terminal_udp_edge_requests(
            lane_id,
            &mut requests,
            &completions,
            &mut cancelled,
            None,
            RuntimeError::RemoteClosed(reason),
        )
        .await;
    }

    if product_retired {
        retire_mpp_udp_product_lifetime(association, product_flow, &mut gateway_lease).await;
        return;
    }

    let association_close_error = match association.close().await {
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
    let close_error = terminal_session_reason
        .map(|reason| RuntimeError::RemoteClosed(reason).to_string())
        .or(association_close_error);
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

async fn retire_mpp_udp_product_lifetime(
    association: DatagramClientAssociation,
    product_flow: OpenedProductFlow,
    gateway_lease: &mut Option<GatewayFlowLease>,
) {
    // Establish one transport-only terminal owner before Product admission,
    // telemetry, and session ownership are released. Its Drop path retains a
    // cancellation-safe TCP retirement lane and QUIC request-stream close.
    let mut retirement = association.begin_product_retirement();
    drop(product_flow);
    if let Some(lease) = gateway_lease.as_mut()
        && let Err(error) = lease.completed(None)
    {
        crate::observability::process_event!(
            Warn,
            "udp_balancer",
            "outcome_feedback_failed",
            "balancer UDP retirement feedback failed: {error}"
        );
    }
    // This lane actor remains the transport-close supervisor after every
    // Product owner above is gone. Cancellation of its ingress owner transfers
    // the actor's JoinHandle before signalling it, so terminal publication is
    // never delegated to an untracked detached future.
    let Some(timeout) = retirement.publication_timeout() else {
        return;
    };
    match bounded_idle_close_publication(timeout, retirement.close()).await {
        Ok(()) => {}
        Err(IdleClosePublicationError::Failed(error)) => {
            crate::observability::process_event!(
                Debug,
                "udp_edge",
                "idle_close_publication_failed",
                "idle UDP association close publication failed after logical retirement: {error}"
            );
        }
        Err(IdleClosePublicationError::TimedOut) => {
            crate::observability::process_event!(
                Debug,
                "udp_edge",
                "idle_close_publication_timed_out",
                "idle UDP association close publication exceeded the carrier-liveness horizon"
            );
        }
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
            result: result.map_err(Arc::new),
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
    idle_timeout: Option<std::time::Duration>,
    activity: Arc<ProductFlowActivity>,
    retirement: Arc<std::sync::Mutex<UdpEdgeRetirementGate>>,
) where
    M: Clone + Eq + Send + Sync + 'static,
    S: NativeUdpIo,
{
    let counter = product_flow
        .runtime_counter()
        .expect("client native UDP flow has one runtime observer");
    let mut idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
    let mut runtime_failed = false;
    let target = initial.target.clone();
    if activity.is_idle(idle_timeout)
        && matches!(
            fence_udp_edge_idle(idle_timeout, &mut requests, &retirement),
            UdpEdgeIdleFence::Retired
        )
    {
        report_terminal_udp_edge_requests(
            lane_id,
            &mut requests,
            &completions,
            &mut cancelled,
            Some(initial),
            RuntimeError::ProductIdleTimeout,
        )
        .await;
        complete_udp_gateway_flow(&mut gateway_lease, None);
        product_flow.complete_runtime();
        return;
    }
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
        if activity.is_idle(idle_timeout) {
            match fence_udp_edge_idle(idle_timeout, &mut requests, &retirement) {
                UdpEdgeIdleFence::Active => {
                    idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
                }
                UdpEdgeIdleFence::Retired => {
                    report_terminal_udp_edge_requests(
                        lane_id,
                        &mut requests,
                        &completions,
                        &mut cancelled,
                        None,
                        RuntimeError::ProductIdleTimeout,
                    )
                    .await;
                    break;
                }
            }
        }
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
                        activity.record();
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
                        // The native association has selected a terminal receive
                        // failure and will never poll this request queue again.
                        // Close first so every concurrent request is either
                        // rejected exactly or owned by the drain below.
                        requests.close();
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
                        report_terminal_udp_edge_requests(
                            lane_id,
                            &mut requests,
                            &completions,
                            &mut cancelled,
                            None,
                            error,
                        )
                        .await;
                        break;
                    }
                }
            }
            () = &mut idle => {
                match fence_udp_edge_idle(
                    idle_timeout,
                    &mut requests,
                    &retirement,
                ) {
                    UdpEdgeIdleFence::Active => {
                        idle = Box::pin(activity.wait_until_idle_candidate(idle_timeout));
                    }
                    UdpEdgeIdleFence::Retired => {
                        report_terminal_udp_edge_requests(
                            lane_id,
                            &mut requests,
                            &completions,
                            &mut cancelled,
                            None,
                            RuntimeError::ProductIdleTimeout,
                        )
                        .await;
                        break;
                    }
                }
            },
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
            result: result.map_err(Arc::new),
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

/// Settle every silently denied datagram already accepted before the idle
/// fence. Closing admission first ensures no request can enter after the drain
/// begins, while one completion remains paired with each accepted request.
async fn report_discarded_udp_edge_requests<M>(
    lane_id: usize,
    requests: &mut mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    cancelled: &mut tokio::sync::watch::Receiver<bool>,
    initial: Option<UdpEdgeRequest<M>>,
) {
    requests.close();
    if initial.is_some()
        && !send_udp_edge_completion(
            completions,
            cancelled,
            UdpEdgeCompletion::Discarded { lane_id },
        )
        .await
    {
        return;
    }
    while requests.recv().await.is_some() {
        if !send_udp_edge_completion(
            completions,
            cancelled,
            UdpEdgeCompletion::Discarded { lane_id },
        )
        .await
        {
            return;
        }
    }
}

/// Close a terminal lane before reporting, then settle every request that was
/// already accepted by its bounded queue with the same terminal cause.
async fn report_terminal_udp_edge_requests<M>(
    lane_id: usize,
    requests: &mut mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    cancelled: &mut tokio::sync::watch::Receiver<bool>,
    initial: Option<UdpEdgeRequest<M>>,
    error: RuntimeError,
) {
    requests.close();
    let error = Arc::new(error);
    if let Some(request) = initial
        && !send_udp_edge_completion(
            completions,
            cancelled,
            UdpEdgeCompletion::Sent {
                lane_id,
                target: request.target,
                metadata: request.metadata,
                result: Err(Arc::clone(&error)),
            },
        )
        .await
    {
        return;
    }
    while let Some(request) = requests.recv().await {
        if !send_udp_edge_completion(
            completions,
            cancelled,
            UdpEdgeCompletion::Sent {
                lane_id,
                target: request.target,
                metadata: request.metadata,
                result: Err(Arc::clone(&error)),
            },
        )
        .await
        {
            return;
        }
    }
}

fn reap_finished_udp_edge_lanes<M>(lanes: &mut Vec<UdpEdgeLane<M>>) {
    lanes.retain(|lane| {
        lane.pending != 0
            || !lane
                .handle
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
    });
}

#[cfg(test)]
fn dispatch_udp_edge_request<M>(
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
    dispatch_udp_edge_request_with_idle_timeout(
        lanes,
        next_lane_id,
        plan,
        mux_limits,
        None,
        completions,
        request,
    )
}

pub(in crate::runtime) fn dispatch_udp_edge_request_with_idle_timeout<M>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    next_lane_id: &mut usize,
    plan: &ClientOutboundPlan,
    mux_limits: MuxLimits,
    idle_timeout: Option<std::time::Duration>,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    request: UdpEdgeRequest<M>,
) -> Result<(), UdpEdgeRequest<M>>
where
    M: Clone + Eq + Send + Sync + 'static,
{
    let queue_slots = udp_edge_queue_slots(mux_limits);
    let lane_limit = queue_slots.min(mux_limits.max_streams);
    let mut request = request;
    loop {
        // A closed request sender marks a terminal reporter, not a finished
        // actor. Reap only the task boundary and only after all of its accepted
        // completions have been observed by the owner.
        reap_finished_udp_edge_lanes(lanes);
        let total_pending = lanes.iter().map(|lane| lane.pending).sum::<usize>();
        if total_pending >= queue_slots {
            return Err(request);
        }

        let mut position = lanes.iter().position(|lane| {
            lane.metadata == request.metadata
                && lane
                    .retirement
                    .lock()
                    .expect("UDP edge retirement gate poisoned")
                    .accepting
                && !lane.requests.is_closed()
        });
        if position.is_none() {
            // Terminal reporters remain live lanes and therefore consume the
            // same association bound. A successor is legal only in spare
            // capacity; it never evicts or aborts a reporter.
            if lanes.len() >= lane_limit {
                return Err(request);
            }
            let lane_id = *next_lane_id;
            *next_lane_id = next_lane_id.saturating_add(1);
            lanes.push(spawn_udp_edge_lane(
                lane_id,
                request.metadata.clone(),
                plan.clone(),
                mux_limits,
                idle_timeout,
                completions.clone(),
            ));
            position = Some(lanes.len() - 1);
        }

        let position = position.expect("UDP edge association exists");
        match try_send_udp_edge_request(
            &lanes[position].requests,
            &lanes[position].retirement,
            request,
        ) {
            Ok(()) => {
                lanes[position].pending = lanes[position].pending.saturating_add(1);
                return Ok(());
            }
            Err(mpsc::error::TrySendError::Full(rejected)) => return Err(rejected),
            Err(mpsc::error::TrySendError::Closed(rejected)) => {
                // The receiver crossed its terminal boundary after selection.
                // This exact request was not accepted; retry admission without
                // removing or aborting the reporter.
                request = rejected;
            }
        }
    }
}

pub(in crate::runtime) fn finish_udp_edge_completion<M>(
    lanes: &mut [UdpEdgeLane<M>],
    completion: &UdpEdgeCompletion<M>,
) {
    let lane_id = match completion {
        UdpEdgeCompletion::Sent { lane_id, .. } | UdpEdgeCompletion::Discarded { lane_id } => {
            lane_id
        }
        UdpEdgeCompletion::Received { .. } => return,
    };
    if let Some(lane) = lanes.iter_mut().find(|lane| lane.lane_id == *lane_id) {
        lane.pending = lane.pending.saturating_sub(1);
    }
}

/// Cancel every lane belonging to one exact external association generation.
///
/// Terminal reporting can briefly overlap a successor with the same metadata.
/// External expiry owns the whole generation and therefore removes every such
/// lane; callers that recycle keys must include their generation in `metadata`.
pub(in crate::runtime) fn remove_udp_edge_lane<M>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    metadata: &M,
) -> bool
where
    M: Eq,
{
    let mut removed = false;
    let mut position = 0;
    while position < lanes.len() {
        if lanes[position].metadata == *metadata {
            spawn_udp_edge_lane_retirement(lanes.swap_remove(position));
            removed = true;
        } else {
            position += 1;
        }
    }
    removed
}

/// Reap one exact internal lane after its task and completion accounting end.
pub(in crate::runtime) fn reap_finished_udp_edge_lane_instance<M>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    lane_id: usize,
) -> bool {
    let Some(position) = lanes.iter().position(|lane| {
        lane.lane_id == lane_id
            && lane.pending == 0
            && lane
                .handle
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
    }) else {
        return false;
    };
    lanes.swap_remove(position);
    true
}

pub(in crate::runtime) async fn close_udp_edge_lanes<M>(mut lanes: Vec<UdpEdgeLane<M>>) {
    let handles = lanes
        .iter_mut()
        .filter_map(UdpEdgeLane::begin_retirement)
        .collect::<Vec<_>>();
    drop(lanes);
    for handle in handles {
        observe_udp_edge_lane_retirement(handle).await;
    }
}

#[cfg(test)]
#[path = "tests_edge.rs"]
mod tests;
