//! Bounded UDP ingress association shared by SOCKS5 and packet-device ingress.
//!
//! One actor owns each local UDP association. Sends and target-originated
//! datagrams are independent, matching SOCKS5 UDP and CONNECT-UDP semantics.

use super::DatagramClientAssociation;
use super::association::DatagramClientReceive;
use crate::protocol::TargetAddr;
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use bytes::Bytes;
use tokio::sync::mpsc;

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

// The ingress flow owns its association actor and therefore its target-side
// socket lifetime. Dropping the owner cannot leave a detached relay task.
impl<M> Drop for UdpEdgeLane<M> {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pub(in crate::runtime) fn udp_edge_queue_slots(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).max(1)
}

pub(in crate::runtime) fn udp_edge_completion_queue(context: &ClientPathContext) -> usize {
    udp_edge_queue_slots(context)
}

fn spawn_udp_edge_lane<M>(
    lane_id: usize,
    metadata: M,
    context: ClientPathContext,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) -> UdpEdgeLane<M>
where
    M: Clone + Eq + Send + Sync + 'static,
{
    let (requests, rx) = mpsc::channel(udp_edge_queue_slots(&context));
    let (cancel, cancelled) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(run_udp_edge_lane(
        lane_id,
        metadata.clone(),
        context,
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
    context: ClientPathContext,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
    mut cancelled: tokio::sync::watch::Receiver<bool>,
) where
    M: Clone + Eq + Send + Sync + 'static,
{
    let mut association = match DatagramClientAssociation::new(context).await {
        Ok(association) => association,
        Err(err) => {
            eprintln!("warning: UDP edge association could not start: {err}");
            return;
        }
    };
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
                    Err(err) => {
                        eprintln!("warning: UDP carrier receive failed: {err}");
                        continue;
                    }
                };
                match association.handle_carrier_frame(event).await {
                    Ok(DatagramClientReceive::Deliver { target, payload, receipt }) => {
                        if !send_udp_edge_completion(
                            &completions,
                            &mut cancelled,
                            UdpEdgeCompletion::Received {
                                target,
                                metadata: local_metadata.clone(),
                                payload,
                            },
                        ).await
                        {
                            break;
                        }
                        if let Err(err) = association.acknowledge_received(receipt).await {
                            eprintln!("warning: UDP response feedback failed: {err}");
                        }
                    }
                    Ok(DatagramClientReceive::Duplicate(receipt)) => {
                        if let Err(err) = association.acknowledge_received(receipt).await {
                            eprintln!("warning: duplicate UDP response feedback failed: {err}");
                        }
                    }
                    Ok(DatagramClientReceive::Control) => {}
                    Err(err) => {
                        eprintln!("warning: UDP carrier frame failed: {err}");
                    }
                }
            }
            _ = tokio::time::sleep_until(retry_deadline), if has_retry => {
                if let Err(err) = association.retry_due_datagram().await {
                    eprintln!("warning: UDP datagram reinjection failed: {err}");
                }
            }
            request = requests.recv() => {
                let Some(request) = request else {
                    break;
                };
                let UdpEdgeRequest {
                    target,
                    payload,
                    ttl_ms,
                    metadata,
                } = request;
                debug_assert!(metadata == local_metadata);
                let result = association
                    .send_to_fresh_datagram_with_route_hint(
                        target.clone(),
                        payload,
                        ttl_ms,
                        None,
                    )
                    .await;
                if !send_udp_edge_completion(
                    &completions,
                    &mut cancelled,
                    UdpEdgeCompletion::Sent {
                        lane_id,
                        target,
                        metadata,
                        result,
                    },
                ).await
                {
                    break;
                }
            }
        }
    }

    if let Err(err) = association.close().await {
        eprintln!("warning: UDP edge association close failed: {err}");
    }
}

async fn send_udp_edge_completion<M>(
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    cancelled: &mut tokio::sync::watch::Receiver<bool>,
    completion: UdpEdgeCompletion<M>,
) -> bool {
    tokio::select! {
        result = completions.send(completion) => result.is_ok(),
        result = cancelled.changed() => {
            result.is_ok() && !*cancelled.borrow()
        }
    }
}

pub(in crate::runtime) fn dispatch_udp_edge_request<M>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    next_lane_id: &mut usize,
    context: &ClientPathContext,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    request: UdpEdgeRequest<M>,
) -> Result<(), UdpEdgeRequest<M>>
where
    M: Clone + Eq + Send + Sync + 'static,
{
    let total_pending = lanes.iter().map(|lane| lane.pending).sum::<usize>();
    if total_pending >= udp_edge_queue_slots(context) {
        return Err(request);
    }
    let mut position = lanes
        .iter()
        .position(|lane| lane.metadata == request.metadata);
    if position.is_none() {
        let lane_limit = udp_edge_queue_slots(context).min(context.mux_limits.max_streams);
        if lanes.len() >= lane_limit {
            return Err(request);
        }
        let lane_id = *next_lane_id;
        *next_lane_id = next_lane_id.saturating_add(1);
        lanes.push(spawn_udp_edge_lane(
            lane_id,
            request.metadata.clone(),
            context.clone(),
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
        if let Err(err) = handle.await {
            eprintln!("warning: UDP edge association task failed: {err}");
        }
    }
}
