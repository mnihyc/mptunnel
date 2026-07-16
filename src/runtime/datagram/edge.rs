//! Bounded UDP ingress dispatch shared by SOCKS5 and packet-device ingress.
//!
//! Edge lanes own reusable product datagram associations. Ingress adapters keep
//! framing and address translation, while this module applies one queue and path
//! selection model to every local UDP edge.

use super::{DatagramClientAssociation, datagram_underlay_candidate_keys};
use crate::model::capacity::{
    MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES, PATH_OPEN_SCORE_BYTES,
    QUIC_PERSISTENT_CONGESTION_THRESHOLD, QUIC_TIMER_GRANULARITY,
};
use crate::model::path::RelayPathKey;
use crate::protocol::TargetAddr;
use crate::runtime::error::RuntimeError;
use crate::runtime::ingress_runtime::DEFAULT_SOCKS5_UDP_TTL_MS;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::model::UdpPathRuntimeModel;
use crate::scheduler::{PathSnapshot, PathState as SchedulerPathState};
use bytes::Bytes;
use std::collections::HashSet;
use tokio::sync::mpsc;

pub(in crate::runtime) struct UdpEdgeRequest<M> {
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) payload: Bytes,
    pub(in crate::runtime) ttl_ms: u32,
    pub(in crate::runtime) metadata: M,
    pub(in crate::runtime) route_hint: Option<RelayPathKey>,
}

pub(in crate::runtime) struct UdpEdgeCompletion<M> {
    pub(in crate::runtime) lane_id: usize,
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) metadata: M,
    pub(in crate::runtime) result: Result<Bytes, RuntimeError>,
}

pub(in crate::runtime) struct UdpEdgeLane<M> {
    lane_id: usize,
    pending: usize,
    route_hint: Option<RelayPathKey>,
    successful_completions: usize,
    requests: mpsc::Sender<UdpEdgeRequest<M>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

// A lane is an association actor owned by its ingress flow, never a detached task.
impl<M> Drop for UdpEdgeLane<M> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pub(in crate::runtime) fn udp_edge_queue_slots(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).max(1)
}

fn datagram_edge_path_lane_parallelism(snapshot: PathSnapshot) -> usize {
    if snapshot.state == SchedulerPathState::Failed {
        return 0;
    }
    let model = UdpPathRuntimeModel::from_snapshot(
        snapshot,
        DEFAULT_SOCKS5_UDP_TTL_MS,
        MAX_PRODUCT_DATAGRAM_PAYLOAD_BYTES,
    );
    let response_window_bytes = (model.pacing_rate_bps.max(1.0) / 8.0
        * model
            .response_timeout
            .max(QUIC_TIMER_GRANULARITY)
            .as_secs_f64())
    .ceil() as usize;
    let initial_window_bytes = PATH_OPEN_SCORE_BYTES.max(model.max_payload_bytes).max(1);
    response_window_bytes
        .div_ceil(initial_window_bytes)
        .max(QUIC_PERSISTENT_CONGESTION_THRESHOLD as usize)
}

pub(in crate::runtime) fn udp_edge_lane_limit(context: &ClientPathContext) -> usize {
    let tcp_parallelism = (0..context.tcp_paths.len())
        .filter_map(|index| context.tcp_path_snapshot(index))
        .map(datagram_edge_path_lane_parallelism)
        .sum::<usize>();
    let udp_parallelism = (0..context.udp_paths.len())
        .filter_map(|index| context.udp_path_snapshot(index))
        .map(datagram_edge_path_lane_parallelism)
        .sum::<usize>();
    udp_edge_queue_slots(context).min(tcp_parallelism.saturating_add(udp_parallelism).max(1))
}

pub(in crate::runtime) fn udp_edge_startup_lane_limit(context: &ClientPathContext) -> usize {
    let queue_slots = udp_edge_queue_slots(context);
    let has_datagram_carrier = !context.tcp_paths.is_empty() || !context.udp_paths.is_empty();
    let hedge_lane = usize::from(queue_slots > 1 && has_datagram_carrier);
    udp_edge_lane_limit(context)
        .min(queue_slots)
        .min(1usize.saturating_add(hedge_lane))
        .max(1)
}

pub(in crate::runtime) fn udp_edge_lane_spawn_allowed(
    lane_count: usize,
    successful_lane_count: usize,
    context: &ClientPathContext,
) -> bool {
    if lane_count < udp_edge_startup_lane_limit(context) {
        return true;
    }
    successful_lane_count > 0
}

fn udp_edge_lane_queue(context: &ClientPathContext) -> usize {
    let lanes = udp_edge_lane_limit(context).max(1);
    (udp_edge_queue_slots(context) / lanes).max(1)
}

pub(in crate::runtime) fn udp_edge_completion_queue(context: &ClientPathContext) -> usize {
    udp_edge_lane_limit(context)
        .saturating_mul(udp_edge_lane_queue(context))
        .max(1)
}

fn spawn_udp_edge_lane<M: Send + 'static>(
    lane_id: usize,
    context: ClientPathContext,
    lane_queue: usize,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) -> UdpEdgeLane<M> {
    let (requests, rx) = mpsc::channel(lane_queue);
    let handle = tokio::spawn(run_udp_edge_lane(lane_id, context, rx, completions));
    UdpEdgeLane {
        lane_id,
        pending: 0,
        route_hint: None,
        successful_completions: 0,
        requests,
        handle: Some(handle),
    }
}

async fn run_udp_edge_lane<M: Send + 'static>(
    lane_id: usize,
    context: ClientPathContext,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) {
    let mut association = match DatagramClientAssociation::new(context).await {
        Ok(association) => association,
        Err(err) => {
            eprintln!("warning: UDP edge lane could not start: {err}");
            return;
        }
    };
    while let Some(request) = requests.recv().await {
        let UdpEdgeRequest {
            target,
            payload,
            ttl_ms,
            metadata,
            route_hint,
        } = request;
        let result = association
            .send_to_fresh_datagram_with_route_hint(target.clone(), payload, ttl_ms, route_hint)
            .await;
        if completions
            .send(UdpEdgeCompletion {
                lane_id,
                target,
                metadata,
                result,
            })
            .await
            .is_err()
        {
            break;
        }
    }
    if let Err(err) = association.close().await {
        eprintln!("warning: UDP edge lane close failed: {err}");
    }
}

pub(in crate::runtime) fn dispatch_udp_edge_request<M: Send + 'static>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    next_lane_id: &mut usize,
    context: &ClientPathContext,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    mut request: UdpEdgeRequest<M>,
) -> Result<(), UdpEdgeRequest<M>> {
    let lane_limit = udp_edge_lane_limit(context);
    let lane_queue = udp_edge_lane_queue(context);
    let successful_lane_count = lanes
        .iter()
        .filter(|lane| lane.successful_completions > 0)
        .count();
    if lanes.is_empty()
        || (lanes.len() < lane_limit
            && lanes.iter().all(|lane| lane.pending > 0)
            && udp_edge_lane_spawn_allowed(lanes.len(), successful_lane_count, context))
    {
        let lane_id = *next_lane_id;
        *next_lane_id = next_lane_id.saturating_add(1);
        lanes.push(spawn_udp_edge_lane(
            lane_id,
            context.clone(),
            lane_queue,
            completions.clone(),
        ));
    }
    request.route_hint = udp_edge_route_hint(
        context,
        request.payload.len(),
        request.ttl_ms,
        lanes
            .iter()
            .filter(|lane| lane.pending > 0)
            .filter_map(|lane| lane.route_hint),
    );

    let Some((position, _)) = lanes
        .iter()
        .enumerate()
        .min_by_key(|(_, lane)| (lane.pending, lane.lane_id))
    else {
        return Err(request);
    };

    let lane_was_idle = lanes[position].pending == 0;
    let route_hint = request.route_hint;
    match lanes[position].requests.try_send(request) {
        Ok(()) => {
            if lane_was_idle {
                lanes[position].route_hint = route_hint;
            }
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

pub(in crate::runtime) fn udp_edge_route_hint(
    context: &ClientPathContext,
    payload_bytes: usize,
    ttl_ms: u32,
    active_routes: impl IntoIterator<Item = RelayPathKey>,
) -> Option<RelayPathKey> {
    let candidates = datagram_underlay_candidate_keys(context, payload_bytes, ttl_ms);
    if candidates.is_empty() {
        return None;
    }
    let active_routes = active_routes.into_iter().collect::<HashSet<_>>();
    candidates
        .iter()
        .copied()
        .find(|candidate| !active_routes.contains(candidate))
        .or_else(|| candidates.first().copied())
}

pub(in crate::runtime) fn finish_udp_edge_completion<M>(
    lanes: &mut [UdpEdgeLane<M>],
    completion: &UdpEdgeCompletion<M>,
) {
    if let Some(lane) = lanes
        .iter_mut()
        .find(|lane| lane.lane_id == completion.lane_id)
    {
        lane.pending = lane.pending.saturating_sub(1);
        if lane.pending == 0 {
            lane.route_hint = None;
        }
        if completion.result.is_ok() {
            lane.successful_completions = lane.successful_completions.saturating_add(1);
        }
    }
}

pub(in crate::runtime) async fn close_udp_edge_lanes<M>(mut lanes: Vec<UdpEdgeLane<M>>) {
    let handles = lanes
        .iter_mut()
        .filter_map(|lane| lane.handle.take())
        .collect::<Vec<_>>();
    drop(lanes);
    for handle in handles {
        if let Err(err) = handle.await {
            eprintln!("warning: UDP edge lane task failed: {err}");
        }
    }
}
