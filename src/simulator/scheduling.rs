//! Simulator-private queueing and scheduling hypotheses.
//!
//! DRR, virtual backlog, tail mode, close-ETA duplication, and shared-bottleneck
//! suspicion live here so simulations cannot masquerade as runtime mechanisms.

use crate::protocol::PathId;
use crate::scheduler::{
    PathScore, PathSnapshot, PathState, QUIC_INITIAL_WINDOW_PACKETS, TrafficClass, path_bdp_bytes,
    path_is_backup, path_is_schedulable, path_pto_ms, payload_tx_ms, score_path,
};
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingMode {
    Normal,
    TailAvoidance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct EnqueueRequest {
    pub(super) flow_id: FlowId,
    pub(super) lane: TrafficClass,
    pub(super) payload_bytes: usize,
    pub(super) remaining_flow_bytes: usize,
    pub(super) duplicate_eligible: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct SchedulerDecision {
    pub(super) flow_id: FlowId,
    pub(super) scheduled_lane: TrafficClass,
    pub(super) mode: SchedulingMode,
    pub(super) path_id: PathId,
    pub(super) duplicate_path_id: Option<PathId>,
    pub(super) estimated_completion_ms: f64,
}

/// Queueing model used only by deterministic simulations.
///
/// Deployed senders own their carrier queues and admission state. The
/// simulator keeps a compact virtual queue here so simulations can model
/// fairness, aggregation, and failure without creating a second runtime.
#[derive(Debug, Default)]
pub(super) struct HeterogeneousScheduler {
    lanes: Vec<LaneQueue>,
    path_queues: Vec<PathQueue>,
}

impl HeterogeneousScheduler {
    pub(super) fn enqueue(&mut self, request: EnqueueRequest) {
        if request.payload_bytes == 0 {
            return;
        }
        let lane_index = self.lane_index(request.lane);
        let lane_queue = &mut self.lanes[lane_index];
        if let Some(flow) = lane_queue
            .flows
            .iter_mut()
            .find(|flow| flow.flow_id == request.flow_id)
        {
            flow.packets.push_back(request);
            return;
        }
        let mut flow = FlowQueue {
            flow_id: request.flow_id,
            deficit_bytes: 0,
            packets: VecDeque::new(),
        };
        flow.packets.push_back(request);
        lane_queue.flows.push_back(flow);
    }

    pub(super) fn queued_path_bytes(&self, path_id: PathId) -> u64 {
        self.path_queues
            .iter()
            .find(|queue| queue.path_id == path_id)
            .map_or(0, |queue| queue.bytes)
    }

    pub(super) fn schedule_next(&mut self, paths: &[PathSnapshot]) -> Option<SchedulerDecision> {
        for lane in priority_order() {
            let lane_index = self.lane_index(lane);
            let flow_count = self.lanes[lane_index].flows.len();
            if flow_count == 0 {
                continue;
            }
            let lane_quantum = self.lanes[lane_index]
                .flows
                .iter()
                .filter_map(|flow| flow.packets.front())
                .map(|packet| deficit_charge_bytes(packet.payload_bytes))
                .max()
                .unwrap_or(1);
            self.lanes[lane_index].deficit_bytes = self.lanes[lane_index]
                .deficit_bytes
                .saturating_add(lane_quantum);

            for _ in 0..flow_count {
                let mut flow = self.lanes[lane_index]
                    .flows
                    .pop_front()
                    .expect("flow exists");
                let Some(packet) = flow.packets.front().copied() else {
                    continue;
                };
                let charge_bytes = deficit_charge_bytes(packet.payload_bytes);
                flow.deficit_bytes = flow.deficit_bytes.saturating_add(charge_bytes);
                if charge_bytes > self.lanes[lane_index].deficit_bytes
                    || charge_bytes > flow.deficit_bytes
                {
                    self.lanes[lane_index].flows.push_back(flow);
                    continue;
                }
                let Some(decision) = self.choose_packet_paths(packet, paths) else {
                    self.lanes[lane_index].flows.push_front(flow);
                    return None;
                };
                self.lanes[lane_index].deficit_bytes = self.lanes[lane_index]
                    .deficit_bytes
                    .saturating_sub(charge_bytes);
                flow.deficit_bytes = flow.deficit_bytes.saturating_sub(charge_bytes);
                flow.packets.pop_front();
                if !flow.packets.is_empty() {
                    self.lanes[lane_index].flows.push_back(flow);
                }
                self.add_path_queue(decision.path_id, packet.payload_bytes as u64);
                if let Some(path_id) = decision.duplicate_path_id {
                    self.add_path_queue(path_id, packet.payload_bytes as u64);
                }
                return Some(decision);
            }
        }
        None
    }

    pub(super) fn advance_time(&mut self, paths: &[PathSnapshot], elapsed_ms: f64) {
        if elapsed_ms <= 0.0 {
            return;
        }
        for queue in &mut self.path_queues {
            let Some(path) = paths.iter().find(|path| path.id == queue.path_id) else {
                queue.bytes = 0;
                continue;
            };
            if !matches!(path.state, PathState::Active | PathState::Suspect) {
                queue.bytes = 0;
                continue;
            }
            let drained = (path.delivery_rate_bps.max(0.0) * elapsed_ms / 8000.0) as u64;
            queue.bytes = queue.bytes.saturating_sub(drained);
        }
    }

    pub(super) fn remove_path(&mut self, path_id: PathId) {
        self.path_queues.retain(|queue| queue.path_id != path_id);
    }

    fn lane_index(&mut self, lane: TrafficClass) -> usize {
        if let Some(index) = self.lanes.iter().position(|queue| queue.lane == lane) {
            return index;
        }
        self.lanes.push(LaneQueue {
            lane,
            deficit_bytes: 0,
            flows: VecDeque::new(),
        });
        self.lanes.len() - 1
    }

    fn choose_packet_paths(
        &self,
        packet: EnqueueRequest,
        paths: &[PathSnapshot],
    ) -> Option<SchedulerDecision> {
        let mode = scheduling_mode(packet, paths);
        let scheduled_lane = scheduled_lane(packet.lane, mode);
        let scored = self.scored_paths(
            paths,
            packet.lane,
            scheduled_lane,
            packet.payload_bytes,
            mode,
        );
        let primary = scored.first().copied()?;
        let duplicate_path_id = duplicate_path(packet, primary, &scored, paths);
        Some(SchedulerDecision {
            flow_id: packet.flow_id,
            scheduled_lane,
            mode,
            path_id: primary.path_id,
            duplicate_path_id,
            estimated_completion_ms: primary.eta_ms,
        })
    }

    fn scored_paths(
        &self,
        paths: &[PathSnapshot],
        original_lane: TrafficClass,
        scheduled_lane: TrafficClass,
        payload_bytes: usize,
        mode: SchedulingMode,
    ) -> Vec<PathScore> {
        let available_is_schedulable = paths.iter().copied().any(|path| {
            path_is_scheduler_candidate(path, original_lane, scheduled_lane)
                && !path_is_backup(path)
        });
        let mut scored = paths
            .iter()
            .copied()
            .filter(|path| path_is_scheduler_candidate(*path, original_lane, scheduled_lane))
            .filter(|path| !available_is_schedulable || !path_is_backup(*path))
            .filter_map(|path| {
                let mut path = path;
                path.queue_bytes = path
                    .queue_bytes
                    .saturating_add(self.queued_path_bytes(path.id));
                score_path(path, scheduled_lane, payload_bytes).map(|mut score| {
                    score.eta_ms += shared_bottleneck_penalty(path, paths);
                    if mode == SchedulingMode::TailAvoidance {
                        score.eta_ms += path_pto_ms(path);
                    }
                    score
                })
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| left.eta_ms.total_cmp(&right.eta_ms));
        scored
    }

    fn add_path_queue(&mut self, path_id: PathId, bytes: u64) {
        if let Some(queue) = self
            .path_queues
            .iter_mut()
            .find(|queue| queue.path_id == path_id)
        {
            queue.bytes = queue.bytes.saturating_add(bytes);
            return;
        }
        self.path_queues.push(PathQueue { path_id, bytes });
    }
}

pub(super) fn schedulable_capacity_bps(paths: &[PathSnapshot], lane: TrafficClass) -> f64 {
    let available_is_schedulable = paths
        .iter()
        .copied()
        .any(|path| path_is_schedulable(path, lane) && !path_is_backup(path));
    paths
        .iter()
        .copied()
        .filter(|path| path_is_schedulable(*path, lane))
        .filter(|path| !available_is_schedulable || !path_is_backup(*path))
        .map(|path| path.delivery_rate_bps.max(0.0))
        .sum()
}

#[derive(Debug)]
struct LaneQueue {
    lane: TrafficClass,
    deficit_bytes: u64,
    flows: VecDeque<FlowQueue>,
}

#[derive(Debug)]
struct FlowQueue {
    flow_id: FlowId,
    deficit_bytes: u64,
    packets: VecDeque<EnqueueRequest>,
}

#[derive(Debug, Clone, Copy)]
struct PathQueue {
    path_id: PathId,
    bytes: u64,
}

fn priority_order() -> [TrafficClass; 5] {
    [
        TrafficClass::Control,
        TrafficClass::RealtimeDatagram,
        TrafficClass::Latency,
        TrafficClass::Throughput,
        TrafficClass::Background,
    ]
}

fn path_is_scheduler_candidate(
    path: PathSnapshot,
    original_lane: TrafficClass,
    scheduled_lane: TrafficClass,
) -> bool {
    (original_lane != TrafficClass::Throughput || path.policy.bulk_allowed)
        && path_is_schedulable(path, scheduled_lane)
}

fn deficit_charge_bytes(payload_bytes: usize) -> u64 {
    (payload_bytes as u64).max(1)
}

fn scheduling_mode(packet: EnqueueRequest, paths: &[PathSnapshot]) -> SchedulingMode {
    if packet.lane == TrafficClass::Throughput
        && packet.remaining_flow_bytes <= adaptive_tail_avoidance_threshold_bytes(paths, packet)
    {
        SchedulingMode::TailAvoidance
    } else {
        SchedulingMode::Normal
    }
}

fn scheduled_lane(lane: TrafficClass, mode: SchedulingMode) -> TrafficClass {
    match (lane, mode) {
        (TrafficClass::Throughput, SchedulingMode::TailAvoidance) => TrafficClass::Latency,
        _ => lane,
    }
}

fn duplicate_path(
    packet: EnqueueRequest,
    primary: PathScore,
    scored: &[PathScore],
    paths: &[PathSnapshot],
) -> Option<PathId> {
    if !packet.duplicate_eligible
        || !matches!(
            packet.lane,
            TrafficClass::Control | TrafficClass::RealtimeDatagram
        )
    {
        return None;
    }
    let primary_snapshot = paths
        .iter()
        .copied()
        .find(|path| path.id == primary.path_id)?;
    scored
        .iter()
        .copied()
        .find(|score| {
            let Some(candidate) = paths.iter().copied().find(|path| path.id == score.path_id)
            else {
                return false;
            };
            let eta_slack = adaptive_duplication_eta_slack_ms(primary_snapshot, candidate);
            let duplicate_tx = payload_tx_ms(candidate, packet.payload_bytes);
            score.path_id != primary.path_id
                && score.eta_ms <= primary.eta_ms + eta_slack
                && duplicate_tx <= eta_slack
        })
        .map(|score| score.path_id)
}

fn shared_bottleneck_penalty(path: PathSnapshot, paths: &[PathSnapshot]) -> f64 {
    paths
        .iter()
        .filter(|other| {
            other.id != path.id
                && matches!(other.state, PathState::Active | PathState::Suspect)
                && other.queue_bytes.saturating_add(other.bytes_in_flight) > 0
                && path_rtt_samples_overlap(path, **other)
        })
        .map(|other| {
            let queued = other.queue_bytes.saturating_add(other.bytes_in_flight) as usize;
            payload_tx_ms(path, queued)
        })
        .fold(0.0, f64::max)
}

fn adaptive_tail_avoidance_threshold_bytes(
    paths: &[PathSnapshot],
    packet: EnqueueRequest,
) -> usize {
    paths
        .iter()
        .copied()
        .filter(|path| path_is_schedulable(*path, TrafficClass::Latency))
        .map(path_bdp_bytes)
        .min()
        .unwrap_or(packet.payload_bytes.max(1))
        .max(packet.payload_bytes.max(1))
}

fn adaptive_duplication_eta_slack_ms(primary: PathSnapshot, candidate: PathSnapshot) -> f64 {
    let pto = path_pto_ms(primary).min(path_pto_ms(candidate));
    let jitter = primary.jitter_ms.max(candidate.jitter_ms).max(0.0);
    (pto / QUIC_INITIAL_WINDOW_PACKETS).max(jitter)
}

fn path_rtt_samples_overlap(path: PathSnapshot, other: PathSnapshot) -> bool {
    let path_window = path.jitter_ms.max(path.srtt_ms.max(1.0) / 4.0);
    let other_window = other.jitter_ms.max(other.srtt_ms.max(1.0) / 4.0);
    (path.srtt_ms - other.srtt_ms).abs() <= path_window.max(other_window)
}

#[cfg(test)]
#[path = "tests_scheduling.rs"]
mod tests;
