use super::FlowLane;
use crate::protocol::{PathCapabilities, PathId, UnderlayProtocol};
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathState {
    Active,
    Suspect,
    Draining,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathFlags {
    pub backup: bool,
    pub expensive: bool,
    pub low_latency: bool,
    pub bulk_allowed: bool,
    pub probe_only: bool,
    pub no_udp: bool,
}

impl Default for PathFlags {
    fn default() -> Self {
        Self {
            backup: false,
            expensive: false,
            low_latency: false,
            bulk_allowed: true,
            probe_only: false,
            no_udp: false,
        }
    }
}

impl From<PathCapabilities> for PathFlags {
    fn from(value: PathCapabilities) -> Self {
        Self {
            backup: value.backup,
            expensive: value.expensive,
            low_latency: value.low_latency,
            bulk_allowed: value.bulk_allowed,
            probe_only: value.probe_only,
            no_udp: value.no_udp,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PathSnapshot {
    pub id: PathId,
    pub underlay: UnderlayProtocol,
    pub state: PathState,
    pub flags: PathFlags,
    pub srtt_ms: f64,
    pub jitter_ms: f64,
    pub delivery_rate_bps: f64,
    pub loss_rate: f64,
    pub queue_bytes: u64,
    pub bytes_in_flight: u64,
    pub product_bytes_in_flight: u64,
    pub active_flows: u32,
    pub active_latency_sensitive_flows: u32,
    pub pacing_rate_bps: f64,
    pub inflight_limit_bytes: u64,
    pub confidence: f64,
    pub app_limited: bool,
}

impl PathSnapshot {
    pub fn new(
        id: PathId,
        underlay: UnderlayProtocol,
        srtt_ms: f64,
        delivery_rate_bps: f64,
    ) -> Self {
        Self {
            id,
            underlay,
            state: PathState::Active,
            flags: PathFlags::default(),
            srtt_ms,
            jitter_ms: 0.0,
            delivery_rate_bps,
            loss_rate: 0.0,
            queue_bytes: 0,
            bytes_in_flight: 0,
            product_bytes_in_flight: 0,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            pacing_rate_bps: delivery_rate_bps,
            inflight_limit_bytes: 0,
            confidence: 1.0,
            app_limited: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SchedulerPolicy {
    pub expensive_penalty_ms: f64,
    pub suspect_penalty_ms: f64,
    pub backup_penalty_ms: f64,
    pub tcp_reorder_penalty_ms: f64,
    pub loss_penalty_scale_ms: f64,
    pub tail_avoidance_threshold_bytes: usize,
    pub duplication_max_payload_bytes: usize,
    pub duplication_max_extra_eta_ms: f64,
    pub shared_bottleneck_rtt_window_ms: f64,
    pub shared_bottleneck_queue_penalty_ms: f64,
    pub tail_avoidance_rtt_penalty_scale: f64,
    pub low_confidence_penalty_ms: f64,
}

impl Default for SchedulerPolicy {
    fn default() -> Self {
        Self {
            expensive_penalty_ms: 25.0,
            suspect_penalty_ms: 250.0,
            backup_penalty_ms: 100.0,
            tcp_reorder_penalty_ms: 50.0,
            loss_penalty_scale_ms: 500.0,
            tail_avoidance_threshold_bytes: 512 * 1024,
            duplication_max_payload_bytes: 4096,
            duplication_max_extra_eta_ms: 15.0,
            shared_bottleneck_rtt_window_ms: 8.0,
            shared_bottleneck_queue_penalty_ms: 20.0,
            tail_avoidance_rtt_penalty_scale: 0.75,
            low_confidence_penalty_ms: 25.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathScore {
    pub path_id: PathId,
    pub eta_ms: f64,
}

pub fn choose_path(
    paths: &[PathSnapshot],
    lane: FlowLane,
    payload_bytes: usize,
    policy: SchedulerPolicy,
) -> Option<PathScore> {
    paths
        .iter()
        .filter_map(|path| score_path(*path, lane, payload_bytes, policy))
        .min_by(|left, right| left.eta_ms.total_cmp(&right.eta_ms))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingMode {
    Normal,
    TailAvoidance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnqueueRequest {
    pub flow_id: FlowId,
    pub lane: FlowLane,
    pub payload_bytes: usize,
    pub remaining_flow_bytes: usize,
    pub duplicate_eligible: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchedulerDecision {
    pub flow_id: FlowId,
    pub lane: FlowLane,
    pub scheduled_lane: FlowLane,
    pub mode: SchedulingMode,
    pub payload_bytes: usize,
    pub path_id: PathId,
    pub duplicate_path_id: Option<PathId>,
    pub estimated_completion_ms: f64,
}

#[derive(Debug, Default)]
pub struct HeterogeneousScheduler {
    lanes: Vec<LaneQueue>,
    path_queues: Vec<PathQueue>,
}

impl HeterogeneousScheduler {
    pub fn enqueue(&mut self, request: EnqueueRequest) {
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

    pub fn has_queued(&self) -> bool {
        self.lanes
            .iter()
            .any(|lane| lane.flows.iter().any(|flow| !flow.packets.is_empty()))
    }

    pub fn queued_path_bytes(&self, path_id: PathId) -> u64 {
        self.path_queues
            .iter()
            .find(|queue| queue.path_id == path_id)
            .map_or(0, |queue| queue.bytes)
    }

    pub fn schedule_next(
        &mut self,
        paths: &[PathSnapshot],
        policy: SchedulerPolicy,
    ) -> Option<SchedulerDecision> {
        for lane in priority_order() {
            let lane_index = self.lane_index(lane);
            let flow_count = self.lanes[lane_index].flows.len();
            if flow_count == 0 {
                continue;
            }
            self.lanes[lane_index].deficit_bytes = self.lanes[lane_index]
                .deficit_bytes
                .saturating_add(lane_quantum_bytes(lane));

            for _ in 0..flow_count {
                let mut flow = self.lanes[lane_index]
                    .flows
                    .pop_front()
                    .expect("flow exists");
                flow.deficit_bytes = flow.deficit_bytes.saturating_add(flow_quantum_bytes(lane));
                let Some(packet) = flow.packets.front().copied() else {
                    continue;
                };
                let charge_bytes = deficit_charge_bytes(packet.lane, packet.payload_bytes);
                if charge_bytes > self.lanes[lane_index].deficit_bytes
                    || charge_bytes > flow.deficit_bytes
                {
                    self.lanes[lane_index].flows.push_back(flow);
                    continue;
                }
                let Some(decision) = self.choose_packet_paths(packet, paths, policy) else {
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

    pub fn advance_time(&mut self, paths: &[PathSnapshot], elapsed_ms: f64) {
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

    pub fn remove_path(&mut self, path_id: PathId) {
        self.path_queues.retain(|queue| queue.path_id != path_id);
    }

    fn lane_index(&mut self, lane: FlowLane) -> usize {
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
        policy: SchedulerPolicy,
    ) -> Option<SchedulerDecision> {
        let mode = scheduling_mode(packet, policy);
        let scheduled_lane = scheduled_lane(packet.lane, mode);
        let scored = self.scored_paths(paths, scheduled_lane, packet.payload_bytes, mode, policy);
        let primary = scored.first().copied()?;
        let duplicate_path_id = duplicate_path(packet, primary, &scored, policy);
        Some(SchedulerDecision {
            flow_id: packet.flow_id,
            lane: packet.lane,
            scheduled_lane,
            mode,
            payload_bytes: packet.payload_bytes,
            path_id: primary.path_id,
            duplicate_path_id,
            estimated_completion_ms: primary.eta_ms,
        })
    }

    fn scored_paths(
        &self,
        paths: &[PathSnapshot],
        lane: FlowLane,
        payload_bytes: usize,
        mode: SchedulingMode,
        policy: SchedulerPolicy,
    ) -> Vec<PathScore> {
        let mut scored = paths
            .iter()
            .filter_map(|path| {
                let mut path = *path;
                path.queue_bytes = path
                    .queue_bytes
                    .saturating_add(self.queued_path_bytes(path.id));
                score_path(path, lane, payload_bytes, policy).map(|mut score| {
                    score.eta_ms += shared_bottleneck_penalty(path, paths, policy);
                    if mode == SchedulingMode::TailAvoidance {
                        score.eta_ms += path.srtt_ms * policy.tail_avoidance_rtt_penalty_scale;
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

#[derive(Debug)]
struct LaneQueue {
    lane: FlowLane,
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

pub fn score_path(
    path: PathSnapshot,
    lane: FlowLane,
    payload_bytes: usize,
    policy: SchedulerPolicy,
) -> Option<PathScore> {
    if !path_is_schedulable(path, lane) {
        return None;
    }

    let rate = effective_path_rate_bps(path, lane);
    let effective_inflight = if path.inflight_limit_bytes > 0 {
        path.bytes_in_flight
            .min(path.inflight_limit_bytes.saturating_mul(2))
    } else {
        path.bytes_in_flight
    };
    let queued_bits = path.queue_bytes.saturating_add(effective_inflight) as f64 * 8.0;
    let payload_bits = payload_bytes as f64 * 8.0;

    let mut eta_ms = path.srtt_ms / 2.0;
    eta_ms += queued_bits / rate * 1000.0;
    eta_ms += payload_bits / rate * 1000.0;
    eta_ms += path.jitter_ms;
    eta_ms += path.loss_rate.clamp(0.0, 1.0) * policy.loss_penalty_scale_ms;
    eta_ms += (1.0 - path.confidence.clamp(0.0, 1.0)) * policy.low_confidence_penalty_ms;
    eta_ms += active_flow_penalty_ms(path, lane);

    if path.state == PathState::Suspect {
        eta_ms += suspect_penalty_ms(lane, policy);
    }
    if path.flags.backup {
        eta_ms += policy.backup_penalty_ms;
    }
    if path.flags.expensive {
        eta_ms += policy.expensive_penalty_ms;
    }
    if path.underlay == UnderlayProtocol::Tcp && prefers_low_reorder(lane) {
        eta_ms += policy.tcp_reorder_penalty_ms;
    }
    if lane == FlowLane::Control && path.flags.low_latency {
        eta_ms -= path.srtt_ms.min(10.0) * 0.25;
    }

    Some(PathScore {
        path_id: path.id,
        eta_ms,
    })
}

fn active_flow_penalty_ms(path: PathSnapshot, lane: FlowLane) -> f64 {
    match lane {
        FlowLane::Throughput | FlowLane::Background => {
            f64::from(path.active_latency_sensitive_flows) * path.srtt_ms.max(1.0)
        }
        FlowLane::Control | FlowLane::RealtimeDatagram | FlowLane::Latency => {
            f64::from(path.active_flows) * path.srtt_ms.max(1.0) * 0.25
        }
    }
}

fn effective_path_rate_bps(path: PathSnapshot, lane: FlowLane) -> f64 {
    let rate = path.pacing_rate_bps.max(path.delivery_rate_bps).max(1.0);
    match lane {
        FlowLane::Throughput | FlowLane::Background => {
            let active_bulk_flows = path
                .active_flows
                .saturating_sub(path.active_latency_sensitive_flows)
                .max(1) as f64;
            rate / active_bulk_flows
        }
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram => rate,
    }
}

fn priority_order() -> [FlowLane; 5] {
    [
        FlowLane::Control,
        FlowLane::RealtimeDatagram,
        FlowLane::Latency,
        FlowLane::Throughput,
        FlowLane::Background,
    ]
}

fn lane_quantum_bytes(lane: FlowLane) -> u64 {
    match lane {
        FlowLane::Control => 128 * 1024,
        FlowLane::RealtimeDatagram => 96 * 1024,
        FlowLane::Latency => 96 * 1024,
        FlowLane::Throughput => 64 * 1024,
        FlowLane::Background => 64 * 1024,
    }
}

fn flow_quantum_bytes(lane: FlowLane) -> u64 {
    match lane {
        FlowLane::Control => 64 * 1024,
        FlowLane::RealtimeDatagram => 64 * 1024,
        FlowLane::Latency => 64 * 1024,
        FlowLane::Throughput => 64 * 1024,
        FlowLane::Background => 32 * 1024,
    }
}

fn deficit_charge_bytes(lane: FlowLane, payload_bytes: usize) -> u64 {
    // DRR fairness assumes bounded transport frames; callers may model a larger logical chunk.
    // Path scoring and queue pressure still use the actual payload bytes.
    let payload_bytes = payload_bytes as u64;
    payload_bytes.min(flow_quantum_bytes(lane)).max(1)
}

fn scheduling_mode(packet: EnqueueRequest, policy: SchedulerPolicy) -> SchedulingMode {
    if packet.lane == FlowLane::Throughput
        && packet.remaining_flow_bytes <= policy.tail_avoidance_threshold_bytes
    {
        SchedulingMode::TailAvoidance
    } else {
        SchedulingMode::Normal
    }
}

fn scheduled_lane(lane: FlowLane, mode: SchedulingMode) -> FlowLane {
    match (lane, mode) {
        (FlowLane::Throughput, SchedulingMode::TailAvoidance) => FlowLane::Latency,
        _ => lane,
    }
}

fn duplicate_path(
    packet: EnqueueRequest,
    primary: PathScore,
    scored: &[PathScore],
    policy: SchedulerPolicy,
) -> Option<PathId> {
    if !packet.duplicate_eligible
        || packet.payload_bytes > policy.duplication_max_payload_bytes
        || !matches!(packet.lane, FlowLane::Control | FlowLane::RealtimeDatagram)
    {
        return None;
    }
    scored
        .iter()
        .copied()
        .find(|score| {
            score.path_id != primary.path_id
                && score.eta_ms <= primary.eta_ms + policy.duplication_max_extra_eta_ms
        })
        .map(|score| score.path_id)
}

fn shared_bottleneck_penalty(
    path: PathSnapshot,
    paths: &[PathSnapshot],
    policy: SchedulerPolicy,
) -> f64 {
    let shares_busy_peer = paths.iter().any(|other| {
        other.id != path.id
            && matches!(other.state, PathState::Active | PathState::Suspect)
            && (other.srtt_ms - path.srtt_ms).abs() <= policy.shared_bottleneck_rtt_window_ms
            && other.queue_bytes.saturating_add(other.bytes_in_flight) > 0
    });
    if shares_busy_peer {
        policy.shared_bottleneck_queue_penalty_ms
    } else {
        0.0
    }
}

fn path_is_schedulable(path: PathSnapshot, lane: FlowLane) -> bool {
    if matches!(path.state, PathState::Failed | PathState::Draining) {
        return false;
    }
    if path.flags.probe_only && lane != FlowLane::Control {
        return false;
    }
    if lane == FlowLane::Throughput && !path.flags.bulk_allowed {
        return false;
    }
    if lane == FlowLane::RealtimeDatagram && path.flags.no_udp {
        return false;
    }
    true
}

fn suspect_penalty_ms(lane: FlowLane, policy: SchedulerPolicy) -> f64 {
    if prefers_low_reorder(lane) {
        0.0
    } else {
        policy.suspect_penalty_ms
    }
}

fn prefers_low_reorder(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mbps(value: f64) -> f64 {
        value * 1_000_000.0
    }

    #[test]
    fn heterogeneous_links_send_interactive_to_low_latency_path() {
        let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
        low_latency.flags.low_latency = true;
        let high_bandwidth =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
        let mut unstable = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 80.0, mbps(100.0));
        unstable.loss_rate = 0.08;
        unstable.jitter_ms = 30.0;

        let choice = choose_path(
            &[low_latency, high_bandwidth, unstable],
            FlowLane::Latency,
            2 * 1024,
            SchedulerPolicy::default(),
        );

        assert_eq!(choice.map(|score| score.path_id), Some(PathId(0)));
    }

    #[test]
    fn heterogeneous_links_send_large_bulk_to_high_bandwidth_path() {
        let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
        let high_bandwidth =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
        let mut unstable = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 80.0, mbps(100.0));
        unstable.loss_rate = 0.08;
        unstable.jitter_ms = 30.0;

        let choice = choose_path(
            &[low_latency, high_bandwidth, unstable],
            FlowLane::Throughput,
            4 * 1024 * 1024,
            SchedulerPolicy::default(),
        );

        assert_eq!(choice.map(|score| score.path_id), Some(PathId(1)));
    }

    #[test]
    fn throughput_scoring_accounts_for_active_bulk_flow_sharing() {
        let mut busy_low_latency =
            PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(200.0));
        busy_low_latency.active_flows = 3;
        let independent = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 80.0, mbps(200.0));

        let choice = choose_path(
            &[busy_low_latency, independent],
            FlowLane::Throughput,
            4 * 1024 * 1024,
            SchedulerPolicy::default(),
        );

        assert_eq!(choice.map(|score| score.path_id), Some(PathId(1)));
    }

    #[test]
    fn failed_and_draining_paths_are_not_schedulable() {
        let mut failed = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1.0, mbps(1000.0));
        failed.state = PathState::Failed;
        let mut draining = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 1.0, mbps(1000.0));
        draining.state = PathState::Draining;
        let active = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 50.0, mbps(10.0));

        let choice = choose_path(
            &[failed, draining, active],
            FlowLane::Latency,
            512,
            SchedulerPolicy::default(),
        );

        assert_eq!(choice.map(|score| score.path_id), Some(PathId(2)));
    }

    #[test]
    fn latency_sensitive_streams_validate_suspect_low_latency_path() {
        let mut low_latency =
            PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, mbps(100.0));
        low_latency.state = PathState::Suspect;
        let active_high_latency =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, mbps(100.0));

        let interactive = choose_path(
            &[low_latency, active_high_latency],
            FlowLane::Latency,
            512,
            SchedulerPolicy::default(),
        );
        let bulk = choose_path(
            &[low_latency, active_high_latency],
            FlowLane::Throughput,
            4 * 1024 * 1024,
            SchedulerPolicy::default(),
        );

        assert_eq!(interactive.map(|score| score.path_id), Some(PathId(0)));
        assert_eq!(bulk.map(|score| score.path_id), Some(PathId(1)));
    }

    #[test]
    fn heterogeneous_scheduler_prioritizes_control_over_bulk_queue() {
        let paths = [
            PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0)),
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 40.0, mbps(100.0)),
        ];
        let mut scheduler = HeterogeneousScheduler::default();
        scheduler.enqueue(EnqueueRequest {
            flow_id: FlowId(1),
            lane: FlowLane::Throughput,
            payload_bytes: 128 * 1024,
            remaining_flow_bytes: 8 * 1024 * 1024,
            duplicate_eligible: false,
        });
        scheduler.enqueue(EnqueueRequest {
            flow_id: FlowId(2),
            lane: FlowLane::Control,
            payload_bytes: 512,
            remaining_flow_bytes: 512,
            duplicate_eligible: true,
        });

        let decision = scheduler
            .schedule_next(&paths, SchedulerPolicy::default())
            .expect("decision");

        assert_eq!(decision.lane, FlowLane::Control);
        assert_eq!(decision.flow_id, FlowId(2));
    }

    #[test]
    fn heterogeneous_scheduler_round_robins_bulk_flows_with_deficit() {
        let paths = [PathSnapshot::new(
            PathId(0),
            UnderlayProtocol::Udp,
            20.0,
            mbps(100.0),
        )];
        let mut scheduler = HeterogeneousScheduler::default();
        for flow_id in [FlowId(1), FlowId(2)] {
            scheduler.enqueue(EnqueueRequest {
                flow_id,
                lane: FlowLane::Throughput,
                payload_bytes: 128 * 1024,
                remaining_flow_bytes: 4 * 1024 * 1024,
                duplicate_eligible: false,
            });
        }

        let first = scheduler
            .schedule_next(&paths, SchedulerPolicy::default())
            .expect("first");
        let second = scheduler
            .schedule_next(&paths, SchedulerPolicy::default())
            .expect("second");

        assert_ne!(first.flow_id, second.flow_id);
    }

    #[test]
    fn heterogeneous_scheduler_schedules_oversized_logical_chunk_with_actual_queue_charge() {
        let paths = [PathSnapshot::new(
            PathId(0),
            UnderlayProtocol::Udp,
            20.0,
            mbps(100.0),
        )];
        let mut scheduler = HeterogeneousScheduler::default();
        scheduler.enqueue(EnqueueRequest {
            flow_id: FlowId(3),
            lane: FlowLane::Throughput,
            payload_bytes: 16 * 1024 * 1024,
            remaining_flow_bytes: 16 * 1024 * 1024,
            duplicate_eligible: false,
        });

        let decision = scheduler
            .schedule_next(&paths, SchedulerPolicy::default())
            .expect("decision");

        assert_eq!(decision.flow_id, FlowId(3));
        assert_eq!(scheduler.queued_path_bytes(PathId(0)), 16 * 1024 * 1024);
    }

    #[test]
    fn heterogeneous_scheduler_switches_bulk_tail_to_latency_sensitive_mode() {
        let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(50.0));
        let high_bandwidth =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
        let mut scheduler = HeterogeneousScheduler::default();
        scheduler.enqueue(EnqueueRequest {
            flow_id: FlowId(7),
            lane: FlowLane::Throughput,
            payload_bytes: 128 * 1024,
            remaining_flow_bytes: 128 * 1024,
            duplicate_eligible: false,
        });

        let decision = scheduler
            .schedule_next(&[low_latency, high_bandwidth], SchedulerPolicy::default())
            .expect("decision");

        assert_eq!(decision.mode, SchedulingMode::TailAvoidance);
        assert_eq!(decision.scheduled_lane, FlowLane::Latency);
        assert_eq!(decision.path_id, PathId(0));
    }

    #[test]
    fn heterogeneous_scheduler_duplicates_small_control_packets_when_cheap() {
        let first = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0));
        let second = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 24.0, mbps(100.0));
        let mut scheduler = HeterogeneousScheduler::default();
        scheduler.enqueue(EnqueueRequest {
            flow_id: FlowId(9),
            lane: FlowLane::Control,
            payload_bytes: 512,
            remaining_flow_bytes: 512,
            duplicate_eligible: true,
        });

        let decision = scheduler
            .schedule_next(&[first, second], SchedulerPolicy::default())
            .expect("decision");

        assert_eq!(decision.path_id, PathId(0));
        assert_eq!(decision.duplicate_path_id, Some(PathId(1)));
        assert_eq!(scheduler.queued_path_bytes(PathId(0)), 512);
        assert_eq!(scheduler.queued_path_bytes(PathId(1)), 512);
    }

    #[test]
    fn heterogeneous_scheduler_penalizes_suspected_shared_bottleneck() {
        let preferred = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 10.0, mbps(100.0));
        let mut busy_peer = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 12.0, mbps(100.0));
        busy_peer.queue_bytes = 1024 * 1024;
        let independent = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 24.0, mbps(100.0));
        let mut scheduler = HeterogeneousScheduler::default();
        scheduler.enqueue(EnqueueRequest {
            flow_id: FlowId(10),
            lane: FlowLane::Latency,
            payload_bytes: 1024,
            remaining_flow_bytes: 1024,
            duplicate_eligible: false,
        });

        let decision = scheduler
            .schedule_next(
                &[preferred, busy_peer, independent],
                SchedulerPolicy::default(),
            )
            .expect("decision");

        assert_eq!(decision.path_id, PathId(2));
    }
}
