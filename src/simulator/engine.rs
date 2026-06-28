use crate::protocol::PathId;
use crate::scheduler::{
    EnqueueRequest, FlowId, FlowLane, HeterogeneousScheduler, PathSnapshot, PathState,
    SchedulerPolicy, SchedulingMode,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct VirtualPath {
    pub snapshot: PathSnapshot,
    pub fail_at_ms: Option<f64>,
}

impl VirtualPath {
    pub fn new(snapshot: PathSnapshot) -> Self {
        Self {
            snapshot,
            fail_at_ms: None,
        }
    }

    pub fn with_failure_at(mut self, fail_at_ms: f64) -> Self {
        self.fail_at_ms = Some(fail_at_ms);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimulatedSend {
    pub flow_id: FlowId,
    pub path_id: PathId,
    pub duplicate_path_id: Option<PathId>,
    pub lane: FlowLane,
    pub scheduled_lane: FlowLane,
    pub mode: SchedulingMode,
    pub payload_bytes: usize,
    pub queued_bytes_after: u64,
    pub estimated_completion_ms: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimulatedChunkAttempt {
    pub path_id: PathId,
    pub lane: FlowLane,
    pub payload_bytes: usize,
    pub scheduled_at_ms: f64,
    pub estimated_completion_ms: f64,
    pub repair_attempt: u32,
    pub delivered: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SimulatedTransfer {
    pub lane: FlowLane,
    pub total_bytes: usize,
    pub chunk_bytes: usize,
    pub start_ms: f64,
    pub completion_ms: f64,
    pub start_capacity_bps: f64,
    pub attempts: Vec<SimulatedChunkAttempt>,
    pub repaired_chunks: usize,
    pub failover_gap_ms: Option<f64>,
}

impl SimulatedTransfer {
    pub fn duration_ms(&self) -> f64 {
        (self.completion_ms - self.start_ms).max(0.0)
    }

    pub fn achieved_goodput_bps(&self) -> f64 {
        let duration_ms = self.duration_ms();
        if duration_ms <= 0.0 {
            return 0.0;
        }
        self.total_bytes as f64 * 8000.0 / duration_ms
    }

    pub fn aggregation_efficiency(&self, server_capacity_bps: f64) -> f64 {
        let capacity = self.start_capacity_bps.min(server_capacity_bps).max(1.0);
        self.achieved_goodput_bps() / capacity
    }

    pub fn path_bytes(&self) -> BTreeMap<PathId, usize> {
        let mut bytes = BTreeMap::new();
        for attempt in self.attempts.iter().filter(|attempt| attempt.delivered) {
            *bytes.entry(attempt.path_id).or_insert(0) += attempt.payload_bytes;
        }
        bytes
    }

    pub fn bulk_tail_penalty_ms(&self) -> f64 {
        let mut completions = self
            .attempts
            .iter()
            .filter(|attempt| attempt.delivered)
            .map(|attempt| attempt.estimated_completion_ms)
            .collect::<Vec<_>>();
        if completions.len() < 2 {
            return 0.0;
        }
        completions.sort_by(f64::total_cmp);
        let p95_index = ((completions.len() - 1) as f64 * 0.95).floor() as usize;
        let p95 = completions[p95_index];
        completions[completions.len() - 1] - p95
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InteractiveBurst {
    pub sends: Vec<SimulatedSend>,
}

impl InteractiveBurst {
    pub fn p95_latency_ms(&self) -> Option<f64> {
        let mut latencies = self
            .sends
            .iter()
            .map(|send| send.estimated_completion_ms)
            .collect::<Vec<_>>();
        if latencies.is_empty() {
            return None;
        }
        latencies.sort_by(f64::total_cmp);
        let index = ((latencies.len() - 1) as f64 * 0.95).ceil() as usize;
        Some(latencies[index])
    }
}

#[derive(Debug)]
pub struct Simulator {
    now_ms: f64,
    policy: SchedulerPolicy,
    paths: Vec<VirtualPath>,
    scheduler: HeterogeneousScheduler,
    next_flow_id: u64,
}

impl Simulator {
    pub fn new(policy: SchedulerPolicy, paths: Vec<VirtualPath>) -> Self {
        Self {
            now_ms: 0.0,
            policy,
            paths,
            scheduler: HeterogeneousScheduler::default(),
            next_flow_id: 0,
        }
    }

    pub fn now_ms(&self) -> f64 {
        self.now_ms
    }

    pub fn paths(&self) -> &[VirtualPath] {
        &self.paths
    }

    pub fn route(&mut self, lane: FlowLane, payload_bytes: usize) -> Option<SimulatedSend> {
        let flow_id = self.allocate_flow_id();
        self.route_flow(
            flow_id,
            lane,
            payload_bytes,
            payload_bytes,
            duplicate_default(lane),
        )
    }

    pub fn route_flow(
        &mut self,
        flow_id: FlowId,
        lane: FlowLane,
        payload_bytes: usize,
        remaining_flow_bytes: usize,
        duplicate_eligible: bool,
    ) -> Option<SimulatedSend> {
        self.apply_failures();
        self.scheduler.enqueue(EnqueueRequest {
            flow_id,
            lane,
            payload_bytes,
            remaining_flow_bytes,
            duplicate_eligible,
        });
        let snapshots = self.path_snapshots();
        let decision = self.scheduler.schedule_next(&snapshots, self.policy)?;
        Some(SimulatedSend {
            flow_id: decision.flow_id,
            path_id: decision.path_id,
            duplicate_path_id: decision.duplicate_path_id,
            lane,
            scheduled_lane: decision.scheduled_lane,
            mode: decision.mode,
            payload_bytes,
            queued_bytes_after: self.scheduler.queued_path_bytes(decision.path_id),
            estimated_completion_ms: self.now_ms + decision.estimated_completion_ms,
        })
    }

    pub fn schedule_transfer(
        &mut self,
        lane: FlowLane,
        total_bytes: usize,
        chunk_bytes: usize,
    ) -> Option<SimulatedTransfer> {
        if total_bytes == 0 || chunk_bytes == 0 {
            return None;
        }
        let start_ms = self.now_ms;
        let start_capacity_bps = self.healthy_capacity_bps();
        let mut remaining = total_bytes;
        let mut attempts = Vec::new();
        let flow_id = self.allocate_flow_id();
        while remaining > 0 {
            let payload_bytes = remaining.min(chunk_bytes);
            let scheduled_at_ms = self.now_ms;
            let send = self.route_flow(flow_id, lane, payload_bytes, remaining, false)?;
            attempts.push(SimulatedChunkAttempt {
                path_id: send.path_id,
                lane,
                payload_bytes,
                scheduled_at_ms,
                estimated_completion_ms: send.estimated_completion_ms,
                repair_attempt: 0,
                delivered: true,
            });
            remaining -= payload_bytes;
        }
        let completion_ms = transfer_completion_ms(&attempts).unwrap_or(start_ms);
        Some(SimulatedTransfer {
            lane,
            total_bytes,
            chunk_bytes,
            start_ms,
            completion_ms,
            start_capacity_bps,
            attempts,
            repaired_chunks: 0,
            failover_gap_ms: None,
        })
    }

    pub fn schedule_transfer_with_repair(
        &mut self,
        lane: FlowLane,
        total_bytes: usize,
        chunk_bytes: usize,
        repair_delay_ms: f64,
    ) -> Option<SimulatedTransfer> {
        let start_ms = self.now_ms;
        let start_capacity_bps = self.healthy_capacity_bps();
        let mut transfer = self.schedule_transfer(lane, total_bytes, chunk_bytes)?;
        transfer.start_capacity_bps = start_capacity_bps;
        let mut repair_attempt = 0u32;
        let mut first_failure_ms = None;
        let mut repaired_chunks = 0usize;

        loop {
            let lost = transfer
                .attempts
                .iter_mut()
                .enumerate()
                .filter_map(|(index, attempt)| {
                    if !attempt.delivered {
                        return None;
                    }
                    let fail_at_ms = self.path_fail_at(attempt.path_id)?;
                    (fail_at_ms >= attempt.scheduled_at_ms
                        && fail_at_ms < attempt.estimated_completion_ms)
                        .then_some((index, fail_at_ms, attempt.payload_bytes))
                })
                .collect::<Vec<_>>();
            if lost.is_empty() {
                break;
            }

            repair_attempt = repair_attempt.saturating_add(1);
            let failure_ms = lost
                .iter()
                .map(|(_, fail_at_ms, _)| *fail_at_ms)
                .min_by(f64::total_cmp)
                .expect("lost chunks have failure times");
            first_failure_ms =
                Some(first_failure_ms.map_or(failure_ms, |first: f64| first.min(failure_ms)));
            for (index, _, _) in &lost {
                transfer.attempts[*index].delivered = false;
            }

            self.advance_to(failure_ms + repair_delay_ms.max(0.0));
            for (_, _, payload_bytes) in lost {
                let scheduled_at_ms = self.now_ms;
                let send = self.route(lane, payload_bytes)?;
                transfer.attempts.push(SimulatedChunkAttempt {
                    path_id: send.path_id,
                    lane,
                    payload_bytes,
                    scheduled_at_ms,
                    estimated_completion_ms: send.estimated_completion_ms,
                    repair_attempt,
                    delivered: true,
                });
                repaired_chunks += 1;
            }
        }

        transfer.start_ms = start_ms;
        transfer.completion_ms = transfer_completion_ms(&transfer.attempts).unwrap_or(start_ms);
        transfer.repaired_chunks = repaired_chunks;
        transfer.failover_gap_ms =
            first_failure_ms.and_then(|failure_ms| failover_gap_ms(&transfer.attempts, failure_ms));
        Some(transfer)
    }

    pub fn route_interactive_burst(
        &mut self,
        request_bytes: usize,
        count: usize,
        spacing_ms: f64,
    ) -> Option<InteractiveBurst> {
        let mut sends = Vec::with_capacity(count);
        for _ in 0..count {
            let started_at_ms = self.now_ms;
            let mut send = self.route(FlowLane::Latency, request_bytes)?;
            send.estimated_completion_ms -= started_at_ms;
            sends.push(send);
            self.advance_to(started_at_ms + spacing_ms.max(0.0));
        }
        Some(InteractiveBurst { sends })
    }

    pub fn healthy_capacity_bps(&self) -> f64 {
        self.paths
            .iter()
            .filter(|path| matches!(path.snapshot.state, PathState::Active | PathState::Suspect))
            .map(|path| path.snapshot.delivery_rate_bps.max(0.0))
            .sum()
    }

    pub fn advance_to(&mut self, now_ms: f64) {
        if now_ms <= self.now_ms {
            self.now_ms = now_ms;
            self.apply_failures();
            return;
        }

        let elapsed_ms = now_ms - self.now_ms;
        self.now_ms = now_ms;
        let snapshots = self.path_snapshots();
        self.scheduler.advance_time(&snapshots, elapsed_ms);
        for path in &mut self.paths {
            if !matches!(path.snapshot.state, PathState::Active | PathState::Suspect) {
                continue;
            }
            let drained = (path.snapshot.delivery_rate_bps.max(0.0) * elapsed_ms / 8000.0) as u64;
            path.snapshot.queue_bytes = path.snapshot.queue_bytes.saturating_sub(drained);
        }
        self.apply_failures();
    }

    fn apply_failures(&mut self) {
        for path in &mut self.paths {
            if path
                .fail_at_ms
                .is_some_and(|fail_at_ms| self.now_ms >= fail_at_ms)
            {
                path.snapshot.state = PathState::Failed;
                path.snapshot.queue_bytes = 0;
                path.snapshot.bytes_in_flight = 0;
                self.scheduler.remove_path(path.snapshot.id);
            }
        }
    }

    fn path_fail_at(&self, path_id: PathId) -> Option<f64> {
        self.paths
            .iter()
            .find(|path| path.snapshot.id == path_id)
            .and_then(|path| path.fail_at_ms)
    }

    fn path_snapshots(&self) -> Vec<PathSnapshot> {
        self.paths.iter().map(|path| path.snapshot).collect()
    }

    fn allocate_flow_id(&mut self) -> FlowId {
        let flow_id = FlowId(self.next_flow_id);
        self.next_flow_id = self.next_flow_id.saturating_add(1);
        flow_id
    }
}

fn duplicate_default(lane: FlowLane) -> bool {
    matches!(lane, FlowLane::Control | FlowLane::RealtimeDatagram)
}

fn transfer_completion_ms(attempts: &[SimulatedChunkAttempt]) -> Option<f64> {
    attempts
        .iter()
        .filter(|attempt| attempt.delivered)
        .map(|attempt| attempt.estimated_completion_ms)
        .max_by(f64::total_cmp)
}

fn failover_gap_ms(attempts: &[SimulatedChunkAttempt], failure_ms: f64) -> Option<f64> {
    let last_before = attempts
        .iter()
        .filter(|attempt| attempt.delivered && attempt.estimated_completion_ms <= failure_ms)
        .map(|attempt| attempt.estimated_completion_ms)
        .max_by(f64::total_cmp)
        .unwrap_or(failure_ms);
    let first_after = attempts
        .iter()
        .filter(|attempt| {
            attempt.delivered
                && attempt.repair_attempt > 0
                && attempt.estimated_completion_ms > failure_ms
        })
        .map(|attempt| attempt.estimated_completion_ms)
        .min_by(f64::total_cmp)?;
    Some(first_after - last_before)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::UnderlayProtocol;
    use crate::scheduler::PathSnapshot;

    fn mbps(value: f64) -> f64 {
        value * 1_000_000.0
    }

    fn assert_between(value: f64, min: f64, max: f64) {
        assert!(
            value >= min && value <= max,
            "{value} is outside [{min}, {max}]"
        );
    }

    #[test]
    fn simulator_keeps_interactive_traffic_off_bulk_queue() {
        let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
        low_latency.flags.low_latency = true;
        let high_bandwidth =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![
                VirtualPath::new(low_latency),
                VirtualPath::new(high_bandwidth),
            ],
        );

        let bulk = simulator
            .route(FlowLane::Throughput, 16 * 1024 * 1024)
            .expect("bulk route");
        let interactive = simulator
            .route(FlowLane::Latency, 1024)
            .expect("interactive route");

        assert_eq!(bulk.path_id, PathId(1));
        assert_eq!(interactive.path_id, PathId(0));
    }

    #[test]
    fn simulator_failure_injection_removes_dead_path() {
        let fast = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0));
        let slow = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 120.0, mbps(100.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![
                VirtualPath::new(fast).with_failure_at(100.0),
                VirtualPath::new(slow),
            ],
        );

        simulator.advance_to(100.0);
        let send = simulator
            .route(FlowLane::Latency, 512)
            .expect("survivor route");

        assert_eq!(send.path_id, PathId(1));
        assert_eq!(simulator.paths()[0].snapshot.state, PathState::Failed);
    }

    #[test]
    fn simulator_bulk_transfer_tracks_aggregation_efficiency() {
        let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
        low_latency.flags.low_latency = true;
        let high_bandwidth =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![
                VirtualPath::new(low_latency),
                VirtualPath::new(high_bandwidth),
            ],
        );

        let transfer = simulator
            .schedule_transfer(FlowLane::Throughput, 64 * 1024 * 1024, 1024 * 1024)
            .expect("bulk transfer");
        let path_bytes = transfer.path_bytes();

        assert!(path_bytes.get(&PathId(0)).copied().unwrap_or_default() > 0);
        assert!(path_bytes.get(&PathId(1)).copied().unwrap_or_default() > 0);
        assert_between(transfer.aggregation_efficiency(mbps(1000.0)), 0.70, 1.05);
    }

    #[test]
    fn simulator_reinjects_failed_chunks_and_reports_failover_gap() {
        let fast = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0));
        let slow = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 80.0, mbps(100.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![
                VirtualPath::new(fast).with_failure_at(40.0),
                VirtualPath::new(slow),
            ],
        );

        let transfer = simulator
            .schedule_transfer_with_repair(FlowLane::Throughput, 4 * 1024 * 1024, 256 * 1024, 10.0)
            .expect("repaired transfer");
        let gap = transfer.failover_gap_ms.expect("failover gap");

        assert!(transfer.repaired_chunks > 0);
        assert_between(gap, 0.0, 500.0);
        assert!(
            transfer
                .attempts
                .iter()
                .any(|attempt| attempt.repair_attempt > 0 && attempt.path_id == PathId(1))
        );
    }

    #[test]
    fn simulator_measures_interactive_p95_under_bulk_load() {
        let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
        low_latency.flags.low_latency = true;
        low_latency.flags.bulk_allowed = false;
        let high_bandwidth =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![
                VirtualPath::new(low_latency),
                VirtualPath::new(high_bandwidth),
            ],
        );

        simulator
            .schedule_transfer(FlowLane::Throughput, 64 * 1024 * 1024, 1024 * 1024)
            .expect("bulk transfer");
        let burst = simulator
            .route_interactive_burst(1024, 20, 5.0)
            .expect("interactive burst");
        let p95 = burst.p95_latency_ms().expect("p95 latency");

        assert_between(p95, 0.0, 40.0);
        assert!(burst.sends.iter().all(|send| send.path_id == PathId(0)));
    }

    #[test]
    fn simulator_reports_bulk_tail_penalty_for_heterogeneous_paths() {
        let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(80.0));
        let high_bandwidth =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![
                VirtualPath::new(low_latency),
                VirtualPath::new(high_bandwidth),
            ],
        );

        let transfer = simulator
            .schedule_transfer(FlowLane::Throughput, 32 * 1024 * 1024, 512 * 1024)
            .expect("bulk transfer");

        assert_between(transfer.bulk_tail_penalty_ms(), 0.0, 250.0);
    }

    #[test]
    fn simulator_duplicates_small_control_packets_when_cheap() {
        let first = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(100.0));
        let second = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 24.0, mbps(100.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![VirtualPath::new(first), VirtualPath::new(second)],
        );

        let send = simulator
            .route(FlowLane::Control, 512)
            .expect("control route");

        assert_eq!(send.path_id, PathId(0));
        assert_eq!(send.duplicate_path_id, Some(PathId(1)));
    }

    #[test]
    fn simulator_routes_bulk_tail_in_tail_avoidance_mode() {
        let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(50.0));
        let high_bandwidth =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![
                VirtualPath::new(low_latency),
                VirtualPath::new(high_bandwidth),
            ],
        );

        let send = simulator
            .route_flow(
                FlowId(77),
                FlowLane::Throughput,
                128 * 1024,
                128 * 1024,
                false,
            )
            .expect("tail route");

        assert_eq!(send.mode, SchedulingMode::TailAvoidance);
        assert_eq!(send.scheduled_lane, FlowLane::Latency);
        assert_eq!(send.path_id, PathId(0));
    }

    #[test]
    fn simulator_avoids_suspected_shared_bottleneck_path() {
        let preferred = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 10.0, mbps(100.0));
        let mut busy_peer = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 12.0, mbps(100.0));
        busy_peer.queue_bytes = 1024 * 1024;
        let independent = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 24.0, mbps(100.0));
        let mut simulator = Simulator::new(
            SchedulerPolicy::default(),
            vec![
                VirtualPath::new(preferred),
                VirtualPath::new(busy_peer),
                VirtualPath::new(independent),
            ],
        );

        let send = simulator
            .route(FlowLane::Latency, 1024)
            .expect("interactive route");

        assert_eq!(send.path_id, PathId(2));
    }
}
