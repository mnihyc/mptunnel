use crate::protocol::{PathId, TrafficClass};
use crate::scheduler::{PathSnapshot, PathState, SchedulerPolicy, choose_path};

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
    pub path_id: PathId,
    pub class: TrafficClass,
    pub payload_bytes: usize,
    pub queued_bytes_after: u64,
    pub estimated_completion_ms: f64,
}

#[derive(Debug)]
pub struct Simulator {
    now_ms: f64,
    policy: SchedulerPolicy,
    paths: Vec<VirtualPath>,
}

impl Simulator {
    pub fn new(policy: SchedulerPolicy, paths: Vec<VirtualPath>) -> Self {
        Self {
            now_ms: 0.0,
            policy,
            paths,
        }
    }

    pub fn now_ms(&self) -> f64 {
        self.now_ms
    }

    pub fn paths(&self) -> &[VirtualPath] {
        &self.paths
    }

    pub fn route(&mut self, class: TrafficClass, payload_bytes: usize) -> Option<SimulatedSend> {
        self.apply_failures();
        let snapshots = self
            .paths
            .iter()
            .map(|path| path.snapshot)
            .collect::<Vec<_>>();
        let score = choose_path(&snapshots, class, payload_bytes, self.policy)?;
        let path = self
            .paths
            .iter_mut()
            .find(|path| path.snapshot.id == score.path_id)
            .expect("chosen path must exist");
        path.snapshot.queue_bytes = path
            .snapshot
            .queue_bytes
            .saturating_add(payload_bytes as u64);

        Some(SimulatedSend {
            path_id: score.path_id,
            class,
            payload_bytes,
            queued_bytes_after: path.snapshot.queue_bytes,
            estimated_completion_ms: self.now_ms + score.eta_ms,
        })
    }

    pub fn advance_to(&mut self, now_ms: f64) {
        if now_ms <= self.now_ms {
            self.now_ms = now_ms;
            self.apply_failures();
            return;
        }

        let elapsed_ms = now_ms - self.now_ms;
        self.now_ms = now_ms;
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
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::UnderlayProtocol;
    use crate::scheduler::PathSnapshot;

    fn mbps(value: f64) -> f64 {
        value * 1_000_000.0
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
            .route(TrafficClass::Bulk, 16 * 1024 * 1024)
            .expect("bulk route");
        let interactive = simulator
            .route(TrafficClass::Interactive, 1024)
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
            .route(TrafficClass::Interactive, 512)
            .expect("survivor route");

        assert_eq!(send.path_id, PathId(1));
        assert_eq!(simulator.paths()[0].snapshot.state, PathState::Failed);
    }
}
