use crate::protocol::{PathCapabilities, PathId, TrafficClass, UnderlayProtocol};

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
}

impl Default for SchedulerPolicy {
    fn default() -> Self {
        Self {
            expensive_penalty_ms: 25.0,
            suspect_penalty_ms: 250.0,
            backup_penalty_ms: 100.0,
            tcp_reorder_penalty_ms: 50.0,
            loss_penalty_scale_ms: 500.0,
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
    class: TrafficClass,
    payload_bytes: usize,
    policy: SchedulerPolicy,
) -> Option<PathScore> {
    paths
        .iter()
        .filter_map(|path| score_path(*path, class, payload_bytes, policy))
        .min_by(|left, right| left.eta_ms.total_cmp(&right.eta_ms))
}

pub fn score_path(
    path: PathSnapshot,
    class: TrafficClass,
    payload_bytes: usize,
    policy: SchedulerPolicy,
) -> Option<PathScore> {
    if !path_is_schedulable(path, class) {
        return None;
    }

    let rate = path.delivery_rate_bps.max(1.0);
    let queued_bits = path.queue_bytes.saturating_add(path.bytes_in_flight) as f64 * 8.0;
    let payload_bits = payload_bytes as f64 * 8.0;

    let mut eta_ms = path.srtt_ms / 2.0;
    eta_ms += queued_bits / rate * 1000.0;
    eta_ms += payload_bits / rate * 1000.0;
    eta_ms += path.jitter_ms;
    eta_ms += path.loss_rate.clamp(0.0, 1.0) * policy.loss_penalty_scale_ms;

    if path.state == PathState::Suspect {
        eta_ms += policy.suspect_penalty_ms;
    }
    if path.flags.backup {
        eta_ms += policy.backup_penalty_ms;
    }
    if path.flags.expensive {
        eta_ms += policy.expensive_penalty_ms;
    }
    if path.underlay == UnderlayProtocol::Tcp && prefers_low_reorder(class) {
        eta_ms += policy.tcp_reorder_penalty_ms;
    }
    if class == TrafficClass::Control && path.flags.low_latency {
        eta_ms -= path.srtt_ms.min(10.0) * 0.25;
    }

    Some(PathScore {
        path_id: path.id,
        eta_ms,
    })
}

fn path_is_schedulable(path: PathSnapshot, class: TrafficClass) -> bool {
    if matches!(path.state, PathState::Failed | PathState::Draining) {
        return false;
    }
    if path.flags.probe_only && class != TrafficClass::Control {
        return false;
    }
    if class == TrafficClass::Bulk && !path.flags.bulk_allowed {
        return false;
    }
    if class == TrafficClass::RealtimeDatagram && path.flags.no_udp {
        return false;
    }
    true
}

fn prefers_low_reorder(class: TrafficClass) -> bool {
    matches!(
        class,
        TrafficClass::Control | TrafficClass::Interactive | TrafficClass::RealtimeDatagram
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
            TrafficClass::Interactive,
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
            TrafficClass::Bulk,
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
            TrafficClass::Interactive,
            512,
            SchedulerPolicy::default(),
        );

        assert_eq!(choice.map(|score| score.path_id), Some(PathId(2)));
    }
}
