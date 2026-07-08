use super::relay_io::{reliable_relay_buffer_len, reliable_relay_scheduler_quantum_cap};
use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct ReliableRelayFlowSignals {
    sent_offset: u64,
    received_offset: u64,
    repair_bytes: usize,
}

impl ReliableRelayFlowSignals {
    pub(super) fn new(sent_offset: u64, received_offset: u64, repair_bytes: usize) -> Self {
        Self {
            sent_offset,
            received_offset,
            repair_bytes,
        }
    }

    pub(super) fn observed_bytes(self) -> u64 {
        self.sent_offset
            .max(self.received_offset)
            .saturating_add(self.repair_bytes as u64)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ReliableRelayFlowDemandTracker {
    current: FlowLane,
    started_at: Instant,
    last_refresh_at: Instant,
    next_rebalance_at: Instant,
    last_rebalance_interval: Duration,
    last_observed_bytes: u64,
    send_rate_bps: f64,
}

impl ReliableRelayFlowDemandTracker {
    pub(super) fn new() -> Self {
        let now = Instant::now();
        Self {
            current: FlowLane::Latency,
            started_at: now,
            last_refresh_at: now,
            next_rebalance_at: now,
            last_rebalance_interval: reliable_flow_rebalance_interval(None),
            last_observed_bytes: 0,
            send_rate_bps: 0.0,
        }
    }

    pub(super) fn refresh(
        &mut self,
        signals: ReliableRelayFlowSignals,
        path: Option<PathSnapshot>,
        mux_limits: MuxLimits,
    ) -> ReliableRelayFlowDecision {
        let now = Instant::now();
        let observed_bytes = signals.observed_bytes();
        let delta_bytes = observed_bytes.saturating_sub(self.last_observed_bytes);
        let elapsed = now.duration_since(self.last_refresh_at);
        if delta_bytes > 0 || elapsed >= QUIC_TIMER_GRANULARITY {
            let sample_rate = delta_bytes as f64 * 8.0
                / elapsed
                    .as_secs_f64()
                    .max(QUIC_TIMER_GRANULARITY.as_secs_f64());
            self.send_rate_bps = if self.send_rate_bps <= 0.0 {
                sample_rate
            } else {
                self.send_rate_bps * 0.75 + sample_rate * 0.25
            };
        }
        self.last_refresh_at = now;
        self.last_observed_bytes = observed_bytes;
        let previous = self.current;
        let rebalance_interval = reliable_flow_rebalance_interval(path);
        self.last_rebalance_interval = rebalance_interval;
        let threshold = reliable_flow_bulk_threshold_bytes(path, mux_limits);
        let demand =
            FlowDemand::reliable_stream(observed_bytes, signals.repair_bytes as u64, threshold);
        let mut demand = demand;
        let flow_age = now.duration_since(self.started_at);
        let idle_gap = delta_bytes == 0 && elapsed >= reliable_flow_interactive_idle_gap(path);
        let rate_threshold = reliable_flow_bulk_rate_threshold_bps(path, mux_limits);
        let rate_proven_bulk = self.send_rate_bps >= rate_threshold;
        let rate_evidence_bytes =
            reliable_flow_rate_bulk_evidence_bytes(path, mux_limits, threshold);
        let prevalidate_bulk = self.current != FlowLane::Throughput
            && observed_bytes
                >= reliable_relay_bulk_prevalidation_threshold_bytes(path, mux_limits);
        let byte_proven_bulk = observed_bytes >= threshold
            && (rate_proven_bulk || flow_age >= reliable_flow_bulk_sustained_age(path));
        let rate_proven_sustained_bulk = rate_proven_bulk && observed_bytes >= rate_evidence_bytes;
        let sustained_bulk = byte_proven_bulk || rate_proven_sustained_bulk;
        if self.current == FlowLane::Throughput && !idle_gap {
            demand.lane = FlowLane::Throughput;
            demand.throughput_weight_ppm = demand
                .throughput_weight_ppm
                .max(FlowDemand::PPM_MAX / 2 + 1);
            demand.latency_weight_ppm =
                FlowDemand::PPM_MAX.saturating_sub(demand.throughput_weight_ppm);
        } else if sustained_bulk {
            demand.lane = FlowLane::Throughput;
            demand.throughput_weight_ppm = demand
                .throughput_weight_ppm
                .max(FlowDemand::PPM_MAX / 2 + 1);
            demand.latency_weight_ppm =
                FlowDemand::PPM_MAX.saturating_sub(demand.throughput_weight_ppm);
        } else if !sustained_bulk {
            demand.lane = FlowLane::Latency;
            demand.throughput_weight_ppm =
                demand.throughput_weight_ppm.min(FlowDemand::PPM_MAX / 2);
            demand.latency_weight_ppm =
                FlowDemand::PPM_MAX.saturating_sub(demand.throughput_weight_ppm);
            if idle_gap {
                self.next_rebalance_at = now;
            }
        }
        self.current = demand.lane;
        let promoted_to_throughput =
            previous != FlowLane::Throughput && self.current == FlowLane::Throughput;
        let rebalance_due = self.current == FlowLane::Throughput
            && (promoted_to_throughput || now >= self.next_rebalance_at);
        ReliableRelayFlowDecision {
            demand,
            previous_lane: previous,
            promoted_to_throughput,
            rebalance_due,
            prevalidate_bulk,
            #[cfg(feature = "lab-diagnostics")]
            observed_bytes,
            #[cfg(feature = "lab-diagnostics")]
            send_rate_bps: self.send_rate_bps,
            #[cfg(feature = "lab-diagnostics")]
            rebalance_interval,
        }
    }

    pub(super) fn should_rebalance(self, update: ReliableRelayFlowDecision) -> bool {
        update.rebalance_due
    }

    pub(super) fn mark_rebalance_attempted(&mut self) {
        self.next_rebalance_at = Instant::now() + self.last_rebalance_interval;
    }
}

fn reliable_flow_bulk_rate_threshold_bps(path: Option<PathSnapshot>, mux_limits: MuxLimits) -> f64 {
    let service_quantum =
        reliable_relay_scheduler_quantum_cap(path, FlowLane::Throughput, mux_limits).max(1);
    service_quantum as f64 * 8.0
        / transport_pto_from_snapshot(path)
            .as_secs_f64()
            .max(QUIC_TIMER_GRANULARITY.as_secs_f64())
}

fn reliable_flow_rate_bulk_evidence_bytes(
    path: Option<PathSnapshot>,
    mux_limits: MuxLimits,
    full_threshold: u64,
) -> u64 {
    let service_quantum =
        reliable_relay_scheduler_quantum_cap(path, FlowLane::Throughput, mux_limits) as u64;
    let floor = service_quantum.max(PATH_OPEN_SCORE_BYTES as u64).max(1);
    floor.min(full_threshold.max(1))
}

fn reliable_flow_interactive_idle_gap(path: Option<PathSnapshot>) -> Duration {
    transport_pto_from_snapshot(path)
}

fn reliable_flow_bulk_sustained_age(path: Option<PathSnapshot>) -> Duration {
    reliable_flow_interactive_idle_gap(path).mul_f64(BBR_DEFAULT_CWND_GAIN)
}

fn reliable_flow_rebalance_interval(path: Option<PathSnapshot>) -> Duration {
    reliable_flow_interactive_idle_gap(path).div_f64(2.0)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ReliableRelayFlowDecision {
    pub(super) demand: FlowDemand,
    pub(super) previous_lane: FlowLane,
    pub(super) promoted_to_throughput: bool,
    pub(super) rebalance_due: bool,
    pub(super) prevalidate_bulk: bool,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) observed_bytes: u64,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) send_rate_bps: f64,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) rebalance_interval: Duration,
}

pub(super) fn reliable_flow_bulk_threshold_bytes(
    path: Option<PathSnapshot>,
    mux_limits: MuxLimits,
) -> u64 {
    let relay_chunk = reliable_relay_buffer_len(mux_limits) as u64;
    let window = mux_limits.max_stream_window_bytes.max(relay_chunk);
    let service_quantum =
        reliable_relay_scheduler_quantum_cap(path, FlowLane::Throughput, mux_limits) as u64;
    let bdp_bytes = path.map_or(relay_chunk, |path| {
        ((path.delivery_rate_bps.max(1.0) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)).ceil() as u64
    });
    bdp_bytes
        .max(service_quantum)
        .max(PATH_OPEN_SCORE_BYTES as u64)
        .min(window)
}

pub(super) fn reliable_relay_bulk_prevalidation_threshold_bytes(
    path: Option<PathSnapshot>,
    mux_limits: MuxLimits,
) -> u64 {
    let bulk_floor = reliable_flow_rate_bulk_evidence_bytes(
        path,
        mux_limits,
        reliable_flow_bulk_threshold_bytes(path, mux_limits),
    );
    let initial_window = PATH_OPEN_SCORE_BYTES as u64;
    let amortized_probe_floor = initial_window.saturating_mul(BBR_MIN_PIPE_CWND_PACKETS as u64);
    if bulk_floor <= initial_window {
        bulk_floor
    } else {
        amortized_probe_floor.clamp(initial_window.saturating_add(1), bulk_floor)
    }
}

pub(super) fn reliable_latency_startup_owner_credit_remaining_bytes(
    lane: FlowLane,
    sent_offset: u64,
    queued_data_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if lane != FlowLane::Latency {
        return usize::MAX;
    }
    let cap = reliable_flow_rate_bulk_evidence_bytes(None, mux_limits, u64::MAX);
    let committed = sent_offset.saturating_add(queued_data_bytes as u64);
    usize::try_from(cap.saturating_sub(committed)).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_demand_rebalances_repeatedly_during_sustained_bulk() {
        let mut tracker = ReliableRelayFlowDemandTracker::new();
        let limits = MuxLimits::default();
        let first = tracker.refresh(
            ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 4, 0, 0),
            None,
            limits,
        );

        assert!(first.promoted_to_throughput);
        assert!(tracker.should_rebalance(first));
        tracker.mark_rebalance_attempted();

        let immediate = tracker.refresh(
            ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 5, 0, 0),
            None,
            limits,
        );
        assert!(!immediate.promoted_to_throughput);
        assert!(!tracker.should_rebalance(immediate));

        tracker.next_rebalance_at = Instant::now() - Duration::from_millis(1);
        let recurring = tracker.refresh(
            ReliableRelayFlowSignals::new(reliable_relay_buffer_len(limits) as u64 * 6, 0, 0),
            None,
            limits,
        );
        assert!(!recurring.promoted_to_throughput);
        assert!(tracker.should_rebalance(recurring));
    }

    #[test]
    fn rate_evidence_does_not_promote_before_service_quantum_floor() {
        let mut tracker = ReliableRelayFlowDemandTracker::new();
        let limits = MuxLimits::default();
        let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
        let below_floor = floor.saturating_sub(1).max(1);

        let decision = tracker.refresh(
            ReliableRelayFlowSignals::new(below_floor, 0, 0),
            None,
            limits,
        );

        assert_eq!(decision.demand.lane, FlowLane::Latency);
        assert!(!decision.promoted_to_throughput);
        assert!(
            decision.prevalidate_bulk,
            "amortized bulk validation should be allowed before full throughput promotion"
        );
    }

    #[test]
    fn initial_window_response_stays_latency_and_does_not_prevalidate_bulk() {
        let mut tracker = ReliableRelayFlowDemandTracker::new();
        let limits = MuxLimits::default();
        let threshold = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
        assert!(threshold > PATH_OPEN_SCORE_BYTES as u64);

        let decision = tracker.refresh(
            ReliableRelayFlowSignals::new(PATH_OPEN_SCORE_BYTES as u64, 0, 0),
            None,
            limits,
        );

        assert_eq!(decision.demand.lane, FlowLane::Latency);
        assert!(!decision.promoted_to_throughput);
        assert!(
            !decision.prevalidate_bulk,
            "one initial window is ordinary short-flow traffic, not enough to spawn per-stream bulk validation"
        );
    }

    #[test]
    fn bulk_prevalidation_uses_amortized_floor_before_throughput_promotion() {
        let mut tracker = ReliableRelayFlowDemandTracker::new();
        let limits = MuxLimits::default();
        let full_floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);
        let prevalidation_floor = reliable_relay_bulk_prevalidation_threshold_bytes(None, limits);

        assert!(prevalidation_floor > PATH_OPEN_SCORE_BYTES as u64);
        assert!(
            prevalidation_floor < full_floor,
            "prevalidation should start after several startup windows, not wait for the full throughput promotion floor"
        );

        let decision = tracker.refresh(
            ReliableRelayFlowSignals::new(prevalidation_floor, 0, 0),
            None,
            limits,
        );

        assert_eq!(decision.demand.lane, FlowLane::Latency);
        assert!(!decision.promoted_to_throughput);
        assert!(decision.prevalidate_bulk);
    }

    #[test]
    fn rate_evidence_promotes_after_service_quantum_floor() {
        let mut tracker = ReliableRelayFlowDemandTracker::new();
        let limits = MuxLimits::default();
        let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);

        let decision = tracker.refresh(ReliableRelayFlowSignals::new(floor, 0, 0), None, limits);

        assert_eq!(decision.demand.lane, FlowLane::Throughput);
        assert!(decision.promoted_to_throughput);
        assert!(tracker.should_rebalance(decision));
    }

    #[test]
    fn latency_startup_owner_credit_stops_after_bulk_evidence_floor() {
        let limits = MuxLimits::default();
        let floor = reliable_flow_rate_bulk_evidence_bytes(None, limits, u64::MAX);

        assert_eq!(
            reliable_latency_startup_owner_credit_remaining_bytes(
                FlowLane::Latency,
                floor.saturating_sub(1),
                0,
                limits,
            ),
            1
        );
        assert_eq!(
            reliable_latency_startup_owner_credit_remaining_bytes(
                FlowLane::Latency,
                floor,
                0,
                limits,
            ),
            0
        );
        assert_eq!(
            reliable_latency_startup_owner_credit_remaining_bytes(
                FlowLane::Latency,
                floor / 2,
                usize::try_from(floor - floor / 2).unwrap(),
                limits,
            ),
            0
        );
        assert_eq!(
            reliable_latency_startup_owner_credit_remaining_bytes(
                FlowLane::Throughput,
                floor,
                usize::MAX,
                limits,
            ),
            usize::MAX
        );
    }
}
