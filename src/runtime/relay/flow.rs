use crate::model::capacity::{
    MIN_RELIABLE_PIPE_PACKETS, PATH_OPEN_SCORE_BYTES, QUIC_TIMER_GRANULARITY,
    reliable_relay_buffer_len, reliable_relay_scheduler_quantum_cap,
};
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReliableRelayFlowSignals {
    sent_offset: u64,
    received_offset: u64,
    pending_product_bytes: usize,
}

impl ReliableRelayFlowSignals {
    pub(in crate::runtime) fn new(sent_offset: u64, received_offset: u64) -> Self {
        Self {
            sent_offset,
            received_offset,
            pending_product_bytes: 0,
        }
    }

    /// Pending product work preserves proven demand while transport
    /// backpressure stops source reads. It is never promotion evidence.
    pub(in crate::runtime) fn with_pending_product_bytes(
        mut self,
        pending_product_bytes: usize,
    ) -> Self {
        self.pending_product_bytes = pending_product_bytes;
        self
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn observed_bytes(self) -> u64 {
        self.sent_offset.max(self.received_offset)
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReliableRelayFlowDemandTracker {
    current: TrafficClass,
    epoch_started_at: Option<Instant>,
    epoch_bytes: u64,
    last_progress_at: Instant,
    next_rebalance_at: Instant,
    last_rebalance_interval: Duration,
    last_sent_offset: u64,
    last_received_offset: u64,
    product_rate_bps: f64,
}

impl ReliableRelayFlowDemandTracker {
    pub(in crate::runtime) fn new() -> Self {
        Self::with_initial_lane(TrafficClass::Latency)
    }

    /// Seeds a reliable stream from one peer hint; later decisions use only
    /// observed demand and live path timing, so the hint cannot become sticky.
    pub(in crate::runtime) fn with_initial_lane(initial: TrafficClass) -> Self {
        let now = Instant::now();
        Self {
            current: if initial.is_bulk() {
                TrafficClass::Throughput
            } else {
                TrafficClass::Latency
            },
            epoch_started_at: None,
            epoch_bytes: 0,
            last_progress_at: now,
            next_rebalance_at: now,
            last_rebalance_interval: reliable_flow_rebalance_interval(None),
            last_sent_offset: 0,
            last_received_offset: 0,
            product_rate_bps: 0.0,
        }
    }

    pub(in crate::runtime) fn refresh(
        &mut self,
        signals: ReliableRelayFlowSignals,
        path: Option<PathSnapshot>,
        mux_limits: MuxLimits,
    ) -> ReliableRelayFlowDecision {
        let now = Instant::now();
        #[cfg(feature = "lab-diagnostics")]
        let observed_bytes = signals.observed_bytes();
        let sent_delta = signals.sent_offset.saturating_sub(self.last_sent_offset);
        let received_delta = signals
            .received_offset
            .saturating_sub(self.last_received_offset);
        let product_delta = sent_delta.max(received_delta);
        if product_delta > 0 {
            self.last_progress_at = now;
            self.epoch_started_at.get_or_insert(now);
            self.epoch_bytes = self.epoch_bytes.saturating_add(product_delta);
        }
        self.last_sent_offset = self.last_sent_offset.max(signals.sent_offset);
        self.last_received_offset = self.last_received_offset.max(signals.received_offset);
        let previous = self.current;
        let rebalance_interval = reliable_flow_rebalance_interval(path);
        self.last_rebalance_interval = rebalance_interval;
        let threshold = reliable_flow_bulk_threshold_bytes(path, mux_limits);
        // Poll frequency is not demand. Fresh bytes prove bulk demand, while
        // already-admitted product work only prevents a proven bulk flow from
        // being mistaken for idle during transport backpressure.
        let has_pending_product_work = signals.pending_product_bytes > 0;
        let idle_gap = product_delta == 0
            && !has_pending_product_work
            && now.duration_since(self.last_progress_at)
                >= reliable_flow_interactive_idle_gap(path);
        if idle_gap {
            self.epoch_started_at = None;
            self.epoch_bytes = 0;
            self.product_rate_bps = 0.0;
        }
        let flow_age = self
            .epoch_started_at
            .map_or(Duration::ZERO, |started_at| now.duration_since(started_at));
        // Average admitted product demand over the active epoch is independent
        // of how often transport events happen to wake this relay task.
        self.product_rate_bps = if self.epoch_bytes == 0 {
            0.0
        } else {
            self.epoch_bytes as f64 * 8.0
                / flow_age
                    .as_secs_f64()
                    .max(QUIC_TIMER_GRANULARITY.as_secs_f64())
        };
        let rate_threshold = reliable_flow_bulk_rate_threshold_bps(path, mux_limits);
        let rate_proven_bulk = self.product_rate_bps >= rate_threshold;
        let rate_evidence_bytes =
            reliable_flow_rate_bulk_evidence_bytes(path, mux_limits, threshold);
        let preopen_additional_paths = !idle_gap
            && self.current != TrafficClass::Throughput
            && self.epoch_bytes >= reliable_relay_bulk_path_open_threshold_bytes(path, mux_limits);
        // Reaching the path-sized demand threshold is itself sufficient bulk
        // evidence. Requiring another event or timer here would deadlock the
        // latency startup admission boundary at exactly the same byte offset.
        let byte_proven_bulk = self.epoch_bytes >= threshold;
        let rate_proven_sustained_bulk =
            rate_proven_bulk && self.epoch_bytes >= rate_evidence_bytes;
        let sustained_bulk = byte_proven_bulk || rate_proven_sustained_bulk;
        let lane = if !idle_gap && (self.current == TrafficClass::Throughput || sustained_bulk) {
            TrafficClass::Throughput
        } else {
            if idle_gap {
                self.next_rebalance_at = now;
            }
            TrafficClass::Latency
        };
        self.current = lane;
        let promoted_to_throughput =
            previous != TrafficClass::Throughput && self.current == TrafficClass::Throughput;
        let rebalance_due = self.current == TrafficClass::Throughput
            && (promoted_to_throughput || now >= self.next_rebalance_at);
        ReliableRelayFlowDecision {
            lane,
            previous_lane: previous,
            promoted_to_throughput,
            rebalance_due,
            preopen_additional_paths,
            #[cfg(feature = "lab-diagnostics")]
            observed_bytes,
            #[cfg(feature = "lab-diagnostics")]
            product_rate_bps: self.product_rate_bps,
            #[cfg(feature = "lab-diagnostics")]
            rebalance_interval,
        }
    }

    pub(in crate::runtime) fn should_rebalance(self, update: ReliableRelayFlowDecision) -> bool {
        update.rebalance_due
    }

    pub(in crate::runtime) fn mark_rebalance_attempted(&mut self) {
        self.next_rebalance_at = Instant::now() + self.last_rebalance_interval;
    }
}

fn reliable_flow_bulk_rate_threshold_bps(path: Option<PathSnapshot>, mux_limits: MuxLimits) -> f64 {
    let service_quantum =
        reliable_relay_scheduler_quantum_cap(path, TrafficClass::Throughput, mux_limits).max(1);
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
        reliable_relay_scheduler_quantum_cap(path, TrafficClass::Throughput, mux_limits) as u64;
    let floor = service_quantum.max(PATH_OPEN_SCORE_BYTES as u64).max(1);
    floor.min(full_threshold.max(1))
}

fn reliable_flow_interactive_idle_gap(path: Option<PathSnapshot>) -> Duration {
    transport_pto_from_snapshot(path)
}

fn reliable_flow_rebalance_interval(path: Option<PathSnapshot>) -> Duration {
    reliable_flow_interactive_idle_gap(path).div_f64(2.0)
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReliableRelayFlowDecision {
    pub(in crate::runtime) lane: TrafficClass,
    pub(in crate::runtime) previous_lane: TrafficClass,
    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(in crate::runtime) promoted_to_throughput: bool,
    pub(in crate::runtime) rebalance_due: bool,
    pub(in crate::runtime) preopen_additional_paths: bool,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) observed_bytes: u64,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) product_rate_bps: f64,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) rebalance_interval: Duration,
}

pub(in crate::runtime) fn reliable_flow_bulk_threshold_bytes(
    path: Option<PathSnapshot>,
    mux_limits: MuxLimits,
) -> u64 {
    let relay_chunk = reliable_relay_buffer_len(mux_limits) as u64;
    let window = mux_limits.max_stream_window_bytes.max(relay_chunk);
    let service_quantum =
        reliable_relay_scheduler_quantum_cap(path, TrafficClass::Throughput, mux_limits) as u64;
    let bdp_bytes = path.map_or(relay_chunk, |path| {
        ((path.delivery_rate_bps.max(1.0) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)).ceil() as u64
    });
    bdp_bytes
        .max(service_quantum)
        .max(PATH_OPEN_SCORE_BYTES as u64)
        .min(window)
}

pub(in crate::runtime) fn reliable_relay_bulk_path_open_threshold_bytes(
    path: Option<PathSnapshot>,
    mux_limits: MuxLimits,
) -> u64 {
    let bulk_floor = reliable_flow_rate_bulk_evidence_bytes(
        path,
        mux_limits,
        reliable_flow_bulk_threshold_bytes(path, mux_limits),
    );
    let initial_window = PATH_OPEN_SCORE_BYTES as u64;
    let amortized_probe_floor = initial_window.saturating_mul(MIN_RELIABLE_PIPE_PACKETS as u64);
    if bulk_floor <= initial_window {
        bulk_floor
    } else {
        amortized_probe_floor.clamp(initial_window.saturating_add(1), bulk_floor)
    }
}

pub(in crate::runtime) fn reliable_latency_startup_credit_remaining_bytes(
    lane: TrafficClass,
    path: Option<PathSnapshot>,
    sent_offset: u64,
    queued_data_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if lane != TrafficClass::Latency {
        return usize::MAX;
    }
    // The admission boundary and unconditional byte classifier must be the
    // same model value. A lower ceiling can stop source reads before the relay
    // has enough evidence to leave latency-oriented scheduling.
    let cap = reliable_flow_bulk_threshold_bytes(path, mux_limits);
    let committed = sent_offset.saturating_add(queued_data_bytes as u64);
    usize::try_from(cap.saturating_sub(committed)).unwrap_or(usize::MAX)
}

#[cfg(test)]
#[path = "flow_test.rs"]
mod tests;
