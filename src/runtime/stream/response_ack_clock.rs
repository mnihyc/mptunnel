//! Finite TCP calibration budget and its causal product ACK-clock evidence.
//! Generic path metrics cannot create or advance this capacity proof.

use crate::model::capacity::{
    MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES, PathRateSample, TRANSPORT_TIMER_GRANULARITY,
};
use std::time::{Duration, Instant};

const RESPONSE_ACK_CLOCK_STAGE_RATE_WINDOW: usize = 5;
const RESPONSE_ACK_CLOCK_MIN_ROBUST_RATE_SAMPLES: usize = 3;
// Product ACKs can arrive in callback bursts after sitting in a control queue.
// Integrate across that burst instead of treating each callback as a clock.
pub(super) const RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED: Duration = Duration::from_millis(100);
const RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED: Duration = Duration::from_secs(2);
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseAckClockRateEvidence {
    pending_bytes: u64,
    pending_fresh_bytes: u64,
    pending_first_sent_at: Instant,
    pending_last_sent_at: Instant,
    previous_window_acked_at: Option<Instant>,
    goodput_last_acked_at: Option<Instant>,
    goodput_bytes: u64,
    goodput_elapsed: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum ResponseAckClockRateEvidenceUpdate {
    Pending,
    Proven {
        sample: Option<PathRateSample>,
        bytes: u64,
        fresh_bytes: u64,
        first_window: bool,
        earliest_sent_at: Instant,
        previous_window_acked_at: Option<Instant>,
        latest_sent_at: Instant,
    },
}

impl ResponseAckClockRateEvidence {
    pub(super) fn new(first_sent_at: Instant) -> Self {
        Self {
            pending_bytes: 0,
            pending_fresh_bytes: 0,
            pending_first_sent_at: first_sent_at,
            pending_last_sent_at: first_sent_at,
            previous_window_acked_at: None,
            goodput_last_acked_at: None,
            goodput_bytes: 0,
            goodput_elapsed: Duration::ZERO,
        }
    }

    fn record_goodput_progress(&mut self, bytes: u64, acked_at: Instant) {
        let Some(previous_acked_at) = self.goodput_last_acked_at.replace(acked_at) else {
            // The first product ACK can include arbitrary assignment residence.
            return;
        };
        let elapsed = acked_at.saturating_duration_since(previous_acked_at);
        if elapsed > RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED {
            // A long idle/stall starts a new causal epoch instead of poisoning
            // resumed bulk traffic with stale wall time.
            self.goodput_bytes = 0;
            self.goodput_elapsed = Duration::ZERO;
            return;
        }
        self.goodput_bytes = self.goodput_bytes.saturating_add(bytes);
        self.goodput_elapsed = self.goodput_elapsed.saturating_add(elapsed);
        while self.goodput_elapsed > RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED {
            self.goodput_bytes = self.goodput_bytes.div_ceil(2);
            self.goodput_elapsed /= 2;
        }
    }

    pub(super) fn goodput_sample(&self) -> Option<PathRateSample> {
        (self.goodput_elapsed >= RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED)
            .then(|| PathRateSample::new(self.goodput_bytes, self.goodput_elapsed))
            .flatten()
    }

    #[cfg(test)]
    pub(super) fn observe(
        &mut self,
        bytes: u64,
        first_sent_at: Instant,
        last_sent_at: Instant,
        acked_at: Instant,
    ) -> ResponseAckClockRateEvidenceUpdate {
        self.observe_with_fresh_bytes(bytes, bytes, first_sent_at, last_sent_at, acked_at)
    }

    pub(super) fn observe_with_fresh_bytes(
        &mut self,
        bytes: u64,
        fresh_bytes: u64,
        first_sent_at: Instant,
        last_sent_at: Instant,
        acked_at: Instant,
    ) -> ResponseAckClockRateEvidenceUpdate {
        self.record_goodput_progress(bytes, acked_at);
        if self.pending_bytes == 0 {
            self.pending_first_sent_at = first_sent_at;
            self.pending_last_sent_at = last_sent_at;
        } else {
            self.pending_first_sent_at = self.pending_first_sent_at.min(first_sent_at);
            self.pending_last_sent_at = self.pending_last_sent_at.max(last_sent_at);
        }
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.pending_fresh_bytes = self
            .pending_fresh_bytes
            .saturating_add(fresh_bytes.min(bytes));
        if self.pending_bytes < PATH_OPEN_SCORE_BYTES as u64 {
            return ResponseAckClockRateEvidenceUpdate::Pending;
        }

        let sample_bytes = self.pending_bytes;
        let sample_fresh_bytes = self.pending_fresh_bytes;
        let previous_window_acked_at = self.previous_window_acked_at;
        let first_window = previous_window_acked_at.is_none();
        let sample_started_at = previous_window_acked_at.unwrap_or(self.pending_first_sent_at);
        let earliest_sent_at = self.pending_first_sent_at;
        let latest_sent_at = self.pending_last_sent_at;
        let ack_clocked = first_window || self.pending_last_sent_at <= sample_started_at;
        self.pending_bytes = 0;
        self.pending_fresh_bytes = 0;
        self.previous_window_acked_at = Some(acked_at);
        let sample = ack_clocked
            .then(|| {
                PathRateSample::new(
                    sample_bytes,
                    acked_at.saturating_duration_since(sample_started_at),
                )
            })
            .flatten();
        ResponseAckClockRateEvidenceUpdate::Proven {
            sample,
            bytes: sample_bytes,
            fresh_bytes: sample_fresh_bytes,
            first_window,
            earliest_sent_at,
            previous_window_acked_at,
            latest_sent_at,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseAckClockCalibrationState {
    pub(super) spent_bytes: u64,
    pub(super) credit_limit_bytes: u64,
    pub(super) max_limit_bytes: u64,
    pub(super) rate_evidence: Option<ResponseAckClockRateEvidence>,
    pub(super) calibrated_rate_bps: Option<f64>,
    stage_rate_samples_bps: [f64; RESPONSE_ACK_CLOCK_STAGE_RATE_WINDOW],
    stage_rate_sample_count: u8,
    stage_rate_sample_cursor: u8,
    pub(super) stage_rate_coverage_floor_bytes: u64,
    pub(super) stage_rate_evidence_bytes: u64,
    pub(super) stage_rate_evidence_elapsed: Duration,
    /// Current-stage bytes ACKed in windows that cannot carry a strict rate.
    /// They consume the finite opportunity to reach the publication floor.
    pub(super) stage_rate_ineligible_bytes: u64,
    pub(super) stage_authorized_at: Instant,
    /// Cumulative spend at stage authorization. The difference to the current
    /// ceiling is the only byte volume fresh enough to prove this stage.
    pub(super) stage_authorized_spent_bytes: u64,
    pub(super) retired: bool,
    /// Lifecycle terminal, not necessarily rate-proven. A robust result exists
    /// only when `calibrated_rate_bps` is populated.
    pub(super) proven: bool,
}

impl ResponseAckClockCalibrationState {
    #[cfg(test)]
    pub(super) fn new(initial_limit_bytes: u64, max_limit_bytes: u64) -> Self {
        let initial_limit_bytes = initial_limit_bytes.min(max_limit_bytes);
        let coverage_floor = if initial_limit_bytes == 0 {
            0
        } else {
            initial_limit_bytes
                .div_ceil(2)
                .max(MIN_RATE_SAMPLE_BYTES)
                .min(initial_limit_bytes)
        };
        Self::new_with_rate_coverage_floor(initial_limit_bytes, max_limit_bytes, coverage_floor)
    }

    pub(super) fn new_with_rate_coverage_floor(
        initial_limit_bytes: u64,
        max_limit_bytes: u64,
        stage_rate_coverage_floor_bytes: u64,
    ) -> Self {
        let initial_limit_bytes = initial_limit_bytes.min(max_limit_bytes);
        let max_limit_bytes = if initial_limit_bytes == 0 {
            0
        } else {
            max_limit_bytes.max(initial_limit_bytes)
        };
        let stage_rate_coverage_floor_bytes = if initial_limit_bytes == 0 {
            0
        } else {
            stage_rate_coverage_floor_bytes
                .max(MIN_RATE_SAMPLE_BYTES)
                .min(max_limit_bytes)
        };
        Self {
            spent_bytes: 0,
            credit_limit_bytes: initial_limit_bytes,
            max_limit_bytes,
            rate_evidence: None,
            calibrated_rate_bps: None,
            stage_rate_samples_bps: [0.0; RESPONSE_ACK_CLOCK_STAGE_RATE_WINDOW],
            stage_rate_sample_count: 0,
            stage_rate_sample_cursor: 0,
            stage_rate_coverage_floor_bytes,
            stage_rate_evidence_bytes: 0,
            stage_rate_evidence_elapsed: Duration::ZERO,
            stage_rate_ineligible_bytes: 0,
            stage_authorized_at: Instant::now(),
            stage_authorized_spent_bytes: 0,
            retired: false,
            proven: false,
        }
    }

    #[cfg(test)]
    pub(super) fn record_ack_clock_sample(
        &mut self,
        sample: PathRateSample,
        earliest_sent_at: Instant,
        acked_at: Instant,
    ) -> bool {
        self.record_ack_clock_window(
            Some(sample),
            sample.bytes(),
            sample.bytes(),
            earliest_sent_at,
            acked_at,
        )
    }

    pub(super) fn record_ack_clock_window(
        &mut self,
        strict_rate_sample: Option<PathRateSample>,
        window_bytes: u64,
        fresh_window_bytes: u64,
        earliest_sent_at: Instant,
        acked_at: Instant,
    ) -> bool {
        if self.retired || self.proven {
            return false;
        }
        let fresh_window_bytes = fresh_window_bytes.min(window_bytes);
        if fresh_window_bytes == 0 {
            return false;
        }
        let strict_rate_sample = strict_rate_sample.filter(|sample| {
            earliest_sent_at >= self.stage_authorized_at
                && fresh_window_bytes == window_bytes
                && sample.bytes() == window_bytes
        });
        if let Some(sample) = strict_rate_sample {
            debug_assert_eq!(sample.bytes(), window_bytes);
            self.stage_rate_evidence_bytes = self
                .stage_rate_evidence_bytes
                .saturating_add(sample.bytes());
            self.stage_rate_evidence_elapsed = self
                .stage_rate_evidence_elapsed
                .saturating_add(sample.elapsed());
        } else {
            self.stage_rate_ineligible_bytes = self
                .stage_rate_ineligible_bytes
                .saturating_add(fresh_window_bytes);
        }
        if self.spent_bytes < self.credit_limit_bytes {
            return false;
        }
        self.advance_fully_spent_stage(acked_at)
    }

    fn advance_fully_spent_stage(&mut self, acked_at: Instant) -> bool {
        debug_assert!(self.spent_bytes >= self.credit_limit_bytes);
        let strict_capacity_bytes = self.stage_strict_capacity_bytes();
        if strict_capacity_bytes < self.stage_rate_coverage_floor_bytes {
            // Authorization growth preserves this measurement stage. The base,
            // provenance time, strict evidence, and clock-establishment debt do
            // not reset until one representative aggregate is accepted.
            return self.top_up_stage_reachability();
        }
        if self.stage_rate_evidence_bytes < self.stage_rate_coverage_floor_bytes {
            // The stage can still produce a representative aggregate. Wait for
            // later ACK windows rather than discarding the remaining capacity.
            return false;
        }
        let aggregate_rate_bps = self.stage_rate_evidence_bytes as f64 * 8.0
            / self
                .stage_rate_evidence_elapsed
                .max(TRANSPORT_TIMER_GRANULARITY)
                .as_secs_f64();
        self.record_stage_rate_sample(aggregate_rate_bps);
        self.reset_stage_rate_evidence();
        // Calibration estimates TCP capacity; ordinary measured admission owns
        // filling that capacity. Once the robust stage median exists, another
        // exclusive doubling stage adds no evidence needed by the scheduler.
        if self.calibrated_rate_bps.is_some() {
            self.proven = true;
            return false;
        }
        if self.credit_limit_bytes < self.max_limit_bytes {
            self.stage_authorized_spent_bytes = self.spent_bytes;
            self.credit_limit_bytes = self
                .credit_limit_bytes
                .saturating_mul(2)
                .min(self.max_limit_bytes);
            self.stage_authorized_at = acked_at;
            true
        } else {
            self.proven = true;
            false
        }
    }

    fn top_up_stage_reachability(&mut self) -> bool {
        let required_credit = self
            .stage_authorized_spent_bytes
            .saturating_add(self.stage_rate_ineligible_bytes)
            .saturating_add(self.stage_rate_coverage_floor_bytes);
        if required_credit > self.max_limit_bytes {
            self.proven = true;
            return false;
        }
        let next_credit = self
            .credit_limit_bytes
            .saturating_mul(2)
            .max(required_credit)
            .min(self.max_limit_bytes);
        if next_credit <= self.credit_limit_bytes {
            self.proven = true;
            return false;
        }
        self.credit_limit_bytes = next_credit;
        true
    }

    pub(super) fn advance_drained_stage(&mut self, acked_at: Instant) -> bool {
        if self.retired || self.proven || self.spent_bytes < self.credit_limit_bytes {
            return false;
        }
        // With no exact OwnerData flight left, every authorized stage byte not
        // already in strict evidence is permanently rate-ineligible.
        let drained_ineligible = self
            .stage_credit_bytes()
            .saturating_sub(self.stage_rate_evidence_bytes);
        self.stage_rate_ineligible_bytes = self.stage_rate_ineligible_bytes.max(drained_ineligible);
        self.advance_fully_spent_stage(acked_at)
    }

    fn record_stage_rate_sample(&mut self, sample_bps: f64) {
        if !sample_bps.is_finite() || sample_bps <= 0.0 {
            return;
        }
        let cursor =
            usize::from(self.stage_rate_sample_cursor) % RESPONSE_ACK_CLOCK_STAGE_RATE_WINDOW;
        self.stage_rate_samples_bps[cursor] = sample_bps;
        self.stage_rate_sample_cursor = ((cursor + 1) % RESPONSE_ACK_CLOCK_STAGE_RATE_WINDOW) as u8;
        self.stage_rate_sample_count = self
            .stage_rate_sample_count
            .saturating_add(1)
            .min(RESPONSE_ACK_CLOCK_STAGE_RATE_WINDOW as u8);

        let sample_count = usize::from(self.stage_rate_sample_count);
        if sample_count < RESPONSE_ACK_CLOCK_MIN_ROBUST_RATE_SAMPLES {
            return;
        }
        let mut ordered = self.stage_rate_samples_bps;
        ordered[..sample_count].sort_by(f64::total_cmp);
        self.calibrated_rate_bps = Some(if sample_count % 2 == 0 {
            let upper = sample_count / 2;
            (ordered[upper - 1] + ordered[upper]) / 2.0
        } else {
            ordered[sample_count / 2]
        });
    }

    fn reset_stage_rate_evidence(&mut self) {
        self.stage_rate_evidence_bytes = 0;
        self.stage_rate_evidence_elapsed = Duration::ZERO;
        self.stage_rate_ineligible_bytes = 0;
    }

    pub(super) fn stage_credit_bytes(&self) -> u64 {
        self.credit_limit_bytes
            .saturating_sub(self.stage_authorized_spent_bytes)
    }

    pub(super) fn stage_strict_capacity_bytes(&self) -> u64 {
        self.stage_credit_bytes()
            .saturating_sub(self.stage_rate_ineligible_bytes)
    }

    pub(super) fn stage_rate_sample_count(&self) -> u8 {
        self.stage_rate_sample_count
    }

    pub(super) fn retire(&mut self) {
        self.reset_stage_rate_evidence();
        self.credit_limit_bytes = self.spent_bytes;
        self.max_limit_bytes = self.spent_bytes;
        self.stage_authorized_spent_bytes = self.spent_bytes;
        self.retired = true;
    }
}

#[cfg(test)]
#[path = "response_ack_clock_test.rs"]
mod tests;
