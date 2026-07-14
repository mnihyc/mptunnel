//! Finite TCP calibration budget and its causal product ACK-clock evidence.
//! Generic path metrics cannot create or advance this capacity proof.

use super::response_topology::{ResponseStreamOutputs, TcpResponseCapacityPrior};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES, PathRateSample, TRANSPORT_TIMER_GRANULARITY,
    product_delivery_samples_override_startup_prior,
};
use crate::model::path::CarrierPathKey;
use crate::protocol::UnderlayProtocol;
use std::collections::HashMap;
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

    #[cfg(any(test, feature = "lab-diagnostics"))]
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

#[cfg(feature = "lab-diagnostics")]
#[derive(Clone, Copy)]
struct ResponseAckClockWindowDiagnostic {
    strict_rate_sample: Option<PathRateSample>,
    window_bytes: u64,
    fresh_window_bytes: u64,
    first_window: bool,
    earliest_sent_at: Instant,
    previous_ack_at: Option<Instant>,
    latest_sent_at: Instant,
    before: ResponseAckClockCalibrationState,
    after: ResponseAckClockCalibrationState,
    credit_grew: bool,
}

#[cfg(feature = "lab-diagnostics")]
fn emit_response_ack_clock_window_diagnostic(
    diagnostic: ResponseAckClockWindowDiagnostic,
    session_id: u64,
    binding_instance_id: u64,
    key: CarrierPathKey,
    output_incarnation: u64,
    now: Instant,
) {
    // Derive observation-only projections from the captured pre-mutation state
    // so the calibration algorithm remains the sole mutation authority.
    let sample_bps = diagnostic
        .strict_rate_sample
        .map(PathRateSample::rate_bps)
        .unwrap_or(0.0);
    let sample_elapsed = diagnostic
        .strict_rate_sample
        .map(PathRateSample::elapsed)
        .unwrap_or(Duration::ZERO);
    let stage_authorized_at = diagnostic.before.stage_authorized_at;
    let stage_authorized_spent_bytes = diagnostic.before.stage_authorized_spent_bytes;
    let stage_credit_bytes = diagnostic.before.stage_credit_bytes();
    let stage_window_eligible = diagnostic.earliest_sent_at >= stage_authorized_at;
    let stage_rate_evidence_accepted = stage_window_eligible
        && diagnostic.strict_rate_sample.is_some()
        && diagnostic.fresh_window_bytes == diagnostic.window_bytes;
    let stage_evidence_bytes = if stage_rate_evidence_accepted {
        diagnostic
            .before
            .stage_rate_evidence_bytes
            .saturating_add(diagnostic.window_bytes)
    } else {
        diagnostic.before.stage_rate_evidence_bytes
    };
    let stage_evidence_elapsed = if stage_rate_evidence_accepted {
        diagnostic
            .before
            .stage_rate_evidence_elapsed
            .saturating_add(sample_elapsed)
    } else {
        diagnostic.before.stage_rate_evidence_elapsed
    };
    let stage_rate_ineligible_bytes =
        if diagnostic.fresh_window_bytes > 0 && !stage_rate_evidence_accepted {
            diagnostic
                .before
                .stage_rate_ineligible_bytes
                .saturating_add(diagnostic.fresh_window_bytes)
        } else {
            diagnostic.before.stage_rate_ineligible_bytes
        };
    let stage_fully_spent = diagnostic.before.spent_bytes >= diagnostic.before.credit_limit_bytes;
    let stage_strict_capacity_bytes =
        stage_credit_bytes.saturating_sub(stage_rate_ineligible_bytes);
    let aggregate_rate_bps = (stage_fully_spent
        && stage_strict_capacity_bytes >= diagnostic.before.stage_rate_coverage_floor_bytes
        && stage_evidence_bytes >= diagnostic.before.stage_rate_coverage_floor_bytes)
        .then(|| {
            stage_evidence_bytes as f64 * 8.0
                / stage_evidence_elapsed
                    .max(TRANSPORT_TIMER_GRANULARITY)
                    .as_secs_f64()
        })
        .unwrap_or(0.0);
    let stage_rate_sample_accepted =
        diagnostic.after.stage_rate_sample_count() > diagnostic.before.stage_rate_sample_count();

    lab_diagnostic(
        "response_ack_clock_calibration",
        format_args!(
            "phase=ack_clock_window session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} rate_bps={} sample_bytes={} fresh_sample_bytes={} sample_elapsed_us={} calibrated_rate_bps={} calibrated_rate_ready={} first_window={} strict_rate_window={} stage_window_eligible={} stage_rate_evidence_accepted={} stage_fully_spent={} stage_rate_sample_accepted={} stage_evidence_bytes={} stage_evidence_elapsed_us={} stage_rate_ineligible_bytes={} stage_rate_coverage_floor_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} aggregate_rate_bps={} spent_bytes={} credit_limit_bytes={} max_limit_bytes={} credit_grew={} proven={} stage_authorized_age_us={} earliest_sent_age_us={} previous_ack_age_us={} latest_sent_age_us={} stage_provenance_slack_us={} causal_slack_us={}",
            session_id,
            binding_instance_id,
            key.underlay,
            key.path_id.0,
            output_incarnation,
            sample_bps,
            diagnostic.window_bytes,
            diagnostic.fresh_window_bytes,
            sample_elapsed.as_micros(),
            diagnostic.after.calibrated_rate_bps.unwrap_or(0.0),
            diagnostic.after.calibrated_rate_bps.is_some(),
            diagnostic.first_window,
            diagnostic.strict_rate_sample.is_some(),
            stage_window_eligible,
            stage_rate_evidence_accepted,
            stage_fully_spent,
            stage_rate_sample_accepted,
            stage_evidence_bytes,
            stage_evidence_elapsed.as_micros(),
            stage_rate_ineligible_bytes,
            diagnostic.after.stage_rate_coverage_floor_bytes,
            stage_authorized_spent_bytes,
            stage_credit_bytes,
            stage_strict_capacity_bytes,
            aggregate_rate_bps,
            diagnostic.after.spent_bytes,
            diagnostic.after.credit_limit_bytes,
            diagnostic.after.max_limit_bytes,
            diagnostic.credit_grew,
            diagnostic.after.proven,
            now.saturating_duration_since(stage_authorized_at)
                .as_micros(),
            now.saturating_duration_since(diagnostic.earliest_sent_at)
                .as_micros(),
            diagnostic.previous_ack_at.map_or(0, |acked_at| {
                now.saturating_duration_since(acked_at).as_micros()
            }),
            now.saturating_duration_since(diagnostic.latest_sent_at)
                .as_micros(),
            diagnostic
                .earliest_sent_at
                .saturating_duration_since(stage_authorized_at)
                .as_micros(),
            diagnostic.previous_ack_at.map_or(0, |acked_at| {
                acked_at
                    .saturating_duration_since(diagnostic.latest_sent_at)
                    .as_micros()
            }),
        ),
    );
}

/// Applies product ACK rate and calibration feedback to already-locked outputs.
///
/// The delivery owner supplies exact, path-proving samples. This helper acquires
/// no locks, publishes no generation, and invokes no binding callback.
pub(super) fn apply_response_ack_clock_release_samples(
    outputs: &mut ResponseStreamOutputs,
    path_samples: HashMap<(CarrierPathKey, u64), (u64, u64, Instant, Instant)>,
    active_calibration_has_owner_flights: bool,
    now: Instant,
    _session_id: u64,
    _binding_instance_id: u64,
) {
    for ((key, output_incarnation), (bytes, fresh_bytes, first_sent_at, last_sent_at)) in
        path_samples
    {
        let identity = (key, output_incarnation);
        let ack_clock_update = if outputs.active_ack_clock_calibration == Some(identity) {
            outputs
                .ack_clock_calibrations
                .get_mut(&identity)
                .filter(|calibration| {
                    calibration.spent_bytes > 0 && !calibration.proven && !calibration.retired
                })
                .map(|calibration| {
                    calibration
                        .rate_evidence
                        .get_or_insert_with(|| ResponseAckClockRateEvidence::new(first_sent_at))
                        .observe_with_fresh_bytes(
                            bytes,
                            fresh_bytes,
                            first_sent_at,
                            last_sent_at,
                            now,
                        )
                })
        } else {
            None
        };
        let ack_clock_window = match ack_clock_update {
            Some(ResponseAckClockRateEvidenceUpdate::Proven {
                sample,
                bytes,
                fresh_bytes,
                first_window,
                earliest_sent_at,
                previous_window_acked_at,
                latest_sent_at,
            }) => Some((
                if first_window { None } else { sample },
                bytes,
                fresh_bytes,
                first_window,
                earliest_sent_at,
                previous_window_acked_at,
                latest_sent_at,
            )),
            _ => None,
        };
        let mut calibration_window_applied = false;
        #[cfg(feature = "lab-diagnostics")]
        let mut calibration_diagnostic = None;
        if let Some((
            strict_rate_sample,
            window_bytes,
            fresh_window_bytes,
            _first_window,
            earliest_sent_at,
            _previous_ack_at,
            _latest_sent_at,
        )) = ack_clock_window
            && let Some(calibration) = outputs.ack_clock_calibrations.get_mut(&identity)
        {
            #[cfg(debug_assertions)]
            let previous_credit = calibration.credit_limit_bytes;
            #[cfg(feature = "lab-diagnostics")]
            let calibration_before = *calibration;
            let credit_grew = calibration.record_ack_clock_window(
                strict_rate_sample,
                window_bytes,
                fresh_window_bytes,
                earliest_sent_at,
                now,
            );
            #[cfg(debug_assertions)]
            debug_assert_eq!(
                credit_grew,
                calibration.credit_limit_bytes > previous_credit
            );
            // Policy only distinguishes whether this exact calibration window
            // found live state; detailed projections belong to diagnostics.
            calibration_window_applied = true;
            #[cfg(feature = "lab-diagnostics")]
            {
                calibration_diagnostic = Some(ResponseAckClockWindowDiagnostic {
                    strict_rate_sample,
                    window_bytes,
                    fresh_window_bytes,
                    first_window: _first_window,
                    earliest_sent_at,
                    previous_ack_at: _previous_ack_at,
                    latest_sent_at: _latest_sent_at,
                    before: calibration_before,
                    after: *calibration,
                    credit_grew,
                });
            }
            #[cfg(not(any(debug_assertions, feature = "lab-diagnostics")))]
            let _ = credit_grew;
        }
        let calibration_snapshot = outputs.ack_clock_calibrations.get(&identity).copied();
        let calibration_identity_active = outputs.active_ack_clock_calibration == Some(identity);
        if let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.incarnation == output_incarnation)
        {
            let udp_assignment_sample = (entry.key.underlay == UnderlayProtocol::Udp)
                .then(|| PathRateSample::new(bytes, now.saturating_duration_since(first_sent_at)))
                .flatten();
            // Flight timestamps mark scheduler assignment, not TCP kernel
            // dispatch. The first exact ACK establishes the clock; later
            // binding-local OwnerData bytes use continuous ACK wall time so
            // callback compression cannot discard the preceding silence.
            let (tcp_ack_clock_sample, tcp_ack_clock_window_complete) =
                if entry.key.underlay == UnderlayProtocol::Tcp {
                    let evidence = entry
                        .tcp_product_rate_evidence
                        .get_or_insert_with(|| ResponseAckClockRateEvidence::new(first_sent_at));
                    let update = evidence.observe_with_fresh_bytes(
                        bytes,
                        bytes,
                        first_sent_at,
                        last_sent_at,
                        now,
                    );
                    (
                        evidence.goodput_sample(),
                        matches!(update, ResponseAckClockRateEvidenceUpdate::Proven { .. }),
                    )
                } else {
                    (None, false)
                };
            let carrier_app_limited = entry
                .local_path_metrics
                .is_some_and(|metrics| metrics.metrics.app_limited);
            let calibrated_rate_bps =
                calibration_snapshot.and_then(|calibration| calibration.calibrated_rate_bps);
            let mut ordinary_tcp_rate_replaces_capacity_prior = false;
            if entry.key.underlay == UnderlayProtocol::Tcp
                && !calibration_identity_active
                && !calibration_window_applied
                && tcp_ack_clock_window_complete
                && let Some(prior) = entry.tcp_capacity_prior.as_mut()
            {
                prior.ordinary_windows = prior.ordinary_windows.saturating_add(1);
                ordinary_tcp_rate_replaces_capacity_prior =
                    product_delivery_samples_override_startup_prior(prior.ordinary_windows)
                        && tcp_ack_clock_sample.is_some();
            }
            if ordinary_tcp_rate_replaces_capacity_prior {
                entry.tcp_capacity_prior = None;
            }
            // A terminal calibration ACK remains exclusive, while either
            // capacity-prior source stays temporary until a fresh ordinary
            // exact-ACK epoch reaches the normal evidence threshold.
            let tcp_measurement_owns_rate = entry.key.underlay == UnderlayProtocol::Tcp
                && (calibration_identity_active
                    || calibration_window_applied
                    || entry.tcp_capacity_prior.is_some()
                    || calibration_snapshot
                        .is_some_and(|calibration| !calibration.proven && !calibration.retired));
            if !tcp_measurement_owns_rate {
                match (
                    entry.key.underlay,
                    tcp_ack_clock_sample,
                    udp_assignment_sample,
                ) {
                    (UnderlayProtocol::Tcp, Some(sample), _) => {
                        // The sample already smooths a bounded byte/time
                        // epoch. Averaging point rates would restore the ACK
                        // compression bias this ratio removes.
                        let rate_bps = sample.rate_bps();
                        entry.tcp_ack_clock_rate_bps = Some(rate_bps);
                        entry.product_progress_rate_bps = Some(rate_bps);
                        entry.delivery_rate_bps = Some(rate_bps);
                    }
                    (UnderlayProtocol::Udp, _, Some(sample)) => {
                        let sample_bps = sample.rate_bps();
                        entry.product_progress_rate_bps =
                            Some(match entry.product_progress_rate_bps {
                                Some(previous) if carrier_app_limited => previous.max(sample_bps),
                                Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                                None => sample_bps,
                            });
                    }
                    _ => {}
                }
            }
            if entry.key.underlay == UnderlayProtocol::Tcp
                && calibration_identity_active
                && let Some(calibrated_rate_bps) = calibrated_rate_bps
            {
                entry.product_progress_rate_bps = Some(calibrated_rate_bps);
                entry.delivery_rate_bps = Some(calibrated_rate_bps);
            }
        }
        #[cfg(feature = "lab-diagnostics")]
        if let Some(diagnostic) = calibration_diagnostic {
            emit_response_ack_clock_window_diagnostic(
                diagnostic,
                _session_id,
                _binding_instance_id,
                key,
                output_incarnation,
                now,
            );
        }
    }
    if !active_calibration_has_owner_flights
        && let Some(identity) = outputs.active_ack_clock_calibration
    {
        // Drain reasons and the previous ceiling are observation-only; the
        // default path retains only the calibrated rate used by policy.
        #[cfg(feature = "lab-diagnostics")]
        let previous_credit = outputs
            .ack_clock_calibrations
            .get(&identity)
            .map_or(0, |calibration| calibration.credit_limit_bytes);
        // Only the calibrated rate crosses into policy; the full state exists
        // solely to explain the transition in lab diagnostics.
        let mut transition_rate_bps = None;
        #[cfg(feature = "lab-diagnostics")]
        let mut transition_snapshot = None;
        #[cfg(feature = "lab-diagnostics")]
        let mut terminal_reason = "credit_remaining";
        let clear_active = match outputs.ack_clock_calibrations.get_mut(&identity) {
            None => {
                #[cfg(feature = "lab-diagnostics")]
                {
                    terminal_reason = "missing_state";
                }
                true
            }
            Some(calibration) => {
                if calibration.proven {
                    transition_rate_bps = calibration.calibrated_rate_bps;
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        transition_snapshot = Some(*calibration);
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        terminal_reason = if calibration.calibrated_rate_bps.is_some() {
                            "robust_rate"
                        } else {
                            "hard_ceiling_no_rate"
                        };
                    }
                    true
                } else if calibration.retired {
                    transition_rate_bps = calibration.calibrated_rate_bps;
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        transition_snapshot = Some(*calibration);
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        terminal_reason = "retired_drain";
                    }
                    true
                } else {
                    #[cfg(feature = "lab-diagnostics")]
                    let previous_stage_rate_samples = calibration.stage_rate_sample_count();
                    if calibration.advance_drained_stage(now) {
                        #[cfg(feature = "lab-diagnostics")]
                        let accepted_stage =
                            calibration.stage_rate_sample_count() > previous_stage_rate_samples;
                        #[cfg(feature = "lab-diagnostics")]
                        {
                            transition_snapshot = Some(*calibration);
                            terminal_reason = if accepted_stage {
                                "drain_stage_advance"
                            } else {
                                "drain_reachability_topup"
                            };
                        }
                        false
                    } else if calibration.proven {
                        transition_rate_bps = calibration.calibrated_rate_bps;
                        #[cfg(feature = "lab-diagnostics")]
                        {
                            transition_snapshot = Some(*calibration);
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        {
                            terminal_reason = if calibration.calibrated_rate_bps.is_some() {
                                "robust_rate"
                            } else {
                                "hard_ceiling_no_rate"
                            };
                        }
                        true
                    } else if calibration.spent_bytes >= calibration.max_limit_bytes {
                        transition_rate_bps = calibration.calibrated_rate_bps;
                        #[cfg(feature = "lab-diagnostics")]
                        {
                            transition_snapshot = Some(*calibration);
                        }
                        calibration.retire();
                        #[cfg(feature = "lab-diagnostics")]
                        {
                            terminal_reason = "hard_ceiling_drain";
                        }
                        true
                    } else if calibration.spent_bytes >= calibration.credit_limit_bytes {
                        transition_rate_bps = calibration.calibrated_rate_bps;
                        #[cfg(feature = "lab-diagnostics")]
                        {
                            transition_snapshot = Some(*calibration);
                        }
                        calibration.retire();
                        #[cfg(feature = "lab-diagnostics")]
                        {
                            terminal_reason = "under_covered_drain";
                        }
                        true
                    } else {
                        false
                    }
                }
            }
        };
        #[cfg(feature = "lab-diagnostics")]
        if clear_active || terminal_reason != "credit_remaining" {
            let terminal = transition_snapshot;
            lab_diagnostic(
                "response_ack_clock_calibration",
                format_args!(
                    "phase={} session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} reason={} active_owner_flights=false calibrated_rate_ready={} calibrated_rate_bps={} spent_bytes={} previous_credit_limit_bytes={} credit_limit_bytes={} max_limit_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} stage_evidence_bytes={} stage_rate_ineligible_bytes={} proven={} retired={}",
                    if clear_active {
                        "terminal"
                    } else {
                        "drain_transition"
                    },
                    _session_id,
                    _binding_instance_id,
                    identity.0.underlay,
                    identity.0.path_id.0,
                    identity.1,
                    terminal_reason,
                    terminal.is_some_and(|state| state.calibrated_rate_bps.is_some()),
                    terminal
                        .and_then(|state| state.calibrated_rate_bps)
                        .unwrap_or(0.0),
                    terminal.map_or(0, |state| state.spent_bytes),
                    previous_credit,
                    terminal.map_or(0, |state| state.credit_limit_bytes),
                    terminal.map_or(0, |state| state.max_limit_bytes),
                    terminal.map_or(0, |state| state.stage_authorized_spent_bytes),
                    terminal.map_or(0, |state| state.stage_credit_bytes()),
                    terminal.map_or(0, |state| state.stage_strict_capacity_bytes()),
                    terminal.map_or(0, |state| state.stage_rate_evidence_bytes),
                    terminal.map_or(0, |state| state.stage_rate_ineligible_bytes),
                    terminal.is_some_and(|state| state.proven),
                    terminal.is_some_and(|state| state.retired),
                ),
            );
        }
        if clear_active {
            outputs.active_ack_clock_calibration = None;
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == identity.0 && entry.incarnation == identity.1)
            {
                // Exclude bounded calibration traffic from the continuous
                // ACK clock that will eventually replace its startup prior.
                entry.tcp_product_rate_evidence = None;
                entry.tcp_ack_clock_rate_bps = None;
                entry.tcp_capacity_prior =
                    transition_rate_bps.map(|rate_bps| TcpResponseCapacityPrior {
                        rate_bps,
                        ordinary_windows: 0,
                    });
                if let Some(prior) = entry.tcp_capacity_prior {
                    entry.product_progress_rate_bps = Some(prior.rate_bps);
                    entry.delivery_rate_bps = Some(prior.rate_bps);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "response_ack_clock_test.rs"]
mod tests;
