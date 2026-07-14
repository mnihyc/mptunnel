use crate::model::admission::bulk_service_horizon_payload_bytes;
use super::response_placement::ResponseRateScope;
use super::response_session::{
    ServerResponsePathSchedulingSnapshot, valid_quic_capacity_proof_candidate_at,
};
use super::*;

// Despite the historical filename, this module owns response path evidence,
// TCP product-ACK calibration lifecycle, and immutable scheduler snapshots.
// Admission and ranking belong to `sender_service`; exact range ownership
// belongs to the parent reliable-path binding.

const RESPONSE_ACK_CLOCK_STAGE_RATE_WINDOW: usize = 5;
const RESPONSE_ACK_CLOCK_MIN_ROBUST_RATE_SAMPLES: usize = 3;
// Product ACKs can arrive in callback bursts after sitting in a control queue.
// Integrate across that burst instead of treating each callback as a clock.
pub(super) const RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED: Duration = Duration::from_millis(100);
const RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED: Duration = Duration::from_secs(2);

/// One carrier output attached to a response stream.
///
/// It owns carrier command access and sender-evidence fields for this stream on
/// this path. Product repair and ordering identity stay in `ResponseStreamBinding`.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseStreamOutputEntry {
    pub(super) key: CarrierPathKey,
    pub(super) path_instance_id: ServerCarrierPathInstanceId,
    pub(super) incarnation: u64,
    pub(super) commands: ReliablePathCommandSender,
    pub(super) role: StreamOpenRole,
    /// Unacknowledged unique OwnerData assigned to this response output.
    /// Repair copies remain in `bytes_in_flight` but never enter this counter.
    pub(super) owner_data_in_flight_bytes: u64,
    pub(super) bytes_in_flight: u64,
    pub(super) product_queue_bytes: u64,
    pub(super) product_progress_rate_bps: Option<f64>,
    pub(super) delivery_rate_bps: Option<f64>,
    /// TCP per-flow goodput from exact OwnerData ACKs. It is not carrier
    /// capacity; assignment-time evidence never publishes a rate or RTT.
    pub(super) tcp_ack_clock_rate_bps: Option<f64>,
    /// Per-output ACK clock; product ordering timestamps can be advanced when a
    /// different path closes a hole and therefore cannot own this boundary.
    pub(super) tcp_product_rate_evidence: Option<ResponseAckClockRateEvidence>,
    /// Temporary carrier-capacity estimate. It may come from a bounded Service
    /// opportunity or exclusive calibration; ordinary exact-ACK evidence must
    /// mature in a separate epoch before replacing it.
    pub(super) tcp_capacity_prior: Option<TcpResponseCapacityPrior>,
    pub(super) srtt_ms: Option<f64>,
    pub(super) delivery_samples: u32,
    /// Cumulative uniquely owned product bytes ACKed on this output.
    ///
    /// The flight ledger increments this only for unambiguous `OwnerData`;
    /// duplicated `RepairData` never contributes.
    pub(super) owner_data_acked_bytes: u64,
    pub(super) local_path_metrics: Option<ServerPathMetricsEntry>,
    pub(super) peer_path_metrics: Option<ServerPathMetricsEntry>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct TcpResponseCapacityPrior {
    pub(super) rate_bps: f64,
    pub(super) ordinary_windows: u32,
}

pub(in crate::runtime) struct ResponseStreamOutputs {
    pub(super) entries: Vec<ResponseStreamOutputEntry>,
    pub(super) ack_clock_calibrations:
        HashMap<(CarrierPathKey, u64), ResponseAckClockCalibrationState>,
    pub(super) active_ack_clock_calibration: Option<(CarrierPathKey, u64)>,
}

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

#[derive(Clone)]
pub(in crate::runtime) struct ResponseSenderPathTarget {
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) session_id: SessionId,
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) binding_instance_id: u64,
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) incarnation: u64,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) attachment_role: StreamOpenRole,
    pub(in crate::runtime) snapshot: PathSnapshot,
    pub(in crate::runtime) owner_data_in_flight_bytes: u64,
    /// Once-captured command pressure used by both projection and commit
    /// revalidation; equality is a value fingerprint, not a queue generation.
    pub(in crate::runtime) command_pending_bytes: u64,
    pub(in crate::runtime) eta_ms: f64,
    /// True only for the persistent response Service snapshot.
    pub(in crate::runtime) is_active: bool,
    /// Request-side Active is independent from response Service ownership.
    pub(in crate::runtime) is_request_active: bool,
    pub(in crate::runtime) has_sender_evidence: bool,
    /// Current-Service feed may use unique product ACK progress or durable
    /// app-limited carrier ACK progress; optional paths still require strict
    /// bulk-rate evidence below.
    pub(in crate::runtime) has_service_feed_evidence: bool,
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
    /// Endpoint-only configuration plus an immature candidate ACK model may
    /// use Service only as a bounded calibration-opportunity prior.
    pub(in crate::runtime) endpoint_only_service_prior_eligible: bool,
    /// Raw receipt marker; handoff may pin it without renewing global freshness.
    pub(in crate::runtime) quic_capacity_proof: Option<QuicCapacityProofCandidate>,
    pub(in crate::runtime) quic_capacity_calibration_attempts: u8,
    pub(in crate::runtime) ack_clock_calibration_eligible: bool,
    pub(in crate::runtime) ack_clock_calibration_proven: bool,
    pub(in crate::runtime) ack_clock_calibration_spent_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_credit_limit_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_max_limit_bytes: u64,
    pub(in crate::runtime) ack_clock_calibration_active: bool,
}

/// Compact identity retained after path ranking. Model snapshots and
/// calibration state are intentionally dropped before the per-frame emit path.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseDispatchTarget {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) incarnation: u64,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) attachment_role: StreamOpenRole,
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
}

impl From<ResponseSenderPathTarget> for ResponseDispatchTarget {
    fn from(target: ResponseSenderPathTarget) -> Self {
        Self {
            key: target.key,
            path_instance_id: target.path_instance_id,
            incarnation: target.incarnation,
            commands: target.commands,
            attachment_role: target.attachment_role,
            has_bulk_rate_evidence: target.has_bulk_rate_evidence,
        }
    }
}

impl From<&ResponseSenderPathTarget> for ResponseDispatchTarget {
    fn from(target: &ResponseSenderPathTarget) -> Self {
        Self {
            key: target.key,
            path_instance_id: target.path_instance_id,
            incarnation: target.incarnation,
            commands: target.commands.clone(),
            attachment_role: target.attachment_role,
            has_bulk_rate_evidence: target.has_bulk_rate_evidence,
        }
    }
}

/// Product byte range currently assigned to a carrier path.
///
/// STREAM_ACK releases this ledger entry from product flight; carrier ACKs only
/// update carrier/path evidence and must not release product repair state.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathFlight {
    pub(super) key: CarrierPathKey,
    pub(super) output_incarnation: u64,
    pub(super) end: u64,
    pub(super) bytes: usize,
    pub(super) sent_at: Instant,
    pub(super) kind: CarrierWorkKind,
    pub(super) evidence_eligible: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathReleasedFlight {
    pub(super) flight: CarrierPathFlight,
    pub(super) path_proving: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathFlightDebt {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathAckedHole {
    pub(super) key: CarrierPathKey,
    pub(super) output_incarnation: u64,
    pub(super) end: u64,
    pub(super) bytes: u64,
    pub(super) kind: CarrierWorkKind,
    pub(super) path_proving: bool,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct ResponseAckOrderingState {
    pub(super) contiguous_frontier: u64,
    pub(super) acked_holes: BTreeMap<u64, Vec<CarrierPathAckedHole>>,
}

pub(in crate::runtime) struct ResponseAckOrderingUpdate {
    pub(super) changed: bool,
    pub(super) contiguous_frontier: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) acked_hole_bytes: u64,
    pub(super) newly_contiguous: Vec<CarrierPathAckedHole>,
}

impl ResponseAckOrderingState {
    pub(super) fn apply_normalized_ack(
        &mut self,
        ranges: &[OffsetRange],
        released: &[(u64, CarrierPathReleasedFlight)],
    ) -> ResponseAckOrderingUpdate {
        let previous_frontier = self.contiguous_frontier;
        let previous_hole_bytes = self.acked_hole_bytes();
        let mut newly_contiguous = Vec::new();

        for (offset, release) in released {
            let flight = release.flight;
            let hole = CarrierPathAckedHole {
                key: flight.key,
                output_incarnation: flight.output_incarnation,
                end: flight.end,
                bytes: flight.bytes as u64,
                kind: flight.kind,
                path_proving: release.path_proving,
            };
            if hole.end <= self.contiguous_frontier {
                newly_contiguous.push(hole);
            } else {
                self.acked_holes.entry(*offset).or_default().push(hole);
            }
        }

        self.advance_contiguous_frontier(ranges);
        let frontier = self.contiguous_frontier;
        self.acked_holes.retain(|_, holes| {
            holes.retain(|hole| {
                if hole.end <= frontier {
                    newly_contiguous.push(*hole);
                    false
                } else {
                    true
                }
            });
            !holes.is_empty()
        });
        let acked_hole_bytes = self.acked_hole_bytes();

        ResponseAckOrderingUpdate {
            changed: previous_frontier != self.contiguous_frontier
                || previous_hole_bytes != acked_hole_bytes
                || !newly_contiguous.is_empty(),
            contiguous_frontier: self.contiguous_frontier,
            acked_hole_bytes,
            newly_contiguous,
        }
    }

    fn advance_contiguous_frontier(&mut self, ranges: &[OffsetRange]) {
        loop {
            let mut next_frontier = self.contiguous_frontier;
            for range in ranges {
                if range.start > next_frontier {
                    break;
                }
                if range.end > next_frontier {
                    next_frontier = range.end;
                }
            }
            for (offset, holes) in self.acked_holes.range(..=next_frontier) {
                if *offset > next_frontier {
                    break;
                }
                for hole in holes {
                    if hole.end > next_frontier {
                        next_frontier = hole.end;
                    }
                }
            }
            if next_frontier == self.contiguous_frontier {
                break;
            }
            self.contiguous_frontier = next_frontier;
        }
    }

    pub(super) fn acked_hole_bytes(&self) -> u64 {
        self.acked_holes
            .values()
            .filter_map(|holes| response_latest_ordering_hole(holes))
            .map(|hole| hole.bytes)
            .sum()
    }
}

pub(in crate::runtime) fn response_latest_ordering_hole(
    holes: &[CarrierPathAckedHole],
) -> Option<&CarrierPathAckedHole> {
    holes
        .iter()
        .rev()
        .find(|hole| hole.kind.is_ordering_owner())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerPathMetricsSource {
    PeerHint,
    LocalSender,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerPathMetricsEntry {
    pub(super) metrics: PathMetrics,
    pub(super) source: ServerPathMetricsSource,
    // Metric age is measured at the source; residence time closes the gap when
    // the local idle publisher is delayed after this snapshot is installed.
    pub(super) recorded_at: Instant,
    // Only the exact capacity transaction creates this marker. Ordinary metric
    // refreshes may carry it to the fixed deadline but cannot mint a new proof.
    pub(super) capacity_proof: Option<QuicCapacityProofCandidate>,
    // TCP uses an independent receiver receipt plus exact socket telemetry.
    pub(super) tcp_capacity_proof: Option<TcpCapacityProofCandidate>,
}

fn server_output_local_path_metrics(
    entry: &ResponseStreamOutputEntry,
) -> Option<ServerPathMetricsEntry> {
    entry.local_path_metrics.filter(|path_metrics| {
        path_metrics.source == ServerPathMetricsSource::LocalSender
            && path_metrics.metrics.direction == PathMetricDirection::ServerToClient
            && path_metrics.metrics.underlay == entry.key.underlay
            && path_metrics.metrics.path_id == entry.key.path_id
    })
}

impl ResponseStreamOutputs {
    pub(super) fn snapshot_for_key(
        &self,
        key: CarrierPathKey,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let now = Instant::now();
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| {
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now)
            })
    }

    pub(super) fn read_backpressure_snapshot(
        &self,
        active_key: Option<CarrierPathKey>,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let now = Instant::now();
        if !lane.is_bulk() {
            return self.entries.last().map(|entry| {
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now)
            });
        }
        self.entries
            .iter()
            .filter(|entry| {
                Some(entry.key) == active_key || server_output_has_sender_evidence(entry)
            })
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (eta_ms, snapshot)
            })
            .min_by(|left, right| left.0.total_cmp(&right.0))
            .map(|(_, snapshot)| snapshot)
    }

    pub(super) fn relay_read_snapshot(
        &self,
        stored_service_key: Option<CarrierPathKey>,
        may_have_mixed_owner_underlays: bool,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> ResponseRelayReadSnapshot {
        let service_key = response_live_ordered_data_owner(stored_service_key, &self.entries);
        let send_path = self.read_backpressure_snapshot(
            service_key,
            session_id,
            lane_tracker,
            lane,
            payload_bytes,
            mux_limits,
        );
        let source_service = service_key.and_then(|key| {
            self.entries
                .iter()
                .find(|entry| {
                    entry.key == key
                        && entry.role != StreamOpenRole::Repair
                        && !entry.commands.is_closed()
                })
                .map(|entry| {
                    // Source staging needs exact identity, local pressure, and
                    // proof only. Avoid rebuilding an unused full path model
                    // while the response outputs lock is held.
                    let active_latency_sensitive_flows = send_path
                        .filter(|path| path.id == key.path_id && path.underlay == key.underlay)
                        .map(|path| path.active_latency_sensitive_flows)
                        .unwrap_or_else(|| {
                            lane_tracker
                                .response_service_snapshot(session_id, key)
                                .active_latency_sensitive_flows
                        });
                    ResponseSourceServiceSnapshot {
                        key,
                        active_latency_sensitive_flows,
                        has_service_feed_evidence:
                            server_output_has_service_feed_evidence_with_limits(entry, mux_limits),
                        has_bulk_rate_evidence: server_output_has_bulk_rate_evidence_with_limits(
                            entry, mux_limits,
                        ),
                    }
                })
        });
        ResponseRelayReadSnapshot {
            send_path,
            source_service,
            independent_source_staging: may_have_mixed_owner_underlays
                && response_outputs_have_live_mixed_owner_underlays(&self.entries),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseBulkOutputSnapshot {
    pub(super) path: PathSnapshot,
    pub(super) quic_capacity_calibration_attempts: u8,
}

pub(super) fn server_bulk_output_snapshot(
    entry: &ResponseStreamOutputEntry,
    session_id: SessionId,
    lane: FlowLane,
    lane_tracker: &ServerPathLaneTracker,
    mux_limits: MuxLimits,
    now: Instant,
) -> PathSnapshot {
    server_bulk_output_snapshot_with_command_pending(
        entry,
        session_id,
        lane,
        lane_tracker,
        mux_limits,
        now,
        entry.commands.pending_bytes(),
    )
    .path
}

pub(super) fn server_bulk_output_snapshot_with_command_pending(
    entry: &ResponseStreamOutputEntry,
    session_id: SessionId,
    lane: FlowLane,
    lane_tracker: &ServerPathLaneTracker,
    mux_limits: MuxLimits,
    now: Instant,
    command_pending_bytes: u64,
) -> ResponseBulkOutputSnapshot {
    let response_scheduling = lane_tracker.response_path_scheduling_snapshot(
        session_id,
        entry.key,
        entry.path_instance_id,
    );
    server_bulk_output_snapshot_with_scheduling(
        entry,
        lane,
        mux_limits,
        now,
        command_pending_bytes,
        response_scheduling,
    )
}

/// Combines output-local evidence with one caller-batched scheduling record.
pub(super) fn server_bulk_output_snapshot_with_scheduling(
    entry: &ResponseStreamOutputEntry,
    lane: FlowLane,
    mux_limits: MuxLimits,
    now: Instant,
    command_pending_bytes: u64,
    response_scheduling: ServerResponsePathSchedulingSnapshot,
) -> ResponseBulkOutputSnapshot {
    let local_carrier_metrics = server_output_local_path_metrics(entry);
    let peer_hint_metrics = (entry.delivery_samples == 0)
        .then_some(entry.peer_path_metrics)
        .flatten();
    let liveness_metrics = local_carrier_metrics.or(peer_hint_metrics);
    let bulk_rate_metrics = local_carrier_metrics
        .filter(|path_metrics| server_path_metrics_has_bulk_rate_evidence(*path_metrics));
    let srtt_ms = liveness_metrics.map_or_else(
        || {
            entry
                .srtt_ms
                .unwrap_or_else(|| default_path_srtt_ms(entry.key.underlay))
        },
        |path_metrics| f64::from(path_metrics.metrics.srtt_us.max(1)) / 1000.0,
    );
    let jitter_ms = liveness_metrics.map_or(0.0, |path_metrics| {
        f64::from(path_metrics.metrics.jitter_us) / 1000.0
    });
    let loss_rate = liveness_metrics
        .filter(|path_metrics| path_metrics.metrics.loss_observed)
        .map_or(0.0, |path_metrics| {
            f64::from(path_metrics.metrics.loss_ppm) / 1_000_000.0
        })
        .clamp(0.0, 1.0);
    let peer_hint_rate_bps = peer_hint_metrics
        .filter(|path_metrics| !path_metrics.metrics.app_limited)
        .map(server_path_metrics_rate_bps);
    let product_owner_rate_bps = (entry.key.underlay == UnderlayProtocol::Tcp)
        .then_some(entry.product_progress_rate_bps)
        .flatten()
        .filter(|_| entry.delivery_samples > 0);
    // QUIC keeps its carrier bandwidth estimate after placement proof expires.
    // Use that estimate for pacing/ETA, while `bulk_rate_metrics` remains the
    // separate authority that may admit or move a whole product flow.
    let udp_carrier_estimate_bps = if entry.key.underlay == UnderlayProtocol::Udp {
        local_carrier_metrics
            .filter(|path_metrics| server_udp_path_metrics_has_durable_rate_estimate(*path_metrics))
            .map(server_path_metrics_estimate_rate_bps)
    } else {
        None
    };
    let model_rate_bps = bulk_rate_metrics
        .map(server_path_metrics_rate_bps)
        .or(udp_carrier_estimate_bps);
    let (prior_rate_bps, prior_rate_scope) = if let Some(rate_bps) = model_rate_bps {
        (rate_bps, ResponseRateScope::PathCapacity)
    } else if let Some(rate_bps) = peer_hint_rate_bps {
        (rate_bps, ResponseRateScope::PathCapacity)
    } else if let Some(rate_bps) = product_owner_rate_bps {
        (rate_bps, ResponseRateScope::PerFlowGoodput)
    } else {
        (
            default_path_rate_bps(entry.key.underlay),
            ResponseRateScope::PathCapacity,
        )
    };
    let (rate_bps, rate_scope) = match (
        entry.key.underlay,
        bulk_rate_metrics,
        entry.delivery_rate_bps,
        product_owner_rate_bps,
    ) {
        (_, Some(path_metrics), _, _) => (
            server_path_metrics_rate_bps(path_metrics),
            ResponseRateScope::PathCapacity,
        ),
        (UnderlayProtocol::Udp, None, _, _) => (prior_rate_bps, prior_rate_scope),
        (UnderlayProtocol::Tcp, None, _, _) if entry.tcp_capacity_prior.is_some() => (
            entry
                .tcp_capacity_prior
                .expect("guarded TCP capacity prior")
                .rate_bps,
            ResponseRateScope::PathCapacity,
        ),
        (UnderlayProtocol::Tcp, None, Some(rate), _)
            if !super::product_delivery_samples_override_startup_prior(entry.delivery_samples) =>
        {
            if rate >= prior_rate_bps {
                (rate, ResponseRateScope::PerFlowGoodput)
            } else {
                (prior_rate_bps, prior_rate_scope)
            }
        }
        (UnderlayProtocol::Tcp, None, Some(rate), _) => (rate, ResponseRateScope::PerFlowGoodput),
        (_, None, None, Some(rate)) => (rate, ResponseRateScope::PerFlowGoodput),
        (_, None, None, None) => (prior_rate_bps, prior_rate_scope),
    };
    let rate_bps = rate_bps.max(1.0);
    let mut snapshot = PathSnapshot::new(entry.key.path_id, entry.key.underlay, srtt_ms, rate_bps);
    snapshot.rate_scope = rate_scope;
    if let Some(path_metrics) = liveness_metrics {
        snapshot.min_rtt_ms = f64::from(path_metrics.metrics.min_rtt_us.max(1)) / 1000.0;
    }
    snapshot.product_progress_rate_bps = entry.product_progress_rate_bps;
    snapshot.has_durable_product_progress =
        server_output_has_durable_product_ack_progress(entry, mux_limits);
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    if let Some(path_metrics) = local_carrier_metrics {
        snapshot.pacing_rate_bps =
            (path_metrics.metrics.pacing_rate_bps.max(1) as f64).max(snapshot.delivery_rate_bps);
    }
    if let Some(path_metrics) = liveness_metrics {
        snapshot.app_limited = path_metrics.metrics.app_limited;
    }
    let metric_queue_bytes =
        local_carrier_metrics.map_or(0, |path_metrics| path_metrics.metrics.queue_bytes);
    snapshot.queue_bytes = metric_queue_bytes.saturating_add(command_pending_bytes);
    snapshot.product_queue_bytes = entry.product_queue_bytes;
    snapshot.bytes_in_flight = match entry.key.underlay {
        UnderlayProtocol::Udp => {
            local_carrier_metrics.map_or(0, |path_metrics| path_metrics.metrics.bytes_in_flight)
        }
        // TCP does not expose packet-level carrier flight to the product layer.
        // Product stream ranges waiting for STREAM_ACK remain in
        // product_bytes_in_flight below; treating them as carrier flight makes
        // the BBR-style send quantum collapse as soon as the product window is
        // full even when the kernel TCP stream is healthy.
        UnderlayProtocol::Tcp => 0,
    };
    snapshot.product_bytes_in_flight = entry.bytes_in_flight;
    snapshot.inflight_limit_bytes = match entry.key.underlay {
        UnderlayProtocol::Udp => local_carrier_metrics
            .map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes),
        UnderlayProtocol::Tcp => {
            bulk_rate_metrics.map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes)
        }
    };
    snapshot.confidence = server_output_confidence(entry, now);
    // Response pressure follows product Service ownership. Control-plane Active
    // attachment roles intentionally remain unchanged across a whole-flow
    // handoff and therefore cannot describe the carrier doing response work.
    let lane_load = response_scheduling.path_load;
    let session_lane_load = response_scheduling.session_load;
    snapshot.active_flows = lane_load.active_flows;
    snapshot.active_latency_sensitive_flows = lane_load.active_latency_sensitive_flows;
    snapshot.session_active_latency_sensitive_flows =
        session_lane_load.active_latency_sensitive_flows;
    let known_bulk_flows = lane_load
        .active_flows
        .saturating_sub(lane_load.active_latency_sensitive_flows);
    if lane.is_bulk() && lane_load.active_latency_sensitive_flows > 0 && known_bulk_flows > 0 {
        let latency_headroom =
            adaptive_reliable_relay_inflight_bytes(Some(snapshot), FlowLane::Latency, mux_limits)
                as u64;
        let protected_queue =
            latency_headroom.saturating_mul(u64::from(lane_load.active_latency_sensitive_flows));
        snapshot.queue_bytes = snapshot.queue_bytes.saturating_add(protected_queue);
    }
    ResponseBulkOutputSnapshot {
        path: snapshot,
        quic_capacity_calibration_attempts: response_scheduling.quic_capacity_calibration_attempts,
    }
}

pub(in crate::runtime) fn server_bulk_output_eta_ms(
    key: CarrierPathKey,
    snapshot: PathSnapshot,
    active_key: Option<CarrierPathKey>,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> f64 {
    let queued_bits = snapshot
        .queue_bytes
        .saturating_add(snapshot.product_queue_bytes)
        .saturating_add(snapshot.bytes_in_flight)
        .saturating_mul(8) as f64;
    let scoring_payload_bytes =
        if lane.is_bulk() && (active_key.is_none() || Some(key) == active_key) {
            bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
        } else {
            payload_bytes
        };
    let payload_bits = scoring_payload_bytes as f64 * 8.0;
    let mut eta_ms = snapshot.srtt_ms / 2.0;
    let effective_rate_bps = snapshot.delivery_rate_bps.max(1.0);
    eta_ms += (queued_bits + payload_bits) / effective_rate_bps * 1000.0;
    eta_ms += snapshot.jitter_ms;
    eta_ms += response_loss_penalty_ms(snapshot);
    if key.underlay == UnderlayProtocol::Udp && lane.is_bulk() {
        eta_ms += udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes);
    }
    let uncertainty = 1.0 - snapshot.confidence.clamp(0.0, 1.0);
    let pto_ms = transport_pto_from_snapshot(Some(snapshot)).as_secs_f64() * 1000.0;
    eta_ms += uncertainty * pto_ms;
    if Some(key) != active_key {
        eta_ms += uncertainty * pto_ms;
        if snapshot.bytes_in_flight > 0 {
            eta_ms +=
                (snapshot.bytes_in_flight as f64 * 8.0 / effective_rate_bps.max(1.0)) * 1000.0;
        }
    }
    eta_ms
}

fn response_loss_penalty_ms(snapshot: PathSnapshot) -> f64 {
    let loss = snapshot.loss_rate.clamp(0.0, 1.0);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let min_progress = PATH_OPEN_SCORE_BYTES as f64
        / ((snapshot.delivery_rate_bps.max(1.0) / 8.0) * (snapshot.srtt_ms.max(1.0) / 1000.0))
            .max(PATH_OPEN_SCORE_BYTES as f64);
    let expected_repairs = loss / (1.0 - loss).max(min_progress);
    expected_repairs * transport_pto_from_snapshot(Some(snapshot)).as_secs_f64() * 1000.0
}

fn confidence_sample_denominator() -> f64 {
    f64::from(RELIABLE_INITIAL_WINDOW_PACKETS as u32)
}

fn server_output_confidence(entry: &ResponseStreamOutputEntry, _now: Instant) -> f64 {
    let delivery_confidence =
        (f64::from(entry.delivery_samples) / confidence_sample_denominator()).clamp(0.0, 1.0);
    let metric_confidence = match server_output_local_path_metrics(entry) {
        Some(
            path_metrics @ ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics,
                ..
            },
        ) if metrics.has_ack_derived_data_sample
            || metrics.confidence_ppm > 0
            || server_quic_capacity_proof(path_metrics).is_some()
            || server_tcp_capacity_proof(path_metrics).is_some() =>
        {
            let capacity_proof = server_quic_capacity_proof(path_metrics);
            if let Some(proof) = capacity_proof {
                // Receipt bytes are exact token evidence. Encoder record count
                // is an integrity check, not a QUIC packet-sample population.
                let receipt_confidence = (proof.received_bytes as f64
                    / proof.sample_floor_bytes.max(1) as f64)
                    .clamp(0.0, 1.0);
                return delivery_confidence.max(receipt_confidence).clamp(0.0, 1.0);
            }
            if let Some(proof) = server_tcp_capacity_proof(path_metrics) {
                let receipt_confidence =
                    (proof.received_bytes as f64 / proof.train_bytes.max(1) as f64).clamp(0.0, 1.0);
                return delivery_confidence.max(receipt_confidence).clamp(0.0, 1.0);
            }
            let source_confidence =
                f64::from(metrics.confidence_ppm).clamp(0.0, 1_000_000.0) / 1_000_000.0;
            let sample_bytes = metrics.data_sample_bytes;
            let sample_count = u64::from(metrics.data_sample_count);
            let sample_floor = server_path_metrics_bulk_sample_floor_bytes(metrics).max(1);
            let byte_confidence = (sample_bytes as f64 / sample_floor as f64).clamp(0.0, 1.0);
            let count_confidence =
                (sample_count as f64 / confidence_sample_denominator()).clamp(0.0, 1.0);
            let sample_confidence = byte_confidence.min(count_confidence);
            if metrics.has_ack_derived_data_sample {
                source_confidence * sample_confidence
            } else {
                source_confidence
            }
        }
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::PeerHint,
            ..
        }) => 0.0,
        _ => 0.0,
    };
    delivery_confidence.max(metric_confidence).clamp(0.0, 1.0)
}

fn server_quic_capacity_proof(
    path_metrics: ServerPathMetricsEntry,
) -> Option<QuicCapacityProofCandidate> {
    let proof = path_metrics.capacity_proof?;
    (path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.underlay == UnderlayProtocol::Udp
        && valid_quic_capacity_proof_candidate_at(proof, Instant::now()))
    .then_some(proof)
}

fn server_tcp_capacity_proof(
    path_metrics: ServerPathMetricsEntry,
) -> Option<TcpCapacityProofCandidate> {
    let proof = path_metrics.tcp_capacity_proof?;
    (path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.underlay == UnderlayProtocol::Tcp
        && valid_tcp_capacity_proof_candidate_at(proof, Instant::now()))
    .then_some(proof)
}

fn server_path_metrics_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    server_quic_capacity_proof(path_metrics)
        .map(|proof| proof.rate_bps.max(1) as f64)
        .or_else(|| {
            server_tcp_capacity_proof(path_metrics).map(|proof| proof.rate_bps.max(1) as f64)
        })
        .unwrap_or_else(|| path_metrics.metrics.delivery_rate_bps.max(1) as f64)
}

fn server_path_metrics_estimate_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    path_metrics
        .capacity_proof
        .filter(|proof| well_formed_quic_capacity_proof_candidate(*proof))
        .map_or_else(
            || path_metrics.metrics.delivery_rate_bps.max(1) as f64,
            |proof| proof.rate_bps.max(1) as f64,
        )
}

fn server_path_metrics_bulk_sample_floor_bytes(metrics: PathMetrics) -> u64 {
    let carrier_floor = metrics
        .inflight_hi_bytes
        .max(metrics.inflight_limit_bytes)
        .max(PATH_OPEN_SCORE_BYTES as u64);
    match metrics.underlay {
        UnderlayProtocol::Tcp => carrier_floor,
        UnderlayProtocol::Udp => {
            let minimum_meaningful_sample = (PATH_OPEN_SCORE_BYTES as u64).saturating_mul(4);
            let startup_graduation_sample = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
                .saturating_div(2)
                .max(minimum_meaningful_sample);
            carrier_floor
                .max(minimum_meaningful_sample)
                .min(startup_graduation_sample)
        }
    }
}

fn server_udp_path_metrics_has_durable_rate_estimate(path_metrics: ServerPathMetricsEntry) -> bool {
    if path_metrics.source != ServerPathMetricsSource::LocalSender
        || path_metrics.metrics.underlay != UnderlayProtocol::Udp
    {
        return false;
    }
    if path_metrics
        .capacity_proof
        .is_some_and(well_formed_quic_capacity_proof_candidate)
    {
        return true;
    }
    let sample_floor = server_path_metrics_bulk_sample_floor_bytes(path_metrics.metrics);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    path_metrics.metrics.has_ack_derived_data_sample
        && path_metrics.metrics.data_sample_count > 0
        && path_metrics
            .metrics
            .data_sample_bytes
            .saturating_add(packet_accounting_slack)
            >= sample_floor
}

fn server_path_metrics_has_bulk_rate_evidence(path_metrics: ServerPathMetricsEntry) -> bool {
    if server_quic_capacity_proof(path_metrics).is_some()
        || server_tcp_capacity_proof(path_metrics).is_some()
    {
        return true;
    }
    let sample_floor = server_path_metrics_bulk_sample_floor_bytes(path_metrics.metrics);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    let effective_metric_age = Duration::from_micros(u64::from(path_metrics.metrics.metric_age_us))
        .saturating_add(Instant::now().saturating_duration_since(path_metrics.recorded_at));
    let native_bulk_proof_is_eligible = !path_metrics.metrics.app_limited
        && (path_metrics.metrics.underlay != UnderlayProtocol::Udp
            || effective_metric_age
                < quic_bulk_proof_freshness_horizon(
                    Duration::from_micros(u64::from(path_metrics.metrics.srtt_us.max(1))),
                    Duration::from_micros(u64::from(path_metrics.metrics.rttvar_us)),
                ));
    path_metrics.source == ServerPathMetricsSource::LocalSender
        // Source expiry is authoritative; age is defense in depth if an idle
        // refresh is delayed or reordered before response admission runs.
        && native_bulk_proof_is_eligible
        && path_metrics.metrics.has_ack_derived_data_sample
        && path_metrics.metrics.data_sample_count > 0
        && path_metrics
            .metrics
            .data_sample_bytes
            .saturating_add(packet_accounting_slack)
            >= sample_floor
}

fn server_path_metrics_has_ack_data_evidence(path_metrics: ServerPathMetricsEntry) -> bool {
    path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.has_ack_derived_data_sample
}

fn server_path_metrics_has_sender_evidence(path_metrics: ServerPathMetricsEntry) -> bool {
    path_metrics.source == ServerPathMetricsSource::LocalSender
        && (server_path_metrics_has_bulk_rate_evidence(path_metrics)
            || server_path_metrics_has_ack_data_evidence(path_metrics)
            || path_metrics.metrics.confidence_ppm > 0)
}

pub(in crate::runtime) fn server_output_has_sender_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    entry.owner_data_acked_bytes > 0
        || entry.delivery_samples > 0
        || entry.delivery_rate_bps.is_some()
        || matches!(
            server_output_local_path_metrics(entry),
            Some(path_metrics) if server_path_metrics_has_sender_evidence(path_metrics)
        )
}

/// Endpoint-only TCP has no carrier hint worth preserving. After an exact
/// startup sample, it may temporarily inherit the proven Service opportunity
/// instead of running a second exclusive measurement transport.
pub(in crate::runtime) fn server_output_accepts_service_capacity_prior(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    entry.key.underlay == UnderlayProtocol::Tcp
        && !product_delivery_samples_override_startup_prior(entry.delivery_samples)
        && !server_output_local_path_metrics(entry)
            .is_some_and(server_path_metrics_has_bulk_rate_evidence)
        && entry.peer_path_metrics.is_some_and(|metrics| {
            metrics.source == ServerPathMetricsSource::PeerHint
                && metrics.metrics.app_limited
                && !metrics.metrics.has_ack_derived_data_sample
        })
}

pub(in crate::runtime) fn server_output_has_durable_product_progress(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    entry.product_progress_rate_bps.is_some()
        && server_output_has_durable_product_ack_progress(entry, mux_limits)
}

fn server_output_has_durable_product_ack_progress(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    // Exact ownership bytes may be durable even when fragmented callbacks do
    // not produce an individual point-rate sample.
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    entry
        .owner_data_acked_bytes
        .saturating_add(accounting_slack)
        >= sample_floor
}

#[cfg(test)]
pub(in crate::runtime) fn server_output_has_bulk_rate_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    server_output_has_bulk_rate_evidence_with_limits(entry, MuxLimits::default())
}

pub(in crate::runtime) fn server_output_has_bulk_rate_evidence_with_limits(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    let has_local_carrier_bulk = matches!(
        server_output_local_path_metrics(entry),
        Some(path_metrics) if server_path_metrics_has_bulk_rate_evidence(path_metrics)
    );
    match entry.key.underlay {
        UnderlayProtocol::Udp => has_local_carrier_bulk,
        UnderlayProtocol::Tcp => {
            has_local_carrier_bulk || server_output_has_durable_product_progress(entry, mux_limits)
        }
    }
}

pub(in crate::runtime) fn server_output_has_service_feed_evidence_with_limits(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    match entry.key.underlay {
        UnderlayProtocol::Udp => {
            server_output_has_durable_product_progress(entry, mux_limits)
                || matches!(
                    server_output_local_path_metrics(entry),
                    Some(path_metrics) if server_udp_path_metrics_has_durable_rate_estimate(path_metrics)
                )
        }
        UnderlayProtocol::Tcp => {
            server_output_has_bulk_rate_evidence_with_limits(entry, mux_limits)
        }
    }
}

pub(super) fn server_output_quic_capacity_proof_marker(
    entry: &ResponseStreamOutputEntry,
) -> Option<QuicCapacityProofCandidate> {
    server_output_local_path_metrics(entry)
        .filter(|path_metrics| {
            path_metrics.source == ServerPathMetricsSource::LocalSender
                && path_metrics.metrics.underlay == UnderlayProtocol::Udp
        })
        .and_then(|path_metrics| path_metrics.capacity_proof)
}

pub(super) fn server_output_fresh_quic_capacity_proof(
    entry: &ResponseStreamOutputEntry,
) -> Option<QuicCapacityProofCandidate> {
    server_output_local_path_metrics(entry).and_then(server_quic_capacity_proof)
}

pub(in crate::runtime) fn record_server_sender_decision(
    session_id: SessionId,
    stream_id: StreamId,
    key: CarrierPathKey,
    frame: &Frame,
    lane: FlowLane,
    reason: &'static str,
    bulk_rate_evidence: Option<bool>,
) {
    #[cfg(feature = "lab-diagnostics")]
    lab_sender_service_decision(
        "server",
        Some(session_id.0),
        stream_id.0,
        reason,
        sender_service_frame_kind(frame),
        reliable_stream_frame_payload_bytes(frame),
        bulk_rate_evidence,
        format_args!(
            "path_underlay={:?} path_id={} lane={:?} pacing_bytes={}",
            key.underlay,
            key.path_id.0,
            lane,
            frame_pacing_bytes(frame),
        ),
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (
        session_id,
        stream_id,
        key,
        frame,
        lane,
        reason,
        bulk_rate_evidence,
    );
}

#[cfg(feature = "lab-diagnostics")]
pub(super) fn sender_service_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}

#[cfg(test)]
mod tests;
