use super::super::bulk_admission::bulk_service_horizon_payload_bytes;
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
    /// Exclusive calibration estimates carrier capacity; ordinary per-flow
    /// ACK evidence must mature in a separate epoch before replacing it.
    pub(super) tcp_calibration_prior: Option<TcpResponseCalibrationPrior>,
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
pub(in crate::runtime) struct TcpResponseCalibrationPrior {
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
        (UnderlayProtocol::Tcp, None, _, _) if entry.tcp_calibration_prior.is_some() => (
            entry
                .tcp_calibration_prior
                .expect("guarded TCP calibration prior")
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
            || server_quic_capacity_proof(path_metrics).is_some() =>
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

fn server_path_metrics_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    server_quic_capacity_proof(path_metrics).map_or_else(
        || path_metrics.metrics.delivery_rate_bps.max(1) as f64,
        |proof| proof.rate_bps.max(1) as f64,
    )
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
    if server_quic_capacity_proof(path_metrics).is_some() {
        return true;
    }
    let sample_floor = server_path_metrics_bulk_sample_floor_bytes(path_metrics.metrics);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    let effective_metric_age = Duration::from_micros(u64::from(path_metrics.metrics.metric_age_us))
        .saturating_add(Instant::now().saturating_duration_since(path_metrics.recorded_at));
    let udp_bulk_proof_is_eligible = path_metrics.metrics.underlay != UnderlayProtocol::Udp
        || (!path_metrics.metrics.app_limited
            && effective_metric_age
                < quic_bulk_proof_freshness_horizon(
                    Duration::from_micros(u64::from(path_metrics.metrics.srtt_us.max(1))),
                    Duration::from_micros(u64::from(path_metrics.metrics.rttvar_us)),
                ));
    path_metrics.source == ServerPathMetricsSource::LocalSender
        // Source expiry is authoritative; age is defense in depth if an idle
        // refresh is delayed or reordered before response admission runs.
        && udp_bulk_proof_is_eligible
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

pub(in crate::runtime) fn reliable_subflow_startup_sample_limit_bytes(
    mux_limits: MuxLimits,
) -> u64 {
    let configured_envelope = (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(1);
    RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
        .saturating_div(2)
        .max(PATH_OPEN_SCORE_BYTES as u64)
        .min(configured_envelope)
}

pub(in crate::runtime) fn reliable_quic_capacity_calibration_session_limit_bytes(
    mux_limits: MuxLimits,
) -> u64 {
    (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(1)
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
mod tests {
    use super::*;

    fn ack_clock_rate_sample(bytes: u64, rate_bps: f64) -> PathRateSample {
        PathRateSample::new(
            bytes,
            Duration::from_secs_f64(bytes as f64 * 8.0 / rate_bps),
        )
        .expect("valid ACK-clock rate sample")
    }

    fn assert_rate_close(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("calibrated rate");
        let relative_error = (actual - expected).abs() / expected.max(1.0);
        assert!(
            relative_error < 1e-6,
            "expected {expected} bps, got {actual} bps"
        );
    }

    fn apply_ack_clock_evidence_update(
        calibration: &mut ResponseAckClockCalibrationState,
        update: ResponseAckClockRateEvidenceUpdate,
        acked_at: Instant,
    ) -> bool {
        let ResponseAckClockRateEvidenceUpdate::Proven {
            sample,
            bytes,
            fresh_bytes,
            first_window,
            earliest_sent_at,
            ..
        } = update
        else {
            return false;
        };
        calibration.record_ack_clock_window(
            if first_window { None } else { sample },
            bytes,
            fresh_bytes,
            earliest_sent_at,
            acked_at,
        )
    }

    #[test]
    fn response_ack_clock_requires_a_later_window_already_in_flight() {
        let started_at = Instant::now();
        let first_acked_at = started_at + Duration::from_millis(100);
        let mut evidence = ResponseAckClockRateEvidence::new(started_at);
        assert!(matches!(
            evidence.observe(
                PATH_OPEN_SCORE_BYTES as u64,
                started_at,
                started_at,
                first_acked_at,
            ),
            ResponseAckClockRateEvidenceUpdate::Proven {
                sample: Some(_),
                first_window: true,
                ..
            }
        ));

        let second_acked_at = first_acked_at + Duration::from_millis(20);
        let second = evidence.observe(
            PATH_OPEN_SCORE_BYTES as u64,
            first_acked_at - Duration::from_millis(10),
            first_acked_at - Duration::from_millis(10),
            second_acked_at,
        );
        let ResponseAckClockRateEvidenceUpdate::Proven {
            sample: Some(sample),
            first_window: false,
            ..
        } = second
        else {
            panic!("a later window already in flight at the previous ACK is ACK-clocked");
        };
        assert_eq!(
            sample.rate_bps(),
            PathRateSample::new(PATH_OPEN_SCORE_BYTES as u64, Duration::from_millis(20),)
                .expect("non-zero sample")
                .rate_bps()
        );

        let mut app_limited = ResponseAckClockRateEvidence::new(started_at);
        let _ = app_limited.observe(
            PATH_OPEN_SCORE_BYTES as u64,
            started_at,
            started_at,
            first_acked_at,
        );
        assert!(
            matches!(
                app_limited.observe(
                    PATH_OPEN_SCORE_BYTES as u64,
                    first_acked_at - Duration::from_millis(1),
                    first_acked_at + Duration::from_millis(1),
                    first_acked_at + Duration::from_millis(40),
                ),
                ResponseAckClockRateEvidenceUpdate::Proven {
                    sample: None,
                    first_window: false,
                    ..
                }
            ),
            "one pre-ACK send cannot make a mostly post-ACK window causal"
        );
    }

    #[test]
    fn response_ack_clock_goodput_is_invariant_to_callback_compression() {
        let bytes = 64 * 1024;
        let started_at = Instant::now();
        let mut even = ResponseAckClockRateEvidence::new(started_at);
        let mut compressed = ResponseAckClockRateEvidence::new(started_at);

        // The first exact ACK establishes the per-output time boundary.
        let _ = even.observe(bytes, started_at, started_at, started_at);
        let _ = compressed.observe(bytes, started_at, started_at, started_at);

        let even_step = Duration::from_micros(104_858);
        let _ = even.observe(bytes, started_at, started_at, started_at + even_step);
        let _ = even.observe(bytes, started_at, started_at, started_at + even_step * 2);

        // The same bytes arrive after one long control-queue delay followed by
        // a 1 ms callback tail. The long interval must remain in the ratio.
        let _ = compressed.observe(
            bytes,
            started_at,
            started_at,
            started_at + Duration::from_micros(208_716),
        );
        let _ = compressed.observe(
            bytes,
            started_at,
            started_at,
            started_at + Duration::from_micros(209_716),
        );

        let even_rate = even.goodput_sample().expect("even goodput").rate_bps();
        let compressed_rate = compressed
            .goodput_sample()
            .expect("compressed goodput")
            .rate_bps();
        let relative_error = (even_rate - compressed_rate).abs() / even_rate;
        assert!(relative_error < 0.001, "{even_rate} vs {compressed_rate}");
        assert!(compressed_rate < 5_100_000.0);
    }

    #[test]
    fn response_ack_clock_goodput_keeps_elapsed_for_mixed_assignment_window() {
        let bytes = 64 * 1024;
        let started_at = Instant::now();
        let mut evidence = ResponseAckClockRateEvidence::new(started_at);
        let _ = evidence.observe(bytes, started_at, started_at, started_at);

        let mixed_acked_at = started_at + Duration::from_millis(200);
        let mixed = evidence.observe(
            bytes,
            started_at + Duration::from_millis(1),
            started_at + Duration::from_millis(1),
            mixed_acked_at,
        );
        assert!(matches!(
            mixed,
            ResponseAckClockRateEvidenceUpdate::Proven { sample: None, .. }
        ));
        let mixed_rate = evidence
            .goodput_sample()
            .expect("mixed window still has causal goodput")
            .rate_bps();

        let _ = evidence.observe(
            bytes,
            started_at,
            started_at,
            mixed_acked_at + Duration::from_millis(1),
        );
        let tail_rate = evidence
            .goodput_sample()
            .expect("compressed tail goodput")
            .rate_bps();
        assert!(tail_rate < 5_300_000.0, "compressed tail was {tail_rate}");
        assert!(tail_rate > mixed_rate);
    }

    #[test]
    fn response_ack_clock_credit_requires_fresh_evidence_for_each_stage() {
        let initial = 2 * 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, 4 * initial);
        let first_stage_at = calibration.stage_authorized_at;
        let first_growth_at = first_stage_at + Duration::from_millis(100);

        calibration.spent_bytes = initial;
        let sample =
            ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 10_000_000.0);
        assert!(calibration.record_ack_clock_sample(
            sample,
            first_stage_at + Duration::from_millis(1),
            first_growth_at,
        ));
        assert_eq!(calibration.credit_limit_bytes, 2 * initial);
        assert_eq!(calibration.calibrated_rate_bps, None);

        calibration.spent_bytes = 2 * initial;
        let stale_sample =
            ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 20_000_000.0);
        assert!(
            !calibration.record_ack_clock_sample(
                stale_sample,
                first_stage_at + Duration::from_millis(2),
                first_growth_at + Duration::from_millis(10),
            ),
            "residual ACKs from the prior stage cannot pre-authorize another stage"
        );
        assert_eq!(calibration.credit_limit_bytes, 2 * initial);
        let second_growth_at = first_growth_at + Duration::from_millis(20);
        let second_sample =
            ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 15_000_000.0);
        assert!(calibration.record_ack_clock_sample(
            second_sample,
            first_growth_at + Duration::from_millis(1),
            second_growth_at,
        ));
        assert_eq!(calibration.credit_limit_bytes, 4 * initial);
        assert_eq!(calibration.calibrated_rate_bps, None);

        calibration.spent_bytes = 4 * initial;
        assert!(!calibration.proven);
        let final_sample =
            ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 18_000_000.0);
        assert!(!calibration.record_ack_clock_sample(
            final_sample,
            second_growth_at + Duration::from_millis(1),
            second_growth_at + Duration::from_millis(20),
        ));
        assert!(calibration.proven);
        assert_rate_close(calibration.calibrated_rate_bps, 15_000_000.0);
    }

    #[test]
    fn response_ack_clock_small_seed_does_not_lower_publication_coverage() {
        let seed = 64 * 1024;
        let coverage_floor = 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
            seed,
            8 * 1024 * 1024,
            coverage_floor,
        );
        assert_eq!(calibration.stage_rate_coverage_floor_bytes, coverage_floor);

        let authorized_at = calibration.stage_authorized_at;
        calibration.spent_bytes = seed;
        assert!(calibration.record_ack_clock_sample(
            ack_clock_rate_sample(seed, 10_000_000.0),
            authorized_at + Duration::from_millis(1),
            authorized_at + Duration::from_millis(100),
        ));
        assert_eq!(calibration.credit_limit_bytes, coverage_floor);
        assert_eq!(calibration.stage_rate_evidence_bytes, seed);
        assert_eq!(calibration.stage_rate_sample_count(), 0);

        calibration.spent_bytes = calibration.credit_limit_bytes;
        assert!(calibration.record_ack_clock_sample(
            ack_clock_rate_sample(coverage_floor - seed, 10_000_000.0),
            authorized_at + Duration::from_millis(2),
            authorized_at + Duration::from_millis(200),
        ));
        assert_eq!(calibration.stage_rate_sample_count(), 1);
        assert_rate_close(Some(calibration.stage_rate_samples_bps[0]), 10_000_000.0);
        assert_eq!(calibration.calibrated_rate_bps, None);
        assert!(!calibration.proven);
    }

    #[test]
    fn response_ack_clock_stops_at_robust_rate_before_resource_max() {
        let initial = 2 * 1024 * 1024;
        let resource_max = 32 * initial;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, resource_max);
        let mut authorized_at = calibration.stage_authorized_at;

        for (index, rate_bps) in [40_000_000.0, 60_000_000.0, 50_000_000.0]
            .into_iter()
            .enumerate()
        {
            calibration.spent_bytes = calibration.credit_limit_bytes;
            let acked_at = authorized_at + Duration::from_millis(20);
            let grew = calibration.record_ack_clock_sample(
                ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, rate_bps),
                authorized_at + Duration::from_millis(1),
                acked_at,
            );
            if index < 2 {
                assert!(grew);
                authorized_at = acked_at;
            } else {
                assert!(!grew, "a robust median ends exclusive calibration");
            }
        }

        assert!(calibration.proven);
        assert_eq!(calibration.spent_bytes, 4 * initial);
        assert_eq!(calibration.credit_limit_bytes, 4 * initial);
        assert_eq!(calibration.max_limit_bytes, resource_max);
        assert_rate_close(calibration.calibrated_rate_bps, 50_000_000.0);
    }

    #[test]
    fn response_ack_clock_stage_rate_floor_respects_initial_resource_limit() {
        let zero = ResponseAckClockCalibrationState::new(0, 64 * 1024);
        assert_eq!(zero.credit_limit_bytes, 0);
        assert_eq!(zero.max_limit_bytes, 0);
        assert_eq!(zero.stage_rate_coverage_floor_bytes, 0);

        let exact_floor =
            ResponseAckClockCalibrationState::new(MIN_RATE_SAMPLE_BYTES, MIN_RATE_SAMPLE_BYTES);
        assert_eq!(
            exact_floor.stage_rate_coverage_floor_bytes,
            MIN_RATE_SAMPLE_BYTES
        );

        let odd_initial = 2 * MIN_RATE_SAMPLE_BYTES + 1;
        let odd = ResponseAckClockCalibrationState::new(odd_initial, odd_initial);
        assert_eq!(odd.stage_rate_coverage_floor_bytes, odd_initial.div_ceil(2));

        let default = ResponseAckClockCalibrationState::new(2 * 1024 * 1024, 64 * 1024 * 1024);
        assert_eq!(default.stage_rate_coverage_floor_bytes, 1024 * 1024);

        let clamped = ResponseAckClockCalibrationState::new(4 * 1024 * 1024, 1024 * 1024);
        assert_eq!(clamped.credit_limit_bytes, 1024 * 1024);
        assert_eq!(clamped.max_limit_bytes, 1024 * 1024);
        assert_eq!(clamped.stage_rate_coverage_floor_bytes, 512 * 1024);
    }

    #[test]
    fn response_ack_clock_stage_rate_aggregates_prefull_windows_and_terminal_tail() {
        let initial = 2 * 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, 2 * initial);
        let stage_authorized_at = calibration.stage_authorized_at;
        assert_eq!(calibration.stage_rate_coverage_floor_bytes, 1024 * 1024);

        calibration.spent_bytes = initial - 64 * 1024;
        let first = ack_clock_rate_sample(512 * 1024, 80_000_000.0);
        let second = ack_clock_rate_sample(512 * 1024, 40_000_000.0);
        assert!(!calibration.record_ack_clock_sample(
            first,
            stage_authorized_at + Duration::from_millis(1),
            stage_authorized_at + Duration::from_millis(20),
        ));
        assert!(!calibration.record_ack_clock_sample(
            second,
            stage_authorized_at + Duration::from_millis(2),
            stage_authorized_at + Duration::from_millis(40),
        ));

        calibration.spent_bytes = initial;
        let tail = ack_clock_rate_sample(64 * 1024, 4_000_000_000.0);
        assert!(calibration.record_ack_clock_sample(
            tail,
            stage_authorized_at + Duration::from_millis(3),
            stage_authorized_at + Duration::from_millis(50),
        ));

        let aggregate_bytes = first.bytes() + second.bytes() + tail.bytes();
        let aggregate_elapsed = first
            .elapsed()
            .saturating_add(second.elapsed())
            .saturating_add(tail.elapsed());
        let expected_rate = aggregate_bytes as f64 * 8.0 / aggregate_elapsed.as_secs_f64();
        assert_eq!(calibration.stage_rate_sample_count, 1);
        assert_rate_close(Some(calibration.stage_rate_samples_bps[0]), expected_rate);
        assert!(calibration.stage_rate_samples_bps[0] < 100_000_000.0);
        assert_eq!(calibration.stage_rate_evidence_bytes, 0);
        assert_eq!(calibration.stage_rate_evidence_elapsed, Duration::ZERO);
    }

    #[test]
    fn response_ack_clock_stage_rate_is_invariant_to_submillisecond_ack_partitioning() {
        let initial = 2 * 1024 * 1024;
        let mut partitioned = ResponseAckClockCalibrationState::new(initial, 2 * initial);
        let partitioned_stage_at = partitioned.stage_authorized_at;
        partitioned.spent_bytes = initial - 1;
        for index in 0..3 {
            let sample = PathRateSample::new(256 * 1024, Duration::from_micros(250))
                .expect("partitioned rate sample");
            assert!(!partitioned.record_ack_clock_sample(
                sample,
                partitioned_stage_at + Duration::from_millis(index + 1),
                partitioned_stage_at + Duration::from_millis(index + 2),
            ));
        }
        partitioned.spent_bytes = initial;
        let final_partition = PathRateSample::new(256 * 1024, Duration::from_micros(250))
            .expect("final partitioned rate sample");
        assert!(partitioned.record_ack_clock_sample(
            final_partition,
            partitioned_stage_at + Duration::from_millis(4),
            partitioned_stage_at + Duration::from_millis(5),
        ));

        let mut combined = ResponseAckClockCalibrationState::new(initial, 2 * initial);
        let combined_stage_at = combined.stage_authorized_at;
        combined.spent_bytes = initial;
        let combined_sample = PathRateSample::new(1024 * 1024, Duration::from_millis(1))
            .expect("combined rate sample");
        assert!(combined.record_ack_clock_sample(
            combined_sample,
            combined_stage_at + Duration::from_millis(1),
            combined_stage_at + Duration::from_millis(2),
        ));

        assert_rate_close(
            Some(partitioned.stage_rate_samples_bps[0]),
            combined.stage_rate_samples_bps[0],
        );
        assert_rate_close(Some(partitioned.stage_rate_samples_bps[0]), 8_388_608_000.0);
    }

    #[test]
    fn response_ack_clock_full_stage_waits_for_representative_coverage() {
        let initial = 2 * 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, initial);
        let stage_authorized_at = calibration.stage_authorized_at;
        calibration.spent_bytes = initial;

        let tail = ack_clock_rate_sample(64 * 1024, 1_000_000.0);
        assert!(!calibration.record_ack_clock_sample(
            tail,
            stage_authorized_at + Duration::from_millis(1),
            stage_authorized_at + Duration::from_millis(100),
        ));

        assert!(!calibration.proven);
        assert_eq!(calibration.stage_rate_sample_count, 0);
        assert_eq!(calibration.calibrated_rate_bps, None);
        assert_eq!(calibration.stage_rate_evidence_bytes, tail.bytes());
        assert_eq!(calibration.stage_rate_evidence_elapsed, tail.elapsed());

        let rest = ack_clock_rate_sample(960 * 1024, 1_000_000.0);
        assert!(!calibration.record_ack_clock_sample(
            rest,
            stage_authorized_at + Duration::from_millis(2),
            stage_authorized_at + Duration::from_millis(200),
        ));
        assert!(calibration.proven);
        assert_eq!(calibration.stage_rate_sample_count, 1);
        assert_eq!(calibration.stage_rate_evidence_bytes, 0);
    }

    #[test]
    fn response_ack_clock_reachability_topup_preserves_strict_stage_evidence() {
        let initial = 512 * 1024;
        let coverage_floor = 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
            initial,
            4 * initial,
            coverage_floor,
        );
        let first_stage_at = calibration.stage_authorized_at;
        calibration.spent_bytes = initial;

        let uncovered = ack_clock_rate_sample(64 * 1024, 40_000_000.0);
        let second_stage_at = first_stage_at + Duration::from_millis(100);
        assert!(calibration.record_ack_clock_sample(
            uncovered,
            first_stage_at + Duration::from_millis(1),
            second_stage_at,
        ));
        assert_eq!(calibration.stage_rate_sample_count, 0);
        assert_eq!(calibration.stage_rate_evidence_bytes, 64 * 1024);
        assert_eq!(calibration.stage_authorized_spent_bytes, 0);
        assert_eq!(calibration.stage_credit_bytes(), coverage_floor);

        calibration.spent_bytes = calibration.credit_limit_bytes;
        let still_seed = ack_clock_rate_sample(coverage_floor - 64 * 1024, 40_000_000.0);
        assert!(calibration.record_ack_clock_sample(
            still_seed,
            second_stage_at + Duration::from_millis(1),
            second_stage_at + Duration::from_millis(40),
        ));
        assert_eq!(calibration.stage_rate_sample_count, 1);
        assert_rate_close(Some(calibration.stage_rate_samples_bps[0]), 40_000_000.0);
        assert!(!calibration.proven);
        assert_eq!(calibration.stage_rate_evidence_bytes, 0);
        assert_eq!(calibration.stage_rate_evidence_elapsed, Duration::ZERO);
    }

    #[test]
    fn response_ack_clock_stage_reserves_capacity_for_clock_establishment() {
        let start = Instant::now();
        let seed = 512 * 1024;
        let coverage_floor = 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
            seed,
            8 * 1024 * 1024,
            coverage_floor,
        );
        calibration.stage_authorized_at = start;
        calibration.spent_bytes = seed;
        let mut evidence = ResponseAckClockRateEvidence::new(start);

        let first_ack = start + Duration::from_millis(100);
        assert!(apply_ack_clock_evidence_update(
            &mut calibration,
            evidence.observe(
                PATH_OPEN_SCORE_BYTES as u64,
                start + Duration::from_millis(1),
                start + Duration::from_millis(1),
                first_ack,
            ),
            first_ack,
        ));
        assert_eq!(
            calibration.stage_rate_ineligible_bytes,
            PATH_OPEN_SCORE_BYTES as u64
        );
        assert_eq!(calibration.stage_strict_capacity_bytes(), coverage_floor);
        assert_eq!(calibration.stage_authorized_spent_bytes, 0);

        calibration.spent_bytes = calibration.credit_limit_bytes;
        let second_ack = start + Duration::from_millis(200);
        assert!(apply_ack_clock_evidence_update(
            &mut calibration,
            evidence.observe(
                PATH_OPEN_SCORE_BYTES as u64,
                start + Duration::from_millis(110),
                start + Duration::from_millis(110),
                second_ack,
            ),
            second_ack,
        ));
        assert_eq!(
            calibration.stage_rate_ineligible_bytes,
            2 * PATH_OPEN_SCORE_BYTES as u64
        );
        assert!(calibration.stage_strict_capacity_bytes() >= coverage_floor);

        calibration.spent_bytes = calibration.credit_limit_bytes;
        let third_ack = start + Duration::from_millis(300);
        assert!(apply_ack_clock_evidence_update(
            &mut calibration,
            evidence.observe(
                coverage_floor,
                start + Duration::from_millis(150),
                start + Duration::from_millis(150),
                third_ack,
            ),
            third_ack,
        ));
        assert_eq!(calibration.stage_rate_sample_count(), 1);
        assert_eq!(calibration.stage_rate_ineligible_bytes, 0);
        assert_eq!(
            calibration.stage_authorized_spent_bytes,
            calibration.spent_bytes
        );
    }

    #[test]
    fn response_ack_clock_coalesced_warmup_can_exceed_one_floor() {
        let start = Instant::now();
        let coverage_floor = 1024 * 1024;
        let initial = 2 * coverage_floor;
        let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
            initial,
            8 * coverage_floor,
            coverage_floor,
        );
        calibration.stage_authorized_at = start;
        calibration.spent_bytes = initial;
        let mut evidence = ResponseAckClockRateEvidence::new(start);
        let warmup_bytes = coverage_floor + PATH_OPEN_SCORE_BYTES as u64;
        let acked_at = start + Duration::from_millis(100);

        assert!(apply_ack_clock_evidence_update(
            &mut calibration,
            evidence.observe(
                warmup_bytes,
                start + Duration::from_millis(1),
                start + Duration::from_millis(2),
                acked_at,
            ),
            acked_at,
        ));

        assert_eq!(calibration.stage_rate_ineligible_bytes, warmup_bytes);
        assert!(calibration.credit_limit_bytes > initial);
        assert!(calibration.stage_strict_capacity_bytes() >= coverage_floor);
        assert!(!calibration.proven);
    }

    #[test]
    fn response_ack_clock_mixed_window_charges_only_fresh_stage_bytes() {
        let start = Instant::now();
        let authorized_at = start + Duration::from_millis(100);
        let coverage_floor = 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
            2 * coverage_floor,
            4 * coverage_floor,
            coverage_floor,
        );
        calibration.stage_authorized_at = authorized_at;
        calibration.spent_bytes = coverage_floor;
        let total_bytes = 2 * PATH_OPEN_SCORE_BYTES as u64;
        let fresh_bytes = PATH_OPEN_SCORE_BYTES as u64;
        let sample = ack_clock_rate_sample(total_bytes, 20_000_000.0);

        assert!(!calibration.record_ack_clock_window(
            Some(sample),
            total_bytes,
            fresh_bytes,
            authorized_at - Duration::from_millis(1),
            authorized_at + Duration::from_millis(100),
        ));

        assert_eq!(calibration.stage_rate_evidence_bytes, 0);
        assert_eq!(calibration.stage_rate_ineligible_bytes, fresh_bytes);
        assert!(
            calibration.stage_rate_evidence_bytes + calibration.stage_rate_ineligible_bytes
                <= calibration.stage_credit_bytes()
        );
    }

    #[test]
    fn response_ack_clock_drained_seed_restores_reachable_credit_or_terminates_at_cap() {
        let seed = 512 * 1024;
        let coverage_floor = 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
            seed,
            8 * coverage_floor,
            coverage_floor,
        );
        calibration.spent_bytes = seed;

        assert!(calibration.advance_drained_stage(Instant::now()));
        assert_eq!(calibration.stage_rate_ineligible_bytes, seed);
        assert_eq!(calibration.stage_strict_capacity_bytes(), coverage_floor);
        assert!(!calibration.proven);

        let mut capped = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
            seed,
            seed,
            coverage_floor,
        );
        capped.spent_bytes = seed;
        assert!(!capped.advance_drained_stage(Instant::now()));
        assert!(capped.proven);
        assert_eq!(capped.calibrated_rate_bps, None);
    }

    #[test]
    fn response_ack_clock_drain_finalizes_prefull_representative_evidence() {
        let initial = 2 * 1024 * 1024;
        let coverage_floor = 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
            initial,
            2 * initial,
            coverage_floor,
        );
        let authorized_at = calibration.stage_authorized_at;
        calibration.spent_bytes = initial - PATH_OPEN_SCORE_BYTES as u64;

        assert!(!calibration.record_ack_clock_sample(
            ack_clock_rate_sample(coverage_floor, 40_000_000.0),
            authorized_at + Duration::from_millis(1),
            authorized_at + Duration::from_millis(100),
        ));
        assert_eq!(calibration.stage_rate_evidence_bytes, coverage_floor);

        calibration.spent_bytes = initial;
        assert!(calibration.advance_drained_stage(authorized_at + Duration::from_millis(200)));
        assert_eq!(calibration.stage_rate_sample_count(), 1);
        assert_rate_close(Some(calibration.stage_rate_samples_bps[0]), 40_000_000.0);
        assert_eq!(calibration.stage_rate_evidence_bytes, 0);
        assert_eq!(calibration.stage_rate_ineligible_bytes, 0);
    }

    #[test]
    fn response_ack_clock_stage_transition_waits_for_coverage_and_rejects_stale_windows() {
        let initial = 2 * 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, 2 * initial);
        let first_stage_at = calibration.stage_authorized_at;
        calibration.spent_bytes = initial - 64 * 1024;
        let partial = ack_clock_rate_sample(512 * 1024, 20_000_000.0);
        assert!(!calibration.record_ack_clock_sample(
            partial,
            first_stage_at + Duration::from_millis(1),
            first_stage_at + Duration::from_millis(20),
        ));

        calibration.spent_bytes = initial;
        let first_growth_at = first_stage_at + Duration::from_millis(100);
        let tail = ack_clock_rate_sample(64 * 1024, 20_000_000.0);
        assert!(!calibration.record_ack_clock_sample(
            tail,
            first_stage_at + Duration::from_millis(2),
            first_growth_at,
        ));
        assert_eq!(calibration.stage_rate_sample_count, 0);
        assert_eq!(calibration.stage_rate_evidence_bytes, 576 * 1024);

        let representative_tail = ack_clock_rate_sample(512 * 1024, 20_000_000.0);
        assert!(calibration.record_ack_clock_sample(
            representative_tail,
            first_stage_at + Duration::from_millis(3),
            first_growth_at + Duration::from_millis(10),
        ));
        assert_eq!(calibration.stage_rate_sample_count, 1);
        assert_eq!(calibration.stage_rate_evidence_bytes, 0);

        calibration.spent_bytes = calibration.credit_limit_bytes;
        let stale = ack_clock_rate_sample(1024 * 1024, 20_000_000.0);
        assert!(!calibration.record_ack_clock_sample(
            stale,
            first_stage_at + Duration::from_millis(4),
            first_growth_at + Duration::from_millis(20),
        ));
        assert_eq!(calibration.credit_limit_bytes, 2 * initial);
        assert!(!calibration.proven);
        assert_eq!(calibration.stage_rate_evidence_bytes, 0);

        calibration.spent_bytes = initial;
        let fresh_partial = ack_clock_rate_sample(512 * 1024, 20_000_000.0);
        assert!(!calibration.record_ack_clock_sample(
            fresh_partial,
            first_growth_at + Duration::from_millis(11),
            first_growth_at + Duration::from_millis(40),
        ));
        assert_eq!(calibration.stage_rate_evidence_bytes, 512 * 1024);
        calibration.retire();
        assert_eq!(calibration.stage_rate_evidence_bytes, 0);
        assert_eq!(calibration.stage_rate_evidence_elapsed, Duration::ZERO);
    }

    #[test]
    fn response_ack_clock_rate_uses_stage_median_instead_of_compressed_ack_peak() {
        let initial = 2 * 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, 32 * initial);
        let stage_samples = [65_000_000.0, 1_740_000_000.0, 73_000_000.0];

        for (index, sample_bps) in stage_samples.into_iter().enumerate() {
            calibration.spent_bytes = calibration.credit_limit_bytes;
            let stage_authorized_at = calibration.stage_authorized_at;
            let sample =
                ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, sample_bps);
            let grew = calibration.record_ack_clock_sample(
                sample,
                stage_authorized_at + Duration::from_millis(1),
                stage_authorized_at + Duration::from_millis(10),
            );
            assert_eq!(grew, index < 2);
        }

        assert!(calibration.proven);
        assert_eq!(calibration.credit_limit_bytes, 4 * initial);
        assert_rate_close(calibration.calibrated_rate_bps, 73_000_000.0);
    }

    #[test]
    fn response_ack_clock_stage_median_matches_v17_stable_path_samples() {
        let initial = 2 * 1024 * 1024;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, 32 * initial);
        for sample_bps in [61_220_000.0, 16_150_000.0, 104_389_000.0] {
            calibration.spent_bytes = calibration.credit_limit_bytes;
            let stage_authorized_at = calibration.stage_authorized_at;
            let sample =
                ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, sample_bps);
            let _ = calibration.record_ack_clock_sample(
                sample,
                stage_authorized_at + Duration::from_millis(1),
                stage_authorized_at + Duration::from_millis(10),
            );
        }
        assert!(calibration.proven);
        assert_rate_close(calibration.calibrated_rate_bps, 61_220_000.0);
    }

    #[test]
    fn response_ack_clock_stage_ring_is_not_a_monotonic_peak_filter() {
        let mut calibration = ResponseAckClockCalibrationState::new(1, 1);
        for sample_bps in [10.0, 20.0, 30.0, 40.0, 50.0] {
            calibration.record_stage_rate_sample(sample_bps);
        }
        assert_eq!(calibration.calibrated_rate_bps, Some(30.0));
        calibration.record_stage_rate_sample(100.0);
        assert_eq!(calibration.calibrated_rate_bps, Some(40.0));
        for sample_bps in [5.0, 1.0, 2.0] {
            calibration.record_stage_rate_sample(sample_bps);
        }
        assert_eq!(calibration.calibrated_rate_bps, Some(5.0));
    }

    #[test]
    fn retired_response_ack_clock_state_cannot_publish_later_generic_acks() {
        let mut calibration = ResponseAckClockCalibrationState::new(1024, 4096);
        calibration.spent_bytes = 1024;
        calibration.retire();
        let stage_authorized_at = calibration.stage_authorized_at;
        let sample = ack_clock_rate_sample(MIN_RATE_SAMPLE_BYTES, 7_000_000_000.0);

        assert!(!calibration.record_ack_clock_sample(
            sample,
            stage_authorized_at + Duration::from_millis(1),
            stage_authorized_at + Duration::from_millis(10),
        ));
        assert_eq!(calibration.calibrated_rate_bps, None);
        assert!(!calibration.proven);
        assert_eq!(calibration.credit_limit_bytes, calibration.spent_bytes);
        assert_eq!(calibration.max_limit_bytes, calibration.spent_bytes);
    }

    #[test]
    fn udp_product_ack_without_unique_owner_rate_is_sender_evidence_not_bulk_rate() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let entry = ResponseStreamOutputEntry {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            },
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 1,
            owner_data_acked_bytes: 0,
            local_path_metrics: None,
            peer_path_metrics: None,
        };

        assert!(
            server_output_has_sender_evidence(&entry),
            "product ACK samples still prove end-to-end sender progress"
        );
        assert!(
            !server_output_has_bulk_rate_evidence(&entry),
            "a UDP product ACK without a path-scoped owner rate is sender evidence, not bulk-rate evidence"
        );
    }

    #[test]
    fn udp_unique_owner_ack_product_rate_does_not_replace_carrier_rate() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let product_rate = 42_000_000.0;
        let mut entry = ResponseStreamOutputEntry {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: Some(product_rate),
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 1,
            owner_data_acked_bytes: reliable_subflow_startup_sample_limit_bytes(
                MuxLimits::default(),
            ),
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 160_000,
                    srtt_us: 160_000,
                    rttvar_us: 5_000,
                    jitter_us: 5_000,
                    delivery_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                    pacing_rate_bps: 200_000_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                    inflight_hi_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                    confidence_ppm: 1_000_000,
                    app_limited: true,
                    has_ack_derived_data_sample: true,
                    data_sample_count: 0,
                    data_sample_bytes: 0,
                },
            }),
            peer_path_metrics: None,
        };

        assert!(!server_output_has_bulk_rate_evidence(&entry));
        assert!(
            server_output_has_service_feed_evidence_with_limits(&entry, MuxLimits::default()),
            "unique product ACK progress may release the current QUIC Service feed without replacing carrier rate"
        );
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(78),
            FlowLane::Throughput,
            &ServerPathLaneTracker::default(),
            MuxLimits::default(),
            Instant::now(),
        );
        assert_eq!(
            snapshot.delivery_rate_bps,
            default_path_rate_bps(UnderlayProtocol::Udp),
            "product STREAM_ACK timing is backlog evidence, not QUIC carrier delivery rate"
        );
        assert_eq!(snapshot.product_progress_rate_bps, Some(product_rate));
        assert!(snapshot.has_durable_product_progress);
        assert_eq!(
            snapshot.pacing_rate_bps, 200_000_000.0,
            "local QUIC pacing remains carrier-owned scheduling evidence even when the carrier ACK sample is app-limited"
        );

        entry.product_progress_rate_bps = None;
        let fragmented_snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(78),
            FlowLane::Throughput,
            &ServerPathLaneTracker::default(),
            MuxLimits::default(),
            Instant::now(),
        );
        assert!(fragmented_snapshot.has_durable_product_progress);
        assert!(fragmented_snapshot.product_progress_rate_bps.is_none());
        assert!(!server_output_has_durable_product_progress(
            &entry,
            MuxLimits::default()
        ));
    }

    #[test]
    fn one_owner_quantum_is_sender_evidence_but_not_bulk_rate_proof() {
        let mux_limits = MuxLimits::default();
        let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
        assert!(sample_floor > BBR_MAX_SEND_QUANTUM_BYTES as u64);

        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let (commands, _receivers) = reliable_path_command_channels(8);
            let entry = ResponseStreamOutputEntry {
                key: CarrierPathKey {
                    underlay,
                    path_id: PathId(13),
                },
                path_instance_id: next_server_carrier_path_instance_id(),
                incarnation: 1,
                commands,
                role: StreamOpenRole::Validation,
                owner_data_in_flight_bytes: 0,
                bytes_in_flight: 0,
                product_queue_bytes: 0,
                product_progress_rate_bps: Some(80_000_000.0),
                delivery_rate_bps: (underlay == UnderlayProtocol::Tcp).then_some(80_000_000.0),
                tcp_ack_clock_rate_bps: None,
                tcp_product_rate_evidence: None,
                tcp_calibration_prior: None,
                srtt_ms: Some(40.0),
                delivery_samples: 1,
                owner_data_acked_bytes: BBR_MAX_SEND_QUANTUM_BYTES as u64,
                local_path_metrics: None,
                peer_path_metrics: None,
            };

            assert!(server_output_has_sender_evidence(&entry), "{underlay:?}");
            assert!(
                !server_output_has_durable_product_progress(&entry, mux_limits),
                "{underlay:?} one-quantum point rate is not durable product progress"
            );
            assert!(
                !server_output_has_bulk_rate_evidence_with_limits(&entry, mux_limits),
                "{underlay:?} must not graduate from one application-limited OwnerData quantum"
            );
        }
    }

    #[test]
    fn local_carrier_bulk_evidence_requires_response_direction_and_exact_path_identity() {
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(21),
        };
        let metrics = PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
            queue_bytes: 0,
            inflight_limit_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
            inflight_hi_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 32,
            data_sample_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
        };
        let entry_with_metrics = |metrics| {
            let (commands, _receivers) = reliable_path_command_channels(8);
            ResponseStreamOutputEntry {
                key,
                path_instance_id: next_server_carrier_path_instance_id(),
                incarnation: 1,
                commands,
                role: StreamOpenRole::Validation,
                owner_data_in_flight_bytes: 0,
                bytes_in_flight: 0,
                product_queue_bytes: 0,
                product_progress_rate_bps: None,
                delivery_rate_bps: None,
                tcp_ack_clock_rate_bps: None,
                tcp_product_rate_evidence: None,
                tcp_calibration_prior: None,
                srtt_ms: None,
                delivery_samples: 0,
                owner_data_acked_bytes: 0,
                local_path_metrics: Some(ServerPathMetricsEntry {
                    source: ServerPathMetricsSource::LocalSender,
                    recorded_at: Instant::now(),
                    capacity_proof: None,
                    metrics,
                }),
                peer_path_metrics: None,
            }
        };

        assert!(server_output_has_bulk_rate_evidence(&entry_with_metrics(
            metrics
        )));
        for invalid in [
            PathMetrics {
                direction: PathMetricDirection::ClientToServer,
                ..metrics
            },
            PathMetrics {
                underlay: UnderlayProtocol::Tcp,
                ..metrics
            },
            PathMetrics {
                path_id: PathId(key.path_id.0 + 1),
                ..metrics
            },
        ] {
            let entry = entry_with_metrics(invalid);
            assert!(!server_output_has_sender_evidence(&entry));
            assert!(!server_output_has_bulk_rate_evidence(&entry));
            let snapshot = server_bulk_output_snapshot(
                &entry,
                SessionId(79),
                FlowLane::Throughput,
                &ServerPathLaneTracker::default(),
                MuxLimits::default(),
                Instant::now(),
            );
            assert_eq!(
                snapshot.delivery_rate_bps,
                default_path_rate_bps(key.underlay),
                "foreign carrier metrics must not influence the response path model"
            );
        }
    }

    #[test]
    fn aged_udp_metric_loses_handoff_rights_but_keeps_sender_reachability() {
        let metrics = PathMetrics {
            path_id: PathId(22),
            underlay: UnderlayProtocol::Udp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 5_000,
            jitter_us: 5_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
            inflight_hi_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 32,
            data_sample_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
        };
        let freshness_horizon = quic_bulk_proof_freshness_horizon(
            Duration::from_micros(u64::from(metrics.srtt_us)),
            Duration::from_micros(u64::from(metrics.rttvar_us)),
        );
        let local = |metrics| ServerPathMetricsEntry {
            source: ServerPathMetricsSource::LocalSender,
            metrics,
            recorded_at: Instant::now(),
            capacity_proof: None,
        };

        let mut fresh = metrics;
        fresh.metric_age_us =
            u32::try_from((freshness_horizon - QUIC_TIMER_GRANULARITY).as_micros())
                .expect("test freshness horizon");
        assert!(server_path_metrics_has_bulk_rate_evidence(local(fresh)));

        let mut aged = metrics;
        aged.metric_age_us =
            u32::try_from(freshness_horizon.as_micros()).expect("test freshness horizon");
        let aged = local(aged);
        assert!(!server_path_metrics_has_bulk_rate_evidence(aged));
        assert!(server_path_metrics_has_sender_evidence(aged));

        let delayed_idle_refresh = ServerPathMetricsEntry {
            source: ServerPathMetricsSource::LocalSender,
            metrics,
            recorded_at: Instant::now() - freshness_horizon,
            capacity_proof: None,
        };
        assert!(!server_path_metrics_has_bulk_rate_evidence(
            delayed_idle_refresh
        ));
        assert!(server_path_metrics_has_sender_evidence(
            delayed_idle_refresh
        ));
    }

    #[test]
    fn accepted_quic_capacity_marker_uses_frozen_floor_rate_and_deadline() {
        let sample_floor = reliable_subflow_startup_sample_limit_bytes(MuxLimits::default());
        let accepted_at = Instant::now();
        let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
        let required_proof_bytes = sample_floor - accounting_slack;
        let proof_elapsed = Duration::from_millis(10);
        let candidate = QuicCapacityProofCandidate {
            token: 31,
            train_bytes: sample_floor,
            sample_floor_bytes: sample_floor,
            accounting_slack_bytes: accounting_slack,
            warmup_bytes: 0,
            required_proof_bytes,
            written_bytes: sample_floor,
            written_data_frame_count: 32,
            receipt_confirmed: true,
            received_bytes: sample_floor,
            proof_elapsed,
            rate_bps: quic_capacity_receipt_rate_bps(sample_floor, proof_elapsed)
                .expect("valid receipt rate"),
            accepted_at,
            expires_at: accepted_at + Duration::from_secs(1),
            proof_validity: Duration::from_secs(1),
        };
        let metrics = PathMetrics {
            path_id: PathId(23),
            underlay: UnderlayProtocol::Udp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: u32::MAX,
            min_rtt_us: 20_000,
            srtt_us: 1,
            rttvar_us: 0,
            jitter_us: 0,
            delivery_rate_bps: 1,
            pacing_rate_bps: 1,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 1,
            inflight_hi_bytes: 1,
            confidence_ppm: 1_000_000,
            app_limited: true,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        };
        let accepted = ServerPathMetricsEntry {
            metrics,
            source: ServerPathMetricsSource::LocalSender,
            recorded_at: accepted_at,
            capacity_proof: Some(candidate),
        };
        assert!(server_path_metrics_has_bulk_rate_evidence(accepted));
        assert_eq!(
            server_path_metrics_rate_bps(accepted),
            candidate.rate_bps as f64
        );
        let (commands, _receivers) = reliable_path_command_channels(8);
        let output = ResponseStreamOutputEntry {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: metrics.path_id,
            },
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Validation,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 0,
            owner_data_acked_bytes: 0,
            local_path_metrics: Some(accepted),
            peer_path_metrics: None,
        };
        assert_eq!(
            server_output_confidence(&output, Instant::now()),
            1.0,
            "exact receipt bytes establish confidence without generic ACK samples"
        );
        let lane_tracker = ServerPathLaneTracker::default();
        let accepted_snapshot = server_bulk_output_snapshot(
            &output,
            SessionId(77),
            FlowLane::Throughput,
            &lane_tracker,
            MuxLimits::default(),
            Instant::now(),
        );
        assert_eq!(
            accepted_snapshot.delivery_rate_bps,
            candidate.rate_bps as f64
        );

        let expired = ServerPathMetricsEntry {
            capacity_proof: Some(QuicCapacityProofCandidate {
                accepted_at: accepted_at - Duration::from_secs(2),
                expires_at: accepted_at - Duration::from_secs(1),
                ..candidate
            }),
            ..accepted
        };
        assert!(!server_path_metrics_has_bulk_rate_evidence(expired));
        assert_eq!(
            server_path_metrics_estimate_rate_bps(expired),
            candidate.rate_bps as f64
        );
        let mut expired_output = output;
        expired_output.local_path_metrics = Some(expired);
        let expired_snapshot = server_bulk_output_snapshot(
            &expired_output,
            SessionId(77),
            FlowLane::Throughput,
            &lane_tracker,
            MuxLimits::default(),
            Instant::now(),
        );
        assert_eq!(
            expired_snapshot.delivery_rate_bps,
            candidate.rate_bps as f64
        );
        assert!(!server_output_has_bulk_rate_evidence(&expired_output));
    }

    #[test]
    fn udp_bulk_rate_evidence_requires_source_fresh_non_app_limited_state() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let mut entry = ResponseStreamOutputEntry {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: Some(500_000_000.0),
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 0,
            owner_data_acked_bytes: 0,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 20_000,
                    srtt_us: 20_000,
                    rttvar_us: 1_000,
                    jitter_us: 1_000,
                    delivery_rate_bps: 200_000_000,
                    pacing_rate_bps: 200_000_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
                    queue_bytes: 0,
                    inflight_limit_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
                    inflight_hi_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
                    confidence_ppm: 1_000_000,
                    app_limited: true,
                    has_ack_derived_data_sample: true,
                    data_sample_count: 32,
                    data_sample_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
                },
            }),
            peer_path_metrics: None,
        };

        assert!(
            !server_output_has_bulk_rate_evidence(&entry),
            "the QUIC tracker keeps fresh historical proof non-app-limited; a published app-limited state cannot authorize placement"
        );
        assert!(
            server_output_has_service_feed_evidence_with_limits(&entry, MuxLimits::default()),
            "a substantial local ACK-derived QUIC sample may feed its current Service while native QUIC still owns cwnd and pacing"
        );
        assert!(server_output_has_sender_evidence(&entry));
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(77),
            FlowLane::Throughput,
            &ServerPathLaneTracker::default(),
            MuxLimits::default(),
            Instant::now(),
        );
        assert_eq!(snapshot.delivery_rate_bps, 200_000_000.0);
        assert!(
            !server_output_has_bulk_rate_evidence(&entry),
            "retaining a QUIC bandwidth estimate must not mint placement authority"
        );

        entry
            .local_path_metrics
            .as_mut()
            .expect("local QUIC sender metrics")
            .metrics
            .app_limited = false;
        assert!(
            server_output_has_bulk_rate_evidence(&entry),
            "the same full-volume sample becomes optional-path proof only after the carrier reports non-app-limited delivery"
        );
    }

    #[test]
    fn udp_app_limited_ack_data_snapshot_keeps_carrier_inflight_limit() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(7),
        };
        let entry = ResponseStreamOutputEntry {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 0,
            owner_data_acked_bytes: 0,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 80_000,
                    srtt_us: 80_000,
                    rttvar_us: 2_000,
                    jitter_us: 2_000,
                    delivery_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                    pacing_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: 2 * 1024 * 1024,
                    inflight_hi_bytes: 2 * 1024 * 1024,
                    confidence_ppm: 0,
                    app_limited: true,
                    has_ack_derived_data_sample: true,
                    data_sample_count: 0,
                    data_sample_bytes: 0,
                },
            }),
            peer_path_metrics: None,
        };

        let lane_tracker = ServerPathLaneTracker::default();
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(77),
            FlowLane::Throughput,
            &lane_tracker,
            MuxLimits::default(),
            Instant::now(),
        );

        assert_eq!(
            snapshot.delivery_rate_bps,
            default_path_rate_bps(UnderlayProtocol::Udp),
            "app-limited ACK-data must not create a tiny bulk-rate model"
        );
        assert_eq!(
            snapshot.inflight_limit_bytes,
            2 * 1024 * 1024,
            "carrier inflight credit is path-local QUIC state and remains usable for bounded exploration"
        );
        assert!(
            !server_output_has_bulk_rate_evidence(&entry),
            "ACK-data seen without non-app-limited samples is not ordinary bulk-rate proof"
        );
    }

    #[test]
    fn local_proof_metrics_are_sender_evidence_not_ack_data_evidence() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(10),
        };
        let entry = ResponseStreamOutputEntry {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 0,
            owner_data_acked_bytes: 0,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 40_000,
                    srtt_us: 40_000,
                    rttvar_us: 2_000,
                    jitter_us: 2_000,
                    delivery_rate_bps: 32_000,
                    pacing_rate_bps: 32_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
                    inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
                    confidence_ppm: 1_000_000,
                    app_limited: true,
                    has_ack_derived_data_sample: false,
                    data_sample_count: 0,
                    data_sample_bytes: 0,
                },
            }),
            peer_path_metrics: None,
        };

        assert!(server_output_has_sender_evidence(&entry));
        assert!(!server_output_has_bulk_rate_evidence(&entry));
    }

    #[test]
    fn udp_tiny_non_app_limited_sample_is_ack_data_not_bulk_rate_evidence() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(9),
        };
        let sample_floor = 2 * 1024 * 1024;
        let entry = ResponseStreamOutputEntry {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 0,
            owner_data_acked_bytes: 0,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 80_000,
                    srtt_us: 80_000,
                    rttvar_us: 2_000,
                    jitter_us: 2_000,
                    delivery_rate_bps: 12_000_000,
                    pacing_rate_bps: 12_000_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: sample_floor,
                    inflight_hi_bytes: sample_floor,
                    confidence_ppm: 1_000_000,
                    app_limited: false,
                    has_ack_derived_data_sample: true,
                    data_sample_count: 4,
                    data_sample_bytes: PATH_OPEN_SCORE_BYTES as u64,
                },
            }),
            peer_path_metrics: None,
        };

        assert!(matches!(
            entry.local_path_metrics,
            Some(path_metrics) if server_path_metrics_has_ack_data_evidence(path_metrics)
        ));
        assert!(
            !server_output_has_bulk_rate_evidence(&entry),
            "bulk-rate promotion requires enough ACKed byte volume, not just non-app-limited ACK count"
        );
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(77),
            FlowLane::Throughput,
            &ServerPathLaneTracker::default(),
            MuxLimits::default(),
            Instant::now(),
        );
        assert_eq!(
            snapshot.delivery_rate_bps,
            default_path_rate_bps(UnderlayProtocol::Udp)
        );
        assert!(snapshot.confidence < 1.0);
    }

    #[test]
    fn udp_startup_window_sample_graduates_even_when_inflight_limit_is_larger() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(11),
        };
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let entry = ResponseStreamOutputEntry {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 0,
            owner_data_acked_bytes: 0,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 160_000,
                    srtt_us: 160_000,
                    rttvar_us: 5_000,
                    jitter_us: 5_000,
                    delivery_rate_bps: 42_000_000,
                    pacing_rate_bps: 42_000_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                    inflight_hi_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                    confidence_ppm: 1_000_000,
                    app_limited: false,
                    has_ack_derived_data_sample: true,
                    data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                    data_sample_bytes: sample_bytes,
                },
            }),
            peer_path_metrics: None,
        };

        assert!(
            server_output_has_bulk_rate_evidence(&entry),
            "a path with substantial non-app-limited QUIC ACK-derived product data must not be trapped below a transient inflight-limit floor"
        );
    }

    #[test]
    fn udp_near_startup_window_sample_graduates_with_packet_accounting_slack() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(12),
        };
        let sample_floor = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let entry = ResponseStreamOutputEntry {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: None,
            delivery_samples: 0,
            owner_data_acked_bytes: 0,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 160_000,
                    srtt_us: 160_000,
                    rttvar_us: 5_000,
                    jitter_us: 5_000,
                    delivery_rate_bps: 42_000_000,
                    pacing_rate_bps: 42_000_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                    inflight_hi_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                    confidence_ppm: 1_000_000,
                    app_limited: false,
                    has_ack_derived_data_sample: true,
                    data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                    data_sample_bytes: sample_floor.saturating_sub(TRANSPORT_MSS_BYTES as u64),
                },
            }),
            peer_path_metrics: None,
        };

        assert!(
            server_output_has_bulk_rate_evidence(&entry),
            "bulk-rate graduation should tolerate packet-accounting slack around the startup evidence floor"
        );
    }

    #[test]
    fn tcp_response_snapshot_persistent_delivery_samples_override_default_prior() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let prior_rate = default_path_rate_bps(UnderlayProtocol::Tcp);
        let entry = ResponseStreamOutputEntry {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            },
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Active,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: Some(prior_rate / 10.0),
            delivery_rate_bps: Some(prior_rate / 10.0),
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_calibration_prior: None,
            srtt_ms: Some(default_path_srtt_ms(UnderlayProtocol::Tcp)),
            delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            owner_data_acked_bytes: reliable_subflow_startup_sample_limit_bytes(
                MuxLimits::default(),
            ),
            local_path_metrics: None,
            peer_path_metrics: None,
        };

        let lane_tracker = ServerPathLaneTracker::default();
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(77),
            FlowLane::Throughput,
            &lane_tracker,
            MuxLimits::default(),
            Instant::now(),
        );

        assert_eq!(snapshot.delivery_rate_bps, prior_rate / 10.0);
    }

    #[test]
    fn response_eta_uses_delivered_rate_not_inflated_quic_pacing_rate() {
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(2),
        };
        let mut baseline = PathSnapshot::new(key.path_id, key.underlay, 100.0, 50_000_000.0);
        baseline.pacing_rate_bps = 50_000_000.0;
        baseline.confidence = 1.0;

        let mut inflated_pacing = baseline;
        inflated_pacing.pacing_rate_bps = 5_000_000_000.0;

        let payload_bytes = 64 * 1024;
        let baseline_eta = server_bulk_output_eta_ms(
            key,
            baseline,
            Some(key),
            FlowLane::Throughput,
            payload_bytes,
            MuxLimits::default(),
        );
        let inflated_eta = server_bulk_output_eta_ms(
            key,
            inflated_pacing,
            Some(key),
            FlowLane::Throughput,
            payload_bytes,
            MuxLimits::default(),
        );

        assert!(
            (baseline_eta - inflated_eta).abs() < 0.001,
            "QUIC pacing is carrier send permission, not delivered product throughput"
        );
    }

    #[test]
    fn response_subflow_eta_uses_owner_quantum_not_service_horizon() {
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let subflow = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let mut snapshot = PathSnapshot::new(subflow.path_id, subflow.underlay, 80.0, 40_000_000.0);
        snapshot.confidence = 1.0;

        let eta_ms = server_bulk_output_eta_ms(
            subflow,
            snapshot,
            Some(service),
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
        );

        assert!(
            eta_ms < 100.0,
            "Subflow ETA must model the next assigned owner range, not a full Service horizon; got {eta_ms:.3}ms"
        );
    }
}
