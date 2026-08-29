//! Native QUIC congestion and ACK instrumentation.

use crate::transport::{LossPolicyPercent, PathMetadata};
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy)]
pub struct CongestionMetrics {
    /// Monotonic identity of the current network-path congestion model.
    pub path_epoch: u64,
    /// Monotonic identity of the current non-app-limited delivery clock.
    pub delivery_clock_epoch: u64,
    pub congestion_window: u64,
    pub bytes_in_flight: Option<u64>,
    pub pending_bytes: u64,
    pub pacing_rate_bps: Option<u64>,
    pub loss_ppm: Option<u32>,
    /// Cumulative bytes declared lost by Quinn's native recovery controller.
    pub lost_bytes: u64,
    pub ecn_ppm: Option<u32>,
    pub newly_acked_bytes: Option<u64>,
    pub non_app_limited_acked_bytes: Option<u64>,
    /// Timed ACK bytes accumulated within `delivery_clock_epoch`.
    ///
    /// Unlike the preceding per-snapshot counters, this is a current-epoch
    /// total. Consumers must subtract a cursor with the same epoch identity.
    pub timed_non_app_limited_acked_bytes: Option<u64>,
    /// Timed ACK/send-clock duration accumulated within `delivery_clock_epoch`.
    pub non_app_limited_ack_elapsed: Option<Duration>,
    /// Current-clock Product bytes carried by timed non-app-limited ACKs.
    pub timed_non_app_limited_delivery_evidence_acked_bytes: u64,
    /// Current-clock ACK samples in the qualified Product-proof tuple.
    pub timed_non_app_limited_delivery_evidence_sample_count: u64,
    /// Current-clock duration in the qualified Product-proof tuple.
    pub timed_non_app_limited_delivery_evidence_elapsed: Duration,
    pub delivery_evidence_written_bytes: u64,
    pub delivery_evidence_cancelled_bytes: u64,
    pub delivery_evidence_pending_ack_bytes: u64,
    pub delivery_evidence_newly_acked_bytes: Option<u64>,
    pub delivery_sample_count: u64,
    pub non_app_limited_delivery_sample_count: u64,
    /// Timed ACK samples accumulated within `delivery_clock_epoch`.
    pub timed_non_app_limited_delivery_sample_count: u64,
    pub app_limited: bool,
}

#[derive(Debug, Default)]
pub(super) struct InstrumentedBbrConfig {
    loss_compensation: LossPolicyPercent,
}

impl InstrumentedBbrConfig {
    pub(super) fn for_path(metadata: &PathMetadata) -> Self {
        Self {
            loss_compensation: metadata.loss_compensation.unwrap_or_default(),
        }
    }

    fn bbr3_config(loss_compensation: LossPolicyPercent) -> quinn::congestion::Bbr3Config {
        let mut config = quinn::congestion::Bbr3Config::default();
        config.loss_compensation_floor(f64::from(loss_compensation.ppm()) / 1_000_000.0);
        config
    }

    fn build_bbr3(
        loss_compensation: LossPolicyPercent,
        now: Instant,
        current_mtu: u16,
    ) -> Box<dyn quinn::congestion::Controller> {
        quinn::congestion::ControllerFactory::build(
            Arc::new(Self::bbr3_config(loss_compensation)),
            now,
            current_mtu,
        )
    }
}

#[derive(Debug, Default)]
pub(super) struct QuicCarrierTelemetry {
    next_path_epoch: AtomicU64,
    current_path_epoch: AtomicU64,
    delivery_evidence_written_bytes: AtomicU64,
    delivery_evidence_cancelled_bytes: AtomicU64,
    delivery_evidence_pending_ack_bytes: AtomicU64,
    delivery_activity_started: Arc<Notify>,
}

#[derive(Debug, Default)]
struct QuicPathTelemetry {
    path_epoch: u64,
    bytes_in_flight: AtomicU64,
    bytes_in_flight_authoritative: AtomicBool,
    // Quinn invokes congestion callbacks through one mutable controller. The
    // sequence makes its cumulative ACK counters coherent for a concurrent
    // metrics reader without putting a lock on the packet ACK hot path.
    ack_snapshot_sequence: AtomicU64,
    delivery_clock_epoch: AtomicU64,
    newly_acked_bytes: AtomicU64,
    non_app_limited_acked_bytes: AtomicU64,
    timed_non_app_limited_acked_bytes: AtomicU64,
    non_app_limited_ack_elapsed_nanos: AtomicU64,
    delivery_sample_count: AtomicU64,
    non_app_limited_delivery_sample_count: AtomicU64,
    timed_non_app_limited_delivery_sample_count: AtomicU64,
    timed_non_app_limited_delivery_evidence_acked_bytes: AtomicU64,
    timed_non_app_limited_delivery_evidence_sample_count: AtomicU64,
    timed_non_app_limited_delivery_evidence_elapsed_nanos: AtomicU64,
    delivery_evidence_acked_bytes: AtomicU64,
    ack_snapshot_cursor: Mutex<QuicAckTelemetryTotals>,
    sent_bytes: AtomicU64,
    lost_bytes: AtomicU64,
    app_limited: AtomicBool,
}

#[derive(Debug, Default, Clone, Copy)]
struct QuicAckTelemetryTotals {
    delivery_clock_epoch: u64,
    acked_bytes: u64,
    non_app_limited_acked_bytes: u64,
    timed_non_app_limited_acked_bytes: u64,
    non_app_limited_ack_elapsed_nanos: u64,
    sample_count: u64,
    non_app_limited_sample_count: u64,
    timed_non_app_limited_sample_count: u64,
    timed_non_app_limited_delivery_evidence_acked_bytes: u64,
    timed_non_app_limited_delivery_evidence_sample_count: u64,
    timed_non_app_limited_delivery_evidence_elapsed_nanos: u64,
    delivery_evidence_acked_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct QuicCarrierTelemetrySnapshot {
    pub(super) path_epoch: u64,
    pub(super) delivery_clock_epoch: u64,
    pub(super) bytes_in_flight: Option<u64>,
    pub(super) newly_acked_bytes: Option<u64>,
    pub(super) non_app_limited_acked_bytes: Option<u64>,
    pub(super) timed_non_app_limited_acked_bytes: Option<u64>,
    pub(super) non_app_limited_ack_elapsed: Option<Duration>,
    pub(super) timed_non_app_limited_delivery_evidence_acked_bytes: u64,
    pub(super) timed_non_app_limited_delivery_evidence_sample_count: u64,
    pub(super) timed_non_app_limited_delivery_evidence_elapsed: Duration,
    pub(super) delivery_sample_count: u64,
    pub(super) non_app_limited_delivery_sample_count: u64,
    pub(super) timed_non_app_limited_delivery_sample_count: u64,
    pub(super) delivery_evidence_newly_acked_bytes: Option<u64>,
    pub(super) loss_ppm: Option<u32>,
    pub(super) lost_bytes: u64,
    pub(super) app_limited: bool,
}

pub(super) struct InstrumentedController {
    inner: Box<dyn quinn::congestion::Controller>,
    loss_compensation: LossPolicyPercent,
    pub(super) telemetry: Arc<QuicCarrierTelemetry>,
    path_telemetry: Arc<QuicPathTelemetry>,
    ack_batch_acked_bytes: u64,
    ack_batch_non_app_limited_acked_bytes: u64,
    ack_batch_sample_count: u64,
    ack_batch_non_app_limited_sample_count: u64,
    ack_batch_earliest_non_app_limited_sent: Option<Instant>,
    ack_batch_latest_non_app_limited_sent: Option<Instant>,
    last_non_app_limited_ack: Option<Instant>,
    non_app_limited_sent_high_watermark: Option<Instant>,
    delivery_clock_epoch: u64,
    next_non_app_limited_ack_starts_epoch: bool,
}

impl std::fmt::Debug for InstrumentedController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedController")
            .field("telemetry", &self.telemetry)
            .finish_non_exhaustive()
    }
}

impl QuicCarrierTelemetry {
    pub(super) fn record_delivery_evidence_written(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.delivery_evidence_written_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        let pending_before = self
            .delivery_evidence_pending_ack_bytes
            .fetch_add(bytes, Ordering::Release);
        if pending_before == 0 {
            // Wake a metrics task that may be waiting on the idle PTO. Further
            // writes remain timer-sampled while product bytes are unacknowledged.
            self.delivery_activity_started.notify_waiters();
        }
    }

    pub(super) fn delivery_evidence_written_bytes(&self) -> u64 {
        self.delivery_evidence_written_bytes.load(Ordering::Acquire)
    }

    pub(super) fn record_delivery_evidence_cancelled(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let pending_before = self
            .delivery_evidence_pending_ack_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                Some(pending.saturating_sub(bytes))
            })
            .expect("delivery-evidence cancellation update is infallible");
        self.delivery_evidence_cancelled_bytes
            .fetch_add(pending_before.min(bytes), Ordering::Release);
    }

    pub(super) fn delivery_evidence_cancelled_bytes(&self) -> u64 {
        self.delivery_evidence_cancelled_bytes
            .load(Ordering::Acquire)
    }

    pub(super) fn delivery_evidence_pending_ack_bytes(&self) -> u64 {
        self.delivery_evidence_pending_ack_bytes
            .load(Ordering::Acquire)
    }

    pub(super) fn delivery_activity_notify(&self) -> Arc<Notify> {
        self.delivery_activity_started.clone()
    }

    fn allocate_path_telemetry(&self) -> Arc<QuicPathTelemetry> {
        let previous = self
            .next_path_epoch
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |epoch| {
                epoch.checked_add(1)
            })
            .expect("QUIC path epoch exhausted");
        let path_epoch = previous + 1;
        self.current_path_epoch.store(path_epoch, Ordering::Release);
        Arc::new(QuicPathTelemetry {
            path_epoch,
            ..QuicPathTelemetry::default()
        })
    }

    fn reconcile_delivery_evidence_ack(&self, path_epoch: u64, acked_bytes: u64) -> u64 {
        if acked_bytes == 0 || self.current_path_epoch.load(Ordering::Acquire) != path_epoch {
            return 0;
        }
        let pending_before = self
            .delivery_evidence_pending_ack_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                Some(pending.saturating_sub(acked_bytes))
            })
            .expect("delivery-evidence ACK update is infallible");
        pending_before.min(acked_bytes)
    }
}

impl QuicPathTelemetry {
    pub(super) fn bytes_in_flight(&self) -> Option<u64> {
        self.bytes_in_flight_authoritative
            .load(Ordering::Acquire)
            .then(|| self.bytes_in_flight.load(Ordering::Relaxed))
            .filter(|_| {
                fence(Ordering::Acquire);
                self.bytes_in_flight_authoritative.load(Ordering::Relaxed)
            })
    }

    fn ack_totals(&self) -> QuicAckTelemetryTotals {
        loop {
            let before = self.ack_snapshot_sequence.load(Ordering::Acquire);
            if before & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let totals = QuicAckTelemetryTotals {
                delivery_clock_epoch: self.delivery_clock_epoch.load(Ordering::Relaxed),
                acked_bytes: self.newly_acked_bytes.load(Ordering::Relaxed),
                non_app_limited_acked_bytes: self
                    .non_app_limited_acked_bytes
                    .load(Ordering::Relaxed),
                timed_non_app_limited_acked_bytes: self
                    .timed_non_app_limited_acked_bytes
                    .load(Ordering::Relaxed),
                non_app_limited_ack_elapsed_nanos: self
                    .non_app_limited_ack_elapsed_nanos
                    .load(Ordering::Relaxed),
                sample_count: self.delivery_sample_count.load(Ordering::Relaxed),
                non_app_limited_sample_count: self
                    .non_app_limited_delivery_sample_count
                    .load(Ordering::Relaxed),
                timed_non_app_limited_sample_count: self
                    .timed_non_app_limited_delivery_sample_count
                    .load(Ordering::Relaxed),
                timed_non_app_limited_delivery_evidence_acked_bytes: self
                    .timed_non_app_limited_delivery_evidence_acked_bytes
                    .load(Ordering::Relaxed),
                timed_non_app_limited_delivery_evidence_sample_count: self
                    .timed_non_app_limited_delivery_evidence_sample_count
                    .load(Ordering::Relaxed),
                timed_non_app_limited_delivery_evidence_elapsed_nanos: self
                    .timed_non_app_limited_delivery_evidence_elapsed_nanos
                    .load(Ordering::Relaxed),
                delivery_evidence_acked_bytes: self
                    .delivery_evidence_acked_bytes
                    .load(Ordering::Relaxed),
            };
            fence(Ordering::Acquire);
            let after = self.ack_snapshot_sequence.load(Ordering::Relaxed);
            if before == after {
                return totals;
            }
        }
    }

    pub(super) fn snapshot(&self) -> QuicCarrierTelemetrySnapshot {
        let mut cursor = self
            .ack_snapshot_cursor
            .lock()
            .expect("QUIC ACK telemetry snapshot lock");
        let totals = self.ack_totals();
        let newly_acked_bytes = totals.acked_bytes.wrapping_sub(cursor.acked_bytes);
        let non_app_limited_acked_bytes = totals
            .non_app_limited_acked_bytes
            .wrapping_sub(cursor.non_app_limited_acked_bytes);
        // Timed fields are cumulative only within `delivery_clock_epoch`.
        // Returning the coherent current-epoch totals prevents a metrics poll
        // that spans an app-limited reset from joining two delivery clocks.
        let timed_non_app_limited_acked_bytes = totals.timed_non_app_limited_acked_bytes;
        let non_app_limited_ack_elapsed_nanos = totals.non_app_limited_ack_elapsed_nanos;
        let delivery_sample_count = totals.sample_count.wrapping_sub(cursor.sample_count);
        let non_app_limited_delivery_sample_count = totals
            .non_app_limited_sample_count
            .wrapping_sub(cursor.non_app_limited_sample_count);
        let timed_non_app_limited_delivery_sample_count = totals.timed_non_app_limited_sample_count;
        let delivery_evidence_newly_acked_bytes = totals
            .delivery_evidence_acked_bytes
            .wrapping_sub(cursor.delivery_evidence_acked_bytes);
        *cursor = totals;
        drop(cursor);
        let sent_bytes = self.sent_bytes.load(Ordering::Relaxed);
        let lost_bytes = self.lost_bytes.load(Ordering::Relaxed);
        let loss_ppm = (sent_bytes > 0).then(|| {
            let ratio = (lost_bytes as f64 / sent_bytes as f64).clamp(0.0, 1.0);
            (ratio * 1_000_000.0).round() as u32
        });
        let bytes_in_flight = self.bytes_in_flight();
        QuicCarrierTelemetrySnapshot {
            path_epoch: self.path_epoch,
            delivery_clock_epoch: totals.delivery_clock_epoch,
            bytes_in_flight,
            newly_acked_bytes: (newly_acked_bytes > 0).then_some(newly_acked_bytes),
            non_app_limited_acked_bytes: (non_app_limited_acked_bytes > 0)
                .then_some(non_app_limited_acked_bytes),
            timed_non_app_limited_acked_bytes: (timed_non_app_limited_acked_bytes > 0)
                .then_some(timed_non_app_limited_acked_bytes),
            non_app_limited_ack_elapsed: (non_app_limited_ack_elapsed_nanos > 0)
                .then(|| Duration::from_nanos(non_app_limited_ack_elapsed_nanos)),
            timed_non_app_limited_delivery_evidence_acked_bytes: totals
                .timed_non_app_limited_delivery_evidence_acked_bytes,
            timed_non_app_limited_delivery_evidence_sample_count: totals
                .timed_non_app_limited_delivery_evidence_sample_count,
            timed_non_app_limited_delivery_evidence_elapsed: Duration::from_nanos(
                totals.timed_non_app_limited_delivery_evidence_elapsed_nanos,
            ),
            delivery_sample_count,
            non_app_limited_delivery_sample_count,
            timed_non_app_limited_delivery_sample_count,
            delivery_evidence_newly_acked_bytes: (delivery_evidence_newly_acked_bytes > 0)
                .then_some(delivery_evidence_newly_acked_bytes),
            loss_ppm,
            lost_bytes,
            app_limited: self.app_limited.load(Ordering::Relaxed),
        }
    }
    fn add_sent(&self, bytes: u64) {
        // Quinn invokes on_sent for ACK-only datagrams too. Until on_end_acks
        // supplies its exact path flight, an additive estimate is not sound.
        // Invalidate that snapshot before the send watermark's release so a
        // reader cannot pair a new send with the preceding authoritative BIF.
        self.bytes_in_flight_authoritative
            .store(false, Ordering::Release);
        self.sent_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    fn publish_ack_batch(&self, totals: QuicAckTelemetryTotals, in_flight: u64, app_limited: bool) {
        if totals.sample_count > 0 {
            self.ack_snapshot_sequence.fetch_add(1, Ordering::AcqRel);
            let current_delivery_clock_epoch = self.delivery_clock_epoch.load(Ordering::Relaxed);
            if totals.delivery_clock_epoch != current_delivery_clock_epoch {
                debug_assert!(
                    totals.delivery_clock_epoch > current_delivery_clock_epoch,
                    "QUIC delivery-clock epochs must advance monotonically"
                );
                self.delivery_clock_epoch
                    .store(totals.delivery_clock_epoch, Ordering::Relaxed);
                self.timed_non_app_limited_acked_bytes
                    .store(0, Ordering::Relaxed);
                self.non_app_limited_ack_elapsed_nanos
                    .store(0, Ordering::Relaxed);
                self.timed_non_app_limited_delivery_sample_count
                    .store(0, Ordering::Relaxed);
                self.timed_non_app_limited_delivery_evidence_acked_bytes
                    .store(0, Ordering::Relaxed);
                self.timed_non_app_limited_delivery_evidence_sample_count
                    .store(0, Ordering::Relaxed);
                self.timed_non_app_limited_delivery_evidence_elapsed_nanos
                    .store(0, Ordering::Relaxed);
            }
            self.newly_acked_bytes
                .fetch_add(totals.acked_bytes, Ordering::Relaxed);
            if totals.non_app_limited_sample_count > 0 {
                self.non_app_limited_acked_bytes
                    .fetch_add(totals.non_app_limited_acked_bytes, Ordering::Relaxed);
                self.non_app_limited_ack_elapsed_nanos
                    .fetch_add(totals.non_app_limited_ack_elapsed_nanos, Ordering::Relaxed);
                self.non_app_limited_delivery_sample_count
                    .fetch_add(totals.non_app_limited_sample_count, Ordering::Relaxed);
                if totals.timed_non_app_limited_sample_count > 0 {
                    self.timed_non_app_limited_acked_bytes
                        .fetch_add(totals.timed_non_app_limited_acked_bytes, Ordering::Relaxed);
                    self.timed_non_app_limited_delivery_sample_count
                        .fetch_add(totals.timed_non_app_limited_sample_count, Ordering::Relaxed);
                    self.timed_non_app_limited_delivery_evidence_acked_bytes
                        .fetch_add(
                            totals.timed_non_app_limited_delivery_evidence_acked_bytes,
                            Ordering::Relaxed,
                        );
                    self.timed_non_app_limited_delivery_evidence_sample_count
                        .fetch_add(
                            totals.timed_non_app_limited_delivery_evidence_sample_count,
                            Ordering::Relaxed,
                        );
                    self.timed_non_app_limited_delivery_evidence_elapsed_nanos
                        .fetch_add(
                            totals.timed_non_app_limited_delivery_evidence_elapsed_nanos,
                            Ordering::Relaxed,
                        );
                }
            }
            self.delivery_evidence_acked_bytes
                .fetch_add(totals.delivery_evidence_acked_bytes, Ordering::Relaxed);
            self.delivery_sample_count
                .fetch_add(totals.sample_count, Ordering::Relaxed);
            self.ack_snapshot_sequence.fetch_add(1, Ordering::Release);
        }
        self.bytes_in_flight.store(in_flight, Ordering::Relaxed);
        self.bytes_in_flight_authoritative
            .store(true, Ordering::Release);
        self.app_limited.store(app_limited, Ordering::Relaxed);
    }

    fn add_lost(&self, lost_bytes: u64) {
        if lost_bytes > 0 {
            self.lost_bytes.fetch_add(lost_bytes, Ordering::Relaxed);
            let _ = self.bytes_in_flight.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(lost_bytes)),
            );
        }
    }
}
impl InstrumentedController {
    #[cfg(test)]
    pub(super) fn new(
        inner: Box<dyn quinn::congestion::Controller>,
        telemetry: Arc<QuicCarrierTelemetry>,
    ) -> Self {
        let path_telemetry = telemetry.allocate_path_telemetry();
        Self::for_path(
            inner,
            LossPolicyPercent::default(),
            telemetry,
            path_telemetry,
        )
    }

    fn for_path(
        inner: Box<dyn quinn::congestion::Controller>,
        loss_compensation: LossPolicyPercent,
        telemetry: Arc<QuicCarrierTelemetry>,
        path_telemetry: Arc<QuicPathTelemetry>,
    ) -> Self {
        Self {
            inner,
            loss_compensation,
            telemetry,
            path_telemetry,
            ack_batch_acked_bytes: 0,
            ack_batch_non_app_limited_acked_bytes: 0,
            ack_batch_sample_count: 0,
            ack_batch_non_app_limited_sample_count: 0,
            ack_batch_earliest_non_app_limited_sent: None,
            ack_batch_latest_non_app_limited_sent: None,
            last_non_app_limited_ack: None,
            non_app_limited_sent_high_watermark: None,
            delivery_clock_epoch: 0,
            next_non_app_limited_ack_starts_epoch: true,
        }
    }

    pub(super) fn snapshot(&self) -> QuicCarrierTelemetrySnapshot {
        self.path_telemetry.snapshot()
    }

    #[cfg(test)]
    pub(super) fn configured_loss_compensation(&self) -> LossPolicyPercent {
        self.loss_compensation
    }

    fn accumulate_ack_telemetry(&mut self, sent: Instant, bytes: u64, app_limited: bool) {
        if bytes == 0 {
            return;
        }
        self.ack_batch_acked_bytes = self.ack_batch_acked_bytes.saturating_add(bytes);
        self.ack_batch_sample_count = self.ack_batch_sample_count.saturating_add(1);
        if app_limited {
            return;
        }
        self.ack_batch_non_app_limited_acked_bytes = self
            .ack_batch_non_app_limited_acked_bytes
            .saturating_add(bytes);
        self.ack_batch_non_app_limited_sample_count = self
            .ack_batch_non_app_limited_sample_count
            .saturating_add(1);
        self.ack_batch_earliest_non_app_limited_sent = Some(
            self.ack_batch_earliest_non_app_limited_sent
                .map_or(sent, |earliest| earliest.min(sent)),
        );
        self.ack_batch_latest_non_app_limited_sent = Some(
            self.ack_batch_latest_non_app_limited_sent
                .map_or(sent, |latest| latest.max(sent)),
        );
    }

    pub(super) fn finish_ack_telemetry(&mut self, now: Instant, in_flight: u64, app_limited: bool) {
        // Delivery rate uses the slower of the ACK and send clocks, expressed
        // as their maximum elapsed time. This excludes propagation RTT from a
        // new epoch while preventing compressed ACKs from inflating rate.
        let batch_sent_range = self
            .ack_batch_earliest_non_app_limited_sent
            .zip(self.ack_batch_latest_non_app_limited_sent);
        if batch_sent_range.is_some() && self.next_non_app_limited_ack_starts_epoch {
            self.delivery_clock_epoch = self
                .delivery_clock_epoch
                .checked_add(1)
                .expect("QUIC delivery-clock epoch exhausted");
            self.next_non_app_limited_ack_starts_epoch = false;
        }
        let non_app_limited_ack_elapsed =
            batch_sent_range.and_then(|(earliest_sent, latest_sent)| {
                let within_batch_send_elapsed =
                    latest_sent.saturating_duration_since(earliest_sent);
                let elapsed = match (
                    self.last_non_app_limited_ack,
                    self.non_app_limited_sent_high_watermark,
                ) {
                    (Some(previous_ack), Some(sent_high_watermark)) => {
                        let ack_elapsed = now.saturating_duration_since(previous_ack);
                        let forward_send_elapsed =
                            latest_sent.saturating_duration_since(sent_high_watermark);
                        ack_elapsed
                            .max(forward_send_elapsed)
                            .max(within_batch_send_elapsed)
                    }
                    _ => within_batch_send_elapsed,
                };
                (!elapsed.is_zero()).then_some(elapsed)
            });
        let mut totals = QuicAckTelemetryTotals {
            delivery_clock_epoch: self.delivery_clock_epoch,
            acked_bytes: self.ack_batch_acked_bytes,
            non_app_limited_acked_bytes: self.ack_batch_non_app_limited_acked_bytes,
            timed_non_app_limited_acked_bytes: non_app_limited_ack_elapsed
                .map_or(0, |_| self.ack_batch_non_app_limited_acked_bytes),
            non_app_limited_ack_elapsed_nanos: non_app_limited_ack_elapsed
                .map_or(0, duration_as_u64_nanos),
            sample_count: self.ack_batch_sample_count,
            non_app_limited_sample_count: self.ack_batch_non_app_limited_sample_count,
            timed_non_app_limited_sample_count: non_app_limited_ack_elapsed
                .map_or(0, |_| self.ack_batch_non_app_limited_sample_count),
            timed_non_app_limited_delivery_evidence_acked_bytes: 0,
            timed_non_app_limited_delivery_evidence_sample_count: 0,
            timed_non_app_limited_delivery_evidence_elapsed_nanos: 0,
            delivery_evidence_acked_bytes: 0,
        };

        self.ack_batch_acked_bytes = 0;
        self.ack_batch_non_app_limited_acked_bytes = 0;
        self.ack_batch_sample_count = 0;
        self.ack_batch_non_app_limited_sample_count = 0;
        self.ack_batch_earliest_non_app_limited_sent = None;
        self.ack_batch_latest_non_app_limited_sent = None;

        if app_limited {
            self.last_non_app_limited_ack = None;
            self.non_app_limited_sent_high_watermark = None;
            self.next_non_app_limited_ack_starts_epoch = true;
        } else if let Some((_, latest_sent)) = batch_sent_range {
            self.last_non_app_limited_ack = Some(now);
            self.non_app_limited_sent_high_watermark = Some(
                self.non_app_limited_sent_high_watermark
                    .map_or(latest_sent, |high_watermark| {
                        high_watermark.max(latest_sent)
                    }),
            );
        }
        totals.delivery_evidence_acked_bytes = self
            .telemetry
            .reconcile_delivery_evidence_ack(self.path_telemetry.path_epoch, totals.acked_bytes);
        let whole_batch_is_non_app_limited =
            totals.sample_count > 0 && totals.non_app_limited_sample_count == totals.sample_count;
        if whole_batch_is_non_app_limited
            && non_app_limited_ack_elapsed.is_some()
            && totals.delivery_evidence_acked_bytes > 0
        {
            // Product attribution is connection-wide, while app-limited
            // classification is packet-specific. A mixed batch has no exact
            // aggregate partition, so conservatively omit it from timed rate
            // proof instead of guessing or adding an atomic operation per ACK.
            totals.timed_non_app_limited_delivery_evidence_acked_bytes =
                totals.delivery_evidence_acked_bytes;
            totals.timed_non_app_limited_delivery_evidence_sample_count =
                totals.timed_non_app_limited_sample_count;
            totals.timed_non_app_limited_delivery_evidence_elapsed_nanos =
                totals.non_app_limited_ack_elapsed_nanos;
        }
        self.path_telemetry
            .publish_ack_batch(totals, in_flight, app_limited);
    }
}

fn duration_as_u64_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl quinn::congestion::ControllerFactory for InstrumentedBbrConfig {
    fn build(
        self: Arc<Self>,
        now: Instant,
        current_mtu: u16,
    ) -> Box<dyn quinn::congestion::Controller> {
        // Use Quinn BBRv3 for the QUIC carrier. mptunnel does not have an
        // operator-provided per-path bandwidth contract, so a fixed-rate
        // Brutal-style controller would either underfill unknown good paths or
        // overload weaker/shared paths. BBR's delivery-rate/RTT model is the
        // stable production default for feeding the product multipath scheduler;
        // QUIC still owns packet pacing, loss recovery, and bytes in flight.
        let inner = Self::build_bbr3(self.loss_compensation, now, current_mtu);
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        let path_telemetry = telemetry.allocate_path_telemetry();
        Box::new(InstrumentedController::for_path(
            inner,
            self.loss_compensation,
            telemetry,
            path_telemetry,
        ))
    }
}

impl quinn::congestion::Controller for InstrumentedController {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.path_telemetry.add_sent(bytes);
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_packet_sent(
        &mut self,
        now: Instant,
        bytes: u16,
        prior_in_flight: u64,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        app_limited: bool,
    ) -> Option<quinn::congestion::PacketDeliveryState> {
        self.inner.on_packet_sent(
            now,
            bytes,
            prior_in_flight,
            packet_number,
            space,
            app_limited,
        )
    }

    fn on_cwnd_limited(&mut self) {
        self.inner.on_cwnd_limited();
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.accumulate_ack_telemetry(sent, bytes, app_limited);
        self.inner
            .on_ack(now, sent, bytes, packet_number, space, app_limited, rtt);
    }

    fn on_ack_with_packet_state(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        packet_state: Option<quinn::congestion::PacketDeliveryState>,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.accumulate_ack_telemetry(sent, bytes, app_limited);
        self.inner.on_ack_with_packet_state(
            now,
            sent,
            bytes,
            app_limited,
            packet_number,
            space,
            packet_state,
            rtt,
        );
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
        space: quinn::congestion::SpaceId,
    ) {
        self.finish_ack_telemetry(now, in_flight, app_limited);
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked, space);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        is_ecn: bool,
        lost_bytes: u64,
        largest_lost: u64,
        space: quinn::congestion::SpaceId,
    ) {
        self.path_telemetry.add_lost(lost_bytes);
        self.inner.on_congestion_event(
            now,
            sent,
            is_persistent_congestion,
            is_ecn,
            lost_bytes,
            largest_lost,
            space,
        );
    }

    fn on_packet_lost(
        &mut self,
        lost_bytes: u16,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        now: Instant,
    ) -> Option<quinn::congestion::RecoveryTransactionId> {
        // Aggregate loss telemetry is recorded once in on_congestion_event.
        self.inner
            .on_packet_lost(lost_bytes, packet_number, space, now)
    }

    fn on_spurious_congestion_event(
        &mut self,
        transaction: quinn::congestion::RecoveryTransactionId,
    ) -> bool {
        self.inner.on_spurious_congestion_event(transaction)
    }

    fn on_recovery_transaction_abandoned(
        &mut self,
        transaction: quinn::congestion::RecoveryTransactionId,
    ) {
        self.inner.on_recovery_transaction_abandoned(transaction);
    }

    fn on_validated_ecn_congestion_event(&mut self) {
        self.inner.on_validated_ecn_congestion_event();
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.inner.on_mtu_update(new_mtu);
    }

    fn on_ack_frequency_update(
        &mut self,
        ack_eliciting_threshold: u64,
        requested_max_ack_delay: Duration,
    ) {
        self.inner
            .on_ack_frequency_update(ack_eliciting_threshold, requested_max_ack_delay);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn metrics(&self) -> quinn::congestion::ControllerMetrics {
        self.inner.metrics()
    }

    fn pacing_rate(&self) -> Option<u64> {
        self.inner.pacing_rate()
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            loss_compensation: self.loss_compensation,
            telemetry: self.telemetry.clone(),
            path_telemetry: self.path_telemetry.clone(),
            ack_batch_acked_bytes: self.ack_batch_acked_bytes,
            ack_batch_non_app_limited_acked_bytes: self.ack_batch_non_app_limited_acked_bytes,
            ack_batch_sample_count: self.ack_batch_sample_count,
            ack_batch_non_app_limited_sample_count: self.ack_batch_non_app_limited_sample_count,
            ack_batch_earliest_non_app_limited_sent: self.ack_batch_earliest_non_app_limited_sent,
            ack_batch_latest_non_app_limited_sent: self.ack_batch_latest_non_app_limited_sent,
            last_non_app_limited_ack: self.last_non_app_limited_ack,
            non_app_limited_sent_high_watermark: self.non_app_limited_sent_high_watermark,
            delivery_clock_epoch: self.delivery_clock_epoch,
            next_non_app_limited_ack_starts_epoch: self.next_non_app_limited_ack_starts_epoch,
        })
    }

    fn fresh_path_box(
        &self,
        now: Instant,
        current_mtu: u16,
    ) -> Option<Box<dyn quinn::congestion::Controller>> {
        // InstrumentedBbrConfig is the sole production constructor for this
        // wrapper, so a fresh path always starts a fresh BBRv3 model. Only the
        // connection-scoped evidence owner survives the network transition.
        let inner = InstrumentedBbrConfig::build_bbr3(self.loss_compensation, now, current_mtu);
        let path_telemetry = self.telemetry.allocate_path_telemetry();
        Some(Box::new(Self::for_path(
            inner,
            self.loss_compensation,
            self.telemetry.clone(),
            path_telemetry,
        )))
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
#[path = "tests_congestion.rs"]
mod tests;
