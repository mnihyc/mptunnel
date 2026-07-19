//! Native QUIC congestion and ACK instrumentation.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy)]
pub struct CongestionMetrics {
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
    pub timed_non_app_limited_acked_bytes: Option<u64>,
    pub non_app_limited_ack_elapsed: Option<Duration>,
    pub delivery_evidence_written_bytes: u64,
    pub delivery_sample_count: u64,
    pub non_app_limited_delivery_sample_count: u64,
    pub timed_non_app_limited_delivery_sample_count: u64,
    pub app_limited: bool,
}

#[derive(Debug, Default)]
pub(super) struct InstrumentedBbrConfig;

#[derive(Debug, Default)]
pub(super) struct QuicCarrierTelemetry {
    bytes_in_flight: AtomicU64,
    bytes_in_flight_authoritative: AtomicBool,
    // Quinn invokes congestion callbacks through one mutable controller. The
    // sequence makes its cumulative ACK counters coherent for a concurrent
    // metrics reader without putting a lock on the packet ACK hot path.
    ack_snapshot_sequence: AtomicU64,
    newly_acked_bytes: AtomicU64,
    non_app_limited_acked_bytes: AtomicU64,
    timed_non_app_limited_acked_bytes: AtomicU64,
    non_app_limited_ack_elapsed_nanos: AtomicU64,
    delivery_sample_count: AtomicU64,
    non_app_limited_delivery_sample_count: AtomicU64,
    timed_non_app_limited_delivery_sample_count: AtomicU64,
    ack_snapshot_cursor: Mutex<QuicAckTelemetryTotals>,
    sent_bytes: AtomicU64,
    lost_bytes: AtomicU64,
    delivery_evidence_written_bytes: AtomicU64,
    delivery_evidence_pending_ack_bytes: AtomicU64,
    delivery_activity_started: Arc<Notify>,
    app_limited: AtomicBool,
}

#[derive(Debug, Default, Clone, Copy)]
struct QuicAckTelemetryTotals {
    acked_bytes: u64,
    non_app_limited_acked_bytes: u64,
    timed_non_app_limited_acked_bytes: u64,
    non_app_limited_ack_elapsed_nanos: u64,
    sample_count: u64,
    non_app_limited_sample_count: u64,
    timed_non_app_limited_sample_count: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct QuicCarrierTelemetrySnapshot {
    pub(super) bytes_in_flight: Option<u64>,
    pub(super) newly_acked_bytes: Option<u64>,
    pub(super) non_app_limited_acked_bytes: Option<u64>,
    pub(super) timed_non_app_limited_acked_bytes: Option<u64>,
    pub(super) non_app_limited_ack_elapsed: Option<Duration>,
    pub(super) delivery_sample_count: u64,
    pub(super) non_app_limited_delivery_sample_count: u64,
    pub(super) timed_non_app_limited_delivery_sample_count: u64,
    pub(super) loss_ppm: Option<u32>,
    pub(super) lost_bytes: u64,
    pub(super) app_limited: bool,
}

pub(super) struct InstrumentedController {
    inner: Box<dyn quinn::congestion::Controller>,
    pub(super) telemetry: Arc<QuicCarrierTelemetry>,
    ack_batch_acked_bytes: u64,
    ack_batch_non_app_limited_acked_bytes: u64,
    ack_batch_sample_count: u64,
    ack_batch_non_app_limited_sample_count: u64,
    ack_batch_earliest_non_app_limited_sent: Option<Instant>,
    ack_batch_latest_non_app_limited_sent: Option<Instant>,
    last_non_app_limited_ack: Option<Instant>,
    non_app_limited_sent_high_watermark: Option<Instant>,
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

    #[cfg(test)]
    pub(super) fn delivery_evidence_pending_ack_bytes(&self) -> u64 {
        self.delivery_evidence_pending_ack_bytes
            .load(Ordering::Acquire)
    }

    pub(super) fn delivery_activity_notify(&self) -> Arc<Notify> {
        self.delivery_activity_started.clone()
    }

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
        let timed_non_app_limited_acked_bytes = totals
            .timed_non_app_limited_acked_bytes
            .wrapping_sub(cursor.timed_non_app_limited_acked_bytes);
        let non_app_limited_ack_elapsed_nanos = totals
            .non_app_limited_ack_elapsed_nanos
            .wrapping_sub(cursor.non_app_limited_ack_elapsed_nanos);
        let delivery_sample_count = totals.sample_count.wrapping_sub(cursor.sample_count);
        let non_app_limited_delivery_sample_count = totals
            .non_app_limited_sample_count
            .wrapping_sub(cursor.non_app_limited_sample_count);
        let timed_non_app_limited_delivery_sample_count = totals
            .timed_non_app_limited_sample_count
            .wrapping_sub(cursor.timed_non_app_limited_sample_count);
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
            bytes_in_flight,
            newly_acked_bytes: (newly_acked_bytes > 0).then_some(newly_acked_bytes),
            non_app_limited_acked_bytes: (non_app_limited_acked_bytes > 0)
                .then_some(non_app_limited_acked_bytes),
            timed_non_app_limited_acked_bytes: (timed_non_app_limited_acked_bytes > 0)
                .then_some(timed_non_app_limited_acked_bytes),
            non_app_limited_ack_elapsed: (non_app_limited_ack_elapsed_nanos > 0)
                .then(|| Duration::from_nanos(non_app_limited_ack_elapsed_nanos)),
            delivery_sample_count,
            non_app_limited_delivery_sample_count,
            timed_non_app_limited_delivery_sample_count,
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
                }
            }
            self.delivery_sample_count
                .fetch_add(totals.sample_count, Ordering::Relaxed);
            self.ack_snapshot_sequence.fetch_add(1, Ordering::Release);
        }
        self.bytes_in_flight.store(in_flight, Ordering::Relaxed);
        self.bytes_in_flight_authoritative
            .store(true, Ordering::Release);
        self.app_limited.store(app_limited, Ordering::Relaxed);
        if totals.acked_bytes > 0 {
            let _ = self.delivery_evidence_pending_ack_bytes.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |pending| Some(pending.saturating_sub(totals.acked_bytes)),
            );
        }
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
    pub(super) fn new(
        inner: Box<dyn quinn::congestion::Controller>,
        telemetry: Arc<QuicCarrierTelemetry>,
    ) -> Self {
        Self {
            inner,
            telemetry,
            ack_batch_acked_bytes: 0,
            ack_batch_non_app_limited_acked_bytes: 0,
            ack_batch_sample_count: 0,
            ack_batch_non_app_limited_sample_count: 0,
            ack_batch_earliest_non_app_limited_sent: None,
            ack_batch_latest_non_app_limited_sent: None,
            last_non_app_limited_ack: None,
            non_app_limited_sent_high_watermark: None,
        }
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
        let totals = QuicAckTelemetryTotals {
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
        } else if let Some((_, latest_sent)) = batch_sent_range {
            self.last_non_app_limited_ack = Some(now);
            self.non_app_limited_sent_high_watermark = Some(
                self.non_app_limited_sent_high_watermark
                    .map_or(latest_sent, |high_watermark| {
                        high_watermark.max(latest_sent)
                    }),
            );
        }
        self.telemetry
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
        // Use Quinn BBR for the QUIC carrier. mptunnel does not have an
        // operator-provided per-path bandwidth contract, so a fixed-rate
        // Brutal-style controller would either underfill unknown good paths or
        // overload weaker/shared paths. BBR's delivery-rate/RTT model is the
        // stable production default for feeding the product multipath scheduler;
        // QUIC still owns packet pacing, loss recovery, and bytes in flight.
        let inner = Arc::new(quinn::congestion::BbrConfig::default()).build(now, current_mtu);
        Box::new(InstrumentedController::new(
            inner,
            Arc::new(QuicCarrierTelemetry::default()),
        ))
    }
}

impl quinn::congestion::Controller for InstrumentedController {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.telemetry.add_sent(bytes);
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_packet_sent(
        &mut self,
        now: Instant,
        bytes: u16,
        prior_in_flight: u64,
        packet_number: u64,
        app_limited: bool,
    ) -> Option<quinn::congestion::PacketDeliveryState> {
        self.inner
            .on_packet_sent(now, bytes, prior_in_flight, packet_number, app_limited)
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.accumulate_ack_telemetry(sent, bytes, app_limited);
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_ack_with_packet_state(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        packet_state: Option<quinn::congestion::PacketDeliveryState>,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.accumulate_ack_telemetry(sent, bytes, app_limited);
        self.inner
            .on_ack_with_packet_state(now, sent, bytes, app_limited, packet_state, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.finish_ack_telemetry(now, in_flight, app_limited);
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.telemetry.add_lost(lost_bytes);
        self.inner
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.inner.on_mtu_update(new_mtu);
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
            telemetry: self.telemetry.clone(),
            ack_batch_acked_bytes: self.ack_batch_acked_bytes,
            ack_batch_non_app_limited_acked_bytes: self.ack_batch_non_app_limited_acked_bytes,
            ack_batch_sample_count: self.ack_batch_sample_count,
            ack_batch_non_app_limited_sample_count: self.ack_batch_non_app_limited_sample_count,
            ack_batch_earliest_non_app_limited_sent: self.ack_batch_earliest_non_app_limited_sent,
            ack_batch_latest_non_app_limited_sent: self.ack_batch_latest_non_app_limited_sent,
            last_non_app_limited_ack: self.last_non_app_limited_ack,
            non_app_limited_sent_high_watermark: self.non_app_limited_sent_high_watermark,
        })
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
#[path = "congestion_test.rs"]
mod tests;
