//! Optional native TCP sender telemetry.
//!
//! Polling stays beside the socket lifecycle. Capability-graded observations
//! preserve unknown host fields; receipt authority remains in `capacity`.

use crate::model::capacity::PATH_OPEN_SCORE_BYTES;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::model::{metric_epoch_now, ratio_to_ppm};
use crate::transport::tcp_telemetry::{
    TcpNativeLossCounters, TcpNativeSnapshot, TcpTelemetrySocket,
};
use std::sync::Once;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

const TCP_METRIC_MIN_INTERVAL: Duration = Duration::from_millis(5);
const TCP_METRIC_MAX_INTERVAL: Duration = Duration::from_millis(250);

/// Exact same-socket sender queue used to establish a receipt-rate baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct TcpSenderQueueSnapshot {
    pub(in crate::runtime) bytes_in_flight: Option<u64>,
    pub(in crate::runtime) notsent_bytes: u32,
}

impl TcpSenderQueueSnapshot {
    /// A completed writer flush is the portable fallback. When available, the
    /// native unsent queue proves that the timed train starts at a wire boundary.
    pub(in crate::runtime) fn is_write_queue_drained(self) -> bool {
        self.notsent_bytes == 0
    }
}

/// Partial native TCP evidence from one exact socket and sampling epoch.
///
/// Private optional fields prevent missing host capabilities from becoming
/// measured zero. Consumers use the typed accessors or a conservative protocol
/// projection; delivery and pacing remain deliberately distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct TcpNativeObservation {
    path_id: PathId,
    direction: PathMetricDirection,
    srtt_us: Option<u32>,
    rttvar_us: Option<u32>,
    bytes_in_flight: Option<u64>,
    queue_bytes: Option<u64>,
    inflight_limit_bytes: Option<u64>,
    inflight_hi_bytes: Option<u64>,
    delivery_rate_bps: Option<u64>,
    pacing_rate_bps: Option<u64>,
    newly_acked_bytes: Option<u64>,
    retransmission_advanced: Option<bool>,
    #[cfg(any(test, feature = "lab-diagnostics"))]
    acked_bytes_since_epoch: Option<u64>,
    loss_ppm: Option<u32>,
    loss_observed: Option<bool>,
    confidence_ppm: Option<u32>,
    app_limited: Option<bool>,
}

impl TcpNativeObservation {
    pub(in crate::runtime) fn path_id(self) -> PathId {
        self.path_id
    }

    pub(in crate::runtime) fn direction(self) -> PathMetricDirection {
        self.direction
    }

    pub(in crate::runtime) fn srtt_us(self) -> Option<u32> {
        self.srtt_us
    }

    pub(in crate::runtime) fn rtt(self) -> Option<(u32, u32)> {
        Some((self.srtt_us?, self.rttvar_us?))
    }

    pub(in crate::runtime) fn rttvar_us(self) -> Option<u32> {
        self.rttvar_us
    }

    pub(in crate::runtime) fn bytes_in_flight(self) -> Option<u64> {
        self.bytes_in_flight
    }

    pub(in crate::runtime) fn inflight_limit_bytes(self) -> Option<u64> {
        self.inflight_limit_bytes
    }

    pub(in crate::runtime) fn flight(self) -> Option<(u64, u64, u64)> {
        Some((
            self.bytes_in_flight?,
            self.inflight_limit_bytes?,
            self.inflight_hi_bytes?,
        ))
    }

    pub(in crate::runtime) fn queue_bytes(self) -> Option<u64> {
        self.queue_bytes
    }

    pub(in crate::runtime) fn loss_ppm(self) -> Option<u32> {
        self.loss_ppm
    }

    pub(in crate::runtime) fn delivery_rate_bps(self) -> Option<u64> {
        self.delivery_rate_bps.filter(|rate| *rate > 0)
    }

    pub(in crate::runtime) fn pacing_rate_bps(self) -> Option<u64> {
        self.pacing_rate_bps
    }

    pub(in crate::runtime) fn newly_acked_bytes(self) -> Option<u64> {
        self.newly_acked_bytes
    }

    /// Reports a native recovery edge without assigning meaning to the
    /// platform-specific retransmission counter's unit.
    #[cfg(any(feature = "lab-diagnostics", test))]
    pub(in crate::runtime) fn retransmission_advanced(self) -> Option<bool> {
        self.retransmission_advanced
    }

    #[cfg(any(test, feature = "lab-diagnostics"))]
    pub(in crate::runtime) fn acked_bytes_since_epoch(self) -> Option<u64> {
        self.acked_bytes_since_epoch
    }

    pub(in crate::runtime) fn app_limited(self) -> Option<bool> {
        self.app_limited
    }

    pub(in crate::runtime) fn delivery_window_covered(self) -> bool {
        self.confidence_ppm == Some(1_000_000)
    }

    pub(in crate::runtime) fn has_flight(self) -> bool {
        self.inflight_limit_bytes.is_some() && self.inflight_hi_bytes.is_some()
    }

    pub(in crate::runtime) fn has_transport_shape(self) -> bool {
        self.srtt_us.is_some()
            || self.rttvar_us.is_some()
            || self.bytes_in_flight.is_some()
            || self.queue_bytes.is_some()
            || self.inflight_limit_bytes.is_some()
            || self.inflight_hi_bytes.is_some()
            || self.loss_ppm.is_some()
            || self.app_limited.is_some()
    }

    pub(in crate::runtime) fn has_native_drain_evidence(self) -> bool {
        self.bytes_in_flight.is_some() && self.queue_bytes.is_some()
    }

    /// Applies only capabilities this snapshot actually contains.
    pub(in crate::runtime) fn apply_transport_shape(self, metrics: &mut PathMetrics) {
        if let Some(srtt_us) = self.srtt_us {
            metrics.srtt_us = srtt_us.max(1);
        }
        if let Some(rttvar_us) = self.rttvar_us {
            metrics.rttvar_us = rttvar_us;
            metrics.jitter_us = rttvar_us;
        }
        if let Some(bytes_in_flight) = self.bytes_in_flight {
            metrics.bytes_in_flight = bytes_in_flight;
        }
        if let Some(inflight_limit_bytes) = self.inflight_limit_bytes {
            metrics.inflight_limit_bytes = inflight_limit_bytes;
        }
        if let Some(inflight_hi_bytes) = self.inflight_hi_bytes {
            metrics.inflight_hi_bytes = inflight_hi_bytes;
        }
        if let Some(queue_bytes) = self.queue_bytes {
            metrics.queue_bytes = queue_bytes;
        }
        if let Some(loss_ppm) = self.loss_ppm {
            metrics.loss_ppm = loss_ppm;
            metrics.loss_observed = self.loss_observed.unwrap_or(false);
        }
        if let Some(app_limited) = self.app_limited {
            metrics.app_limited = app_limited;
        }
    }

    /// Projects only a coherent complete sample into the wire/scheduler shape.
    /// Partial observations stay typed so absent capabilities cannot erase a
    /// prior registry entry or acquire a fresh recorded-at timestamp.
    pub(in crate::runtime) fn complete_path_metrics(self) -> Option<PathMetrics> {
        let (srtt_us, rttvar_us) = self.rtt()?;
        let (bytes_in_flight, inflight_limit_bytes, inflight_hi_bytes) = self.flight()?;
        let queue_bytes = self.queue_bytes?;
        let delivery_rate_bps = self.delivery_rate_bps?;
        let loss_ppm = self.loss_ppm?;
        let loss_observed = self.loss_observed?;
        let confidence_ppm = self.confidence_ppm?;
        let app_limited = self.app_limited?;
        Some(PathMetrics {
            path_id: self.path_id,
            underlay: UnderlayProtocol::Tcp,
            direction: self.direction,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: srtt_us.max(1),
            rttvar_us,
            jitter_us: rttvar_us,
            delivery_rate_bps: delivery_rate_bps.max(1),
            pacing_rate_bps: self
                .pacing_rate_bps()
                .filter(|rate| *rate > 0)
                .or_else(|| self.delivery_rate_bps())
                .unwrap_or(1),
            loss_ppm,
            ecn_ppm: 0,
            loss_observed,
            ecn_observed: false,
            bytes_in_flight,
            queue_bytes,
            inflight_limit_bytes,
            inflight_hi_bytes,
            confidence_ppm,
            app_limited,
            // Native TCP ACK counters cannot identify authenticated product data.
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        })
    }
}

/// Keeps polling in the carrier task so telemetry cannot outlive its socket or
/// exact registry registration.
#[derive(Debug)]
pub(in crate::runtime) struct TcpMetricPublisher {
    socket: TcpTelemetrySocket,
    tracker: Option<TcpSenderMetricTracker>,
    next_sample_at: Instant,
}

impl TcpMetricPublisher {
    pub(in crate::runtime) fn capture(socket: &TcpStream) -> Option<Self> {
        match TcpTelemetrySocket::capture(socket) {
            Ok(Some(socket)) => Some(Self {
                socket,
                tracker: None,
                next_sample_at: Instant::now(),
            }),
            Ok(None) => {
                warn_portable_tcp_capacity_fallback(None);
                None
            }
            Err(error) => {
                warn_portable_tcp_capacity_fallback(Some(&error));
                None
            }
        }
    }

    /// Starts the cumulative sender epoch after authenticated readiness bytes.
    pub(in crate::runtime) fn begin_epoch(&mut self) {
        self.tracker = match self.socket.snapshot() {
            Ok(Some(snapshot)) if snapshot.flight.is_some() => {
                Some(TcpSenderMetricTracker::new(snapshot))
            }
            Ok(Some(snapshot)) => {
                warn_portable_tcp_capacity_fallback(None);
                Some(TcpSenderMetricTracker::new(snapshot))
            }
            Ok(None) => {
                warn_portable_tcp_capacity_fallback(None);
                None
            }
            Err(error) => {
                warn_portable_tcp_capacity_fallback(Some(&error));
                None
            }
        };
        self.next_sample_at = Instant::now();
    }

    /// Queries exact sender queues without advancing periodic metric cadence.
    pub(in crate::runtime) fn sender_queue_snapshot(&self) -> Option<TcpSenderQueueSnapshot> {
        let snapshot = self.socket.snapshot().ok().flatten()?;
        Some(TcpSenderQueueSnapshot {
            bytes_in_flight: snapshot.flight.and_then(|flight| flight.bytes_in_flight),
            notsent_bytes: snapshot.notsent_bytes?,
        })
    }

    /// Next adaptive same-socket sample chosen from the latest native RTT.
    pub(in crate::runtime) fn next_observation_at(&self) -> Instant {
        self.next_sample_at
    }

    pub(in crate::runtime) fn maybe_observe(
        &mut self,
        path_id: PathId,
        direction: PathMetricDirection,
        force: bool,
    ) -> Option<TcpNativeObservation> {
        let now = Instant::now();
        if !force && now < self.next_sample_at {
            return None;
        }
        // A missing or failed snapshot must not turn every carrier event into a
        // native syscall. Successful RTT evidence may shorten this backoff.
        self.next_sample_at = now.checked_add(TCP_METRIC_MAX_INTERVAL).unwrap_or(now);
        let current = self.socket.snapshot().ok().flatten()?;
        if self.tracker.is_none() {
            self.tracker = Some(TcpSenderMetricTracker::new(current));
            return None;
        }
        let observation = self
            .tracker
            .as_mut()
            .expect("native TCP tracker established above")
            .observe(path_id, direction, current);
        let interval = observation
            .srtt_us()
            .map(|srtt_us| Duration::from_micros(u64::from(srtt_us.max(1))) / 2)
            .unwrap_or(TCP_METRIC_MAX_INTERVAL)
            .clamp(TCP_METRIC_MIN_INTERVAL, TCP_METRIC_MAX_INTERVAL);
        self.next_sample_at = now.checked_add(interval).unwrap_or(now);
        Some(observation)
    }
}

fn warn_portable_tcp_capacity_fallback(error: Option<&std::io::Error>) {
    static WARN_ONCE: Once = Once::new();
    WARN_ONCE.call_once(|| {
        eprintln!(
            "warning: native TCP flight telemetry unavailable{}; multipath uses the portable Data ACK capacity fallback and may have lower throughput on high-bandwidth, high-latency links",
            error.map_or(String::new(), |error| format!(" ({error})")),
        );
    });
}

/// Deltas one exact TCP sender from a post-authentication baseline.
#[derive(Debug)]
pub(in crate::runtime) struct TcpSenderMetricTracker {
    bytes_acked_baseline: Option<u64>,
    previous_bytes_acked: Option<u64>,
    previous_retransmission_counter: Option<u64>,
    delivery_window_floor_bytes: u64,
    loss: Option<TcpLossTracker>,
}

/// Accumulates modular kernel counters so a long-lived fast carrier can cross
/// the 32-bit TCP_INFO boundary without losing its evidence epoch.
#[derive(Debug)]
struct TcpLossTracker {
    previous: TcpNativeLossCounters,
    sent_segments: u64,
    retransmits: u64,
}

impl TcpSenderMetricTracker {
    pub(in crate::runtime) fn new(baseline: TcpNativeSnapshot) -> Self {
        let delivery_window_floor_bytes = baseline
            .flight
            .map(|flight| flight.inflight_limit_bytes)
            .unwrap_or(PATH_OPEN_SCORE_BYTES as u64)
            .max(PATH_OPEN_SCORE_BYTES as u64);
        Self {
            bytes_acked_baseline: baseline.bytes_acked,
            previous_bytes_acked: baseline.bytes_acked,
            previous_retransmission_counter: baseline.retransmission_counter,
            delivery_window_floor_bytes,
            loss: baseline.loss.map(|previous| TcpLossTracker {
                previous,
                sent_segments: 0,
                retransmits: 0,
            }),
        }
    }

    pub(in crate::runtime) fn observe(
        &mut self,
        path_id: PathId,
        direction: PathMetricDirection,
        current: TcpNativeSnapshot,
    ) -> TcpNativeObservation {
        let (srtt_us, rttvar_us) = current
            .rtt
            .map(|rtt| (Some(rtt.srtt_us), rtt.rttvar_us))
            .unwrap_or((None, None));
        let (bytes_in_flight, inflight_limit_bytes, inflight_hi_bytes) = current
            .flight
            .map(|flight| {
                (
                    flight.bytes_in_flight,
                    Some(flight.inflight_limit_bytes),
                    Some(
                        flight
                            .inflight_hi_bytes
                            .unwrap_or(flight.inflight_limit_bytes),
                    ),
                )
            })
            .unwrap_or((None, None, None));
        let loss_ppm = current.loss.and_then(|current| {
            let tracker = self.loss.get_or_insert(TcpLossTracker {
                previous: current,
                sent_segments: 0,
                retransmits: 0,
            });
            let sent_segments = current
                .data_segments_out
                .wrapping_sub(tracker.previous.data_segments_out);
            let retransmits = current
                .retransmits
                .wrapping_sub(tracker.previous.retransmits);
            tracker.previous = current;
            tracker.sent_segments = tracker
                .sent_segments
                .saturating_add(u64::from(sent_segments));
            tracker.retransmits = tracker.retransmits.saturating_add(u64::from(retransmits));
            (tracker.sent_segments > 0)
                .then(|| ratio_to_ppm(tracker.retransmits as f64 / tracker.sent_segments as f64))
        });
        let loss_observed = loss_ppm.map(|_| true);
        let acked_bytes_since_epoch = self
            .bytes_acked_baseline
            .zip(current.bytes_acked)
            .map(|(baseline, current)| current.saturating_sub(baseline));
        let confidence_ppm = acked_bytes_since_epoch.map(|acked_bytes| {
            ratio_to_ppm(acked_bytes as f64 / self.delivery_window_floor_bytes.max(1) as f64)
        });
        let newly_acked_bytes = match (self.previous_bytes_acked, current.bytes_acked) {
            (Some(previous), Some(current)) => Some(current.saturating_sub(previous)),
            (None, Some(_)) | (_, None) => None,
        };
        if current.bytes_acked.is_some() {
            self.previous_bytes_acked = current.bytes_acked;
        }
        let retransmission_advanced = match (
            self.previous_retransmission_counter,
            current.retransmission_counter,
        ) {
            (Some(previous), Some(current)) => Some(current != previous),
            (None, Some(_)) | (_, None) => None,
        };
        if current.retransmission_counter.is_some() {
            self.previous_retransmission_counter = current.retransmission_counter;
        }

        TcpNativeObservation {
            path_id,
            direction,
            srtt_us,
            rttvar_us,
            bytes_in_flight,
            queue_bytes: current.notsent_bytes.map(u64::from),
            inflight_limit_bytes,
            inflight_hi_bytes,
            delivery_rate_bps: current
                .delivery_rate_bytes_per_second
                .map(bytes_per_second_to_bits),
            pacing_rate_bps: current
                .pacing_rate_bytes_per_second
                .filter(|rate| *rate != u64::MAX)
                .map(bytes_per_second_to_bits),
            newly_acked_bytes,
            retransmission_advanced,
            #[cfg(any(test, feature = "lab-diagnostics"))]
            acked_bytes_since_epoch,
            loss_ppm,
            loss_observed,
            confidence_ppm,
            app_limited: current.app_limited,
        }
    }
}

fn bytes_per_second_to_bits(rate: u64) -> u64 {
    rate.saturating_mul(8)
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
