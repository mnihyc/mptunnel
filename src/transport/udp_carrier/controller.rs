use super::crypto::{DIR_CLIENT_TO_SERVER, DIR_SERVER_TO_CLIENT};
use super::packet::{
    MAX_PROBED_DATAGRAM_BYTES, SAFE_TARGET_DATAGRAM_BYTES, max_frame_fragment_payload,
    max_frame_fragment_payload_for_datagram,
};
use super::window::AckedPacket;
use crate::mux::MuxLimits;
use std::time::{Duration, Instant};

pub(super) const INITIAL_RTT: Duration = Duration::from_millis(100);
pub(super) const MIN_RTO: Duration = Duration::from_millis(25);
pub(super) const MAX_RTO: Duration = Duration::from_secs(1);
pub(super) const RETRANSMIT_TICK_FRACTION: u32 = 4;
pub(super) const MAX_ACK_DELAY: Duration = Duration::from_millis(25);
pub(super) const PACKET_LOSS_THRESHOLD: u64 = 3;
const MAX_PACKET_LOSS_THRESHOLD: u64 = 16;
pub(super) const PTO_PROBE_PACKET_LIMIT: usize = 2;
const MAX_PTO_BACKOFF_SHIFT: u32 = 3;
pub(super) const STARTUP_MIN_FLIGHT_PACKETS: usize = 64;
pub(super) const STARTUP_MAX_FLIGHT_PACKETS: usize = 1024;
const STARTUP_BUDGET_DIVISOR: usize = 64;
pub(super) const STARTUP_PACING_GAIN: f64 = 2.0;

#[derive(Debug, Clone, Copy)]
pub(super) struct RttEstimator {
    pub(super) srtt: Duration,
    pub(super) rttvar: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct LossDetectionParams {
    pub(super) packet_threshold: u64,
    pub(super) time_threshold: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct UdpPathControllerSnapshot {
    pub(super) direction: u8,
    pub(super) srtt: Duration,
    pub(super) rttvar: Duration,
    pub(super) min_rtt: Duration,
    pub(super) min_rtt_observed: bool,
    pub(super) delivery_rate_bps: f64,
    pub(super) pacing_rate_bps: f64,
    pub(super) inflight_hi: usize,
    pub(super) bytes_in_flight: usize,
    pub(super) target_datagram_bytes: usize,
    pub(super) loss_events: u64,
    pub(super) spurious_loss_events: u64,
    pub(super) packet_loss_threshold: u64,
    pub(super) pto_count: u32,
    pub(super) app_limited: bool,
    pub(super) delivery_sample_count: u64,
    pub(super) last_delivery_sample_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct UdpPathController {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) direction: u8,
    pub(super) rtt: RttEstimator,
    pub(super) min_rtt: Duration,
    pub(super) delivery_rate_bps: f64,
    pub(super) pacing_rate_bps: f64,
    pub(super) inflight_hi: usize,
    pub(super) bytes_in_flight: usize,
    pub(super) min_rtt_observed: bool,
    pub(super) rate_sample_ack_started_at: Option<Instant>,
    pub(super) rate_sample_first_sent_at: Option<Instant>,
    pub(super) rate_sample_last_sent_at: Option<Instant>,
    pub(super) rate_sample_delivered_bytes: usize,
    pub(super) rate_sample_app_limited: bool,
    pub(super) app_limited: bool,
    pub(super) next_send_at: Instant,
    pub(super) target_datagram_bytes: usize,
    pub(super) pmtu_acked_bytes: usize,
    pub(super) loss_events: u64,
    pub(super) spurious_loss_events: u64,
    pub(super) packet_loss_threshold: u64,
    pub(super) pto_count: u32,
    pub(super) delivery_sample_count: u64,
    pub(super) last_delivery_sample_at: Option<Instant>,
}

impl RttEstimator {
    pub(super) fn new() -> Self {
        Self {
            srtt: INITIAL_RTT,
            rttvar: INITIAL_RTT / 2,
        }
    }

    pub(super) fn observe(&mut self, sample: Duration) {
        let srtt = duration_to_secs(self.srtt);
        let rttvar = duration_to_secs(self.rttvar);
        let sample = duration_to_secs(sample);
        let next_rttvar = 0.75 * rttvar + 0.25 * (srtt - sample).abs();
        let next_srtt = 0.875 * srtt + 0.125 * sample;
        self.srtt = secs_to_duration(next_srtt);
        self.rttvar = secs_to_duration(next_rttvar);
    }

    pub(super) fn pto(self) -> Duration {
        let variance = self.rttvar * 4;
        self.srtt
            .saturating_add(variance.max(Duration::from_millis(1)))
            .saturating_add(MAX_ACK_DELAY)
            .clamp(MIN_RTO, MAX_RTO)
    }
}

impl UdpPathController {
    pub(super) fn new(mux_limits: MuxLimits, direction: u8) -> Self {
        debug_assert!(matches!(
            direction,
            DIR_CLIENT_TO_SERVER | DIR_SERVER_TO_CLIENT
        ));
        let fragment = max_frame_fragment_payload().max(1);
        let budget = carrier_pending_byte_budget(mux_limits);
        let startup_inflight = udp_startup_inflight_bytes(fragment, budget);
        let pacing_rate_bps =
            bytes_per_rtt_to_bps(startup_inflight, INITIAL_RTT) * STARTUP_PACING_GAIN;
        let now = Instant::now();
        Self {
            direction,
            rtt: RttEstimator::new(),
            min_rtt: INITIAL_RTT,
            delivery_rate_bps: pacing_rate_bps,
            pacing_rate_bps,
            inflight_hi: startup_inflight,
            bytes_in_flight: 0,
            min_rtt_observed: false,
            rate_sample_ack_started_at: None,
            rate_sample_first_sent_at: None,
            rate_sample_last_sent_at: None,
            rate_sample_delivered_bytes: 0,
            rate_sample_app_limited: true,
            app_limited: true,
            next_send_at: now,
            target_datagram_bytes: SAFE_TARGET_DATAGRAM_BYTES,
            pmtu_acked_bytes: 0,
            loss_events: 0,
            spurious_loss_events: 0,
            packet_loss_threshold: PACKET_LOSS_THRESHOLD,
            pto_count: 0,
            delivery_sample_count: 0,
            last_delivery_sample_at: None,
        }
    }

    pub(super) fn snapshot(self) -> UdpPathControllerSnapshot {
        UdpPathControllerSnapshot {
            direction: self.direction,
            srtt: self.rtt.srtt,
            rttvar: self.rtt.rttvar,
            min_rtt: self.min_rtt,
            min_rtt_observed: self.min_rtt_observed,
            delivery_rate_bps: self.delivery_rate_bps,
            pacing_rate_bps: self.pacing_rate_bps,
            inflight_hi: self.inflight_hi,
            bytes_in_flight: self.bytes_in_flight,
            target_datagram_bytes: self.target_datagram_bytes,
            loss_events: self.loss_events,
            spurious_loss_events: self.spurious_loss_events,
            packet_loss_threshold: self.packet_loss_threshold,
            pto_count: self.pto_count,
            app_limited: self.app_limited,
            delivery_sample_count: self.delivery_sample_count,
            last_delivery_sample_at: self.last_delivery_sample_at,
        }
    }

    pub(super) fn send_delay(
        &mut self,
        packet_len: usize,
        pending_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
    ) -> Option<Duration> {
        self.refresh_limits(mux_limits);
        if pending_bytes.saturating_add(packet_len) > carrier_pending_byte_budget(mux_limits) {
            return Some(self.rto() / RETRANSMIT_TICK_FRACTION);
        }
        if self.bytes_in_flight.saturating_add(packet_len) > self.inflight_hi {
            return Some(self.rtt.srtt / RETRANSMIT_TICK_FRACTION);
        }
        let granularity = pacing_granularity(self.rtt.srtt);
        if self.next_send_at <= now + granularity {
            return None;
        }
        Some(self.next_send_at.duration_since(now))
    }

    pub(super) fn on_packet_sent(&mut self, packet_len: usize, now: Instant) {
        self.bytes_in_flight = self.bytes_in_flight.saturating_add(packet_len);
        let gap = secs_to_duration(packet_len as f64 * 8.0 / self.pacing_rate_bps.max(1.0));
        let base = self.next_send_at.max(now);
        self.next_send_at = base + gap;
    }

    pub(super) fn packet_run_segment_budget(self, packet_len: usize, max_segments: usize) -> usize {
        if packet_len == 0 {
            return 1;
        }
        let max_segments = max_segments.max(1);
        let quantum = pacing_granularity(self.rtt.srtt);
        let quantum_bytes =
            (self.pacing_rate_bps.max(1.0) * duration_to_secs(quantum) / 8.0).round() as usize;
        quantum_bytes
            .saturating_div(packet_len)
            .clamp(1, max_segments)
    }

    pub(super) fn on_packets_acked(
        &mut self,
        released: &[AckedPacket],
        spurious_losses: u64,
        ack_delay: Duration,
        now: Instant,
        mux_limits: MuxLimits,
    ) -> LossDetectionParams {
        if released.is_empty() && spurious_losses == 0 {
            return self.loss_detection_params(None);
        }
        let mut delivered_data = 0usize;
        let mut acked_bytes = 0usize;
        let mut latest_rtt = None;
        let mut first_sent_at = None;
        let mut last_sent_at = None;
        let mut app_limited_data = false;
        for acked in released {
            let packet = &acked.packet;
            acked_bytes = acked_bytes.saturating_add(packet.encoded_len);
            if packet.sample.counts_delivery_rate() {
                delivered_data = delivered_data.saturating_add(packet.encoded_len);
                app_limited_data |= packet.sample.app_limited();
                first_sent_at = Some(
                    first_sent_at
                        .map_or(packet.sent_at, |first: Instant| first.min(packet.sent_at)),
                );
                last_sent_at = Some(
                    last_sent_at.map_or(packet.sent_at, |last: Instant| last.max(packet.sent_at)),
                );
            }
            self.bytes_in_flight = self.bytes_in_flight.saturating_sub(packet.encoded_len);
            if packet.retransmit_count == 0
                && latest_rtt.is_none_or(|(packet_number, _)| acked.packet_number > packet_number)
            {
                latest_rtt = Some((acked.packet_number, now.duration_since(packet.sent_at)));
            }
        }
        if let Some((_, sample)) = latest_rtt {
            if self.min_rtt_observed {
                self.min_rtt = self.min_rtt.min(sample);
            } else {
                self.min_rtt = sample;
                self.min_rtt_observed = true;
            }
            let capped_ack_delay = ack_delay.min(MAX_ACK_DELAY);
            let adjusted = if sample >= self.min_rtt.saturating_add(capped_ack_delay) {
                sample.saturating_sub(capped_ack_delay)
            } else {
                sample
            };
            self.rtt.observe(adjusted);
        }
        if spurious_losses > 0 {
            self.spurious_loss_events = self.spurious_loss_events.saturating_add(spurious_losses);
            self.packet_loss_threshold = self
                .packet_loss_threshold
                .saturating_add(spurious_losses)
                .min(MAX_PACKET_LOSS_THRESHOLD);
        }
        if delivered_data > 0 {
            self.observe_delivery_rate(
                delivered_data,
                app_limited_data,
                first_sent_at,
                last_sent_at,
                now,
            );
            self.app_limited = app_limited_data;
            self.inflight_hi = self
                .inflight_hi
                .saturating_add(delivered_data)
                .min(self.inflight_model_ceiling(mux_limits));
        }
        self.pto_count = 0;
        self.pmtu_acked_bytes = self.pmtu_acked_bytes.saturating_add(acked_bytes);
        self.maybe_grow_datagram_size();
        self.refresh_limits(mux_limits);
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_diagnostic(
            "udp_controller_ack",
            format_args!(
                "direction={} acked_bytes={} delivered_data_bytes={} app_limited={} inflight_bytes={} inflight_hi={} target_datagram_bytes={} srtt_ms={:.3} min_rtt_ms={:.3} delivery_rate_mbps={:.3} pacing_rate_mbps={:.3} packet_loss_threshold={} spurious_loss_events={}",
                self.direction,
                acked_bytes,
                delivered_data,
                self.app_limited,
                self.bytes_in_flight,
                self.inflight_hi,
                self.target_datagram_bytes,
                self.rtt.srtt.as_secs_f64() * 1000.0,
                self.min_rtt.as_secs_f64() * 1000.0,
                self.delivery_rate_bps / 1_000_000.0,
                self.pacing_rate_bps / 1_000_000.0,
                self.packet_loss_threshold,
                self.spurious_loss_events,
            ),
        );
        self.loss_detection_params(latest_rtt.map(|(_, rtt)| rtt))
    }

    pub(super) fn on_loss(&mut self, lost_bytes: usize, mux_limits: MuxLimits) {
        if lost_bytes == 0 {
            return;
        }
        self.loss_events = self.loss_events.saturating_add(1);
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(lost_bytes);
        let fragment = max_frame_fragment_payload().max(1);
        let min_flight = fragment * 16;
        let floor = self
            .bytes_in_flight
            .saturating_add(fragment * 4)
            .max(min_flight);
        let flight_reference = self
            .inflight_hi
            .max(self.bytes_in_flight)
            .max(lost_bytes)
            .max(fragment);
        let loss_pressure = (lost_bytes as f64 / flight_reference as f64).clamp(0.0, 1.0);
        let reduced = (self.inflight_hi as f64 * (1.0 - loss_pressure)).round() as usize;
        self.inflight_hi = reduced
            .max(floor)
            .min(carrier_pending_byte_budget(mux_limits));
        self.target_datagram_bytes = SAFE_TARGET_DATAGRAM_BYTES.max(
            (self.target_datagram_bytes as f64 * (1.0 - loss_pressure / 2.0)).round() as usize,
        );
        self.pmtu_acked_bytes = 0;
        self.refresh_limits(mux_limits);
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_diagnostic(
            "udp_controller_loss",
            format_args!(
                "lost_bytes={} loss_pressure={:.6} inflight_bytes={} inflight_hi={} target_datagram_bytes={} loss_events={} pacing_rate_mbps={:.3}",
                lost_bytes,
                loss_pressure,
                self.bytes_in_flight,
                self.inflight_hi,
                self.target_datagram_bytes,
                self.loss_events,
                self.pacing_rate_bps / 1_000_000.0,
            ),
        );
    }

    pub(super) fn on_probe_timeout(&mut self, probe_bytes: usize) {
        if probe_bytes == 0 {
            return;
        }
        self.pto_count = self.pto_count.saturating_add(1);
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_diagnostic(
            "udp_controller_pto",
            format_args!(
                "direction={} probe_bytes={} inflight_bytes={} inflight_hi={} target_datagram_bytes={} pacing_rate_mbps={:.3} pto_count={}",
                self.direction,
                probe_bytes,
                self.bytes_in_flight,
                self.inflight_hi,
                self.target_datagram_bytes,
                self.pacing_rate_bps / 1_000_000.0,
                self.pto_count,
            ),
        );
    }

    pub(super) fn rto(self) -> Duration {
        let multiplier = 1_u32 << self.pto_count.min(MAX_PTO_BACKOFF_SHIFT);
        self.rtt.pto().saturating_mul(multiplier)
    }

    fn loss_detection_params(self, latest_rtt: Option<Duration>) -> LossDetectionParams {
        let base = self.rtt.srtt.max(latest_rtt.unwrap_or(self.rtt.srtt));
        LossDetectionParams {
            packet_threshold: self.packet_loss_threshold,
            time_threshold: base.mul_f64(9.0 / 8.0).max(Duration::from_millis(1)),
        }
    }

    pub(super) fn frame_fragment_payload_len(self) -> usize {
        max_frame_fragment_payload_for_datagram(self.target_datagram_bytes)
    }

    fn refresh_limits(&mut self, mux_limits: MuxLimits) {
        let fragment = max_frame_fragment_payload().max(1);
        let min_flight = fragment * 16;
        let budget = carrier_pending_byte_budget(mux_limits);
        let model_ceiling = self.inflight_model_ceiling(mux_limits);
        self.inflight_hi = self
            .inflight_hi
            .clamp(min_flight, model_ceiling.max(min_flight).min(budget));
        self.pacing_rate_bps = self.delivery_rate_bps.max(1.0);
    }

    fn observe_delivery_rate(
        &mut self,
        delivered_bytes: usize,
        app_limited: bool,
        first_sent_at: Option<Instant>,
        last_sent_at: Option<Instant>,
        now: Instant,
    ) {
        if delivered_bytes == 0 {
            return;
        }
        if self.rate_sample_ack_started_at.is_none() {
            self.rate_sample_ack_started_at = Some(now);
        }
        self.rate_sample_delivered_bytes = self
            .rate_sample_delivered_bytes
            .saturating_add(delivered_bytes);
        self.rate_sample_app_limited &= app_limited;
        if let Some(first_sent_at) = first_sent_at {
            self.rate_sample_first_sent_at = Some(
                self.rate_sample_first_sent_at
                    .map_or(first_sent_at, |first| first.min(first_sent_at)),
            );
        }
        if let Some(last_sent_at) = last_sent_at {
            self.rate_sample_last_sent_at = Some(
                self.rate_sample_last_sent_at
                    .map_or(last_sent_at, |last| last.max(last_sent_at)),
            );
        }

        let ack_elapsed = self
            .rate_sample_ack_started_at
            .map(|started| now.duration_since(started))
            .unwrap_or_default();
        let send_elapsed = match (
            self.rate_sample_first_sent_at,
            self.rate_sample_last_sent_at,
        ) {
            (Some(first), Some(last)) => last.duration_since(first),
            _ => Duration::ZERO,
        };
        let interval = ack_elapsed.max(send_elapsed);
        if interval < self.delivery_rate_sample_interval() {
            return;
        }

        let sample_rate =
            self.rate_sample_delivered_bytes as f64 * 8.0 / duration_to_secs(interval);
        let current_rate = self.delivery_rate_bps.max(1.0);
        let bounded_sample = if sample_rate > current_rate {
            sample_rate.min(current_rate * STARTUP_PACING_GAIN)
        } else {
            sample_rate.max(1.0)
        };
        if self.rate_sample_app_limited {
            if bounded_sample > current_rate {
                self.delivery_rate_bps = bounded_sample;
            }
        } else if bounded_sample > current_rate {
            self.delivery_rate_bps = bounded_sample;
        } else {
            #[cfg(feature = "lab-diagnostics")]
            crate::lab_diagnostics::lab_diagnostic(
                "udp_controller_low_rate_sample_ignored",
                format_args!(
                    "direction={} delivered_bytes={} interval_ms={:.3} sample_rate_mbps={:.3} current_rate_mbps={:.3}",
                    self.direction,
                    self.rate_sample_delivered_bytes,
                    interval.as_secs_f64() * 1000.0,
                    bounded_sample / 1_000_000.0,
                    current_rate / 1_000_000.0,
                ),
            );
        }
        self.delivery_sample_count = self.delivery_sample_count.saturating_add(1);
        self.last_delivery_sample_at = Some(now);
        self.rate_sample_ack_started_at = None;
        self.rate_sample_first_sent_at = None;
        self.rate_sample_last_sent_at = None;
        self.rate_sample_delivered_bytes = 0;
        self.rate_sample_app_limited = true;
    }

    pub(super) fn delivery_rate_sample_interval(self) -> Duration {
        (self.min_rtt.max(MIN_RTO) / 2).max(Duration::from_millis(1))
    }

    fn inflight_model_ceiling(self, mux_limits: MuxLimits) -> usize {
        let fragment = max_frame_fragment_payload().max(1);
        let min_flight = fragment * 16;
        let budget = carrier_pending_byte_budget(mux_limits);
        let bdp_bytes = ((self.delivery_rate_bps * duration_to_secs(self.min_rtt.max(MIN_RTO)))
            / 8.0)
            .round() as usize;
        bdp_bytes
            .saturating_mul(2)
            .max(self.bytes_in_flight.saturating_add(fragment * 4))
            .max(min_flight)
            .min(budget)
    }

    fn maybe_grow_datagram_size(&mut self) {
        if self.target_datagram_bytes >= MAX_PROBED_DATAGRAM_BYTES {
            return;
        }
        let probe_interval = self.target_datagram_bytes.saturating_mul(256);
        if self.pmtu_acked_bytes < probe_interval {
            return;
        }
        self.pmtu_acked_bytes = 0;
        let step = (self.target_datagram_bytes / 16).clamp(16, 64);
        self.target_datagram_bytes = self
            .target_datagram_bytes
            .saturating_add(step)
            .min(MAX_PROBED_DATAGRAM_BYTES);
    }
}

pub(super) fn carrier_pending_byte_budget(mux_limits: MuxLimits) -> usize {
    let window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX / 2);
    window
        .saturating_add(mux_limits.max_repair_bytes)
        .max(max_frame_fragment_payload() * 64)
}

pub(super) fn udp_startup_inflight_bytes(fragment: usize, budget: usize) -> usize {
    let min_flight = fragment.saturating_mul(STARTUP_MIN_FLIGHT_PACKETS);
    let max_flight = fragment.saturating_mul(STARTUP_MAX_FLIGHT_PACKETS);
    let budget_fraction = budget / STARTUP_BUDGET_DIVISOR.max(1);
    budget_fraction.clamp(min_flight, max_flight).min(budget)
}

pub(super) fn duration_to_secs(value: Duration) -> f64 {
    value.as_secs_f64().max(0.000_001)
}

pub(super) fn secs_to_duration(value: f64) -> Duration {
    Duration::from_secs_f64(value.max(0.000_001))
}

pub(super) fn bytes_per_rtt_to_bps(bytes: usize, rtt: Duration) -> f64 {
    bytes as f64 * 8.0 / duration_to_secs(rtt)
}

pub(super) fn pacing_granularity(srtt: Duration) -> Duration {
    (srtt / 128)
        .max(Duration::from_micros(250))
        .min(Duration::from_millis(2))
}
