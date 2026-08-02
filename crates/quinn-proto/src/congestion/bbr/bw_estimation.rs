use std::fmt::{Debug, Display, Formatter};

use super::min_max::MinMax;
use crate::congestion::PacketDeliveryState;
use crate::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
struct PendingRateSample {
    prior_delivered: u64,
    send_elapsed: Duration,
    ack_elapsed: Duration,
    packet_number: u64,
    app_limited: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BandwidthEstimation {
    total_acked: u64,
    delivered_time: Option<Instant>,
    first_sent_time: Option<Instant>,
    max_filter: MinMax,
    acked_at_last_window: u64,
    pending_sample: Option<PendingRateSample>,
}

impl BandwidthEstimation {
    pub(crate) fn on_packet_sent(
        &mut self,
        now: Instant,
        _bytes: u16,
        prior_in_flight: u64,
        _packet_number: u64,
        _app_limited: bool,
    ) -> PacketDeliveryState {
        if prior_in_flight == 0 {
            self.first_sent_time = Some(now);
            self.delivered_time = Some(now);
        }
        let first_sent_time = *self.first_sent_time.get_or_insert(now);
        let delivered_time = *self.delivered_time.get_or_insert(now);

        PacketDeliveryState {
            delivered: self.total_acked,
            delivered_time,
            send_elapsed_ns: duration_as_nanos_u64(now.saturating_duration_since(first_sent_time)),
        }
    }

    pub(crate) fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        packet_number: u64,
        app_limited: bool,
        packet_state: PacketDeliveryState,
    ) {
        self.total_acked = self.total_acked.saturating_add(bytes);
        self.delivered_time = Some(now);

        let first_sent_time = self.first_sent_time.unwrap_or(sent);
        let newest = self.pending_sample.is_none()
            || sent > first_sent_time
            || (sent == first_sent_time
                && self
                    .pending_sample
                    .is_some_and(|sample| packet_number > sample.packet_number));
        if !newest {
            return;
        }

        self.pending_sample = Some(PendingRateSample {
            prior_delivered: packet_state.delivered,
            send_elapsed: Duration::from_nanos(packet_state.send_elapsed_ns),
            ack_elapsed: now.saturating_duration_since(packet_state.delivered_time),
            packet_number,
            app_limited,
        });
        self.first_sent_time = Some(sent);
    }

    pub(crate) fn on_ack_without_packet_state(&mut self, now: Instant, bytes: u64) {
        self.total_acked = self.total_acked.saturating_add(bytes);
        self.delivered_time = Some(now);
    }

    pub(crate) fn bytes_acked_this_window(&self) -> u64 {
        self.total_acked - self.acked_at_last_window
    }

    pub(crate) fn end_acks(&mut self, current_round: u64, min_rtt: Duration) -> Option<bool> {
        let sample = self.pending_sample.take();
        self.acked_at_last_window = self.total_acked;
        let sample = sample?;
        let interval = sample.send_elapsed.max(sample.ack_elapsed);
        if interval < min_rtt {
            return None;
        }

        let delivered = self.total_acked.saturating_sub(sample.prior_delivered);
        let bandwidth = Self::bw_from_delta(delivered, interval)?;
        if !sample.app_limited || bandwidth >= self.max_filter.get() {
            self.max_filter.update_max(current_round, bandwidth);
        }
        Some(sample.app_limited)
    }

    pub(crate) fn get_estimate(&self) -> u64 {
        self.max_filter.get()
    }

    pub(crate) fn bw_from_delta(bytes: u64, delta: Duration) -> Option<u64> {
        let window_duration_ns = delta.as_nanos();
        if window_duration_ns == 0 {
            return None;
        }
        let bytes_per_second = (u128::from(bytes) * 1_000_000_000) / window_duration_ns;
        Some(bytes_per_second.min(u128::from(u64::MAX)) as u64)
    }
}

fn duration_as_nanos_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl Display for BandwidthEstimation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.3} MB/s",
            self.get_estimate() as f32 / (1024 * 1024) as f32
        )
    }
}

#[cfg(test)]
#[path = "bw_estimation_test.rs"]
mod tests;
