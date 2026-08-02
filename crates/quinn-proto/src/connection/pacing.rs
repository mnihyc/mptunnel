//! Pacing of packet transmissions.

use crate::{Duration, Instant};

use tracing::warn;

/// A simple token-bucket pacer
///
/// The pacer's capacity is derived on a fraction of the congestion window
/// which can be sent in regular intervals
/// Once the bucket is empty, further transmission is blocked.
/// The bucket refills at a rate slightly faster
/// than one congestion window per RTT, as recommended in
/// <https://tools.ietf.org/html/draft-ietf-quic-recovery-34#section-7.7>
pub(super) struct Pacer {
    capacity: u64,
    last_window: u64,
    last_mtu: u16,
    tokens: u64,
    prev: Instant,
}

impl Pacer {
    /// Obtains a new [`Pacer`].
    pub(super) fn new(smoothed_rtt: Duration, window: u64, mtu: u16, now: Instant) -> Self {
        let capacity = optimal_capacity(smoothed_rtt, window, mtu);
        Self {
            capacity,
            last_window: window,
            last_mtu: mtu,
            tokens: capacity,
            prev: now,
        }
    }

    /// Record that a packet has been transmitted.
    pub(super) fn on_transmit(&mut self, packet_length: u16) {
        self.tokens = self.tokens.saturating_sub(packet_length.into())
    }

    /// Return how long we need to wait before sending `bytes_to_send`
    ///
    /// If we can send a packet right away, this returns `None`. Otherwise, returns `Some(d)`,
    /// where `d` is the time before this function should be called again.
    ///
    /// The 5/4 ratio used here comes from the suggestion that N = 1.25 in the draft IETF RFC for
    /// QUIC.
    pub(super) fn delay(
        &mut self,
        smoothed_rtt: Duration,
        bytes_to_send: u64,
        mtu: u16,
        window: u64,
        now: Instant,
        controller_pacing_rate: Option<u64>,
    ) -> Option<Instant> {
        debug_assert_ne!(
            window, 0,
            "zero-sized congestion control window is nonsense"
        );

        let controller_pacing_rate = controller_pacing_rate.filter(|rate| *rate != 0);
        let pacing_rate =
            controller_pacing_rate.or_else(|| default_pacing_rate(smoothed_rtt, window));
        let capacity = controller_pacing_rate.map_or_else(
            || optimal_capacity(smoothed_rtt, window, mtu),
            |rate| optimal_capacity_for_rate(rate, mtu),
        );

        if capacity != self.capacity || window != self.last_window || mtu != self.last_mtu {
            self.capacity = capacity;

            // Clamp the tokens
            self.tokens = self.capacity.min(self.tokens);
            self.last_window = window;
            self.last_mtu = mtu;
        }

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        let time_elapsed = now.checked_duration_since(self.prev).unwrap_or_else(|| {
            warn!("received a timestamp early than a previous recorded time, ignoring");
            Default::default()
        });

        let pacing_rate = pacing_rate?;
        let new_tokens = bytes_for_duration(pacing_rate, time_elapsed);
        self.tokens = self.tokens.saturating_add(new_tokens).min(self.capacity);

        // Preserve sub-byte elapsed time when the connection is polled faster than tokens accrue.
        if new_tokens > 0 {
            self.prev = now;
        }

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        let refill = bytes_to_send.max(self.capacity) - self.tokens;
        Some(self.prev + duration_for_bytes(refill, pacing_rate))
    }
}

fn default_pacing_rate(smoothed_rtt: Duration, window: u64) -> Option<u64> {
    let rtt_nanos = smoothed_rtt.as_nanos();
    if rtt_nanos == 0 || window > u64::from(u32::MAX) {
        return None;
    }

    let bytes_per_second = (u128::from(window) * 5 * NANOS_PER_SECOND) / (4 * rtt_nanos);
    Some(bytes_per_second.min(u128::from(u64::MAX)) as u64)
}

fn optimal_capacity_for_rate(pacing_rate: u64, mtu: u16) -> u64 {
    let capacity = u128::from(pacing_rate) * PACING_BURST_INTERVAL_NANOS / NANOS_PER_SECOND;
    (capacity.min(u128::from(u64::MAX)) as u64).clamp(
        MIN_BURST_SIZE * u64::from(mtu),
        MAX_BURST_SIZE * u64::from(mtu),
    )
}

fn bytes_for_duration(rate: u64, elapsed: Duration) -> u64 {
    let bytes = u128::from(rate) * elapsed.as_nanos() / NANOS_PER_SECOND;
    bytes.min(u128::from(u64::MAX)) as u64
}

fn duration_for_bytes(bytes: u64, rate: u64) -> Duration {
    let numerator = u128::from(bytes) * NANOS_PER_SECOND;
    let nanos = numerator.div_ceil(u128::from(rate));
    Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
}

/// Calculates a pacer capacity for a certain window and RTT
///
/// The goal is to emit a burst (of size `capacity`) in timer intervals
/// which compromise between
/// - ideally distributing datagrams over time
/// - constantly waking up the connection to produce additional datagrams
///
/// Too short burst intervals means we will never meet them since the timer
/// accuracy in user-space is not high enough. If we miss the interval by more
/// than 25%, we will lose that part of the congestion window since no additional
/// tokens for the extra-elapsed time can be stored.
///
/// Too long burst intervals make pacing less effective.
fn optimal_capacity(smoothed_rtt: Duration, window: u64, mtu: u16) -> u64 {
    let rtt = smoothed_rtt.as_nanos().max(1);

    let capacity = ((window as u128 * BURST_INTERVAL_NANOS) / rtt) as u64;

    // Small bursts are less efficient (no GSO), could increase latency and don't effectively
    // use the channel's buffer capacity. Large bursts might block the connection on sending.
    capacity.clamp(MIN_BURST_SIZE * mtu as u64, MAX_BURST_SIZE * mtu as u64)
}

/// The burst interval
///
/// The capacity will we refilled in 4/5 of that time.
/// 2ms is chosen here since framework timers might have 1ms precision.
/// If kernel-level pacing is supported later a higher time here might be
/// more applicable.
const BURST_INTERVAL_NANOS: u128 = 2_000_000; // 2ms

const PACING_BURST_INTERVAL_NANOS: u128 = BURST_INTERVAL_NANOS * 4 / 5;

const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Allows some usage of GSO, and doesn't slow down the handshake.
const MIN_BURST_SIZE: u64 = 10;

/// Creating 256 packets took 1ms in a benchmark, so larger bursts don't make sense.
const MAX_BURST_SIZE: u64 = 256;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn does_not_panic_on_bad_instant() {
        let old_instant = Instant::now();
        let new_instant = old_instant + Duration::from_micros(15);
        let rtt = Duration::from_micros(400);

        assert!(Pacer::new(rtt, 30000, 1500, new_instant)
            .delay(Duration::from_micros(0), 0, 1500, 1, old_instant, None,)
            .is_none());
        assert!(Pacer::new(rtt, 30000, 1500, new_instant)
            .delay(Duration::from_micros(0), 1600, 1500, 1, old_instant, None,)
            .is_none());
        assert!(Pacer::new(rtt, 30000, 1500, new_instant)
            .delay(
                Duration::from_micros(0),
                1500,
                1500,
                3000,
                old_instant,
                None,
            )
            .is_none());
    }

    #[test]
    fn derives_initial_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let pacer = Pacer::new(rtt, window, mtu, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(Duration::from_millis(0), window, mtu, now);
        assert_eq!(pacer.capacity, MAX_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(rtt, 1, mtu, now);
        assert_eq!(pacer.capacity, MIN_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);
    }

    #[test]
    fn adjusts_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);
        let initial_tokens = pacer.tokens;

        pacer.delay(rtt, mtu as u64, mtu, window * 2, now, None);
        assert_eq!(
            pacer.capacity,
            (2 * window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens);

        pacer.delay(rtt, mtu as u64, mtu, window / 2, now, None);
        assert_eq!(
            pacer.capacity,
            (window as u128 / 2 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens / 2);

        pacer.delay(rtt, mtu as u64, mtu * 2, window, now, None);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );

        pacer.delay(rtt, mtu as u64, 20_000, window, now, None);
        assert_eq!(pacer.capacity, 20_000_u64 * MIN_BURST_SIZE);
    }

    #[test]
    fn computes_pause_correctly() {
        let window = 2_000_000u64;
        let mtu = 1000;
        let rtt = Duration::from_millis(50);
        let old_instant = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, old_instant);
        let packet_capacity = pacer.capacity / mtu as u64;

        for _ in 0..packet_capacity {
            assert_eq!(
                pacer.delay(rtt, mtu as u64, mtu, window, old_instant, None,),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        let pace_duration = Duration::from_nanos((BURST_INTERVAL_NANOS * 4 / 5) as u64);

        assert_eq!(
            pacer
                .delay(rtt, mtu as u64, mtu, window, old_instant, None,)
                .expect("Send must be delayed")
                .duration_since(old_instant),
            pace_duration
        );

        // Refill half of the tokens
        assert_eq!(
            pacer.delay(
                rtt,
                mtu as u64,
                mtu,
                window,
                old_instant + pace_duration / 2,
                None,
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity / 2);

        for _ in 0..packet_capacity / 2 {
            assert_eq!(
                pacer.delay(rtt, mtu as u64, mtu, window, old_instant, None,),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        // Refill all capacity by waiting more than the expected duration
        assert_eq!(
            pacer.delay(
                rtt,
                mtu as u64,
                mtu,
                window,
                old_instant + pace_duration * 3 / 2,
                None,
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity);
    }

    #[test]
    fn controller_pacing_rate_delays_normal_datagrams_after_burst() {
        let rtt = Duration::from_millis(50);
        let now = Instant::now();
        let mtu = 1500;
        let pacing_rate = 200_000_000;
        let mut pacer = Pacer::new(rtt, 2_000_000, mtu, now);
        assert_eq!(
            pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(pacing_rate)),
            None,
        );
        let packet_capacity = pacer.tokens / u64::from(mtu);

        for _ in 0..packet_capacity {
            assert_eq!(
                pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(pacing_rate)),
                None,
            );
            pacer.on_transmit(mtu);
        }

        let refill = pacer.capacity - pacer.tokens;
        assert_eq!(
            pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(pacing_rate)),
            Some(now + duration_for_bytes(refill, pacing_rate)),
        );
    }

    #[test]
    fn lower_controller_rate_immediately_clamps_burst_and_tokens() {
        let rtt = Duration::from_millis(50);
        let now = Instant::now();
        let mtu = 1500;
        let mut pacer = Pacer::new(rtt, 2_000_000, mtu, now);

        pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(100_000_000));
        let startup_capacity = pacer.capacity;
        pacer.tokens = startup_capacity;
        pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(10_000_000));

        assert!(pacer.capacity < startup_capacity);
        assert_eq!(pacer.capacity, 16_000);
        assert_eq!(pacer.tokens, pacer.capacity);
    }

    #[test]
    fn rapid_polls_preserve_sub_byte_refill_time() {
        let rtt = Duration::from_secs(1);
        let now = Instant::now();
        let mut pacer = Pacer::new(rtt, 1, 1, now);
        pacer.capacity = 1;
        pacer.tokens = 0;

        for tenth in 1..10 {
            assert!(pacer
                .delay(
                    rtt,
                    1,
                    1,
                    1,
                    now + Duration::from_millis(tenth * 100),
                    Some(1)
                )
                .is_some());
            assert_eq!(pacer.prev, now);
        }

        assert_eq!(
            pacer.delay(rtt, 1, 1, 1, now + Duration::from_secs(1), Some(1)),
            None,
        );
    }
}
