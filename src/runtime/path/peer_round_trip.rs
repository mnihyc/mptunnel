//! Peer timing from authenticated, same-carrier MPP exchanges.
//!
//! This is not a native transport measurement, a one-way delay, or an
//! application response time. One owner belongs to one physical carrier;
//! replacement never inherits its samples. Timing grants no delivery credit.

use crate::model::timing::PeerTiming;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum PeerRoundTripSource {
    Readiness,
    PathProof,
    Heartbeat,
}

impl PeerRoundTripSource {
    pub(in crate::runtime) const fn name(self) -> &'static str {
        match self {
            Self::Readiness => "readiness_exchange",
            Self::PathProof => "path_proof",
            Self::Heartbeat => "heartbeat",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct PeerRoundTripObservation {
    pub(in crate::runtime) elapsed: Duration,
    pub(in crate::runtime) observed_at: Instant,
    pub(in crate::runtime) source: PeerRoundTripSource,
}

impl PeerRoundTripObservation {
    pub(in crate::runtime) fn accept(
        slot: &mut Option<Self>,
        elapsed: Duration,
        observed_at: Instant,
        source: PeerRoundTripSource,
    ) {
        if elapsed.is_zero() || slot.is_some_and(|previous| previous.observed_at >= observed_at) {
            return;
        }
        *slot = Some(Self {
            elapsed,
            observed_at,
            source,
        });
    }
}

/// Shared by the exact carrier's authenticated reader and sender projections.
/// No Product ACK, native counter or target-open completion can update it.
#[derive(Debug)]
pub(in crate::runtime) struct PeerRoundTrip {
    max_age: Duration,
    startup: PeerTiming,
    state: Mutex<Option<(PeerRoundTripObservation, PeerTiming)>>,
}

/// Read-only capability carried by Product outputs. It cannot record an
/// exchange or obtain the writer held by authenticated carrier owners.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct PeerRoundTripReader(std::sync::Arc<PeerRoundTrip>);

impl PeerRoundTripReader {
    pub(in crate::runtime) fn timing_at(&self, now: Instant) -> PeerTiming {
        self.0.timing_or_prior_at(now)
    }
}

impl PeerRoundTrip {
    pub(in crate::runtime) fn reader(self: &std::sync::Arc<Self>) -> PeerRoundTripReader {
        PeerRoundTripReader(self.clone())
    }

    pub(in crate::runtime) fn new(
        interval: Duration,
        timeout: Duration,
        startup: PeerTiming,
    ) -> Self {
        Self {
            // Allow one scheduled challenge and its existing response budget.
            max_age: interval.saturating_add(timeout),
            startup: PeerTiming::prior(startup.srtt_ms(), startup.rttvar_ms()),
            state: Mutex::new(None),
        }
    }

    /// A converged, carrier-owned peer observation for workflow fixtures.
    #[cfg(test)]
    pub(in crate::runtime) fn for_test(srtt_ms: f64, rttvar_ms: f64) -> std::sync::Arc<Self> {
        let limits = crate::mux::MuxLimits::default();
        let timing = PeerTiming::new(srtt_ms, rttvar_ms);
        std::sync::Arc::new(Self {
            max_age: limits.tcp_path_heartbeat_interval + limits.tcp_path_heartbeat_timeout,
            startup: PeerTiming::prior(crate::runtime::path::model::default_path_srtt_ms(), 0.0),
            state: Mutex::new(Some((
                PeerRoundTripObservation {
                    elapsed: Duration::from_secs_f64(srtt_ms / 1000.0),
                    observed_at: Instant::now(),
                    source: PeerRoundTripSource::PathProof,
                },
                timing,
            ))),
        })
    }

    pub(in crate::runtime) fn record(
        &self,
        elapsed: Duration,
        observed_at: Instant,
        source: PeerRoundTripSource,
    ) {
        if elapsed.is_zero() {
            return;
        }
        let mut state = self.state.lock().expect("peer timing lock");
        if state.is_some_and(|(last, _)| last.observed_at >= observed_at) {
            return;
        }
        let sample = elapsed.as_secs_f64() * 1000.0;
        let timing = match *state {
            Some((last, previous))
                if observed_at.saturating_duration_since(last.observed_at) <= self.max_age
                    && !(last.source == PeerRoundTripSource::Readiness
                        && source != PeerRoundTripSource::Readiness) =>
            {
                // RFC 6298's paired estimator (alpha=1/8, beta=1/4), not its
                // TCP retransmission policy. Variation uses the OLD SRTT.
                PeerTiming::new(
                    previous.srtt_ms() * 0.875 + sample * 0.125,
                    previous.rttvar_ms() * 0.75 + (previous.srtt_ms() - sample).abs() * 0.25,
                )
            }
            // Readiness includes admission work. Start steady exchanges with
            // their own estimate instead of carrying that startup work forever.
            _ => PeerTiming::new(sample, sample / 2.0),
        };
        *state = Some((
            PeerRoundTripObservation {
                elapsed,
                observed_at,
                source,
            },
            timing,
        ));
    }

    pub(in crate::runtime) fn last(&self) -> Option<PeerRoundTripObservation> {
        self.state
            .lock()
            .expect("peer timing lock")
            .map(|(last, _)| last)
    }

    pub(in crate::runtime) fn timing_or_prior_at(&self, now: Instant) -> PeerTiming {
        self.timing_at(now).unwrap_or(self.startup)
    }

    pub(in crate::runtime) fn timing_at(&self, now: Instant) -> Option<PeerTiming> {
        self.state
            .lock()
            .expect("peer timing lock")
            .filter(|(last, _)| {
                now.checked_duration_since(last.observed_at)
                    .is_some_and(|age| age <= self.max_age)
            })
            .map(|(_, timing)| timing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_estimator_restarts_after_readiness_or_expiry_without_renewing_last_sample() {
        let now = Instant::now();
        let prior = PeerTiming::prior(333.0, 0.0);
        let timing = PeerRoundTrip::new(Duration::from_secs(10), Duration::from_secs(30), prior);
        timing.record(Duration::from_secs(2), now, PeerRoundTripSource::Readiness);
        timing.record(
            Duration::from_millis(160),
            now + Duration::from_secs(1),
            PeerRoundTripSource::Heartbeat,
        );
        assert_eq!(
            timing.timing_at(now + Duration::from_secs(1)),
            Some(PeerTiming::new(160.0, 80.0))
        );
        timing.record(
            Duration::from_millis(80),
            now + Duration::from_secs(2),
            PeerRoundTripSource::PathProof,
        );
        assert_eq!(
            timing.timing_at(now + Duration::from_secs(2)),
            Some(PeerTiming::new(150.0, 80.0))
        );
        timing.record(
            Duration::from_millis(1),
            now + Duration::from_secs(2),
            PeerRoundTripSource::Heartbeat,
        );
        timing.record(
            Duration::ZERO,
            now + Duration::from_secs(3),
            PeerRoundTripSource::Heartbeat,
        );
        assert_eq!(timing.last().unwrap().elapsed, Duration::from_millis(80));
        assert_eq!(
            timing.timing_or_prior_at(now + Duration::from_secs(43)),
            prior
        );
        timing.record(
            Duration::from_millis(20),
            now + Duration::from_secs(43),
            PeerRoundTripSource::Heartbeat,
        );
        assert_eq!(
            timing.timing_at(now + Duration::from_secs(43)),
            Some(PeerTiming::new(20.0, 10.0))
        );
    }

    #[test]
    fn last_peer_round_trip_is_not_smoothed_with_another_source() {
        let now = Instant::now();
        let mut sample = None;
        PeerRoundTripObservation::accept(
            &mut sample,
            Duration::from_millis(160),
            now,
            PeerRoundTripSource::Readiness,
        );
        PeerRoundTripObservation::accept(
            &mut sample,
            Duration::from_millis(200),
            now + Duration::from_secs(1),
            PeerRoundTripSource::PathProof,
        );
        assert_eq!(sample.unwrap().elapsed, Duration::from_millis(200));
        assert_eq!(sample.unwrap().source, PeerRoundTripSource::PathProof);
    }

    #[test]
    fn older_and_zero_samples_cannot_replace_last_peer_round_trip() {
        let now = Instant::now();
        let mut sample = None;
        PeerRoundTripObservation::accept(
            &mut sample,
            Duration::from_millis(160),
            now,
            PeerRoundTripSource::Readiness,
        );
        PeerRoundTripObservation::accept(
            &mut sample,
            Duration::from_millis(1),
            now - Duration::from_secs(1),
            PeerRoundTripSource::PathProof,
        );
        PeerRoundTripObservation::accept(
            &mut sample,
            Duration::ZERO,
            now + Duration::from_secs(1),
            PeerRoundTripSource::PathProof,
        );
        assert_eq!(sample.unwrap().elapsed, Duration::from_millis(160));
        assert_eq!(sample.unwrap().observed_at, now);
    }
}
