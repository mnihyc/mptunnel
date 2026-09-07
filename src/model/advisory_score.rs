//! Coherent directional timing publications for live path snapshots.
//!
//! Timing scope and epoch validation are independent of allocation policy.

use super::service_rate::DirectionalServiceRateScope;
use std::time::Duration;

/// Producer-owned identity of one coherent directional timing publication.
///
/// This epoch has no scheduling weight. It exists only to prevent a producer
/// from assembling R and J from different observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DirectionalTimingEpoch(u64);

impl DirectionalTimingEpoch {
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[cfg(test)]
    pub(crate) const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Checked, non-reusing publication identity owned by one timing producer.
///
/// Exhaustion is deliberately represented by `None`: timing remains advisory,
/// so a producer that can no longer name a fresh tuple preserves its last
/// accepted value without failing carrier I/O, rate publication, or Product
/// work.
#[derive(Debug)]
pub(crate) struct DirectionalTimingEpochIssuer {
    next: Option<u64>,
}

impl Default for DirectionalTimingEpochIssuer {
    fn default() -> Self {
        Self { next: Some(1) }
    }
}

impl DirectionalTimingEpochIssuer {
    pub(crate) fn issue(&mut self) -> Option<DirectionalTimingEpoch> {
        let current = self.next?;
        self.next = current.checked_add(1);
        Some(DirectionalTimingEpoch::from_raw(current))
    }

    #[cfg(test)]
    pub(crate) fn set_next_for_test(&mut self, next: u64) {
        self.next = Some(next);
    }
}

/// Checked round-trip component of one producer timing publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirectionalRoundTripTime {
    scope: DirectionalServiceRateScope,
    epoch: DirectionalTimingEpoch,
    value: Duration,
}

impl DirectionalRoundTripTime {
    /// Preserves an already typed producer duration without an `f64` round
    /// trip. `Duration` is intrinsically finite and nonnegative.
    pub(crate) const fn from_duration(
        scope: DirectionalServiceRateScope,
        epoch: DirectionalTimingEpoch,
        value: Duration,
    ) -> Self {
        Self {
            scope,
            epoch,
            value,
        }
    }

    /// Rejects non-finite, negative, or unrepresentable raw observations.
    pub(crate) fn checked_from_millis(
        scope: DirectionalServiceRateScope,
        epoch: DirectionalTimingEpoch,
        milliseconds: f64,
    ) -> Result<Self, DirectionalTimingModelError> {
        Ok(Self {
            scope,
            epoch,
            value: checked_duration_from_millis(milliseconds)
                .ok_or(DirectionalTimingModelError::InvalidRoundTripTime)?,
        })
    }

    pub(crate) const fn scope(self) -> DirectionalServiceRateScope {
        self.scope
    }

    pub(crate) const fn epoch(self) -> DirectionalTimingEpoch {
        self.epoch
    }

    pub(crate) const fn value(self) -> Duration {
        self.value
    }
}

/// Checked variation component of one producer timing publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirectionalTimingVariation {
    scope: DirectionalServiceRateScope,
    epoch: DirectionalTimingEpoch,
    value: Duration,
}

impl DirectionalTimingVariation {
    /// Preserves an already typed producer duration without an `f64` round
    /// trip. `Duration` is intrinsically finite and nonnegative.
    pub(crate) const fn from_duration(
        scope: DirectionalServiceRateScope,
        epoch: DirectionalTimingEpoch,
        value: Duration,
    ) -> Self {
        Self {
            scope,
            epoch,
            value,
        }
    }

    /// Rejects non-finite, negative, or unrepresentable raw observations.
    pub(crate) fn checked_from_millis(
        scope: DirectionalServiceRateScope,
        epoch: DirectionalTimingEpoch,
        milliseconds: f64,
    ) -> Result<Self, DirectionalTimingModelError> {
        Ok(Self {
            scope,
            epoch,
            value: checked_duration_from_millis(milliseconds)
                .ok_or(DirectionalTimingModelError::InvalidVariation)?,
        })
    }

    pub(crate) const fn scope(self) -> DirectionalServiceRateScope {
        self.scope
    }

    pub(crate) const fn epoch(self) -> DirectionalTimingEpoch {
        self.epoch
    }

    pub(crate) const fn value(self) -> Duration {
        self.value
    }
}

/// One coherent immutable directional timing tuple `(R, optional J)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirectionalTiming {
    scope: DirectionalServiceRateScope,
    epoch: DirectionalTimingEpoch,
    round_trip_time: Duration,
    variation: Option<Duration>,
}

impl DirectionalTiming {
    /// Combines only components from the same carrier/direction and epoch.
    ///
    /// Passing J as `None` is valid and never borrows a value from another
    /// source. Carrier activation is part of `DirectionalServiceRateScope`
    /// through its exact carrier-instance identity.
    pub(crate) fn checked_from_parts(
        round_trip_time: DirectionalRoundTripTime,
        variation: Option<DirectionalTimingVariation>,
    ) -> Result<Self, DirectionalTimingModelError> {
        if let Some(variation) = variation {
            if variation.scope() != round_trip_time.scope() {
                return Err(DirectionalTimingModelError::ScopeMismatch {
                    expected: round_trip_time.scope(),
                    observed: variation.scope(),
                });
            }
            if variation.epoch() != round_trip_time.epoch() {
                return Err(DirectionalTimingModelError::EpochMismatch {
                    expected: round_trip_time.epoch(),
                    observed: variation.epoch(),
                });
            }
        }
        Ok(Self {
            scope: round_trip_time.scope(),
            epoch: round_trip_time.epoch(),
            round_trip_time: round_trip_time.value(),
            variation: variation.map(DirectionalTimingVariation::value),
        })
    }

    pub(crate) const fn scope(self) -> DirectionalServiceRateScope {
        self.scope
    }

    #[cfg(test)]
    pub(crate) const fn epoch(self) -> DirectionalTimingEpoch {
        self.epoch
    }

    #[cfg(test)]
    pub(crate) const fn round_trip_time(self) -> Duration {
        self.round_trip_time
    }

    #[cfg(test)]
    pub(crate) const fn variation(self) -> Option<Duration> {
        self.variation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectionalTimingModelError {
    InvalidRoundTripTime,
    InvalidVariation,
    ScopeMismatch {
        expected: DirectionalServiceRateScope,
        observed: DirectionalServiceRateScope,
    },
    EpochMismatch {
        expected: DirectionalTimingEpoch,
        observed: DirectionalTimingEpoch,
    },
}

fn checked_duration_from_millis(milliseconds: f64) -> Option<Duration> {
    (milliseconds.is_finite() && milliseconds >= 0.0)
        .then_some(milliseconds / 1_000.0)
        .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::path::CarrierPathInstanceId;
    use crate::model::service_rate::DirectionalServiceRateScope;
    use crate::protocol::PathMetricDirection;
    use std::time::Duration;

    fn scope(instance: u64, direction: PathMetricDirection) -> DirectionalServiceRateScope {
        DirectionalServiceRateScope::new(CarrierPathInstanceId::from_raw(instance), direction)
    }

    fn timing(
        exact_scope: DirectionalServiceRateScope,
        epoch: u64,
        srtt_ms: f64,
        variation_ms: Option<f64>,
    ) -> DirectionalTiming {
        let epoch = DirectionalTimingEpoch::from_raw(epoch);
        let round_trip =
            DirectionalRoundTripTime::checked_from_millis(exact_scope, epoch, srtt_ms).unwrap();
        let variation = variation_ms.map(|value| {
            DirectionalTimingVariation::checked_from_millis(exact_scope, epoch, value).unwrap()
        });
        DirectionalTiming::checked_from_parts(round_trip, variation).unwrap()
    }

    #[test]
    fn timing_parts_reject_cross_scope_or_cross_epoch_mixing_and_raw_malformed_values() {
        let exact_scope = scope(3, PathMetricDirection::ClientToServer);
        let other_carrier = scope(4, PathMetricDirection::ClientToServer);
        let other_direction = scope(3, PathMetricDirection::ServerToClient);
        let epoch_1 = DirectionalTimingEpoch::from_raw(1);
        let epoch_2 = DirectionalTimingEpoch::from_raw(2);
        let round_trip =
            DirectionalRoundTripTime::checked_from_millis(exact_scope, epoch_1, 100.0).unwrap();

        for variation in [
            DirectionalTimingVariation::checked_from_millis(other_carrier, epoch_1, 4.0).unwrap(),
            DirectionalTimingVariation::checked_from_millis(other_direction, epoch_1, 4.0).unwrap(),
        ] {
            assert!(matches!(
                DirectionalTiming::checked_from_parts(round_trip, Some(variation)),
                Err(DirectionalTimingModelError::ScopeMismatch { .. })
            ));
        }
        assert_eq!(
            DirectionalTiming::checked_from_parts(
                round_trip,
                Some(
                    DirectionalTimingVariation::checked_from_millis(exact_scope, epoch_2, 4.0,)
                        .unwrap(),
                ),
            ),
            Err(DirectionalTimingModelError::EpochMismatch {
                expected: epoch_1,
                observed: epoch_2,
            }),
        );

        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.001] {
            assert_eq!(
                DirectionalRoundTripTime::checked_from_millis(exact_scope, epoch_1, invalid),
                Err(DirectionalTimingModelError::InvalidRoundTripTime),
            );
            assert_eq!(
                DirectionalTimingVariation::checked_from_millis(exact_scope, epoch_1, invalid),
                Err(DirectionalTimingModelError::InvalidVariation),
            );
        }

        let accepted = timing(exact_scope, 1, 100.0, Some(4.0));
        let rejected =
            DirectionalRoundTripTime::checked_from_millis(exact_scope, epoch_2, f64::NAN);
        assert_eq!(
            rejected,
            Err(DirectionalTimingModelError::InvalidRoundTripTime)
        );
        // The pure constructor yields no replacement value. Retaining this
        // prior accepted tuple is the runtime producer's state transition.
        assert_eq!(accepted.round_trip_time(), Duration::from_millis(100));
        assert_eq!(accepted.variation(), Some(Duration::from_millis(4)));
    }
}
