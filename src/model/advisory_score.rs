//! Exact-action advisory service score from RFC Section 10.2.

use super::service_rate::{
    DirectionalServiceRate, DirectionalServiceRateScope, NormalizedMppWorkBytes, ServiceRateValue,
};
use std::cmp::Ordering;
use std::time::Duration;

const BITS_MILLISECONDS_PER_BYTE_SECOND: u128 = 8_000;
const MINIMUM_TIMING_UNCERTAINTY: Duration = Duration::from_millis(1);

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

    pub(crate) const fn as_u64(self) -> u64 {
        self.0
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

    pub(crate) const fn epoch(self) -> DirectionalTimingEpoch {
        self.epoch
    }

    pub(crate) const fn round_trip_time(self) -> Duration {
        self.round_trip_time
    }

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

/// Producer-complete input for one exact action score.
///
/// Option fields distinguish a missing/rejected producer value from a valid
/// zero-like value. The scorer has no internal fallback authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExactActionScoreInput {
    action_scope: DirectionalServiceRateScope,
    work: Option<NormalizedMppWorkBytes>,
    service_rate: Option<DirectionalServiceRate>,
    timing: Option<DirectionalTiming>,
}

impl ExactActionScoreInput {
    pub(crate) const fn new(
        action_scope: DirectionalServiceRateScope,
        work: Option<NormalizedMppWorkBytes>,
        service_rate: Option<DirectionalServiceRate>,
        timing: Option<DirectionalTiming>,
    ) -> Self {
        Self {
            action_scope,
            work,
            service_rate,
            timing,
        }
    }
}

/// Advisory score of one exact action.
///
/// `Unrankable` remains an eligible result. Structural owners sort it after
/// rankable actions in the same tier and may still attempt it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActionScore {
    Rankable {
        service_time: Duration,
        uncertainty: Duration,
    },
    Unrankable,
}

impl ActionScore {
    pub(crate) const fn service_time(self) -> Option<Duration> {
        match self {
            Self::Rankable { service_time, .. } => Some(service_time),
            Self::Unrankable => None,
        }
    }

    pub(crate) const fn uncertainty(self) -> Option<Duration> {
        match self {
            Self::Rankable { uncertainty, .. } => Some(uncertainty),
            Self::Unrankable => None,
        }
    }

    fn canonical_rank_cmp(self, other: Self) -> Ordering {
        match (self, other) {
            (
                Self::Rankable {
                    service_time: left, ..
                },
                Self::Rankable {
                    service_time: right,
                    ..
                },
            ) => left.cmp(&right),
            (Self::Rankable { .. }, Self::Unrankable) => Ordering::Less,
            (Self::Unrankable, Self::Rankable { .. }) => Ordering::Greater,
            (Self::Unrankable, Self::Unrankable) => Ordering::Equal,
        }
    }
}

/// Computes the exact-action score without choosing or repairing inputs.
pub(crate) fn exact_action_score(input: ExactActionScoreInput) -> ActionScore {
    let Some(work) = input.work else {
        return ActionScore::Unrankable;
    };
    let Some(service_rate) = input.service_rate else {
        return ActionScore::Unrankable;
    };
    let Some(timing) = input.timing else {
        return ActionScore::Unrankable;
    };
    if service_rate.scope() != input.action_scope || timing.scope() != input.action_scope {
        return ActionScore::Unrankable;
    }

    let propagation = timing.round_trip_time() / 2;
    let service = match service_rate.value() {
        ServiceRateValue::UnlimitedStartup => Duration::ZERO,
        ServiceRateValue::Finite(rate) => {
            let Some(numerator) =
                u128::from(work.get()).checked_mul(BITS_MILLISECONDS_PER_BYTE_SECOND)
            else {
                return ActionScore::Unrankable;
            };
            let denominator = u128::from(rate.get());
            let quotient = numerator / denominator;
            let Some(service_milliseconds) =
                quotient.checked_add(u128::from(numerator % denominator != 0))
            else {
                return ActionScore::Unrankable;
            };
            let Some(service) = duration_from_millis_u128(service_milliseconds) else {
                return ActionScore::Unrankable;
            };
            service
        }
    };
    let Some(service_time) = propagation.checked_add(service) else {
        return ActionScore::Unrankable;
    };
    let uncertainty = timing
        .variation()
        .unwrap_or(Duration::ZERO)
        .max(MINIMUM_TIMING_UNCERTAINTY);
    ActionScore::Rankable {
        service_time,
        uncertainty,
    }
}

/// One exact action plus the caller-owned ties needed inside one structural
/// policy tier.
///
/// `O` is `()` unless the caller has established the evidence-free configured
/// order from RFC Section 7.2. `K` must be the full canonical action identity;
/// this model never manufactures it from a reusable path id or input index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicallyRankedAction<O, K, V> {
    score: ActionScore,
    configured_order: O,
    key: K,
    value: V,
}

impl<O, K, V> CanonicallyRankedAction<O, K, V> {
    pub(crate) const fn new(score: ActionScore, configured_order: O, key: K, value: V) -> Self {
        Self {
            score,
            configured_order,
            key,
            value,
        }
    }

    pub(crate) const fn score(&self) -> ActionScore {
        self.score
    }

    pub(crate) const fn configured_order(&self) -> &O {
        &self.configured_order
    }

    pub(crate) const fn key(&self) -> &K {
        &self.key
    }

    pub(crate) const fn value(&self) -> &V {
        &self.value
    }

    pub(crate) const fn value_mut(&mut self) -> &mut V {
        &mut self.value
    }

    pub(crate) fn into_value(self) -> V {
        self.value
    }
}

/// Establishes the permutation-invariant base order for one structural tier.
pub(crate) fn sort_canonical_base_order<O: Ord, K: Ord, V>(
    actions: &mut [CanonicallyRankedAction<O, K, V>],
) {
    actions.sort_by(|left, right| {
        left.score
            .canonical_rank_cmp(right.score)
            .then_with(|| left.configured_order.cmp(&right.configured_order))
            .then_with(|| left.key.cmp(&right.key))
    });
}

/// Result of the one allowed post-sort incumbent operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncumbentPromotion {
    NotFound,
    AlreadyFirst,
    Promoted,
    ChallengerDisplaced,
    UnrankableBaseOrder,
}

/// Optionally promotes one exact incumbent to the first attempt.
///
/// The input must already be in canonical base order and contain only one
/// structural tier. This is deliberately a slice operation rather than a
/// comparator: pairwise uncertainty is non-transitive and must never be fed
/// to `sort_by`. Rotation preserves the relative order of every fallback, so
/// a failed incumbent commit proceeds with the unchanged canonical base order.
pub(crate) fn promote_incumbent_for_first_attempt<O, K: Eq, V>(
    canonical_base_order: &mut [CanonicallyRankedAction<O, K, V>],
    incumbent_key: &K,
) -> IncumbentPromotion {
    let Some(incumbent_index) = canonical_base_order
        .iter()
        .position(|action| action.key() == incumbent_key)
    else {
        return IncumbentPromotion::NotFound;
    };
    if incumbent_index == 0 {
        return IncumbentPromotion::AlreadyFirst;
    }

    let challenger = canonical_base_order[0].score();
    let incumbent = canonical_base_order[incumbent_index].score();
    let retain_incumbent = match (incumbent, challenger) {
        (
            ActionScore::Rankable {
                service_time: incumbent_service,
                uncertainty: incumbent_uncertainty,
            },
            ActionScore::Rankable {
                service_time: challenger_service,
                uncertainty: challenger_uncertainty,
            },
        ) => {
            let challenger_strictly_displaces = incumbent_service
                .checked_sub(challenger_service)
                .zip(incumbent_uncertainty.checked_add(challenger_uncertainty))
                .is_some_and(|(advantage, uncertainty)| advantage > uncertainty);
            // Overflow cannot prove the required strict advantage. Retaining
            // the incumbent is the conservative result; saturating the sum
            // would fabricate a comparable boundary.
            !challenger_strictly_displaces
        }
        (ActionScore::Rankable { .. }, ActionScore::Unrankable) => true,
        (ActionScore::Unrankable, ActionScore::Rankable { .. }) => false,
        (ActionScore::Unrankable, ActionScore::Unrankable) => {
            return IncumbentPromotion::UnrankableBaseOrder;
        }
    };

    if !retain_incumbent {
        return IncumbentPromotion::ChallengerDisplaced;
    }
    canonical_base_order[..=incumbent_index].rotate_right(1);
    IncumbentPromotion::Promoted
}

fn checked_duration_from_millis(milliseconds: f64) -> Option<Duration> {
    (milliseconds.is_finite() && milliseconds >= 0.0)
        .then_some(milliseconds / 1_000.0)
        .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
}

fn duration_from_millis_u128(milliseconds: u128) -> Option<Duration> {
    let seconds = u64::try_from(milliseconds / 1_000).ok()?;
    let subsec_millis = u32::try_from(milliseconds % 1_000).ok()?;
    Some(Duration::new(seconds, subsec_millis * 1_000_000))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::path::CarrierPathInstanceId;
    use crate::model::service_rate::{
        DirectionalServiceRate, DirectionalServiceRateScope, NORMALIZED_STREAM_DATA_OVERHEAD_BYTES,
        NormalizedMppWorkBytes,
    };
    use crate::protocol::PathMetricDirection;
    use crate::transport::RateHint;
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

    fn finite_rate(exact_scope: DirectionalServiceRateScope, bps: u64) -> DirectionalServiceRate {
        DirectionalServiceRate::from_startup_hint(exact_scope, RateHint::BitsPerSecond(bps))
            .unwrap()
    }

    fn score(
        exact_scope: DirectionalServiceRateScope,
        work: Option<NormalizedMppWorkBytes>,
        service_rate: Option<DirectionalServiceRate>,
        timing: Option<DirectionalTiming>,
    ) -> ActionScore {
        exact_action_score(ExactActionScoreInput::new(
            exact_scope,
            work,
            service_rate,
            timing,
        ))
    }

    #[test]
    fn finite_scores_use_exact_integer_units_and_round_only_service_time() {
        let exact_scope = scope(1, PathMetricDirection::ClientToServer);
        let rate_100m = finite_rate(exact_scope, 100_000_000);
        let timing_100ms = timing(exact_scope, 1, 100.0, Some(3.0));

        assert_eq!(
            score(
                exact_scope,
                NormalizedMppWorkBytes::checked_stream_data(1).ok(),
                Some(rate_100m),
                Some(timing_100ms),
            ),
            ActionScore::Rankable {
                service_time: Duration::from_millis(51),
                uncertainty: Duration::from_millis(3),
            },
        );
        assert_eq!(
            score(
                exact_scope,
                NormalizedMppWorkBytes::checked_stream_data(65_536).ok(),
                Some(rate_100m),
                Some(timing_100ms),
            ),
            ActionScore::Rankable {
                service_time: Duration::from_millis(56),
                uncertainty: Duration::from_millis(3),
            },
        );

        let rate_200m = finite_rate(exact_scope, 200_000_000);
        let work = NormalizedMppWorkBytes::checked_stream_data(4 * 1024 * 1024).ok();
        for (srtt_ms, expected_ms) in [(20.0, 178), (80.0, 208)] {
            assert_eq!(
                score(
                    exact_scope,
                    work,
                    Some(rate_200m),
                    Some(timing(exact_scope, 2, srtt_ms, None)),
                ),
                ActionScore::Rankable {
                    service_time: Duration::from_millis(expected_ms),
                    uncertainty: Duration::from_millis(1),
                },
            );
        }

        assert_eq!(
            score(
                exact_scope,
                NormalizedMppWorkBytes::checked_stream_data(1).ok(),
                Some(rate_100m),
                Some(timing(exact_scope, 3, 101.0, None)),
            ),
            ActionScore::Rankable {
                service_time: Duration::from_micros(51_500),
                uncertainty: Duration::from_millis(1),
            },
            "half-SRTT retains sub-millisecond precision while service time rounds up",
        );
    }

    #[test]
    fn unlimited_startup_contributes_no_numeric_service_sentinel() {
        let exact_scope = scope(1, PathMetricDirection::ServerToClient);
        let unlimited =
            DirectionalServiceRate::from_startup_hint(exact_scope, RateHint::Unlimited).unwrap();
        assert_eq!(
            score(
                exact_scope,
                NormalizedMppWorkBytes::checked_stream_data(4 * 1024 * 1024).ok(),
                Some(unlimited),
                Some(timing(exact_scope, 1, 101.0, None)),
            ),
            ActionScore::Rankable {
                service_time: Duration::from_micros(50_500),
                uncertainty: Duration::from_millis(1),
            },
        );
    }

    #[test]
    fn missing_mismatched_or_overflowing_inputs_are_unrankable() {
        let exact_scope = scope(7, PathMetricDirection::ClientToServer);
        let wrong_carrier = scope(8, PathMetricDirection::ClientToServer);
        let wrong_direction = scope(7, PathMetricDirection::ServerToClient);
        let work = NormalizedMppWorkBytes::checked_stream_data(1).ok();
        let rate = finite_rate(exact_scope, 100_000_000);
        let valid_timing = timing(exact_scope, 1, 100.0, None);
        let overflow_timing = DirectionalTiming::checked_from_parts(
            DirectionalRoundTripTime::from_duration(
                exact_scope,
                DirectionalTimingEpoch::from_raw(2),
                Duration::from_secs(2),
            ),
            None,
        )
        .unwrap();

        for result in [
            score(exact_scope, None, Some(rate), Some(valid_timing)),
            score(exact_scope, work, None, Some(valid_timing)),
            score(exact_scope, work, Some(rate), None),
            score(
                exact_scope,
                work,
                Some(finite_rate(wrong_carrier, 100_000_000)),
                Some(valid_timing),
            ),
            score(
                exact_scope,
                work,
                Some(rate),
                Some(timing(wrong_direction, 1, 100.0, None)),
            ),
            score(
                exact_scope,
                NormalizedMppWorkBytes::checked_stream_data(
                    u64::MAX - NORMALIZED_STREAM_DATA_OVERHEAD_BYTES,
                )
                .ok(),
                Some(finite_rate(exact_scope, 1)),
                Some(valid_timing),
            ),
            score(
                exact_scope,
                NormalizedMppWorkBytes::checked_stream_data(
                    u64::MAX - NORMALIZED_STREAM_DATA_OVERHEAD_BYTES,
                )
                .ok(),
                Some(finite_rate(exact_scope, 8)),
                Some(overflow_timing),
            ),
        ] {
            assert_eq!(result, ActionScore::Unrankable);
        }
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

    #[test]
    fn absent_and_explicit_zero_variation_use_the_one_millisecond_floor() {
        let exact_scope = scope(1, PathMetricDirection::ClientToServer);
        let work = NormalizedMppWorkBytes::checked_stream_data(1).ok();
        let rate = finite_rate(exact_scope, 100_000_000);

        for variation in [None, Some(0.0), Some(0.25)] {
            assert_eq!(
                score(
                    exact_scope,
                    work,
                    Some(rate),
                    Some(timing(exact_scope, 1, 100.0, variation)),
                )
                .uncertainty(),
                Some(Duration::from_millis(1)),
            );
        }
    }

    #[test]
    fn one_millisecond_floor_has_the_exact_strict_retention_boundary() {
        for variation in [None, Some(0.0)] {
            let exact_scope = scope(1, PathMetricDirection::ClientToServer);
            let work = NormalizedMppWorkBytes::checked_stream_data(1).ok();
            let rate = finite_rate(exact_scope, 100_000_000);
            let challenger_score = score(
                exact_scope,
                work,
                Some(rate),
                Some(timing(exact_scope, 1, 98.0, variation)),
            );
            let incumbent_102 = ActionScore::Rankable {
                service_time: Duration::from_millis(52),
                uncertainty: Duration::from_millis(1),
            };
            let incumbent_103 = ActionScore::Rankable {
                service_time: Duration::from_millis(53),
                uncertainty: Duration::from_millis(1),
            };

            let mut equality = vec![
                CanonicallyRankedAction::new(challenger_score, (), 1_u8, ()),
                CanonicallyRankedAction::new(incumbent_102, (), 9_u8, ()),
            ];
            sort_canonical_base_order(&mut equality);
            assert_eq!(
                promote_incumbent_for_first_attempt(&mut equality, &9),
                IncumbentPromotion::Promoted,
                "a two-millisecond advantage equals the two one-millisecond floors",
            );

            let mut strict = vec![
                CanonicallyRankedAction::new(challenger_score, (), 1_u8, ()),
                CanonicallyRankedAction::new(incumbent_103, (), 9_u8, ()),
            ];
            sort_canonical_base_order(&mut strict);
            assert_eq!(
                promote_incumbent_for_first_attempt(&mut strict, &9),
                IncumbentPromotion::ChallengerDisplaced,
                "a three-millisecond advantage strictly exceeds both floors",
            );
        }
    }

    fn direct_score(service_ms: u64, uncertainty_ms: u64) -> ActionScore {
        ActionScore::Rankable {
            service_time: Duration::from_millis(service_ms),
            uncertainty: Duration::from_millis(uncertainty_ms),
        }
    }

    fn candidate(
        key: u8,
        service_ms: u64,
        uncertainty_ms: u64,
    ) -> CanonicallyRankedAction<(), u8, u8> {
        CanonicallyRankedAction::new(direct_score(service_ms, uncertainty_ms), (), key, key)
    }

    #[test]
    fn canonical_base_order_is_permutation_invariant_and_does_not_sort_by_uncertainty() {
        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for permutation in permutations {
            let source = [
                candidate(3, 100, 80),
                candidate(1, 100, 1),
                candidate(2, 105, 1),
            ];
            let mut actions = permutation.map(|index| source[index].clone()).to_vec();
            sort_canonical_base_order(&mut actions);
            assert_eq!(
                actions
                    .iter()
                    .map(|action| *action.key())
                    .collect::<Vec<_>>(),
                vec![1, 3, 2],
            );
        }
    }

    #[test]
    fn evidence_free_configured_order_precedes_the_final_action_identity() {
        let score = direct_score(100, 1);
        let mut actions = vec![
            CanonicallyRankedAction::new(score, (2_u8, 0_u8), 1_u8, ()),
            CanonicallyRankedAction::new(score, (1_u8, 3_u8), 9_u8, ()),
            CanonicallyRankedAction::new(score, (1_u8, 3_u8), 4_u8, ()),
        ];
        sort_canonical_base_order(&mut actions);
        assert_eq!(
            actions
                .iter()
                .map(|action| (*action.configured_order(), *action.key()))
                .collect::<Vec<_>>(),
            vec![((1, 3), 4), ((1, 3), 9), ((2, 0), 1)],
        );
    }

    #[test]
    fn unrankable_actions_sort_last_but_remain_in_the_attempt_order() {
        let mut sole = vec![CanonicallyRankedAction::new(
            ActionScore::Unrankable,
            (),
            7_u8,
            "sole",
        )];
        sort_canonical_base_order(&mut sole);
        assert_eq!(sole.len(), 1);
        assert_eq!(*sole[0].value(), "sole");

        let mut mixed = vec![
            CanonicallyRankedAction::new(ActionScore::Unrankable, (), 1_u8, 1_u8),
            candidate(2, 100, 1),
            CanonicallyRankedAction::new(ActionScore::Unrankable, (), 0_u8, 0_u8),
        ];
        sort_canonical_base_order(&mut mixed);
        assert_eq!(
            mixed.iter().map(|action| *action.key()).collect::<Vec<_>>(),
            vec![2, 0, 1],
        );
        assert_eq!(
            promote_incumbent_for_first_attempt(&mut mixed, &1),
            IncumbentPromotion::ChallengerDisplaced,
            "an unrankable incumbent cannot displace a rankable challenger",
        );

        let mut both_unrankable = vec![
            CanonicallyRankedAction::new(ActionScore::Unrankable, (), 1_u8, ()),
            CanonicallyRankedAction::new(ActionScore::Unrankable, (), 2_u8, ()),
        ];
        sort_canonical_base_order(&mut both_unrankable);
        assert_eq!(
            promote_incumbent_for_first_attempt(&mut both_unrankable, &2),
            IncumbentPromotion::UnrankableBaseOrder,
        );
        assert_eq!(
            both_unrankable
                .iter()
                .map(|action| *action.key())
                .collect::<Vec<_>>(),
            vec![1, 2],
        );
    }

    #[test]
    fn incumbent_deadband_is_strict_and_promotion_preserves_fallback_base_order() {
        let mut equality = vec![
            candidate(1, 100, 3),
            candidate(2, 105, 1),
            candidate(9, 107, 4),
        ];
        sort_canonical_base_order(&mut equality);
        let canonical_without_incumbent = equality
            .iter()
            .filter(|action| *action.key() != 9)
            .map(|action| *action.key())
            .collect::<Vec<_>>();
        assert_eq!(
            promote_incumbent_for_first_attempt(&mut equality, &9),
            IncumbentPromotion::Promoted,
            "a seven-millisecond advantage equals U_i + U_c and retains the incumbent",
        );
        assert_eq!(
            equality
                .iter()
                .map(|action| *action.key())
                .collect::<Vec<_>>(),
            vec![9, 1, 2],
        );
        assert_eq!(
            equality
                .iter()
                .skip(1)
                .map(|action| *action.key())
                .collect::<Vec<_>>(),
            canonical_without_incumbent,
            "an incumbent commit failure continues the unchanged canonical fallback order",
        );

        let mut strict = vec![
            candidate(9, 108, 4),
            candidate(1, 100, 3),
            candidate(2, 105, 1),
        ];
        sort_canonical_base_order(&mut strict);
        assert_eq!(
            promote_incumbent_for_first_attempt(&mut strict, &9),
            IncumbentPromotion::ChallengerDisplaced,
        );
        assert_eq!(
            strict
                .iter()
                .map(|action| *action.key())
                .collect::<Vec<_>>(),
            vec![1, 2, 9],
        );
    }

    #[test]
    fn incumbent_promotion_is_one_post_sort_operation_for_three_candidates() {
        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for permutation in permutations {
            let source = [
                candidate(1, 100, 3),
                candidate(2, 105, 50),
                candidate(9, 107, 4),
            ];
            let mut actions = permutation.map(|index| source[index].clone()).to_vec();
            sort_canonical_base_order(&mut actions);
            promote_incumbent_for_first_attempt(&mut actions, &9);
            assert_eq!(
                actions
                    .iter()
                    .map(|action| *action.key())
                    .collect::<Vec<_>>(),
                vec![9, 1, 2],
            );
        }
    }
}
