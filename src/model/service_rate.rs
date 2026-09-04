//! Typed directional service-rate and canonical Core-work primitives.
//!
//! This module deliberately models only the rate input to the advisory rank
//! in RFC Section 10.2. It does not pace, admit, reserve, or account Product
//! work. A rate belongs to one exact carrier instance and original-sender
//! direction, and its source is selected semantically rather than by comparing
//! numeric values.

use super::path::CarrierPathInstanceId;
use crate::protocol::PathMetricDirection;
use crate::transport::RateHint;
use std::num::NonZeroU64;

const BITS_PER_BYTE: u64 = 8;
const MILLIS_PER_SECOND: u64 = 1_000;
const PORTABLE_STARTUP_PAYLOAD_BYTES: u64 = 14_600;
const PORTABLE_STARTUP_RTT_MS: u64 = 333;
const STREAM_DATA_PAYLOAD_FIELDS_BYTES: u64 = 8 + 8 + 4;

/// Canonical pre-native Core overhead of one unsplit `STREAM_DATA` frame.
///
/// Native record, crypto, HTTP/3, QUIC, and TCP framing are outside this work
/// domain. Keeping those bytes out gives the same action the same work value
/// on every carrier family.
pub(crate) const NORMALIZED_STREAM_DATA_OVERHEAD_BYTES: u64 =
    crate::protocol::codec::FRAME_HEADER_LEN as u64 + STREAM_DATA_PAYLOAD_FIELDS_BYTES;

/// Positive finite service rate in normalized-MPP bits per second.
///
/// Integer storage is part of the authority contract: configured values above
/// `2^53` must not pass through `f64` and lose identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PositiveRateBps(NonZeroU64);

impl PositiveRateBps {
    pub(crate) fn checked_new(bits_per_second: u64) -> Result<Self, ServiceRateModelError> {
        NonZeroU64::new(bits_per_second)
            .map(Self)
            .ok_or(ServiceRateModelError::ZeroFiniteRate)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Exact immutable scope of one scheduling-rate authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirectionalServiceRateScope {
    carrier_instance_id: CarrierPathInstanceId,
    direction: PathMetricDirection,
}

impl DirectionalServiceRateScope {
    pub(crate) const fn new(
        carrier_instance_id: CarrierPathInstanceId,
        direction: PathMetricDirection,
    ) -> Self {
        Self {
            carrier_instance_id,
            direction,
        }
    }

    pub(crate) const fn carrier_instance_id(self) -> CarrierPathInstanceId {
        self.carrier_instance_id
    }

    pub(crate) const fn direction(self) -> PathMetricDirection {
        self.direction
    }
}

/// Semantic source of the effective advisory service rate.
///
/// There is intentionally no TCP native-operational variant: Core Profile 7
/// names no such adapter. TCP telemetry remains diagnostic until another
/// profile declares a complete adapter contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceRateBasis {
    PortableStartup,
    ConfiguredStartup,
    UnlimitedStartup,
    QuinnBbr3NativeOperationalV1,
}

/// Effective value without inventing a numeric representation for Unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceRateValue {
    Finite(PositiveRateBps),
    UnlimitedStartup,
}

/// One immutable effective rate projection for an exact directional scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DirectionalServiceRate {
    scope: DirectionalServiceRateScope,
    basis: ServiceRateBasis,
    value: ServiceRateValue,
}

impl DirectionalServiceRate {
    /// Resolves the immutable endpoint-local startup basis.
    pub(crate) fn from_startup_hint(
        scope: DirectionalServiceRateScope,
        hint: RateHint,
    ) -> Result<Self, ServiceRateModelError> {
        let (basis, value) = match hint {
            RateHint::Unknown => (
                ServiceRateBasis::PortableStartup,
                ServiceRateValue::Finite(portable_startup_rate()?),
            ),
            RateHint::BitsPerSecond(bits_per_second) => (
                ServiceRateBasis::ConfiguredStartup,
                ServiceRateValue::Finite(PositiveRateBps::checked_new(bits_per_second)?),
            ),
            RateHint::Unlimited => (
                ServiceRateBasis::UnlimitedStartup,
                ServiceRateValue::UnlimitedStartup,
            ),
        };
        Ok(Self {
            scope,
            basis,
            value,
        })
    }

    /// Replaces either startup basis, or an earlier publication, with one
    /// finite observation from the named Quinn adapter.
    ///
    /// This is source replacement, never `max(startup, native)`. The caller
    /// remains responsible for the activation/controller/revision chronology
    /// required by RFC Section 10.2.1 before constructing the observation.
    pub(crate) fn replace_with_quinn_bbr3_native_operational(
        self,
        observation: QuinnBbr3NativeOperationalRate,
    ) -> Result<Self, ServiceRateModelError> {
        if observation.scope != self.scope {
            return Err(ServiceRateModelError::ScopeMismatch {
                expected: self.scope,
                observed: observation.scope,
            });
        }
        Ok(Self {
            scope: self.scope,
            basis: ServiceRateBasis::QuinnBbr3NativeOperationalV1,
            value: ServiceRateValue::Finite(observation.rate),
        })
    }

    pub(crate) const fn scope(self) -> DirectionalServiceRateScope {
        self.scope
    }

    pub(crate) const fn basis(self) -> ServiceRateBasis {
        self.basis
    }

    pub(crate) const fn value(self) -> ServiceRateValue {
        self.value
    }
}

/// Qualified finite publication from `QuinnBbr3NativeOperationalV1`.
///
/// Activation and controller chronology live in the existing native adapter;
/// this value carries the exact scope and checked normalized rate across the
/// pure model boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuinnBbr3NativeOperationalRate {
    scope: DirectionalServiceRateScope,
    rate: PositiveRateBps,
}

impl QuinnBbr3NativeOperationalRate {
    pub(crate) fn checked_new(
        scope: DirectionalServiceRateScope,
        bits_per_second: u64,
    ) -> Result<Self, ServiceRateModelError> {
        Ok(Self {
            scope,
            rate: PositiveRateBps::checked_new(bits_per_second)?,
        })
    }

    pub(crate) const fn scope(self) -> DirectionalServiceRateScope {
        self.scope
    }

    pub(crate) const fn rate(self) -> PositiveRateBps {
        self.rate
    }
}

/// Canonical pre-native encoded MPP work in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NormalizedMppWorkBytes(u64);

impl NormalizedMppWorkBytes {
    /// Returns the encoded Core work of one unsplit `STREAM_DATA` action.
    pub(crate) fn checked_stream_data(payload_bytes: u64) -> Result<Self, ServiceRateModelError> {
        payload_bytes
            .checked_add(NORMALIZED_STREAM_DATA_OVERHEAD_BYTES)
            .map(Self)
            .ok_or(ServiceRateModelError::ArithmeticOverflow)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// Computes the portable Unknown-startup prior from canonical Core work.
pub(crate) fn portable_startup_rate() -> Result<PositiveRateBps, ServiceRateModelError> {
    let work = NormalizedMppWorkBytes::checked_stream_data(PORTABLE_STARTUP_PAYLOAD_BYTES)?;
    let numerator = work
        .get()
        .checked_mul(BITS_PER_BYTE)
        .and_then(|value| value.checked_mul(MILLIS_PER_SECOND))
        .ok_or(ServiceRateModelError::ArithmeticOverflow)?;
    let bits_per_second = checked_ceil_div(numerator, PORTABLE_STARTUP_RTT_MS)?;
    PositiveRateBps::checked_new(bits_per_second)
}

fn checked_ceil_div(numerator: u64, denominator: u64) -> Result<u64, ServiceRateModelError> {
    if denominator == 0 {
        return Err(ServiceRateModelError::DivisionByZero);
    }
    let quotient = numerator / denominator;
    if numerator % denominator == 0 {
        Ok(quotient)
    } else {
        quotient
            .checked_add(1)
            .ok_or(ServiceRateModelError::ArithmeticOverflow)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceRateModelError {
    ZeroFiniteRate,
    ArithmeticOverflow,
    DivisionByZero,
    ScopeMismatch {
        expected: DirectionalServiceRateScope,
        observed: DirectionalServiceRateScope,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::codec::{CodecLimits, encode_frame};
    use crate::protocol::{Frame, StreamId};
    use bytes::Bytes;

    fn scope(instance: u64, direction: PathMetricDirection) -> DirectionalServiceRateScope {
        DirectionalServiceRateScope::new(CarrierPathInstanceId::from_raw(instance), direction)
    }

    #[test]
    fn portable_startup_uses_canonical_work_and_ceiling() {
        let work = NormalizedMppWorkBytes::checked_stream_data(14_600).unwrap();
        assert_eq!(work.get(), 14_630);
        assert_eq!(portable_startup_rate().unwrap().get(), 351_472);

        let exact_scope = scope(3, PathMetricDirection::ClientToServer);
        let startup =
            DirectionalServiceRate::from_startup_hint(exact_scope, RateHint::Unknown).unwrap();
        assert_eq!(startup.scope(), exact_scope);
        assert_eq!(startup.scope().carrier_instance_id().as_u64(), 3);
        assert_eq!(
            startup.scope().direction(),
            PathMetricDirection::ClientToServer
        );
        assert_eq!(startup.basis(), ServiceRateBasis::PortableStartup);
        assert_eq!(
            startup.value(),
            ServiceRateValue::Finite(PositiveRateBps::checked_new(351_472).unwrap())
        );
    }

    #[test]
    fn finite_rates_are_positive_and_preserve_values_above_two_to_the_fifty_third() {
        assert_eq!(
            PositiveRateBps::checked_new(0),
            Err(ServiceRateModelError::ZeroFiniteRate)
        );

        let exact = (1_u64 << 53) + 1;
        let configured = DirectionalServiceRate::from_startup_hint(
            scope(1, PathMetricDirection::ClientToServer),
            RateHint::BitsPerSecond(exact),
        )
        .unwrap();
        assert_eq!(configured.basis(), ServiceRateBasis::ConfiguredStartup);
        assert_eq!(
            configured.value(),
            ServiceRateValue::Finite(PositiveRateBps::checked_new(exact).unwrap())
        );
    }

    #[test]
    fn unlimited_startup_has_no_numeric_rate() {
        let unlimited = DirectionalServiceRate::from_startup_hint(
            scope(1, PathMetricDirection::ClientToServer),
            RateHint::Unlimited,
        )
        .unwrap();
        assert_eq!(unlimited.basis(), ServiceRateBasis::UnlimitedStartup);
        assert_eq!(unlimited.value(), ServiceRateValue::UnlimitedStartup);
    }

    #[test]
    fn quinn_native_operational_replaces_instead_of_maximizing_startup() {
        let exact_scope = scope(7, PathMetricDirection::ServerToClient);
        let configured = DirectionalServiceRate::from_startup_hint(
            exact_scope,
            RateHint::BitsPerSecond(500_000_000),
        )
        .unwrap();
        let native = QuinnBbr3NativeOperationalRate::checked_new(exact_scope, 2_000_000).unwrap();
        assert_eq!(native.scope(), exact_scope);
        assert_eq!(native.rate().get(), 2_000_000);

        let replaced = configured
            .replace_with_quinn_bbr3_native_operational(native)
            .unwrap();
        assert_eq!(
            replaced.basis(),
            ServiceRateBasis::QuinnBbr3NativeOperationalV1
        );
        assert_eq!(
            replaced.value(),
            ServiceRateValue::Finite(PositiveRateBps::checked_new(2_000_000).unwrap())
        );
    }

    #[test]
    fn quinn_replacement_rejects_other_direction_and_carrier_instance() {
        let exact_scope = scope(7, PathMetricDirection::ClientToServer);
        let startup =
            DirectionalServiceRate::from_startup_hint(exact_scope, RateHint::Unknown).unwrap();

        for wrong_scope in [
            scope(7, PathMetricDirection::ServerToClient),
            scope(8, PathMetricDirection::ClientToServer),
        ] {
            let observation =
                QuinnBbr3NativeOperationalRate::checked_new(wrong_scope, 10_000_000).unwrap();
            assert_eq!(
                startup.replace_with_quinn_bbr3_native_operational(observation),
                Err(ServiceRateModelError::ScopeMismatch {
                    expected: exact_scope,
                    observed: wrong_scope,
                })
            );
        }
    }

    #[test]
    fn normalized_stream_data_work_matches_unsplit_core_codec() {
        for payload_bytes in [1_usize, 14_600] {
            let frame = Frame::StreamData {
                stream_id: StreamId(17),
                offset: 23,
                payload: Bytes::from(vec![0; payload_bytes]),
            };
            let encoded = encode_frame(&frame, CodecLimits::default()).unwrap();
            let normalized =
                NormalizedMppWorkBytes::checked_stream_data(payload_bytes as u64).unwrap();
            assert_eq!(normalized.get(), encoded.len() as u64);
            assert_eq!(normalized.get(), payload_bytes as u64 + 30);
        }
    }

    #[test]
    fn normalized_stream_data_work_rejects_overflow() {
        assert_eq!(
            NormalizedMppWorkBytes::checked_stream_data(u64::MAX),
            Err(ServiceRateModelError::ArithmeticOverflow)
        );
    }
}
