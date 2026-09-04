//! Runtime ownership of one native QUIC carrier-rate authority.
//!
//! The model reducer remains pure. This owner supplies the exact transport
//! activation fence, serializes the reducer, and deliberately retains no
//! parallel copy of its activation, controller, revision, or rate.

use crate::model::advisory_score::{
    DirectionalRoundTripTime, DirectionalTiming, DirectionalTimingEpochIssuer,
    DirectionalTimingVariation,
};
use crate::model::carrier_rate_authority::{
    CarrierRateAuthorityBasis, CarrierRateAuthorityError, CarrierRateAuthorityMode,
    CarrierRateAuthorityScope, CarrierRateAuthoritySnapshot, CarrierRateAuthorityStamp,
    CarrierRateAuthorityTransition, NativeCarrierRateAuthority, NativeCarrierRateInputError,
    NativeCarrierRatePublicationTicket, NativeCarrierRateSourceSnapshot,
    NativeCarrierTransportCurrent, NativeCarrierTransportExhaustion,
};
use crate::model::service_rate::{DirectionalServiceRate, ServiceRateModelError};
use crate::transport::RateHint;
use crate::transport::quic::{
    NativeControllerAuthoritySnapshot, NativeControllerObservationKind,
    NativeControllerShapeSnapshot,
};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;

/// Failure at the serialized native-authority/transport boundary.
///
/// `TransportSourceChanged` and a stale central stamp are the only ordinary
/// retry cases. Invalid checked input, terminal transport state, and reducer
/// contract violations fail closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum NativeCarrierRateAuthorityRuntimeError {
    CoordinatorPoisoned,
    ActivationFencePoisoned,
    TransportSourceBindingMismatch,
    TransportSourceUnavailable,
    TransportUninitialized,
    TransportTerminal(quinn::congestion::ControllerActivationTerminal),
    TransportSourceChanged,
    SchedulingShapeUnavailable,
    MalformedTransportSource,
    Input(NativeCarrierRateInputError),
    Startup(ServiceRateModelError),
    Authority(CarrierRateAuthorityError),
    BindingScopeMismatch {
        existing: CarrierRateAuthorityScope,
        requested: CarrierRateAuthorityScope,
    },
}

impl NativeCarrierRateAuthorityRuntimeError {
    /// Only an observation race or a central expected-G race may be retried.
    pub(in crate::runtime) fn is_retryable_publication(self) -> bool {
        matches!(
            self,
            Self::TransportSourceChanged
                | Self::Authority(CarrierRateAuthorityError::StaleStamp)
                | Self::Authority(CarrierRateAuthorityError::ActiveTransportMismatch)
                | Self::Authority(CarrierRateAuthorityError::NativeActivationReused)
        )
    }

    pub(in crate::runtime) fn is_transport_terminal(self) -> bool {
        matches!(
            self,
            Self::TransportTerminal(_) | Self::Authority(CarrierRateAuthorityError::Terminal)
        )
    }
}

impl From<NativeCarrierRateInputError> for NativeCarrierRateAuthorityRuntimeError {
    fn from(error: NativeCarrierRateInputError) -> Self {
        Self::Input(error)
    }
}

impl From<CarrierRateAuthorityError> for NativeCarrierRateAuthorityRuntimeError {
    fn from(error: CarrierRateAuthorityError) -> Self {
        Self::Authority(error)
    }
}

impl From<ServiceRateModelError> for NativeCarrierRateAuthorityRuntimeError {
    fn from(error: ServiceRateModelError) -> Self {
        Self::Startup(error)
    }
}

/// Result returned by one accepted source publication.
///
/// The snapshot is read from the reducer while its mutex is still held after
/// compare/apply. A later health/registry publisher can consume this exact
/// accepted result without introducing a second listener on Quinn's notify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct NativeCarrierRatePublication {
    transition: CarrierRateAuthorityTransition,
    stamp: CarrierRateAuthorityStamp,
    snapshot: Option<CarrierRateAuthoritySnapshot>,
}

/// One live, scope-validated authority read for a scheduling decision.
///
/// This value is projected directly from the serialized coordinator. It is
/// neither a second cache nor permission to mutate Product ownership without
/// revalidating its stamp at the eventual commit boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "native authority decisions must retain their exact stamp"]
pub(in crate::runtime) struct NativeCarrierRateDecisionSnapshot {
    snapshot: CarrierRateAuthoritySnapshot,
}

/// One activation-coherent native scheduling bundle.
///
/// `rate_bps` and `basis` come only from the central authority. RTT, window,
/// flight, pacing, and application-limited state come from the exact active
/// Quinn `PathData` that owns the matching `(A, I)`. Shared ACK/loss telemetry
/// is intentionally absent because it can span same-lineage activations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "native scheduling shape must retain its exact authority stamp"]
pub(in crate::runtime) struct NativeCarrierSchedulingShapeSnapshot {
    decision: NativeCarrierRateDecisionSnapshot,
    directional_timing: Option<DirectionalTiming>,
    srtt: Duration,
    rttvar: Duration,
    congestion_window: u64,
    bytes_in_flight: u64,
    current_mtu: u16,
    pacing_rate_bps: Option<u64>,
    app_limited: bool,
}

impl NativeCarrierRateDecisionSnapshot {
    pub(in crate::runtime) fn stamp(self) -> CarrierRateAuthorityStamp {
        self.snapshot.stamp()
    }

    pub(in crate::runtime) fn service_rate(self) -> DirectionalServiceRate {
        self.snapshot
            .service_rate()
            .expect("runtime decision snapshots are restricted to Native mode")
    }

    pub(in crate::runtime) fn finite_rate_bps(self) -> Option<u64> {
        self.snapshot.finite_rate_bps()
    }

    pub(in crate::runtime) fn basis(self) -> CarrierRateAuthorityBasis {
        self.snapshot.basis()
    }
}

impl NativeCarrierSchedulingShapeSnapshot {
    pub(in crate::runtime) fn decision(self) -> NativeCarrierRateDecisionSnapshot {
        self.decision
    }

    pub(in crate::runtime) fn stamp(self) -> CarrierRateAuthorityStamp {
        self.decision.stamp()
    }

    pub(in crate::runtime) fn service_rate(self) -> DirectionalServiceRate {
        self.decision.service_rate()
    }

    pub(in crate::runtime) fn finite_rate_bps(self) -> Option<u64> {
        self.decision.finite_rate_bps()
    }

    pub(in crate::runtime) fn basis(self) -> CarrierRateAuthorityBasis {
        self.decision.basis()
    }

    pub(in crate::runtime) fn directional_timing(self) -> Option<DirectionalTiming> {
        self.directional_timing
    }

    pub(in crate::runtime) fn srtt(self) -> Duration {
        self.srtt
    }

    pub(in crate::runtime) fn rttvar(self) -> Duration {
        self.rttvar
    }

    pub(in crate::runtime) fn congestion_window(self) -> u64 {
        self.congestion_window
    }

    pub(in crate::runtime) fn bytes_in_flight(self) -> u64 {
        self.bytes_in_flight
    }

    pub(in crate::runtime) fn current_mtu(self) -> u16 {
        self.current_mtu
    }

    pub(in crate::runtime) fn pacing_rate_bps(self) -> Option<u64> {
        self.pacing_rate_bps
    }

    pub(in crate::runtime) fn app_limited(self) -> bool {
        self.app_limited
    }
}

impl NativeCarrierRatePublication {
    pub(in crate::runtime) fn transition(self) -> CarrierRateAuthorityTransition {
        self.transition
    }

    pub(in crate::runtime) fn snapshot(self) -> Option<CarrierRateAuthoritySnapshot> {
        self.snapshot
    }

    pub(in crate::runtime) fn stamp(self) -> CarrierRateAuthorityStamp {
        self.stamp
    }
}

/// One connection-local native scheduling-rate authority.
///
/// Lock order is always Quinn activation fence, then coordinator mutex. A
/// transport source read happens before taking the fence and is never repeated
/// while the fence is held.
#[derive(Debug)]
pub(in crate::runtime) struct NativeCarrierRateAuthorityHandle {
    coordinator: Mutex<NativeCarrierRateAuthority>,
    transport: NativeCarrierRateTransportSource,
    /// Latest activation-coherent native shape. This cache is deliberately
    /// rate-free: its stamp binds it to the one central authority value.
    scheduling_shape: Mutex<NativeCarrierSchedulingShapeCache>,
    accepted_change: watch::Sender<CarrierRateAuthorityStamp>,
}

/// Cloneable owner of the one binding cell shared by all clones of a physical
/// QUIC connection.
#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct NativeCarrierRateAuthorityBinding {
    inner: Arc<OnceLock<Arc<NativeCarrierRateAuthorityHandle>>>,
}

#[derive(Debug, Clone, Copy)]
struct CoherentNativeCarrierSource {
    activation: u64,
    controller: u64,
    operational_rate_bps: Option<u128>,
}

#[derive(Debug, Clone, Copy)]
struct CoherentNativeCarrierShape {
    source: CoherentNativeCarrierSource,
    srtt: Duration,
    rttvar: Duration,
    congestion_window: u64,
    bytes_in_flight: u64,
    current_mtu: u16,
    pacing_rate_bps: Option<u64>,
    app_limited: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ValidatedNativeCarrierShape {
    stamp: CarrierRateAuthorityStamp,
    directional_timing: Option<DirectionalTiming>,
    srtt: Duration,
    rttvar: Duration,
    congestion_window: u64,
    bytes_in_flight: u64,
    current_mtu: u16,
    pacing_rate_bps: Option<u64>,
    app_limited: bool,
}

#[derive(Debug)]
struct NativeCarrierSchedulingShapeCache {
    current: Option<ValidatedNativeCarrierShape>,
    timing_epochs: DirectionalTimingEpochIssuer,
}

impl NativeCarrierSchedulingShapeCache {
    fn new(stamp: CarrierRateAuthorityStamp, shape: CoherentNativeCarrierShape) -> Self {
        let mut cache = Self {
            current: None,
            timing_epochs: DirectionalTimingEpochIssuer::default(),
        };
        cache.replace(stamp, shape);
        cache
    }

    fn replace(
        &mut self,
        stamp: CarrierRateAuthorityStamp,
        shape: CoherentNativeCarrierShape,
    ) -> ValidatedNativeCarrierShape {
        let prior = self.current.filter(|current| {
            current.stamp.scope() == stamp.scope()
                && current.stamp.native_activation() == stamp.native_activation()
        });
        let prior_timing = prior.and_then(|current| current.directional_timing);
        let directional_timing = if shape.srtt.is_zero()
            || prior
                .is_some_and(|current| current.srtt == shape.srtt && current.rttvar == shape.rttvar)
        {
            prior_timing
        } else {
            self.timing_epochs
                .issue()
                .map(|epoch| {
                    DirectionalTiming::checked_from_parts(
                        DirectionalRoundTripTime::from_duration(stamp.scope(), epoch, shape.srtt),
                        Some(DirectionalTimingVariation::from_duration(
                            stamp.scope(),
                            epoch,
                            shape.rttvar,
                        )),
                    )
                    .expect("one validated native shape has one exact timing scope and epoch")
                })
                .or(prior_timing)
        };
        let replacement =
            ValidatedNativeCarrierShape::from_coherent(stamp, shape, directional_timing);
        self.current = Some(replacement);
        replacement
    }
}

impl ValidatedNativeCarrierShape {
    fn from_coherent(
        stamp: CarrierRateAuthorityStamp,
        shape: CoherentNativeCarrierShape,
        directional_timing: Option<DirectionalTiming>,
    ) -> Self {
        Self {
            stamp,
            directional_timing,
            srtt: shape.srtt,
            rttvar: shape.rttvar,
            congestion_window: shape.congestion_window,
            bytes_in_flight: shape.bytes_in_flight,
            current_mtu: shape.current_mtu,
            pacing_rate_bps: shape.pacing_rate_bps,
            app_limited: shape.app_limited,
        }
    }

    fn with_decision(
        self,
        decision: NativeCarrierRateDecisionSnapshot,
    ) -> NativeCarrierSchedulingShapeSnapshot {
        debug_assert_eq!(self.stamp, decision.stamp());
        NativeCarrierSchedulingShapeSnapshot {
            decision,
            directional_timing: self.directional_timing,
            srtt: self.srtt,
            rttvar: self.rttvar,
            congestion_window: self.congestion_window,
            bytes_in_flight: self.bytes_in_flight,
            current_mtu: self.current_mtu,
            pacing_rate_bps: self.pacing_rate_bps,
            app_limited: self.app_limited,
        }
    }
}

/// Opaque capability coupling one physical connection's coherent source and
/// its exact activation fence. The owner token never leaves this module.
#[derive(Debug)]
struct NativeCarrierRateTransportSource {
    owner: Arc<()>,
    connection: Option<crate::transport::quic::Connection>,
    activation_fence: quinn::congestion::ControllerActivationFence,
    #[cfg(test)]
    test_current_activation: Mutex<u64>,
}

#[derive(Debug, Clone, Copy)]
enum NativeCarrierTransportFenceState {
    Uninitialized,
    Live(u64),
    Terminal(quinn::congestion::ControllerActivationTerminal),
}

#[derive(Debug)]
struct BoundCoherentNativeCarrierSource {
    owner: Arc<()>,
    source: CoherentNativeCarrierSource,
}

#[derive(Debug)]
struct BoundCoherentNativeCarrierShape {
    owner: Arc<()>,
    shape: CoherentNativeCarrierShape,
}

impl NativeCarrierRateTransportSource {
    fn from_connection(connection: crate::transport::quic::Connection) -> Self {
        let activation_fence = connection.native_controller_activation_fence();
        Self {
            owner: Arc::new(()),
            connection: Some(connection),
            activation_fence,
            #[cfg(test)]
            test_current_activation: Mutex::new(0),
        }
    }

    fn capture(
        &self,
    ) -> Result<BoundCoherentNativeCarrierSource, NativeCarrierRateAuthorityRuntimeError> {
        let connection = self
            .connection
            .as_ref()
            .ok_or(NativeCarrierRateAuthorityRuntimeError::TransportSourceUnavailable)?;
        Ok(BoundCoherentNativeCarrierSource {
            owner: self.owner.clone(),
            source: CoherentNativeCarrierSource::from_transport(
                connection.native_controller_authority_snapshot(),
            )?,
        })
    }

    fn capture_shape(
        &self,
    ) -> Result<BoundCoherentNativeCarrierShape, NativeCarrierRateAuthorityRuntimeError> {
        let connection = self
            .connection
            .as_ref()
            .ok_or(NativeCarrierRateAuthorityRuntimeError::TransportSourceUnavailable)?;
        Ok(BoundCoherentNativeCarrierShape {
            owner: self.owner.clone(),
            shape: CoherentNativeCarrierShape::from_transport(
                connection.native_controller_shape_snapshot(),
            )?,
        })
    }

    fn require_owner(
        &self,
        source: &BoundCoherentNativeCarrierSource,
    ) -> Result<(), NativeCarrierRateAuthorityRuntimeError> {
        if Arc::ptr_eq(&self.owner, &source.owner) {
            Ok(())
        } else {
            Err(NativeCarrierRateAuthorityRuntimeError::TransportSourceBindingMismatch)
        }
    }

    fn require_shape_owner(
        &self,
        shape: &BoundCoherentNativeCarrierShape,
    ) -> Result<(), NativeCarrierRateAuthorityRuntimeError> {
        if Arc::ptr_eq(&self.owner, &shape.owner) {
            Ok(())
        } else {
            Err(NativeCarrierRateAuthorityRuntimeError::TransportSourceBindingMismatch)
        }
    }

    fn with_current<T>(
        &self,
        inspect: impl FnOnce(NativeCarrierTransportFenceState) -> T,
    ) -> Result<T, NativeCarrierRateAuthorityRuntimeError> {
        #[cfg(test)]
        if self.connection.is_none() {
            let activation = *self
                .test_current_activation
                .lock()
                .map_err(|_| NativeCarrierRateAuthorityRuntimeError::ActivationFencePoisoned)?;
            return Ok(inspect(if activation == 0 {
                NativeCarrierTransportFenceState::Uninitialized
            } else {
                NativeCarrierTransportFenceState::Live(activation)
            }));
        }
        self.activation_fence
            .with_current(|state| {
                inspect(match state {
                    quinn::congestion::ControllerActivationState::Uninitialized => {
                        NativeCarrierTransportFenceState::Uninitialized
                    }
                    quinn::congestion::ControllerActivationState::Live(current) => {
                        NativeCarrierTransportFenceState::Live(current.opaque_serial())
                    }
                    quinn::congestion::ControllerActivationState::Terminal(reason) => {
                        NativeCarrierTransportFenceState::Terminal(reason)
                    }
                })
            })
            .map_err(|_| NativeCarrierRateAuthorityRuntimeError::ActivationFencePoisoned)
    }

    #[cfg(test)]
    fn for_test(current_activation: u64) -> Self {
        Self {
            owner: Arc::new(()),
            connection: None,
            activation_fence: quinn::congestion::ControllerActivationFence::new(),
            test_current_activation: Mutex::new(current_activation),
        }
    }

    #[cfg(test)]
    fn set_current_activation_for_test(
        &self,
        activation: u64,
    ) -> Result<(), NativeCarrierRateAuthorityRuntimeError> {
        *self
            .test_current_activation
            .lock()
            .map_err(|_| NativeCarrierRateAuthorityRuntimeError::ActivationFencePoisoned)? =
            activation;
        Ok(())
    }

    #[cfg(test)]
    fn bind_for_test(
        &self,
        source: CoherentNativeCarrierSource,
    ) -> BoundCoherentNativeCarrierSource {
        BoundCoherentNativeCarrierSource {
            owner: self.owner.clone(),
            source,
        }
    }

    #[cfg(test)]
    fn bind_shape_for_test(
        &self,
        shape: CoherentNativeCarrierShape,
    ) -> BoundCoherentNativeCarrierShape {
        BoundCoherentNativeCarrierShape {
            owner: self.owner.clone(),
            shape,
        }
    }
}

impl CoherentNativeCarrierSource {
    fn from_transport(
        source: NativeControllerAuthoritySnapshot,
    ) -> Result<Self, NativeCarrierRateAuthorityRuntimeError> {
        let operational_rate_bps = match (source.kind(), source.operational_rate_bps()) {
            (NativeControllerObservationKind::Absent, None) => None,
            (NativeControllerObservationKind::Valid, Some(rate)) => Some(u128::from(rate.get())),
            _ => return Err(NativeCarrierRateAuthorityRuntimeError::MalformedTransportSource),
        };
        Ok(Self {
            activation: source.activation().opaque_serial(),
            controller: source.controller().opaque_serial(),
            // The transport snapshot is already in bits/s. Do not multiply it
            // again at this adapter boundary.
            operational_rate_bps,
        })
    }

    fn checked_source(
        self,
        scope: CarrierRateAuthorityScope,
    ) -> Result<NativeCarrierRateSourceSnapshot, NativeCarrierRateAuthorityRuntimeError> {
        NativeCarrierRateSourceSnapshot::checked_from_bits_per_second(
            scope,
            self.activation,
            self.controller,
            self.operational_rate_bps,
        )
        .map_err(Into::into)
    }

    #[cfg(test)]
    // This fixture constructor names every native ownership-envelope field so
    // tests cannot silently inherit production defaults.
    #[allow(clippy::too_many_arguments)]
    fn checked_for_test(
        activation: u64,
        controller: u64,
        operational_rate_bps: Option<u128>,
    ) -> Result<Self, NativeCarrierRateAuthorityRuntimeError> {
        // Exercise the same checked facade constructor before allowing a fake
        // coherent source into a unit test.
        let scope = CarrierRateAuthorityScope::new(
            crate::model::path::CarrierPathInstanceId::from_raw(1),
            crate::protocol::PathMetricDirection::ClientToServer,
        );
        let _ = NativeCarrierRateSourceSnapshot::checked_from_bits_per_second(
            scope,
            activation,
            controller,
            operational_rate_bps,
        )?;
        Ok(Self {
            activation,
            controller,
            operational_rate_bps,
        })
    }
}

impl CoherentNativeCarrierShape {
    fn from_transport(
        shape: NativeControllerShapeSnapshot,
    ) -> Result<Self, NativeCarrierRateAuthorityRuntimeError> {
        Ok(Self {
            source: CoherentNativeCarrierSource {
                activation: shape.activation().opaque_serial(),
                controller: shape.controller().opaque_serial(),
                operational_rate_bps: shape
                    .operational_rate_bps()
                    .map(|rate| u128::from(rate.get())),
            },
            srtt: shape.smoothed_rtt(),
            rttvar: shape.rtt_variance(),
            congestion_window: shape.congestion_window(),
            bytes_in_flight: shape.bytes_in_flight(),
            current_mtu: shape.current_mtu(),
            pacing_rate_bps: shape.pacing_rate_bps().map(std::num::NonZeroU64::get),
            app_limited: shape.app_limited(),
        })
    }

    #[cfg(test)]
    fn checked_for_test(
        activation: u64,
        controller: u64,
        operational_rate_bps: Option<u128>,
        srtt: Duration,
        rttvar: Duration,
        congestion_window: u64,
        bytes_in_flight: u64,
        current_mtu: u16,
        pacing_rate_bps: Option<u64>,
        app_limited: bool,
    ) -> Result<Self, NativeCarrierRateAuthorityRuntimeError> {
        if pacing_rate_bps == Some(0) {
            return Err(NativeCarrierRateAuthorityRuntimeError::MalformedTransportSource);
        }
        Ok(Self {
            source: CoherentNativeCarrierSource::checked_for_test(
                activation,
                controller,
                operational_rate_bps,
            )?,
            srtt,
            rttvar,
            congestion_window,
            bytes_in_flight,
            current_mtu,
            pacing_rate_bps,
            app_limited,
        })
    }
}

impl NativeCarrierRateAuthorityHandle {
    /// Construct only from a coherent source captured after the owning Quinn
    /// connection has released its state lock.
    pub(in crate::runtime) fn construct(
        scope: CarrierRateAuthorityScope,
        startup_hint: RateHint,
        connection: crate::transport::quic::Connection,
    ) -> Result<Arc<Self>, NativeCarrierRateAuthorityRuntimeError> {
        let startup = DirectionalServiceRate::from_startup_hint(scope, startup_hint)?;
        let transport = NativeCarrierRateTransportSource::from_connection(connection);
        let bound_shape = transport.capture_shape()?;
        transport.require_shape_owner(&bound_shape)?;
        let shape = bound_shape.shape;
        let source = shape.source;
        let checked_source = source.checked_source(scope)?;
        let (coordinator, scheduling_shape) = transport.with_current(|state| match state {
            NativeCarrierTransportFenceState::Live(current) if current == source.activation => {
                let coordinator = NativeCarrierRateAuthority::new(scope, startup, checked_source)
                    .map_err(NativeCarrierRateAuthorityRuntimeError::from)?;
                let scheduling_shape =
                    NativeCarrierSchedulingShapeCache::new(coordinator.stamp(), shape);
                Ok((coordinator, scheduling_shape))
            }
            NativeCarrierTransportFenceState::Live(_) => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged)
            }
            NativeCarrierTransportFenceState::Uninitialized => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportUninitialized)
            }
            NativeCarrierTransportFenceState::Terminal(reason) => Err(
                NativeCarrierRateAuthorityRuntimeError::TransportTerminal(reason),
            ),
        })??;
        let (accepted_change, _) = watch::channel(coordinator.stamp());
        Ok(Arc::new(Self {
            coordinator: Mutex::new(coordinator),
            transport,
            scheduling_shape: Mutex::new(scheduling_shape),
            accepted_change,
        }))
    }

    #[cfg(test)]
    pub(in crate::runtime) fn snapshot(
        &self,
    ) -> Result<Option<CarrierRateAuthoritySnapshot>, NativeCarrierRateAuthorityRuntimeError> {
        self.coordinator
            .lock()
            .map(|coordinator| coordinator.snapshot())
            .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)
    }

    /// Read the one central live snapshot and prove that it belongs to the
    /// exact carrier-direction scope expected by this decision consumer.
    #[cfg(test)]
    pub(in crate::runtime) fn decision_snapshot(
        &self,
        requested: CarrierRateAuthorityScope,
    ) -> Result<NativeCarrierRateDecisionSnapshot, NativeCarrierRateAuthorityRuntimeError> {
        self.transport.with_current(|state| match state {
            NativeCarrierTransportFenceState::Live(current_activation) => {
                let current =
                    NativeCarrierTransportCurrent::checked_from_raw(requested, current_activation)?;
                let coordinator = self
                    .coordinator
                    .lock()
                    .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
                let existing = coordinator.stamp().scope();
                if existing != requested {
                    return Err(
                        NativeCarrierRateAuthorityRuntimeError::BindingScopeMismatch {
                            existing,
                            requested,
                        },
                    );
                }
                let snapshot = coordinator.snapshot().ok_or(
                    NativeCarrierRateAuthorityRuntimeError::Authority(
                        CarrierRateAuthorityError::Terminal,
                    ),
                )?;
                if snapshot.mode() != CarrierRateAuthorityMode::NativeOperational {
                    return Err(NativeCarrierRateAuthorityRuntimeError::Authority(
                        CarrierRateAuthorityError::WrongMode,
                    ));
                }
                coordinator.commit_if_current(snapshot.stamp(), current, || ())?;
                Ok(NativeCarrierRateDecisionSnapshot { snapshot })
            }
            NativeCarrierTransportFenceState::Uninitialized => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportUninitialized)
            }
            NativeCarrierTransportFenceState::Terminal(reason) => Err(
                NativeCarrierRateAuthorityRuntimeError::TransportTerminal(reason),
            ),
        })?
    }

    /// Cadence-writer operation: capture one active Quinn shape, bind it to the
    /// exact current central Native authority, and replace the rate-free shape
    /// cache under `fence -> coordinator -> shape` lock order.
    ///
    /// Quinn's connection state is read and released first. The later
    /// activation-fence -> coordinator transaction rejects any intervening
    /// install/rollback and any `(scope, A, I)` mismatch. The central rate may
    /// legitimately lag a newer same-controller transport observation during
    /// its bounded publication interval; the returned rate is nevertheless
    /// always the one central `G` value, never the raw shape value.
    pub(in crate::runtime) fn refresh_scheduling_shape(
        &self,
        requested: CarrierRateAuthorityScope,
    ) -> Result<NativeCarrierSchedulingShapeSnapshot, NativeCarrierRateAuthorityRuntimeError> {
        let shape = self.transport.capture_shape()?;
        self.validate_scheduling_shape(requested, shape)
    }

    fn validate_scheduling_shape(
        &self,
        requested: CarrierRateAuthorityScope,
        bound: BoundCoherentNativeCarrierShape,
    ) -> Result<NativeCarrierSchedulingShapeSnapshot, NativeCarrierRateAuthorityRuntimeError> {
        self.transport.require_shape_owner(&bound)?;
        let shape = bound.shape;
        let checked_source = shape.source.checked_source(requested)?;
        self.transport.with_current(|state| match state {
            NativeCarrierTransportFenceState::Live(current_activation)
                if current_activation == shape.source.activation =>
            {
                let current =
                    NativeCarrierTransportCurrent::checked_from_raw(requested, current_activation)?;
                let coordinator = self
                    .coordinator
                    .lock()
                    .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
                let existing = coordinator.stamp().scope();
                if existing != requested {
                    return Err(
                        NativeCarrierRateAuthorityRuntimeError::BindingScopeMismatch {
                            existing,
                            requested,
                        },
                    );
                }
                let snapshot = coordinator.snapshot().ok_or(
                    NativeCarrierRateAuthorityRuntimeError::Authority(
                        CarrierRateAuthorityError::Terminal,
                    ),
                )?;
                if !snapshot.matches_native_source(&checked_source) {
                    return Err(NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged);
                }
                coordinator
                    .commit_if_current(snapshot.stamp(), current, || {
                        let validated = self
                            .scheduling_shape
                            .lock()
                            .map_err(|_| {
                                NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned
                            })?
                            .replace(snapshot.stamp(), shape);
                        Ok(validated.with_decision(NativeCarrierRateDecisionSnapshot { snapshot }))
                    })
                    .map_err(NativeCarrierRateAuthorityRuntimeError::from)?
            }
            NativeCarrierTransportFenceState::Live(_) => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged)
            }
            NativeCarrierTransportFenceState::Uninitialized => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportUninitialized)
            }
            NativeCarrierTransportFenceState::Terminal(reason) => Err(
                NativeCarrierRateAuthorityRuntimeError::TransportTerminal(reason),
            ),
        })?
    }

    /// Scheduler-reader operation. It never clones the Quinn controller.
    /// Return the latest cadence shape only while its complete central stamp
    /// and the live transport activation still match.
    pub(in crate::runtime) fn scheduling_shape_snapshot(
        &self,
        requested: CarrierRateAuthorityScope,
    ) -> Result<NativeCarrierSchedulingShapeSnapshot, NativeCarrierRateAuthorityRuntimeError> {
        self.transport.with_current(|state| match state {
            NativeCarrierTransportFenceState::Live(current_activation) => {
                let current =
                    NativeCarrierTransportCurrent::checked_from_raw(requested, current_activation)?;
                let coordinator = self
                    .coordinator
                    .lock()
                    .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
                let existing = coordinator.stamp().scope();
                if existing != requested {
                    return Err(
                        NativeCarrierRateAuthorityRuntimeError::BindingScopeMismatch {
                            existing,
                            requested,
                        },
                    );
                }
                let snapshot = coordinator.snapshot().ok_or(
                    NativeCarrierRateAuthorityRuntimeError::Authority(
                        CarrierRateAuthorityError::Terminal,
                    ),
                )?;
                if snapshot.mode() != CarrierRateAuthorityMode::NativeOperational {
                    return Err(NativeCarrierRateAuthorityRuntimeError::Authority(
                        CarrierRateAuthorityError::WrongMode,
                    ));
                }
                coordinator
                    .commit_if_current(snapshot.stamp(), current, || {
                        let shape = self
                            .scheduling_shape
                            .lock()
                            .map_err(|_| {
                                NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned
                            })?
                            .current
                            .ok_or(
                                NativeCarrierRateAuthorityRuntimeError::SchedulingShapeUnavailable,
                            )?;
                        if shape.stamp != snapshot.stamp() {
                            return Err(
                                NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged,
                            );
                        }
                        Ok(shape.with_decision(NativeCarrierRateDecisionSnapshot { snapshot }))
                    })
                    .map_err(NativeCarrierRateAuthorityRuntimeError::from)?
            }
            NativeCarrierTransportFenceState::Uninitialized => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportUninitialized)
            }
            NativeCarrierTransportFenceState::Terminal(reason) => Err(
                NativeCarrierRateAuthorityRuntimeError::TransportTerminal(reason),
            ),
        })?
    }

    pub(in crate::runtime) fn stamp(
        &self,
    ) -> Result<CarrierRateAuthorityStamp, NativeCarrierRateAuthorityRuntimeError> {
        self.coordinator
            .lock()
            .map(|coordinator| coordinator.stamp())
            .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)
    }

    /// Durable cursor for accepted central authority changes.
    ///
    /// The stamp is only a wake/cursor. After observing it, consumers must
    /// reread the authority through its scope/fence-validated decision API.
    pub(in crate::runtime) fn accepted_change_cursor(
        &self,
    ) -> watch::Receiver<CarrierRateAuthorityStamp> {
        self.accepted_change.subscribe()
    }

    /// Capture expected G, read one coherent transport source without either
    /// runtime lock held, then compare/apply under fence -> coordinator order.
    pub(in crate::runtime) fn refresh(
        &self,
    ) -> Result<NativeCarrierRatePublication, NativeCarrierRateAuthorityRuntimeError> {
        let (scope, ticket) = {
            let coordinator = self
                .coordinator
                .lock()
                .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
            (
                coordinator.stamp().scope(),
                coordinator.capture_publication_ticket()?,
            )
        };
        let source = self.transport.capture()?;
        self.refresh_checked(scope, ticket, source)
    }

    fn refresh_checked(
        &self,
        scope: CarrierRateAuthorityScope,
        ticket: NativeCarrierRatePublicationTicket,
        source: BoundCoherentNativeCarrierSource,
    ) -> Result<NativeCarrierRatePublication, NativeCarrierRateAuthorityRuntimeError> {
        self.transport.require_owner(&source)?;
        let source = source.source;
        let checked_source = source.checked_source(scope)?;
        let publication = self.transport.with_current(|state| match state {
            NativeCarrierTransportFenceState::Live(current) => {
                if current != source.activation {
                    return Err(NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged);
                }
                self.compare_apply_locked(scope, ticket, checked_source, current)
            }
            NativeCarrierTransportFenceState::Uninitialized => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportUninitialized)
            }
            NativeCarrierTransportFenceState::Terminal(
                quinn::congestion::ControllerActivationTerminal::Exhausted,
            ) => self.terminate_exhaustion_locked(scope, ticket),
            NativeCarrierTransportFenceState::Terminal(reason) => Err(
                NativeCarrierRateAuthorityRuntimeError::TransportTerminal(reason),
            ),
        })??;
        // The transport fence and coordinator guard have both been released.
        // Waking here cannot invert fence -> coordinator ordering, and the
        // cursor carries the accepted central stamp rather than another rate.
        self.publish_accepted_change(publication);
        Ok(publication)
    }

    fn compare_apply_locked(
        &self,
        scope: CarrierRateAuthorityScope,
        ticket: NativeCarrierRatePublicationTicket,
        source: NativeCarrierRateSourceSnapshot,
        current_activation: u64,
    ) -> Result<NativeCarrierRatePublication, NativeCarrierRateAuthorityRuntimeError> {
        let current = NativeCarrierTransportCurrent::checked_from_raw(scope, current_activation)?;
        let mut coordinator = self
            .coordinator
            .lock()
            .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
        let transition = coordinator.compare_apply(ticket, source, current)?;
        let snapshot = coordinator.snapshot();
        Ok(NativeCarrierRatePublication {
            transition,
            stamp: coordinator.stamp(),
            snapshot,
        })
    }

    fn terminate_exhaustion_locked(
        &self,
        scope: CarrierRateAuthorityScope,
        ticket: NativeCarrierRatePublicationTicket,
    ) -> Result<NativeCarrierRatePublication, NativeCarrierRateAuthorityRuntimeError> {
        // Reaching Quinn's exact Exhausted fence state proves that its checked
        // sequence consumed the final live serial. A source captured before
        // taking this fence may legitimately lag at MAX-2, so it must not be
        // used as the exhaustion proof.
        let exhaustion =
            NativeCarrierTransportExhaustion::checked_after_last_live(scope, u64::MAX - 1)?;
        let mut coordinator = self
            .coordinator
            .lock()
            .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
        let transition = coordinator.terminate_transport_exhaustion(ticket, exhaustion)?;
        Ok(NativeCarrierRatePublication {
            transition,
            stamp: coordinator.stamp(),
            snapshot: coordinator.snapshot(),
        })
    }

    fn publish_accepted_change(&self, publication: NativeCarrierRatePublication) {
        if matches!(
            publication.transition(),
            CarrierRateAuthorityTransition::Applied | CarrierRateAuthorityTransition::Terminal
        ) {
            // `send_replace` retains the latest accepted stamp even with no
            // active receiver, making late subscription and coalescing safe.
            self.accepted_change.send_replace(publication.stamp());
        }
    }

    /// Run an ownership-transfer closure only while the exact transport A and
    /// the complete central `(scope, A, I, G)` stamp remain current.
    ///
    /// The closure executes with both locks held. It must not call back into
    /// Quinn, whose connection lock precedes this activation fence.
    pub(in crate::runtime) fn commit_if_current<R>(
        &self,
        expected: CarrierRateAuthorityStamp,
        transfer_ownership: impl FnOnce() -> R,
    ) -> Result<R, NativeCarrierRateAuthorityRuntimeError> {
        self.transport.with_current(|state| match state {
            NativeCarrierTransportFenceState::Live(current) => {
                self.commit_under_live_activation(expected, current, transfer_ownership)
            }
            NativeCarrierTransportFenceState::Uninitialized => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportUninitialized)
            }
            NativeCarrierTransportFenceState::Terminal(reason) => Err(
                NativeCarrierRateAuthorityRuntimeError::TransportTerminal(reason),
            ),
        })?
    }

    /// Run a Native ownership transfer with the current full scheduling shape
    /// held under `activation fence -> coordinator -> shape`. A central stamp
    /// does not revise for same-controller RTT/window updates, so callers that
    /// publish bytes must consume this shape rather than merely rechecking the
    /// stamp captured during advisory planning.
    pub(in crate::runtime) fn commit_with_current_scheduling_shape<R>(
        &self,
        expected: CarrierRateAuthorityStamp,
        transfer_ownership: impl FnOnce(NativeCarrierSchedulingShapeSnapshot) -> R,
    ) -> Result<R, NativeCarrierRateAuthorityRuntimeError> {
        self.transport.with_current(|state| match state {
            NativeCarrierTransportFenceState::Live(current_activation) => {
                let current = NativeCarrierTransportCurrent::checked_from_raw(
                    expected.scope(),
                    current_activation,
                )?;
                let coordinator = self
                    .coordinator
                    .lock()
                    .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
                let snapshot = coordinator.snapshot().ok_or(
                    NativeCarrierRateAuthorityRuntimeError::Authority(
                        CarrierRateAuthorityError::Terminal,
                    ),
                )?;
                if snapshot.mode() != CarrierRateAuthorityMode::NativeOperational {
                    return Err(NativeCarrierRateAuthorityRuntimeError::Authority(
                        CarrierRateAuthorityError::WrongMode,
                    ));
                }
                coordinator
                    .commit_if_current(expected, current, || {
                        let shape = self
                            .scheduling_shape
                            .lock()
                            .map_err(|_| {
                                NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned
                            })?
                            .current
                            .ok_or(
                                NativeCarrierRateAuthorityRuntimeError::SchedulingShapeUnavailable,
                            )?;
                        if shape.stamp != snapshot.stamp() {
                            return Err(
                                NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged,
                            );
                        }
                        Ok(transfer_ownership(shape.with_decision(
                            NativeCarrierRateDecisionSnapshot { snapshot },
                        )))
                    })
                    .map_err(NativeCarrierRateAuthorityRuntimeError::from)?
            }
            NativeCarrierTransportFenceState::Uninitialized => {
                Err(NativeCarrierRateAuthorityRuntimeError::TransportUninitialized)
            }
            NativeCarrierTransportFenceState::Terminal(reason) => Err(
                NativeCarrierRateAuthorityRuntimeError::TransportTerminal(reason),
            ),
        })?
    }

    fn commit_under_live_activation<R>(
        &self,
        expected: CarrierRateAuthorityStamp,
        current_activation: u64,
        transfer_ownership: impl FnOnce() -> R,
    ) -> Result<R, NativeCarrierRateAuthorityRuntimeError> {
        let current =
            NativeCarrierTransportCurrent::checked_from_raw(expected.scope(), current_activation)?;
        let coordinator = self
            .coordinator
            .lock()
            .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
        coordinator
            .commit_if_current(expected, current, transfer_ownership)
            .map_err(Into::into)
    }

    #[cfg(test)]
    fn new_for_test(
        scope: CarrierRateAuthorityScope,
        startup_hint: RateHint,
        initial: CoherentNativeCarrierSource,
    ) -> Result<Arc<Self>, NativeCarrierRateAuthorityRuntimeError> {
        let startup = DirectionalServiceRate::from_startup_hint(scope, startup_hint)?;
        let transport = NativeCarrierRateTransportSource::for_test(initial.activation);
        let coordinator =
            NativeCarrierRateAuthority::new(scope, startup, initial.checked_source(scope)?)?;
        let scheduling_shape = NativeCarrierSchedulingShapeCache::new(
            coordinator.stamp(),
            CoherentNativeCarrierShape {
                source: initial,
                srtt: Duration::ZERO,
                rttvar: Duration::ZERO,
                congestion_window: 0,
                bytes_in_flight: 0,
                current_mtu: 1200,
                pacing_rate_bps: None,
                app_limited: true,
            },
        );
        let (accepted_change, _) = watch::channel(coordinator.stamp());
        Ok(Arc::new(Self {
            coordinator: Mutex::new(coordinator),
            transport,
            scheduling_shape: Mutex::new(scheduling_shape),
            accepted_change,
        }))
    }

    #[cfg(test)]
    pub(in crate::runtime) fn from_observation_for_test(
        scope: CarrierRateAuthorityScope,
        startup_prior_bps: u64,
        activation: u64,
        controller: u64,
        operational_rate_bps: Option<u128>,
    ) -> Result<Arc<Self>, NativeCarrierRateAuthorityRuntimeError> {
        Self::new_for_test(
            scope,
            RateHint::BitsPerSecond(startup_prior_bps),
            CoherentNativeCarrierSource::checked_for_test(
                activation,
                controller,
                operational_rate_bps,
            )?,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn from_startup_hint_for_test(
        scope: CarrierRateAuthorityScope,
        startup_hint: RateHint,
        activation: u64,
        controller: u64,
        operational_rate_bps: Option<u128>,
    ) -> Result<Arc<Self>, NativeCarrierRateAuthorityRuntimeError> {
        Self::new_for_test(
            scope,
            startup_hint,
            CoherentNativeCarrierSource::checked_for_test(
                activation,
                controller,
                operational_rate_bps,
            )?,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn publish_observation_for_test(
        &self,
        activation: u64,
        controller: u64,
        operational_rate_bps: Option<u128>,
    ) -> Result<NativeCarrierRatePublication, NativeCarrierRateAuthorityRuntimeError> {
        self.refresh_at_activation_for_test(
            CoherentNativeCarrierSource::checked_for_test(
                activation,
                controller,
                operational_rate_bps,
            )?,
            activation,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn advance_transport_activation_for_test(
        &self,
        activation: u64,
    ) -> Result<(), NativeCarrierRateAuthorityRuntimeError> {
        self.transport.set_current_activation_for_test(activation)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn set_next_timing_epoch_for_test(&self, next: u64) {
        self.scheduling_shape
            .lock()
            .expect("native scheduling shape test lock")
            .timing_epochs
            .set_next_for_test(next);
    }

    #[cfg(test)]
    fn scheduling_shape_for_test(
        &self,
        requested: CarrierRateAuthorityScope,
        shape: CoherentNativeCarrierShape,
    ) -> Result<NativeCarrierSchedulingShapeSnapshot, NativeCarrierRateAuthorityRuntimeError> {
        let shape = self.transport.bind_shape_for_test(shape);
        self.validate_scheduling_shape(requested, shape)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn refresh_scheduling_shape_for_test(
        &self,
        requested: CarrierRateAuthorityScope,
        activation: u64,
        controller: u64,
        operational_rate_bps: Option<u128>,
        srtt: Duration,
        rttvar: Duration,
        congestion_window: u64,
        bytes_in_flight: u64,
        current_mtu: u16,
        pacing_rate_bps: Option<u64>,
        app_limited: bool,
    ) -> Result<NativeCarrierSchedulingShapeSnapshot, NativeCarrierRateAuthorityRuntimeError> {
        self.scheduling_shape_for_test(
            requested,
            CoherentNativeCarrierShape::checked_for_test(
                activation,
                controller,
                operational_rate_bps,
                srtt,
                rttvar,
                congestion_window,
                bytes_in_flight,
                current_mtu,
                pacing_rate_bps,
                app_limited,
            )?,
        )
    }

    #[cfg(test)]
    fn refresh_at_activation_for_test(
        &self,
        source: CoherentNativeCarrierSource,
        current_activation: u64,
    ) -> Result<NativeCarrierRatePublication, NativeCarrierRateAuthorityRuntimeError> {
        self.transport
            .set_current_activation_for_test(current_activation)?;
        let source = self.transport.bind_for_test(source);
        self.refresh_bound_at_activation_for_test(source, current_activation)
    }

    #[cfg(test)]
    fn refresh_bound_at_activation_for_test(
        &self,
        source: BoundCoherentNativeCarrierSource,
        current_activation: u64,
    ) -> Result<NativeCarrierRatePublication, NativeCarrierRateAuthorityRuntimeError> {
        self.transport.require_owner(&source)?;
        let (scope, ticket) = {
            let coordinator = self
                .coordinator
                .lock()
                .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
            (
                coordinator.stamp().scope(),
                coordinator.capture_publication_ticket()?,
            )
        };
        let source = source.source;
        if source.activation != current_activation {
            return Err(NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged);
        }
        let publication = self.compare_apply_locked(
            scope,
            ticket,
            source.checked_source(scope)?,
            current_activation,
        )?;
        self.publish_accepted_change(publication);
        Ok(publication)
    }

    #[cfg(test)]
    fn bind_source_for_test(
        &self,
        source: CoherentNativeCarrierSource,
    ) -> BoundCoherentNativeCarrierSource {
        self.transport.bind_for_test(source)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn terminate_exhaustion_for_test(
        &self,
    ) -> Result<NativeCarrierRatePublication, NativeCarrierRateAuthorityRuntimeError> {
        let (scope, ticket) = {
            let coordinator = self
                .coordinator
                .lock()
                .map_err(|_| NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned)?;
            (
                coordinator.stamp().scope(),
                coordinator.capture_publication_ticket()?,
            )
        };
        let publication = self.terminate_exhaustion_locked(scope, ticket)?;
        self.publish_accepted_change(publication);
        Ok(publication)
    }
}

impl NativeCarrierRateAuthorityBinding {
    pub(in crate::runtime) fn get(&self) -> Option<Arc<NativeCarrierRateAuthorityHandle>> {
        self.inner.get().cloned()
    }

    pub(in crate::runtime) fn requested_scope_is_bound(
        &self,
        requested: CarrierRateAuthorityScope,
    ) -> Result<Option<Arc<NativeCarrierRateAuthorityHandle>>, NativeCarrierRateAuthorityRuntimeError>
    {
        let Some(existing) = self.get() else {
            return Ok(None);
        };
        let existing_scope = existing.stamp()?.scope();
        if existing_scope != requested {
            return Err(
                NativeCarrierRateAuthorityRuntimeError::BindingScopeMismatch {
                    existing: existing_scope,
                    requested,
                },
            );
        }
        Ok(Some(existing))
    }

    /// Returns `(bound handle, won initialization)`. Only the winner may spawn
    /// the one connection-level publisher task.
    pub(in crate::runtime) fn install(
        &self,
        requested: CarrierRateAuthorityScope,
        candidate: Arc<NativeCarrierRateAuthorityHandle>,
    ) -> Result<(Arc<NativeCarrierRateAuthorityHandle>, bool), NativeCarrierRateAuthorityRuntimeError>
    {
        match self.inner.set(candidate.clone()) {
            Ok(()) => Ok((candidate, true)),
            Err(_) => self
                .requested_scope_is_bound(requested)?
                .map(|existing| (existing, false))
                .ok_or(NativeCarrierRateAuthorityRuntimeError::CoordinatorPoisoned),
        }
    }
}

#[cfg(test)]
#[path = "tests_authority.rs"]
mod tests;
