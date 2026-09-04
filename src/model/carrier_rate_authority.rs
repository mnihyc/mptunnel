//! Exclusive scheduling-rate authority for one carrier direction.
//!
//! This is a pure reducer kernel. It owns no socket, task, clock, estimator,
//! or runtime source binding. One reducer has the fixed scope
//! `(carrier incarnation, original-sender direction)` and a single checked
//! revision that never resets while that scope is live. A separate transport
//! activation fence changes synchronously at every native `PathData` install
//! or restore, including a same-controller-identity clone. Native activation,
//! rate/basis changes, the one-way Native-to-Receipt switch, and Receipt term
//! publication/retirement all advance the central revision.
//!
//! The runtime must eventually wrap the private kernel seam below with one
//! serialized, opaque, activation-bound source. In particular, general
//! metrics and a caller-selected "current" stamp are not native authority.

use super::service_rate::{DirectionalServiceRate, QuinnBbr3NativeOperationalRate};
pub(crate) use super::service_rate::{
    DirectionalServiceRateScope as CarrierRateAuthorityScope, PositiveRateBps as CarrierRateBps,
};
#[cfg(test)]
use crate::transport::RateHint;
use std::num::NonZeroU64;
use std::sync::Arc;

#[cfg(test)]
#[path = "tests_carrier_rate_authority.rs"]
mod tests;

/// Failure to project a native controller value into the scheduler lattice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarrierRateConversionError {
    Zero,
    NotByteAligned,
    OutOfRange,
}

/// Checked gain-free native-controller operational bandwidth.
///
/// Construction is intentionally private to this kernel. The future runtime
/// adapter must produce it only from a coherent opaque controller observation
/// after performing one checked bytes/s-to-bits/s conversion.
fn checked_native_operational_rate(
    bits_per_second: u128,
) -> Result<CarrierRateBps, CarrierRateConversionError> {
    if !bits_per_second.is_multiple_of(8) {
        return Err(CarrierRateConversionError::NotByteAligned);
    }
    let bits_per_second =
        u64::try_from(bits_per_second).map_err(|_| CarrierRateConversionError::OutOfRange)?;
    CarrierRateBps::checked_new(bits_per_second).map_err(|_| CarrierRateConversionError::Zero)
}

/// Checked non-reused mutation identity within one carrier-direction scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CarrierRateAuthorityRevision(u64);

impl CarrierRateAuthorityRevision {
    const INITIAL: Self = Self(1);
    const TERMINAL: Self = Self(u64::MAX);

    #[cfg(test)]
    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

/// Token captured by a scheduler decision for precommit revalidation.
///
/// It is deliberately a consumer token, not permission to publish source
/// state back into the reducer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CarrierRateAuthorityStamp {
    scope: CarrierRateAuthorityScope,
    native_activation: Option<NativeTransportActivation>,
    native_controller: Option<NativeControllerIdentity>,
    revision: CarrierRateAuthorityRevision,
}

impl CarrierRateAuthorityStamp {
    pub(crate) fn scope(self) -> CarrierRateAuthorityScope {
        self.scope
    }

    pub(crate) fn native_activation(self) -> Option<NativeTransportActivation> {
        self.native_activation
    }

    #[cfg(test)]
    fn native_controller(self) -> Option<NativeControllerIdentity> {
        self.native_controller
    }

    pub(crate) fn revision(self) -> CarrierRateAuthorityRevision {
        self.revision
    }
}

/// Epoch-local source contract. Receipt cannot return to Native in this scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarrierRateAuthorityMode {
    NativeOperational,
    #[cfg(test)]
    Receipt,
}

/// Checked identity of one ReceiptMode lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct ReceiptModeGeneration(NonZeroU64);

/// Checked monotonically issued identity of one immutable ReceiptMode term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg(test)]
pub(crate) struct ReceiptTermId(NonZeroU64);

/// Full Receipt term identity. A bare term ID is never scheduling provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct ReceiptTermKey {
    generation: ReceiptModeGeneration,
    term_id: ReceiptTermId,
}

#[cfg(test)]
impl ReceiptTermKey {
    pub(crate) fn generation(self) -> ReceiptModeGeneration {
        self.generation
    }

    pub(crate) fn term_id(self) -> ReceiptTermId {
        self.term_id
    }
}

/// Exact source of the currently projected positive rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarrierRateAuthorityBasis {
    StartupPrior,
    NativeOperational,
    #[cfg(test)]
    ReceiptFallback,
    #[cfg(test)]
    ReceiptTerm(ReceiptTermKey),
}

/// Effective value carried by the legacy reducer.
///
/// Production Native mode always uses the typed directional service-rate
/// model. Receipt mode is retained only as finite legacy proof machinery and
/// cannot manufacture a numeric representation for Unlimited startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CarrierRateAuthorityValue {
    Native(DirectionalServiceRate),
    #[cfg(test)]
    ReceiptFinite(CarrierRateBps),
}

/// Immutable rate and identity read by one scheduler decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "authority snapshots must be revalidated before commit"]
pub(crate) struct CarrierRateAuthoritySnapshot {
    stamp: CarrierRateAuthorityStamp,
    mode: CarrierRateAuthorityMode,
    basis: CarrierRateAuthorityBasis,
    value: CarrierRateAuthorityValue,
}

impl CarrierRateAuthoritySnapshot {
    pub(crate) fn stamp(self) -> CarrierRateAuthorityStamp {
        self.stamp
    }

    pub(crate) fn mode(self) -> CarrierRateAuthorityMode {
        self.mode
    }

    pub(crate) fn basis(self) -> CarrierRateAuthorityBasis {
        self.basis
    }

    /// Returns the typed service rate for production Native mode.
    ///
    /// Receipt is intentionally absent from this semantic model: it is kept
    /// only for the finite legacy reducer tests below.
    pub(crate) fn service_rate(self) -> Option<DirectionalServiceRate> {
        match self.value {
            CarrierRateAuthorityValue::Native(rate) => Some(rate),
            #[cfg(test)]
            CarrierRateAuthorityValue::ReceiptFinite(_) => None,
        }
    }

    /// Returns a finite numeric rate when the selected semantic value is
    /// finite. Unlimited startup remains `None`; it is never replaced by a
    /// large sentinel.
    pub(crate) fn finite_rate_bps(self) -> Option<u64> {
        match self.value {
            CarrierRateAuthorityValue::Native(rate) => rate.finite_rate_bps(),
            #[cfg(test)]
            CarrierRateAuthorityValue::ReceiptFinite(rate) => Some(rate.get()),
        }
    }

    /// Returns whether one checked transport source names the exact native
    /// controller lifetime carried by this central snapshot.
    ///
    /// The observation value is deliberately excluded. A same-activation
    /// controller read may lag a later operational-rate callback within the
    /// bounded publication interval, while `(scope, A, I)` must never be
    /// allowed to cross. Keeping this comparison inside the reducer module
    /// preserves equality-only controller identity outside the model.
    pub(crate) fn matches_native_source(self, source: &NativeCarrierRateSourceSnapshot) -> bool {
        self.mode == CarrierRateAuthorityMode::NativeOperational
            && self.stamp.scope == source.scope
            && self.stamp.native_activation == Some(source.transport_activation)
            && self.stamp.native_controller == Some(source.controller)
    }
}

/// Equality-only identity of the concrete native controller source.
///
/// There is intentionally no ordering or raw-value accessor. The eventual
/// runtime adapter must mint this from an opaque controller binding; this pure
/// kernel exposes no production constructor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NativeControllerIdentity(NonZeroU64);

/// Exact activation lifetime of one installed native transport controller.
///
/// This is distinct from the controller identity and from the central
/// authority revision. A same-identity `PathData::from_previous` clone gets a
/// fresh activation. The trusted transport issuer allocates these strictly
/// increasingly within this reducer scope; order is used only to reject reuse
/// and is never exposed as capacity, health, or path-rank evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativeTransportActivation(NonZeroU64);

impl NativeTransportActivation {
    const TERMINAL_RAW: u64 = u64::MAX;

    fn checked_from_raw(raw: u64) -> Result<Self, NativeTransportActivationError> {
        let value = NonZeroU64::new(raw).ok_or(NativeTransportActivationError::Zero)?;
        if raw == Self::TERMINAL_RAW {
            return Err(NativeTransportActivationError::Exhausted);
        }
        Ok(Self(value))
    }
}

/// Invalid live transport-activation identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeTransportActivationError {
    Zero,
    Exhausted,
}

/// Rejected input at the pure Native authority adapter boundary.
///
/// The boundary accepts no unchecked rate or identity. In particular, the
/// operational rate is already in bits/s and must be a positive point on the
/// transport's exact 8-bit/s lattice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeCarrierRateInputError {
    TransportActivationZero,
    TransportActivationExhausted,
    ControllerIdentityZero,
    OperationalRate(CarrierRateConversionError),
    ExhaustionBeforeLastLiveActivation,
}

fn map_transport_activation_error(
    error: NativeTransportActivationError,
) -> NativeCarrierRateInputError {
    match error {
        NativeTransportActivationError::Zero => {
            NativeCarrierRateInputError::TransportActivationZero
        }
        NativeTransportActivationError::Exhausted => {
            NativeCarrierRateInputError::TransportActivationExhausted
        }
    }
}

/// One checked coherent read of the exact active native controller.
///
/// This value is affine and its fields are opaque. A serialized runtime
/// coordinator may construct it only from one transport-side
/// `(scope, A, I, B_op)` snapshot; it cannot be assembled from general
/// metrics. Scope is mandatory because `A` and `I` have no cross-fence
/// meaning.
#[derive(Debug)]
#[must_use = "a coherent native source snapshot must be compare-applied or discarded"]
pub(crate) struct NativeCarrierRateSourceSnapshot {
    scope: CarrierRateAuthorityScope,
    transport_activation: NativeTransportActivation,
    controller: NativeControllerIdentity,
    observation: NativeControllerObservation,
}

impl NativeCarrierRateSourceSnapshot {
    pub(crate) fn checked_from_bits_per_second(
        scope: CarrierRateAuthorityScope,
        transport_activation: u64,
        controller_identity: u64,
        operational_bits_per_second: Option<u128>,
    ) -> Result<Self, NativeCarrierRateInputError> {
        let transport_activation =
            NativeTransportActivation::checked_from_raw(transport_activation)
                .map_err(map_transport_activation_error)?;
        let controller = NativeControllerIdentity(
            NonZeroU64::new(controller_identity)
                .ok_or(NativeCarrierRateInputError::ControllerIdentityZero)?,
        );
        let observation = operational_bits_per_second
            .map(checked_native_operational_rate)
            .transpose()
            .map_err(NativeCarrierRateInputError::OperationalRate)?
            .map_or(
                NativeControllerObservation::Absent,
                NativeControllerObservation::Operational,
            );
        Ok(Self {
            scope,
            transport_activation,
            controller,
            observation,
        })
    }
}

/// Separately sampled scope-bound live transport activation used as an
/// apply/precommit fence. It deliberately carries no controller rate or
/// central revision.
#[derive(Debug)]
#[must_use = "the current transport activation must fence an authority operation"]
pub(crate) struct NativeCarrierTransportCurrent {
    scope: CarrierRateAuthorityScope,
    transport_activation: NativeTransportActivation,
}

impl NativeCarrierTransportCurrent {
    pub(crate) fn checked_from_raw(
        scope: CarrierRateAuthorityScope,
        raw: u64,
    ) -> Result<Self, NativeCarrierRateInputError> {
        NativeTransportActivation::checked_from_raw(raw)
            .map(|transport_activation| Self {
                scope,
                transport_activation,
            })
            .map_err(map_transport_activation_error)
    }
}

/// Affine transport proof that the checked activation sequence is exhausted.
///
/// The runtime issuer still owes the actual checked-increment linearization;
/// this constructor only prevents any value other than the last live serial
/// from entering the pure reducer seam.
#[derive(Debug)]
#[must_use = "transport exhaustion must terminalize its authority scope"]
pub(crate) struct NativeCarrierTransportExhaustion(NativeTransportActivationExhausted);

impl NativeCarrierTransportExhaustion {
    pub(crate) fn checked_after_last_live(
        scope: CarrierRateAuthorityScope,
        last_live_activation: u64,
    ) -> Result<Self, NativeCarrierRateInputError> {
        if last_live_activation != NativeTransportActivation::TERMINAL_RAW - 1 {
            return Err(NativeCarrierRateInputError::ExhaustionBeforeLastLiveActivation);
        }
        let last_live_activation =
            NativeTransportActivation::checked_from_raw(last_live_activation)
                .map_err(map_transport_activation_error)?;
        Ok(Self(NativeTransportActivationExhausted {
            scope,
            last_live_activation,
        }))
    }
}

/// Reducer-private instance key. Arc identity prevents a capability issued by
/// a different reducer with an accidentally equal public scope from applying.
#[derive(Debug)]
struct AuthorityInstanceKey(Arc<()>);

impl AuthorityInstanceKey {
    fn new() -> Self {
        Self(Arc::new(()))
    }

    fn duplicate(&self) -> Self {
        Self(Arc::clone(&self.0))
    }

    fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Non-cloneable capability bound to one native-controller activation.
///
/// `transport_activation` is frozen when this exact native object becomes
/// active. It stays fixed while ordinary rate updates advance the separate
/// central authority revision. Reactivating retained controller A after A -> B
/// therefore produces A3, distinct from stale A1.
#[derive(Debug)]
#[must_use = "dropping an activation capability fences that native publisher"]
struct NativeControllerActivation {
    authority_key: AuthorityInstanceKey,
    transport_activation: NativeTransportActivation,
    controller: NativeControllerIdentity,
}

/// Private native-controller observation kind.
///
/// Absence means no update and never revokes initialized state. Structural
/// invalidation is intentionally not a variant: it requires the separate
/// fenced `NativeToReceiptReady` proof below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeControllerObservation {
    Absent,
    Operational(CarrierRateBps),
}

/// One transport-issued coherent activation event for an installed controller.
///
/// Fresh controllers carry `Absent` and therefore start at `C0`. A retained
/// controller restored after a failed candidate may carry its current native
/// operational state, preserving the controller rather than fabricating a
/// cold restart. The future runtime source capability must mint this event
/// atomically from the exact controller being activated.
#[derive(Debug)]
struct NativeControllerActivationEvent {
    transport_activation: NativeTransportActivation,
    controller: NativeControllerIdentity,
    observation: NativeControllerObservation,
}

/// Synchronous proof of the transport's exact currently active native object.
///
/// This proof is deliberately separate from reducer state: while an
/// asynchronous activation event is awaiting publication, the transport can
/// already be at A3 while the reducer still stores `(A1, G1)`. The future
/// runtime adapter must back this non-cloneable value by a guard/CAS over the
/// real active pointer; this pure kernel provides no constructor.
#[derive(Debug)]
#[must_use = "native transport freshness must be checked at the commit boundary"]
struct ActiveNativeTransportGuard {
    scope: CarrierRateAuthorityScope,
    transport_activation: NativeTransportActivation,
}

/// Transport-issued proof that the activation sequence has no live successor.
///
/// The terminal value is never representable as `NativeTransportActivation`.
/// This affine proof is the only kernel input that maps transport activation
/// exhaustion to the reducer's absorbing terminal state.
#[derive(Debug)]
#[must_use = "transport activation exhaustion must terminalize its authority scope"]
struct NativeTransportActivationExhausted {
    scope: CarrierRateAuthorityScope,
    last_live_activation: NativeTransportActivation,
}

/// Opaque observation proposal captured from one coherent native snapshot.
///
/// `expected` is issuer-owned compare/apply state, not a caller-supplied fresh
/// stamp. It orders observations within one activation: an older same-A rate
/// cannot overwrite a newer accepted rate. The future runtime issuer must
/// capture G, read the exact A/controller state, and retain a transport guard
/// across compare/apply as one transaction.
///
/// The reducer proves ordering, not observation latency. The adapter still
/// owes a bounded `D_pub` and a switch-free coherent snapshot/apply transaction
/// for each proposal.
#[derive(Debug)]
#[must_use = "a captured native observation must be compare-applied or discarded"]
struct NativeControllerObservationProposal {
    authority_key: AuthorityInstanceKey,
    expected: CarrierRateAuthorityStamp,
    transport_activation: NativeTransportActivation,
    controller: NativeControllerIdentity,
    observation: NativeControllerObservation,
}

/// Affine permission to perform one NativeMode ownership transfer while both
/// central `(A, G)` and the live transport A are fenced.
///
/// The borrows prevent reducer mutation or guard replacement while `commit`
/// runs. This is only a compile-time seam: the runtime guard must actually hold
/// the transport switch fence (or perform an equivalent writer-side CAS).
#[derive(Debug)]
#[must_use = "native scheduling authority must be consumed at ownership transfer"]
struct NativeAuthorityCommitPermit<'fence> {
    _authority: &'fence CarrierRateAuthorityReducer,
    _active_transport: &'fence mut ActiveNativeTransportGuard,
    #[cfg(test)]
    stamp: CarrierRateAuthorityStamp,
}

impl NativeAuthorityCommitPermit<'_> {
    #[cfg(test)]
    fn stamp(&self) -> CarrierRateAuthorityStamp {
        debug_assert!(self._authority.is_current(self.stamp));
        debug_assert_eq!(
            self._active_transport.transport_activation,
            self.stamp
                .native_activation
                .expect("NativeMode permit always carries activation")
        );
        self.stamp
    }

    fn commit<R>(self, transfer_ownership: impl FnOnce() -> R) -> R {
        transfer_ownership()
    }
}

/// Positive achieved-service rate admitted by the ReceiptMode proof model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
struct ReceiptAuthorityRate(CarrierRateBps);

#[cfg(test)]
impl ReceiptAuthorityRate {
    fn checked_from_bits_per_second(
        bits_per_second: u64,
    ) -> Result<Self, CarrierRateConversionError> {
        CarrierRateBps::checked_new(bits_per_second)
            .map(Self)
            .map_err(|_| CarrierRateConversionError::Zero)
    }

    fn rate(self) -> CarrierRateBps {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
struct ReceiptAuthorityTerm {
    id: ReceiptTermId,
    rate: ReceiptAuthorityRate,
}

/// Non-cloneable ownership of one reducer's fixed ReceiptMode generation.
#[derive(Debug)]
#[must_use = "Receipt publication must stay bound to its mode generation"]
#[cfg(test)]
struct ReceiptModeCapability {
    authority_key: AuthorityInstanceKey,
    generation: ReceiptModeGeneration,
}

/// Proof that the exact native activation was fenced at this revision.
///
/// There is deliberately no callable constructor. The serialized runtime
/// coordinator must eventually mint it only after old source publication and
/// the old decision semantics are fenced.
#[derive(Debug)]
#[cfg(test)]
struct FencedNativeController {
    authority_key: AuthorityInstanceKey,
    expected: CarrierRateAuthorityStamp,
    transport_activation: NativeTransportActivation,
    controller: NativeControllerIdentity,
}

/// Proof that one exact ReceiptMode generation is fully prepared.
#[derive(Debug)]
#[cfg(test)]
struct PreparedReceiptMode {
    generation: ReceiptModeGeneration,
}

/// Atomic Native-to-Receipt transition proof.
///
/// This command is non-cloneable and has no constructor. It must not escape
/// the exclusive serialized transaction that combines its two fields.
#[derive(Debug)]
#[must_use = "a fenced native source must be committed or terminalized"]
#[cfg(test)]
struct NativeToReceiptReady {
    fenced: FencedNativeController,
    receipt: PreparedReceiptMode,
}

/// Opaque asynchronous publication from a validated Receipt acquisition.
///
/// All fields are issuer-owned and immutable. In particular, callers do not
/// supply a fresh stamp, generation, term identity, or rate to the reducer API.
#[derive(Debug)]
#[must_use = "validated Receipt publication must be applied exactly once"]
#[cfg(test)]
struct ReceiptTermPublication {
    authority_key: AuthorityInstanceKey,
    expected: CarrierRateAuthorityStamp,
    generation: ReceiptModeGeneration,
    term: ReceiptAuthorityTerm,
}

/// Opaque retirement of the exact active Receipt term installed above.
#[derive(Debug)]
#[must_use = "Receipt expiry must retire only its exact published term"]
#[cfg(test)]
struct ReceiptTermRetirement {
    authority_key: AuthorityInstanceKey,
    expected: CarrierRateAuthorityStamp,
    generation: ReceiptModeGeneration,
    term_id: ReceiptTermId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CarrierRateAuthorityState {
    Native {
        transport_activation: NativeTransportActivation,
        controller: NativeControllerIdentity,
        operational: Option<CarrierRateBps>,
    },
    #[cfg(test)]
    Receipt {
        generation: ReceiptModeGeneration,
        fallback: CarrierRateBps,
        last_term_id: Option<ReceiptTermId>,
        active: Option<ReceiptAuthorityTerm>,
    },
    Terminal,
}

/// Result of one accepted reducer event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarrierRateAuthorityTransition {
    Unchanged,
    Applied,
    Terminal,
}

/// A rejected event never changes the reducer or advances its revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarrierRateAuthorityError {
    AuthorityInstanceMismatch,
    AuthorityScopeMismatch,
    StaleStamp,
    NativeActivationMismatch,
    WrongMode,
    Terminal,
    #[cfg(test)]
    ReceiptGenerationMismatch,
    #[cfg(test)]
    ReceiptRateDoesNotExceedFallback,
    #[cfg(test)]
    ReceiptTermMismatch,
    #[cfg(test)]
    ReceiptTermReused,
    NativeActivationReused,
    ActiveTransportMismatch,
    #[cfg(test)]
    UnlimitedReceiptFallbackUnavailable,
}

/// Issuer-owned compare/apply ticket for one Native publication attempt.
///
/// Callers cannot choose `G` or transplant a ticket between reducer instances.
/// The serialized coordinator captures this token before reading the coherent
/// transport source and consumes it exactly once at compare/apply.
#[derive(Debug)]
#[must_use = "a captured Native publication ticket must be consumed or discarded"]
pub(crate) struct NativeCarrierRatePublicationTicket {
    authority_key: AuthorityInstanceKey,
    expected: CarrierRateAuthorityStamp,
}

/// Production-facing pure facade for one Native carrier direction.
///
/// This facade owns the reducer as its only persistent state. It deliberately
/// contains no lock, task, timer, Quinn type, estimator, or cached copy of
/// `(A, I, G, rate)`. A future runtime owner must serialize calls and retain
/// its transport activation fence while supplying `NativeCarrierTransportCurrent`.
#[derive(Debug)]
pub(crate) struct NativeCarrierRateAuthority {
    reducer: CarrierRateAuthorityReducer,
}

#[derive(Debug)]
struct NativeControllerInstallation {
    transition: CarrierRateAuthorityTransition,
    #[cfg(test)]
    activation: Option<NativeControllerActivation>,
}

#[derive(Debug)]
#[cfg(test)]
struct ReceiptTermPublicationOutcome {
    transition: CarrierRateAuthorityTransition,
    retirement: Option<ReceiptTermRetirement>,
}

#[derive(Debug)]
#[cfg(test)]
struct NativeToReceiptOutcome {
    transition: CarrierRateAuthorityTransition,
    receipt: Option<ReceiptModeCapability>,
}

/// Pure exclusive authority reducer for one exact carrier direction.
///
/// Mutation methods below are a private kernel seam, not a runtime adapter.
/// The runtime must supply the opaque source binding and serialized issuer
/// before changing their visibility or wiring them into scheduling.
#[derive(Debug)]
struct CarrierRateAuthorityReducer {
    authority_key: AuthorityInstanceKey,
    scope: CarrierRateAuthorityScope,
    revision: CarrierRateAuthorityRevision,
    startup: DirectionalServiceRate,
    state: CarrierRateAuthorityState,
}

impl CarrierRateAuthorityReducer {
    fn new_native_with_startup(
        scope: CarrierRateAuthorityScope,
        startup: DirectionalServiceRate,
        initial: NativeControllerActivationEvent,
    ) -> (Self, NativeControllerActivation) {
        let authority_key = AuthorityInstanceKey::new();
        let activation = NativeControllerActivation {
            authority_key: authority_key.duplicate(),
            transport_activation: initial.transport_activation,
            controller: initial.controller,
        };
        let operational = match initial.observation {
            NativeControllerObservation::Absent => None,
            NativeControllerObservation::Operational(rate) => Some(rate),
        };
        (
            Self {
                authority_key,
                scope,
                revision: CarrierRateAuthorityRevision::INITIAL,
                startup,
                state: CarrierRateAuthorityState::Native {
                    transport_activation: initial.transport_activation,
                    controller: initial.controller,
                    operational,
                },
            },
            activation,
        )
    }

    #[cfg(test)]
    fn new_native(
        scope: CarrierRateAuthorityScope,
        startup_prior: CarrierRateBps,
        initial: NativeControllerActivationEvent,
    ) -> (Self, NativeControllerActivation) {
        let startup = DirectionalServiceRate::from_startup_hint(
            scope,
            RateHint::BitsPerSecond(startup_prior.get()),
        )
        .expect("positive test startup is a valid typed rate");
        Self::new_native_with_startup(scope, startup, initial)
    }

    #[cfg(test)]
    fn new_receipt(
        scope: CarrierRateAuthorityScope,
        startup_prior: CarrierRateBps,
        generation: ReceiptModeGeneration,
    ) -> (Self, ReceiptModeCapability) {
        let authority_key = AuthorityInstanceKey::new();
        let receipt = ReceiptModeCapability {
            authority_key: authority_key.duplicate(),
            generation,
        };
        let startup = DirectionalServiceRate::from_startup_hint(
            scope,
            RateHint::BitsPerSecond(startup_prior.get()),
        )
        .expect("positive legacy Receipt startup is a valid typed rate");
        (
            Self {
                authority_key,
                scope,
                revision: CarrierRateAuthorityRevision::INITIAL,
                startup,
                state: CarrierRateAuthorityState::Receipt {
                    generation,
                    fallback: startup_prior,
                    last_term_id: None,
                    active: None,
                },
            },
            receipt,
        )
    }

    fn stamp(&self) -> CarrierRateAuthorityStamp {
        let (native_activation, native_controller) = match self.state {
            CarrierRateAuthorityState::Native {
                transport_activation,
                controller,
                ..
            } => (Some(transport_activation), Some(controller)),
            #[cfg(test)]
            CarrierRateAuthorityState::Receipt { .. } => (None, None),
            CarrierRateAuthorityState::Terminal => (None, None),
        };
        CarrierRateAuthorityStamp {
            scope: self.scope,
            native_activation,
            native_controller,
            revision: self.revision,
        }
    }

    fn snapshot(&self) -> Option<CarrierRateAuthoritySnapshot> {
        let (mode, basis, value) = match self.state {
            CarrierRateAuthorityState::Native { operational, .. } => match operational {
                Some(operational) => {
                    let operational =
                        QuinnBbr3NativeOperationalRate::checked_new(self.scope, operational.get())
                            .expect("native observations are positive by construction");
                    let service_rate = self
                        .startup
                        .replace_with_quinn_bbr3_native_operational(operational)
                        .expect("native observation is constructed for this reducer scope");
                    (
                        CarrierRateAuthorityMode::NativeOperational,
                        CarrierRateAuthorityBasis::NativeOperational,
                        CarrierRateAuthorityValue::Native(service_rate),
                    )
                }
                None => (
                    CarrierRateAuthorityMode::NativeOperational,
                    CarrierRateAuthorityBasis::StartupPrior,
                    CarrierRateAuthorityValue::Native(self.startup),
                ),
            },
            #[cfg(test)]
            CarrierRateAuthorityState::Receipt {
                generation,
                fallback,
                active,
                ..
            } => match active {
                Some(term) => (
                    CarrierRateAuthorityMode::Receipt,
                    CarrierRateAuthorityBasis::ReceiptTerm(ReceiptTermKey {
                        generation,
                        term_id: term.id,
                    }),
                    CarrierRateAuthorityValue::ReceiptFinite(term.rate.rate()),
                ),
                None => (
                    CarrierRateAuthorityMode::Receipt,
                    CarrierRateAuthorityBasis::ReceiptFallback,
                    CarrierRateAuthorityValue::ReceiptFinite(fallback),
                ),
            },
            CarrierRateAuthorityState::Terminal => return None,
        };
        Some(CarrierRateAuthoritySnapshot {
            stamp: self.stamp(),
            mode,
            basis,
            value,
        })
    }

    #[cfg(test)]
    fn is_current(&self, stamp: CarrierRateAuthorityStamp) -> bool {
        self.state != CarrierRateAuthorityState::Terminal && self.stamp() == stamp
    }

    /// NativeMode precommit must validate both central `(A, G)` and the live
    /// transport A. Central equality alone is insufficient during asynchronous
    /// PathData activation publication.
    fn authorize_native_precommit<'fence>(
        &'fence self,
        stamp: CarrierRateAuthorityStamp,
        active_transport: &'fence mut ActiveNativeTransportGuard,
    ) -> Result<NativeAuthorityCommitPermit<'fence>, CarrierRateAuthorityError> {
        self.require_current(stamp)?;
        let CarrierRateAuthorityState::Native {
            transport_activation,
            ..
        } = self.state
        else {
            return Err(CarrierRateAuthorityError::WrongMode);
        };
        self.require_active_transport(active_transport, transport_activation)?;
        Ok(NativeAuthorityCommitPermit {
            _authority: self,
            _active_transport: active_transport,
            #[cfg(test)]
            stamp,
        })
    }

    /// Private kernel seam for one coherent active-controller proposal.
    /// There is no callable constructor and no caller-provided stamp argument.
    fn apply_native_observation(
        &mut self,
        proposal: NativeControllerObservationProposal,
        active_transport: &mut ActiveNativeTransportGuard,
    ) -> Result<CarrierRateAuthorityTransition, CarrierRateAuthorityError> {
        self.require_key(&proposal.authority_key)?;
        self.require_current(proposal.expected)?;
        let CarrierRateAuthorityState::Native {
            transport_activation,
            controller,
            ..
        } = self.state
        else {
            return Err(CarrierRateAuthorityError::WrongMode);
        };
        if transport_activation != proposal.transport_activation
            || controller != proposal.controller
        {
            return Err(CarrierRateAuthorityError::NativeActivationMismatch);
        }
        self.require_active_transport(active_transport, proposal.transport_activation)?;
        let NativeControllerObservation::Operational(observed) = proposal.observation else {
            return Ok(CarrierRateAuthorityTransition::Unchanged);
        };
        self.commit(CarrierRateAuthorityState::Native {
            transport_activation,
            controller,
            operational: Some(observed),
        })
    }

    /// Installs one exact native transport activation.
    ///
    /// Calling this method asserts that the active `PathData`/controller object
    /// changed. It therefore always advances the central authority revision,
    /// even when controller identity, observation, and projected rate compare
    /// equal. A locator-only change retaining the exact active object must not
    /// call this method.
    fn install_native_controller(
        &mut self,
        current: &NativeControllerActivation,
        next: NativeControllerActivationEvent,
    ) -> Result<NativeControllerInstallation, CarrierRateAuthorityError> {
        let (current_activation, _, _) = self.require_native_activation(current)?;
        if next.transport_activation.0.get() <= current_activation.0.get() {
            return Err(CarrierRateAuthorityError::NativeActivationReused);
        }
        let operational = match next.observation {
            NativeControllerObservation::Absent => None,
            NativeControllerObservation::Operational(rate) => Some(rate),
        };
        let Some(_) = self.advance_live_revision()? else {
            return Ok(NativeControllerInstallation {
                transition: CarrierRateAuthorityTransition::Terminal,
                #[cfg(test)]
                activation: None,
            });
        };
        self.state = CarrierRateAuthorityState::Native {
            transport_activation: next.transport_activation,
            controller: next.controller,
            operational,
        };
        Ok(NativeControllerInstallation {
            transition: CarrierRateAuthorityTransition::Applied,
            #[cfg(test)]
            activation: Some(NativeControllerActivation {
                authority_key: self.authority_key.duplicate(),
                transport_activation: next.transport_activation,
                controller: next.controller,
            }),
        })
    }

    #[cfg(test)]
    fn revoke_native_to_receipt(
        &mut self,
        ready: NativeToReceiptReady,
        active_transport: &mut ActiveNativeTransportGuard,
    ) -> Result<NativeToReceiptOutcome, CarrierRateAuthorityError> {
        self.require_key(&ready.fenced.authority_key)?;
        self.require_current(ready.fenced.expected)?;
        let CarrierRateAuthorityState::Native {
            transport_activation,
            controller,
            operational,
        } = self.state
        else {
            return Err(CarrierRateAuthorityError::WrongMode);
        };
        if transport_activation != ready.fenced.transport_activation
            || controller != ready.fenced.controller
        {
            return Err(CarrierRateAuthorityError::NativeActivationMismatch);
        }
        self.require_active_transport(active_transport, transport_activation)?;
        let startup = self.startup.value().finite_rate();
        let fallback = match (startup, operational) {
            (Some(startup), Some(operational)) => startup.min(operational),
            (Some(startup), None) => startup,
            (None, Some(operational)) => operational,
            (None, None) => {
                return Err(CarrierRateAuthorityError::UnlimitedReceiptFallbackUnavailable);
            }
        };
        let generation = ready.receipt.generation;
        let transition = self.commit(CarrierRateAuthorityState::Receipt {
            generation,
            fallback,
            last_term_id: None,
            active: None,
        })?;
        let receipt = (transition == CarrierRateAuthorityTransition::Applied).then(|| {
            ReceiptModeCapability {
                authority_key: self.authority_key.duplicate(),
                generation,
            }
        });
        Ok(NativeToReceiptOutcome {
            transition,
            receipt,
        })
    }

    #[cfg(test)]
    fn publish_receipt_term(
        &mut self,
        publication: ReceiptTermPublication,
    ) -> Result<ReceiptTermPublicationOutcome, CarrierRateAuthorityError> {
        self.require_key(&publication.authority_key)?;
        self.require_current(publication.expected)?;
        let CarrierRateAuthorityState::Receipt {
            generation,
            fallback,
            last_term_id,
            active,
        } = self.state
        else {
            return Err(CarrierRateAuthorityError::WrongMode);
        };
        if publication.generation != generation {
            return Err(CarrierRateAuthorityError::ReceiptGenerationMismatch);
        }
        if publication.term.rate.rate() <= fallback {
            return Err(CarrierRateAuthorityError::ReceiptRateDoesNotExceedFallback);
        }
        if active.is_some_and(|term| term.id == publication.term.id) {
            if active == Some(publication.term) {
                return Ok(ReceiptTermPublicationOutcome {
                    transition: CarrierRateAuthorityTransition::Unchanged,
                    retirement: None,
                });
            }
            return Err(CarrierRateAuthorityError::ReceiptTermMismatch);
        }
        if last_term_id.is_some_and(|last| publication.term.id <= last) {
            return Err(CarrierRateAuthorityError::ReceiptTermReused);
        }
        let term = publication.term;
        let transition = self.commit(CarrierRateAuthorityState::Receipt {
            generation,
            fallback,
            last_term_id: Some(term.id),
            active: Some(term),
        })?;
        let retirement = (transition == CarrierRateAuthorityTransition::Applied).then(|| {
            ReceiptTermRetirement {
                authority_key: self.authority_key.duplicate(),
                expected: self.stamp(),
                generation,
                term_id: term.id,
            }
        });
        Ok(ReceiptTermPublicationOutcome {
            transition,
            retirement,
        })
    }

    #[cfg(test)]
    fn retire_receipt_term(
        &mut self,
        retirement: ReceiptTermRetirement,
    ) -> Result<CarrierRateAuthorityTransition, CarrierRateAuthorityError> {
        self.require_key(&retirement.authority_key)?;
        self.require_current(retirement.expected)?;
        let CarrierRateAuthorityState::Receipt {
            generation,
            fallback,
            last_term_id,
            active,
        } = self.state
        else {
            return Err(CarrierRateAuthorityError::WrongMode);
        };
        if retirement.generation != generation {
            return Err(CarrierRateAuthorityError::ReceiptGenerationMismatch);
        }
        if active.is_none_or(|term| term.id != retirement.term_id) {
            return Err(CarrierRateAuthorityError::ReceiptTermMismatch);
        }
        self.commit(CarrierRateAuthorityState::Receipt {
            generation,
            fallback,
            last_term_id,
            active: None,
        })
    }

    fn terminate(&mut self) -> Result<CarrierRateAuthorityTransition, CarrierRateAuthorityError> {
        if self.state == CarrierRateAuthorityState::Terminal {
            return Err(CarrierRateAuthorityError::Terminal);
        }
        self.revision = CarrierRateAuthorityRevision::TERMINAL;
        self.state = CarrierRateAuthorityState::Terminal;
        Ok(CarrierRateAuthorityTransition::Terminal)
    }

    fn terminate_native_transport_exhaustion(
        &mut self,
        current: &NativeControllerActivation,
        exhausted: NativeTransportActivationExhausted,
    ) -> Result<CarrierRateAuthorityTransition, CarrierRateAuthorityError> {
        let (transport_activation, _, _) = self.require_native_activation(current)?;
        if exhausted.scope != self.scope
            || exhausted.last_live_activation.0.get() != NativeTransportActivation::TERMINAL_RAW - 1
            || transport_activation.0.get() > exhausted.last_live_activation.0.get()
        {
            return Err(CarrierRateAuthorityError::ActiveTransportMismatch);
        }
        self.terminate()
    }

    fn require_key(
        &self,
        authority_key: &AuthorityInstanceKey,
    ) -> Result<(), CarrierRateAuthorityError> {
        if self.authority_key.matches(authority_key) {
            Ok(())
        } else {
            Err(CarrierRateAuthorityError::AuthorityInstanceMismatch)
        }
    }

    fn require_current(
        &self,
        expected: CarrierRateAuthorityStamp,
    ) -> Result<(), CarrierRateAuthorityError> {
        if self.state == CarrierRateAuthorityState::Terminal {
            return Err(CarrierRateAuthorityError::Terminal);
        }
        if expected != self.stamp() {
            return Err(CarrierRateAuthorityError::StaleStamp);
        }
        Ok(())
    }

    fn require_active_transport(
        &self,
        active_transport: &ActiveNativeTransportGuard,
        expected_activation: NativeTransportActivation,
    ) -> Result<(), CarrierRateAuthorityError> {
        if active_transport.scope == self.scope
            && active_transport.transport_activation == expected_activation
        {
            Ok(())
        } else {
            Err(CarrierRateAuthorityError::ActiveTransportMismatch)
        }
    }

    fn require_native_activation(
        &self,
        activation: &NativeControllerActivation,
    ) -> Result<
        (
            NativeTransportActivation,
            NativeControllerIdentity,
            Option<CarrierRateBps>,
        ),
        CarrierRateAuthorityError,
    > {
        if self.state == CarrierRateAuthorityState::Terminal {
            return Err(CarrierRateAuthorityError::Terminal);
        }
        self.require_key(&activation.authority_key)?;
        let CarrierRateAuthorityState::Native {
            transport_activation,
            controller,
            operational,
        } = self.state
        else {
            return Err(CarrierRateAuthorityError::WrongMode);
        };
        if transport_activation != activation.transport_activation
            || controller != activation.controller
        {
            return Err(CarrierRateAuthorityError::NativeActivationMismatch);
        }
        Ok((transport_activation, controller, operational))
    }

    /// Advances `G` once for a semantic change whose successor state embeds
    /// the fresh revision or otherwise cannot be compared before advancing.
    /// `None` means the checked sequence entered its absorbing terminal value.
    fn advance_live_revision(
        &mut self,
    ) -> Result<Option<CarrierRateAuthorityRevision>, CarrierRateAuthorityError> {
        if self.state == CarrierRateAuthorityState::Terminal {
            return Err(CarrierRateAuthorityError::Terminal);
        }
        let Some(next_revision) = self.revision.0.checked_add(1) else {
            unreachable!("only the terminal tombstone owns u64::MAX")
        };
        if next_revision == u64::MAX {
            self.revision = CarrierRateAuthorityRevision::TERMINAL;
            self.state = CarrierRateAuthorityState::Terminal;
            return Ok(None);
        }
        let next_revision = CarrierRateAuthorityRevision(next_revision);
        self.revision = next_revision;
        Ok(Some(next_revision))
    }

    fn commit(
        &mut self,
        successor: CarrierRateAuthorityState,
    ) -> Result<CarrierRateAuthorityTransition, CarrierRateAuthorityError> {
        if self.state == CarrierRateAuthorityState::Terminal {
            return Err(CarrierRateAuthorityError::Terminal);
        }
        if self.state == successor {
            return Ok(CarrierRateAuthorityTransition::Unchanged);
        }
        let Some(_next_revision) = self.advance_live_revision()? else {
            return Ok(CarrierRateAuthorityTransition::Terminal);
        };
        self.state = successor;
        Ok(CarrierRateAuthorityTransition::Applied)
    }

    #[cfg(test)]
    fn set_revision_for_test(&mut self, revision: u64) {
        assert!((1..u64::MAX).contains(&revision));
        self.revision = CarrierRateAuthorityRevision(revision);
    }
}

impl NativeCarrierRateAuthority {
    /// Initializes one Native authority from one already-checked coherent,
    /// scope-bound active-controller snapshot.
    pub(crate) fn new(
        scope: CarrierRateAuthorityScope,
        startup: DirectionalServiceRate,
        initial: NativeCarrierRateSourceSnapshot,
    ) -> Result<Self, CarrierRateAuthorityError> {
        if initial.scope != scope || startup.scope() != scope {
            return Err(CarrierRateAuthorityError::AuthorityScopeMismatch);
        }
        let initial = NativeControllerActivationEvent {
            transport_activation: initial.transport_activation,
            controller: initial.controller,
            observation: initial.observation,
        };
        let (reducer, _initial_activation_capability) =
            CarrierRateAuthorityReducer::new_native_with_startup(scope, startup, initial);
        Ok(Self { reducer })
    }

    pub(crate) fn snapshot(&self) -> Option<CarrierRateAuthoritySnapshot> {
        self.reducer.snapshot()
    }

    pub(crate) fn stamp(&self) -> CarrierRateAuthorityStamp {
        self.reducer.stamp()
    }

    /// Captures the coordinator-owned expected `G` before a transport read.
    pub(crate) fn capture_publication_ticket(
        &self,
    ) -> Result<NativeCarrierRatePublicationTicket, CarrierRateAuthorityError> {
        match self.reducer.state {
            CarrierRateAuthorityState::Native { .. } => Ok(NativeCarrierRatePublicationTicket {
                authority_key: self.reducer.authority_key.duplicate(),
                expected: self.reducer.stamp(),
            }),
            #[cfg(test)]
            CarrierRateAuthorityState::Receipt { .. } => Err(CarrierRateAuthorityError::WrongMode),
            CarrierRateAuthorityState::Terminal => Err(CarrierRateAuthorityError::Terminal),
        }
    }

    /// Compare-applies one coherent Native snapshot against its issuer ticket.
    ///
    /// Same-activation proposals are ordered by captured `G`; absence retains
    /// any initialized rate. A strictly newer activation installs only that
    /// source's observation, so absence projects `C0`. Intermediate transport
    /// activations may be coalesced, but the separately supplied current
    /// transport activation must equal the source at this apply boundary.
    /// Both proofs must name this reducer's exact carrier-direction scope.
    pub(crate) fn compare_apply(
        &mut self,
        ticket: NativeCarrierRatePublicationTicket,
        source: NativeCarrierRateSourceSnapshot,
        current_transport: NativeCarrierTransportCurrent,
    ) -> Result<CarrierRateAuthorityTransition, CarrierRateAuthorityError> {
        self.reducer.require_key(&ticket.authority_key)?;
        self.reducer.require_current(ticket.expected)?;
        if source.scope != self.reducer.scope
            || current_transport.scope != self.reducer.scope
            || source.scope != current_transport.scope
        {
            return Err(CarrierRateAuthorityError::AuthorityScopeMismatch);
        }
        if current_transport.transport_activation != source.transport_activation {
            return Err(CarrierRateAuthorityError::ActiveTransportMismatch);
        }

        let CarrierRateAuthorityState::Native {
            transport_activation: central_activation,
            controller: central_controller,
            ..
        } = self.reducer.state
        else {
            return Err(CarrierRateAuthorityError::WrongMode);
        };
        let mut active_transport = ActiveNativeTransportGuard {
            scope: self.reducer.scope,
            transport_activation: current_transport.transport_activation,
        };

        if source.transport_activation == central_activation {
            if source.controller != central_controller {
                return Err(CarrierRateAuthorityError::NativeActivationMismatch);
            }
            return self.reducer.apply_native_observation(
                NativeControllerObservationProposal {
                    authority_key: ticket.authority_key,
                    expected: ticket.expected,
                    transport_activation: source.transport_activation,
                    controller: source.controller,
                    observation: source.observation,
                },
                &mut active_transport,
            );
        }
        if source.transport_activation.0.get() < central_activation.0.get() {
            return Err(CarrierRateAuthorityError::NativeActivationReused);
        }

        self.reducer
            .require_active_transport(&active_transport, source.transport_activation)?;
        let current = NativeControllerActivation {
            authority_key: self.reducer.authority_key.duplicate(),
            transport_activation: central_activation,
            controller: central_controller,
        };
        self.reducer
            .install_native_controller(
                &current,
                NativeControllerActivationEvent {
                    transport_activation: source.transport_activation,
                    controller: source.controller,
                    observation: source.observation,
                },
            )
            .map(|installation| installation.transition)
    }

    /// Runs the ownership-transfer closure only while both the complete
    /// central stamp and the separately supplied live transport A match.
    pub(crate) fn commit_if_current<R>(
        &self,
        decision: CarrierRateAuthorityStamp,
        current_transport: NativeCarrierTransportCurrent,
        transfer_ownership: impl FnOnce() -> R,
    ) -> Result<R, CarrierRateAuthorityError> {
        if current_transport.scope != self.reducer.scope {
            return Err(CarrierRateAuthorityError::AuthorityScopeMismatch);
        }
        let mut active_transport = ActiveNativeTransportGuard {
            scope: self.reducer.scope,
            transport_activation: current_transport.transport_activation,
        };
        let permit = self
            .reducer
            .authorize_native_precommit(decision, &mut active_transport)?;
        Ok(permit.commit(transfer_ownership))
    }

    /// Publishes a checked transport-activation exhaustion as the reducer's
    /// absorbing terminal state. The expected-G ticket prevents an older
    /// exhaustion proof from terminating a newer central mutation. Central A
    /// may lag the transport's final live A because ordinary publication is
    /// asynchronous; the trusted scope-bound proof still closes that state.
    pub(crate) fn terminate_transport_exhaustion(
        &mut self,
        ticket: NativeCarrierRatePublicationTicket,
        exhaustion: NativeCarrierTransportExhaustion,
    ) -> Result<CarrierRateAuthorityTransition, CarrierRateAuthorityError> {
        self.reducer.require_key(&ticket.authority_key)?;
        self.reducer.require_current(ticket.expected)?;
        let CarrierRateAuthorityState::Native {
            transport_activation,
            controller,
            ..
        } = self.reducer.state
        else {
            return Err(CarrierRateAuthorityError::WrongMode);
        };
        let current = NativeControllerActivation {
            authority_key: ticket.authority_key,
            transport_activation,
            controller,
        };
        self.reducer
            .terminate_native_transport_exhaustion(&current, exhaustion.0)
    }
}
