use super::*;
use crate::model::path::CarrierPathInstanceId;
use crate::model::service_rate::{ServiceRateBasis, ServiceRateValue};
use crate::protocol::PathMetricDirection;

fn scope(carrier: u64, direction: PathMetricDirection) -> CarrierRateAuthorityScope {
    CarrierRateAuthorityScope::new(CarrierPathInstanceId::from_raw(carrier), direction)
}

fn bps(rate: u64) -> CarrierRateBps {
    CarrierRateBps::checked_from_bits_per_second(rate).expect("positive test rate")
}

fn startup(authority_scope: CarrierRateAuthorityScope, rate: u64) -> DirectionalServiceRate {
    DirectionalServiceRate::from_startup_hint(authority_scope, RateHint::BitsPerSecond(rate))
        .expect("positive test startup")
}

fn native_bps(rate: u64) -> CarrierRateBps {
    checked_native_operational_rate(u128::from(rate)).expect("positive byte-aligned native rate")
}

fn controller(raw: u64) -> NativeControllerIdentity {
    NativeControllerIdentity(NonZeroU64::new(raw).expect("nonzero controller identity"))
}

fn transport_activation(raw: u64) -> NativeTransportActivation {
    NativeTransportActivation::checked_from_raw(raw).expect("live transport activation")
}

fn generation(raw: u64) -> ReceiptModeGeneration {
    ReceiptModeGeneration(NonZeroU64::new(raw).expect("nonzero Receipt generation"))
}

fn term_id(raw: u64) -> ReceiptTermId {
    ReceiptTermId(NonZeroU64::new(raw).expect("nonzero Receipt term"))
}

fn receipt_rate(rate: u64) -> ReceiptAuthorityRate {
    ReceiptAuthorityRate::checked_from_bits_per_second(rate).expect("positive Receipt rate")
}

fn operational(rate: u64) -> NativeControllerObservation {
    NativeControllerObservation::Operational(native_bps(rate))
}

fn fresh_activation(
    activation: u64,
    controller: NativeControllerIdentity,
) -> NativeControllerActivationEvent {
    NativeControllerActivationEvent {
        transport_activation: transport_activation(activation),
        controller,
        observation: NativeControllerObservation::Absent,
    }
}

fn retained_activation(
    activation: u64,
    controller: NativeControllerIdentity,
    rate: u64,
) -> NativeControllerActivationEvent {
    NativeControllerActivationEvent {
        transport_activation: transport_activation(activation),
        controller,
        observation: operational(rate),
    }
}

fn transport_guard(
    scope: CarrierRateAuthorityScope,
    activation: NativeTransportActivation,
) -> ActiveNativeTransportGuard {
    ActiveNativeTransportGuard {
        scope,
        transport_activation: activation,
    }
}

fn native_proposal(
    reducer: &CarrierRateAuthorityReducer,
    activation: &NativeControllerActivation,
    observation: NativeControllerObservation,
) -> NativeControllerObservationProposal {
    NativeControllerObservationProposal {
        authority_key: activation.authority_key.duplicate(),
        expected: reducer.stamp(),
        transport_activation: activation.transport_activation,
        controller: activation.controller,
        observation,
    }
}

fn apply_current_native(
    reducer: &mut CarrierRateAuthorityReducer,
    activation: &NativeControllerActivation,
    observation: NativeControllerObservation,
) -> Result<CarrierRateAuthorityTransition, CarrierRateAuthorityError> {
    let proposal = native_proposal(reducer, activation, observation);
    let current_activation = reducer
        .stamp()
        .native_activation()
        .unwrap_or(activation.transport_activation);
    let mut active_transport = transport_guard(reducer.scope, current_activation);
    reducer.apply_native_observation(proposal, &mut active_transport)
}

fn assert_projection(
    reducer: &CarrierRateAuthorityReducer,
    mode: CarrierRateAuthorityMode,
    basis: CarrierRateAuthorityBasis,
    rate: u64,
) -> CarrierRateAuthoritySnapshot {
    let snapshot = reducer.snapshot().expect("live authority snapshot");
    assert_eq!(snapshot.mode(), mode);
    assert_eq!(snapshot.basis(), basis);
    assert_eq!(snapshot.finite_rate_bps(), Some(rate));
    snapshot
}

fn ready_receipt(
    reducer: &CarrierRateAuthorityReducer,
    activation: &NativeControllerActivation,
    generation: ReceiptModeGeneration,
) -> NativeToReceiptReady {
    NativeToReceiptReady {
        fenced: FencedNativeController {
            authority_key: activation.authority_key.duplicate(),
            expected: reducer.stamp(),
            transport_activation: activation.transport_activation,
            controller: activation.controller,
        },
        receipt: PreparedReceiptMode { generation },
    }
}

fn receipt_publication(
    reducer: &CarrierRateAuthorityReducer,
    receipt: &ReceiptModeCapability,
    id: u64,
    rate: u64,
) -> ReceiptTermPublication {
    ReceiptTermPublication {
        authority_key: receipt.authority_key.duplicate(),
        expected: reducer.stamp(),
        generation: receipt.generation,
        term: ReceiptAuthorityTerm {
            id: term_id(id),
            rate: receipt_rate(rate),
        },
    }
}

#[test]
fn constructors_project_one_fixed_carrier_direction_scope() {
    let native_scope = scope(11, PathMetricDirection::ClientToServer);
    let (native, activation) = CarrierRateAuthorityReducer::new_native(
        native_scope,
        bps(24),
        fresh_activation(1, controller(7)),
    );
    let native_snapshot = assert_projection(
        &native,
        CarrierRateAuthorityMode::NativeOperational,
        CarrierRateAuthorityBasis::StartupPrior,
        24,
    );
    assert_eq!(native_snapshot.stamp().scope(), native_scope);
    assert_eq!(
        native_snapshot.stamp().native_activation(),
        Some(transport_activation(1))
    );
    assert_eq!(
        native_snapshot.stamp().native_controller(),
        Some(controller(7))
    );
    assert_eq!(native_snapshot.stamp().revision().as_u64(), 1);
    assert!(native.is_current(native_snapshot.stamp()));
    let mut active_transport = transport_guard(native_scope, transport_activation(1));
    let permit = native
        .authorize_native_precommit(native_snapshot.stamp(), &mut active_transport)
        .expect("current A/G authorizes one fenced ownership transfer");
    assert_eq!(permit.stamp(), native_snapshot.stamp());
    let mut transferred = false;
    permit.commit(|| transferred = true);
    assert!(transferred);
    assert_eq!(native_scope.carrier_instance_id().as_u64(), 11);
    assert_eq!(
        native_scope.direction(),
        PathMetricDirection::ClientToServer
    );
    assert_eq!(activation.controller, controller(7));
    assert_eq!(activation.transport_activation, transport_activation(1));

    let receipt_scope = scope(12, PathMetricDirection::ServerToClient);
    let receipt_generation = generation(3);
    let (receipt, receipt_capability) =
        CarrierRateAuthorityReducer::new_receipt(receipt_scope, bps(40), receipt_generation);
    let receipt_snapshot = assert_projection(
        &receipt,
        CarrierRateAuthorityMode::Receipt,
        CarrierRateAuthorityBasis::ReceiptFallback,
        40,
    );
    assert_eq!(receipt_snapshot.stamp().scope(), receipt_scope);
    assert_eq!(receipt_snapshot.stamp().native_activation(), None);
    assert_eq!(receipt_snapshot.stamp().native_controller(), None);
    assert_eq!(receipt_capability.generation, receipt_generation);
}

#[test]
fn native_boundary_rejects_zero_unaligned_and_out_of_u64_rates() {
    let exact = checked_native_operational_rate(100_000_000).expect("100 Mbit/s is representable");
    assert_eq!(exact.get(), 100_000_000);
    assert_eq!(
        checked_native_operational_rate(0),
        Err(CarrierRateConversionError::Zero)
    );
    assert_eq!(
        checked_native_operational_rate(9),
        Err(CarrierRateConversionError::NotByteAligned)
    );
    let boundary = checked_native_operational_rate(u128::from(u64::MAX - 7))
        .expect("largest exact 8-bit/s lattice value");
    assert_eq!(boundary.get(), u64::MAX - 7);
    assert_eq!(
        checked_native_operational_rate(u128::from(u64::MAX) + 1),
        Err(CarrierRateConversionError::OutOfRange),
        "the conceptual u64::MAX + 1 value is rejected before entering Rust's u64 boundary"
    );
    assert_eq!(
        NativeTransportActivation::checked_from_raw(0),
        Err(NativeTransportActivationError::Zero)
    );
    assert_eq!(
        NativeTransportActivation::checked_from_raw(u64::MAX),
        Err(NativeTransportActivationError::Exhausted),
        "the terminal transport token is never a live activation"
    );
    assert!(NativeTransportActivation::checked_from_raw(u64::MAX - 1).is_ok());
}

#[test]
fn native_observation_is_direct_exact_and_absence_never_revokes() {
    let (mut reducer, activation) = CarrierRateAuthorityReducer::new_native(
        scope(1, PathMetricDirection::ClientToServer),
        bps(80),
        fresh_activation(1, controller(1)),
    );
    let startup = reducer.snapshot().expect("startup");

    assert_eq!(
        apply_current_native(
            &mut reducer,
            &activation,
            NativeControllerObservation::Absent,
        ),
        Ok(CarrierRateAuthorityTransition::Unchanged)
    );
    assert_eq!(reducer.snapshot(), Some(startup));

    assert_eq!(
        apply_current_native(&mut reducer, &activation, operational(80)),
        Ok(CarrierRateAuthorityTransition::Applied),
        "equal numeric value still changes authority basis"
    );
    let initialized = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::NativeOperational,
        CarrierRateAuthorityBasis::NativeOperational,
        80,
    );
    assert_eq!(
        initialized.stamp().native_activation(),
        startup.stamp().native_activation(),
        "an ordinary rate/basis update advances G without changing transport activation A"
    );
    assert_eq!(
        initialized.stamp().native_controller(),
        startup.stamp().native_controller(),
        "an ordinary rate/basis update also retains equality-only controller identity I"
    );
    assert!(!reducer.is_current(startup.stamp()));

    assert_eq!(
        apply_current_native(&mut reducer, &activation, operational(400)),
        Ok(CarrierRateAuthorityTransition::Applied)
    );
    let high = reducer.snapshot().expect("high rate");
    assert_eq!(
        apply_current_native(&mut reducer, &activation, operational(40)),
        Ok(CarrierRateAuthorityTransition::Applied),
        "a controller downshift replaces rather than maximizes"
    );
    let low = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::NativeOperational,
        CarrierRateAuthorityBasis::NativeOperational,
        40,
    );
    assert!(low.stamp().revision() > high.stamp().revision());
    assert_eq!(
        apply_current_native(
            &mut reducer,
            &activation,
            NativeControllerObservation::Absent,
        ),
        Ok(CarrierRateAuthorityTransition::Unchanged),
        "missing observation must retain initialized native state"
    );
    assert_eq!(reducer.snapshot(), Some(low));
    assert_eq!(
        apply_current_native(&mut reducer, &activation, operational(40)),
        Ok(CarrierRateAuthorityTransition::Unchanged)
    );
    assert_ne!(initialized.stamp(), low.stamp());
}

#[test]
fn unlimited_startup_is_semantic_and_cannot_fabricate_a_receipt_fallback() {
    let authority_scope = scope(101, PathMetricDirection::ClientToServer);
    let startup = DirectionalServiceRate::from_startup_hint(authority_scope, RateHint::Unlimited)
        .expect("Unlimited is a valid semantic startup value");
    let (mut reducer, activation) = CarrierRateAuthorityReducer::new_native_with_startup(
        authority_scope,
        startup,
        fresh_activation(1, controller(41)),
    );

    let snapshot = reducer.snapshot().expect("live Unlimited startup");
    let service_rate = snapshot.service_rate().expect("Native typed service rate");
    assert_eq!(service_rate.basis(), ServiceRateBasis::UnlimitedStartup);
    assert_eq!(service_rate.value(), ServiceRateValue::UnlimitedStartup);
    assert_eq!(snapshot.finite_rate_bps(), None);

    let before = reducer.snapshot();
    let ready = ready_receipt(&reducer, &activation, generation(1));
    let mut active_transport = transport_guard(authority_scope, activation.transport_activation);
    assert!(matches!(
        reducer.revoke_native_to_receipt(ready, &mut active_transport),
        Err(CarrierRateAuthorityError::UnlimitedReceiptFallbackUnavailable)
    ));
    assert_eq!(reducer.snapshot(), before);

    apply_current_native(&mut reducer, &activation, operational(80))
        .expect("finite native observation replaces Unlimited startup");
    let native = reducer.snapshot().expect("finite native replacement");
    assert_eq!(native.finite_rate_bps(), Some(80));
    assert_eq!(
        native.service_rate().expect("Native service rate").basis(),
        ServiceRateBasis::QuinnBbr3NativeOperationalV1
    );
}

#[test]
fn delayed_same_activation_observation_cannot_overwrite_newer_rate() {
    let authority_scope = scope(13, PathMetricDirection::ClientToServer);
    let (mut reducer, activation) = CarrierRateAuthorityReducer::new_native(
        authority_scope,
        bps(80),
        fresh_activation(1, controller(1)),
    );
    let delayed_old = native_proposal(&reducer, &activation, operational(40));
    let accepted_new = native_proposal(&reducer, &activation, operational(160));
    let mut active_transport = transport_guard(authority_scope, transport_activation(1));

    assert_eq!(
        reducer.apply_native_observation(accepted_new, &mut active_transport),
        Ok(CarrierRateAuthorityTransition::Applied)
    );
    let after_new = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::NativeOperational,
        CarrierRateAuthorityBasis::NativeOperational,
        160,
    );
    assert_eq!(
        reducer.apply_native_observation(delayed_old, &mut active_transport),
        Err(CarrierRateAuthorityError::StaleStamp),
        "issuer-owned expected G prevents an older same-A read from rolling the rate back"
    );
    assert_eq!(reducer.snapshot(), Some(after_new));
}

#[test]
fn activation_fence_covers_install_rollback_and_same_identity_clone() {
    let identity_a = controller(1);
    let identity_b = controller(2);
    let (mut reducer, activation_a1) = CarrierRateAuthorityReducer::new_native(
        scope(2, PathMetricDirection::ServerToClient),
        bps(80),
        fresh_activation(1, identity_a),
    );
    apply_current_native(&mut reducer, &activation_a1, operational(160)).expect("initialize A1");
    let a1_rate_stamp = reducer.stamp();

    let replacement_b = reducer
        .install_native_controller(&activation_a1, fresh_activation(2, identity_b))
        .expect("activate fresh B");
    assert_eq!(
        replacement_b.transition,
        CarrierRateAuthorityTransition::Applied
    );
    let activation_b2 = replacement_b.activation.expect("B activation");
    let b2 = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::NativeOperational,
        CarrierRateAuthorityBasis::StartupPrior,
        80,
    );
    assert!(b2.stamp().revision() > a1_rate_stamp.revision());
    assert_eq!(
        b2.stamp().native_activation(),
        Some(transport_activation(2))
    );
    assert_eq!(b2.stamp().native_controller(), Some(identity_b));
    assert_eq!(
        apply_current_native(&mut reducer, &activation_a1, operational(800)),
        Err(CarrierRateAuthorityError::NativeActivationMismatch),
        "queued A1 observations are stale after B2 activates"
    );

    let replacement_a = reducer
        .install_native_controller(&activation_b2, retained_activation(3, identity_a, 160))
        .expect("restore retained A with one coherent controller snapshot");
    let activation_a3 = replacement_a.activation.expect("A3 activation");
    assert_ne!(
        activation_a3.transport_activation,
        activation_a1.transport_activation
    );
    assert_eq!(activation_a3.controller, activation_a1.controller);
    let a3 = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::NativeOperational,
        CarrierRateAuthorityBasis::NativeOperational,
        160,
    );
    assert!(a3.stamp().revision() > b2.stamp().revision());
    assert_eq!(
        a3.stamp().native_activation(),
        Some(transport_activation(3))
    );
    assert_eq!(a3.stamp().native_controller(), Some(identity_a));

    assert_eq!(
        apply_current_native(&mut reducer, &activation_a1, operational(800)),
        Err(CarrierRateAuthorityError::NativeActivationMismatch)
    );
    let current_a_stamp = reducer.stamp();
    assert_eq!(
        apply_current_native(&mut reducer, &activation_b2, operational(800)),
        Err(CarrierRateAuthorityError::NativeActivationMismatch),
        "old B input has no API that can pair it with current A's stamp"
    );
    assert_eq!(reducer.stamp(), current_a_stamp);
    assert_eq!(reducer.snapshot(), Some(a3));

    assert_eq!(
        apply_current_native(&mut reducer, &activation_a3, operational(800)),
        Ok(CarrierRateAuthorityTransition::Applied)
    );
    let before_clone = reducer.snapshot().expect("A3 rate update");
    let same_identity_clone = reducer
        .install_native_controller(&activation_a3, retained_activation(4, identity_a, 160))
        .expect("same identity clone is a distinct activation");
    assert_eq!(
        same_identity_clone.transition,
        CarrierRateAuthorityTransition::Applied
    );
    let activation_a4 = same_identity_clone.activation.expect("A4 activation");
    let a4 = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::NativeOperational,
        CarrierRateAuthorityBasis::NativeOperational,
        160,
    );
    assert_eq!(
        a4.stamp().native_activation(),
        Some(transport_activation(4))
    );
    assert_eq!(a4.stamp().native_controller(), Some(identity_a));
    assert!(a4.stamp().revision() > before_clone.stamp().revision());
    assert_eq!(
        apply_current_native(&mut reducer, &activation_a3, operational(400)),
        Err(CarrierRateAuthorityError::NativeActivationMismatch)
    );
    assert_eq!(
        apply_current_native(&mut reducer, &activation_a4, operational(400)),
        Ok(CarrierRateAuthorityTransition::Applied)
    );
}

#[test]
fn install_and_rollback_between_polls_still_fence_the_old_decision() {
    let identity_a = controller(1);
    let identity_b = controller(2);
    let (mut reducer, activation_a1) = CarrierRateAuthorityReducer::new_native(
        scope(9, PathMetricDirection::ClientToServer),
        bps(80),
        retained_activation(1, identity_a, 160),
    );
    let decision_under_a1 = reducer.snapshot().expect("A1 decision");
    let delayed_a1 = native_proposal(&reducer, &activation_a1, operational(800));

    // Transport has already installed B/A2 and restored A/A3, but its FIFO
    // activation events have not yet reached the central reducer. This is the
    // decisive asynchronous gap: central `(A1, G1)` equality alone is true,
    // while an actual precommit against transport A3 must fail.
    let mut transport_at_a3 = transport_guard(reducer.scope, transport_activation(3));
    assert!(reducer.is_current(decision_under_a1.stamp()));
    assert!(matches!(
        reducer.authorize_native_precommit(decision_under_a1.stamp(), &mut transport_at_a3),
        Err(CarrierRateAuthorityError::ActiveTransportMismatch)
    ));
    assert_eq!(
        reducer.apply_native_observation(delayed_a1, &mut transport_at_a3),
        Err(CarrierRateAuthorityError::ActiveTransportMismatch)
    );
    assert_eq!(reducer.snapshot(), Some(decision_under_a1));

    let activation_b2 = reducer
        .install_native_controller(&activation_a1, fresh_activation(2, identity_b))
        .expect("install B entirely between consumer polls")
        .activation
        .expect("B2 capability");
    let activation_a3 = reducer
        .install_native_controller(&activation_b2, retained_activation(3, identity_a, 160))
        .expect("roll back A entirely between consumer polls")
        .activation
        .expect("A3 capability");

    let decision_under_a3 = reducer.snapshot().expect("next consumer poll");
    assert_eq!(
        decision_under_a3.stamp().native_activation(),
        Some(transport_activation(3))
    );
    assert_eq!(
        decision_under_a3.stamp().revision().as_u64(),
        decision_under_a1.stamp().revision().as_u64() + 2
    );
    assert!(!reducer.is_current(decision_under_a1.stamp()));
    assert!(reducer.is_current(decision_under_a3.stamp()));
    assert_eq!(
        apply_current_native(&mut reducer, &activation_a1, operational(800)),
        Err(CarrierRateAuthorityError::NativeActivationMismatch)
    );
    assert_eq!(
        apply_current_native(&mut reducer, &activation_b2, operational(800)),
        Err(CarrierRateAuthorityError::NativeActivationMismatch)
    );
    assert_eq!(
        apply_current_native(&mut reducer, &activation_a3, operational(800)),
        Ok(CarrierRateAuthorityTransition::Applied)
    );
}

#[test]
fn transport_activation_ids_are_never_reused() {
    let (mut reducer, activation_a1) = CarrierRateAuthorityReducer::new_native(
        scope(10, PathMetricDirection::ServerToClient),
        bps(80),
        fresh_activation(7, controller(1)),
    );
    let before = reducer.snapshot();
    assert!(matches!(
        reducer.install_native_controller(&activation_a1, fresh_activation(7, controller(2))),
        Err(CarrierRateAuthorityError::NativeActivationReused)
    ));
    assert!(matches!(
        reducer.install_native_controller(&activation_a1, fresh_activation(6, controller(2))),
        Err(CarrierRateAuthorityError::NativeActivationReused)
    ));
    assert_eq!(reducer.snapshot(), before);

    let activation_b8 = reducer
        .install_native_controller(&activation_a1, fresh_activation(8, controller(2)))
        .expect("fresh activation")
        .activation
        .expect("B8 capability");
    assert!(matches!(
        reducer.install_native_controller(&activation_b8, fresh_activation(7, controller(1))),
        Err(CarrierRateAuthorityError::NativeActivationReused)
    ));
}

#[test]
fn activation_capability_and_controller_identity_cannot_cross_reducer_instances() {
    let shared_scope = scope(3, PathMetricDirection::ClientToServer);
    let (first, first_activation) = CarrierRateAuthorityReducer::new_native(
        shared_scope,
        bps(80),
        fresh_activation(1, controller(1)),
    );
    let (mut second, _) = CarrierRateAuthorityReducer::new_native(
        shared_scope,
        bps(80),
        fresh_activation(1, controller(2)),
    );
    assert_ne!(first.stamp(), second.stamp());
    assert_eq!(first.stamp().native_controller(), Some(controller(1)));
    assert_eq!(second.stamp().native_controller(), Some(controller(2)));
    let before = second.snapshot();
    assert_eq!(
        apply_current_native(&mut second, &first_activation, operational(160)),
        Err(CarrierRateAuthorityError::AuthorityInstanceMismatch)
    );
    assert_eq!(second.snapshot(), before);
}

#[test]
fn structural_invalidation_is_fenced_one_way_and_nonpromoting() {
    let (mut reducer, activation) = CarrierRateAuthorityReducer::new_native(
        scope(4, PathMetricDirection::ServerToClient),
        bps(80),
        fresh_activation(1, controller(7)),
    );
    apply_current_native(&mut reducer, &activation, operational(40)).expect("native downshift");

    let stale_ready = ready_receipt(&reducer, &activation, generation(6));
    apply_current_native(&mut reducer, &activation, operational(48))
        .expect("intervening native update");
    let mut stale_active_transport =
        transport_guard(reducer.scope, activation.transport_activation);
    assert!(matches!(
        reducer.revoke_native_to_receipt(stale_ready, &mut stale_active_transport),
        Err(CarrierRateAuthorityError::StaleStamp)
    ));

    assert_eq!(
        apply_current_native(
            &mut reducer,
            &activation,
            NativeControllerObservation::Absent,
        ),
        Ok(CarrierRateAuthorityTransition::Unchanged),
        "absence remains distinct from structural invalidation"
    );
    let ready = ready_receipt(&reducer, &activation, generation(7));
    let mut active_transport = transport_guard(reducer.scope, activation.transport_activation);
    let switched = reducer
        .revoke_native_to_receipt(ready, &mut active_transport)
        .expect("fenced structural switch");
    assert_eq!(switched.transition, CarrierRateAuthorityTransition::Applied);
    let receipt = switched.receipt.expect("Receipt capability");
    assert_eq!(receipt.generation, generation(7));
    let projected = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::Receipt,
        CarrierRateAuthorityBasis::ReceiptFallback,
        48,
    );
    assert_eq!(projected.stamp().native_activation(), None);
    assert_eq!(projected.stamp().native_controller(), None);
    assert_eq!(
        apply_current_native(&mut reducer, &activation, operational(400)),
        Err(CarrierRateAuthorityError::WrongMode)
    );
    assert_eq!(reducer.snapshot(), Some(projected));
}

#[test]
fn native_to_receipt_requires_a_live_transport_activation_fence() {
    let authority_scope = scope(14, PathMetricDirection::ClientToServer);
    let (mut reducer, activation_a1) = CarrierRateAuthorityReducer::new_native(
        authority_scope,
        bps(80),
        fresh_activation(1, controller(1)),
    );
    let ready_under_a1 = ready_receipt(&reducer, &activation_a1, generation(1));
    let mut transport_already_at_a2 = transport_guard(authority_scope, transport_activation(2));
    let before = reducer.snapshot();

    assert!(matches!(
        reducer.revoke_native_to_receipt(ready_under_a1, &mut transport_already_at_a2),
        Err(CarrierRateAuthorityError::ActiveTransportMismatch)
    ));
    assert_eq!(reducer.snapshot(), before);
}

#[test]
fn receipt_terms_preserve_generation_and_never_reuse_identity() {
    let receipt_generation = generation(5);
    let (mut reducer, receipt) = CarrierRateAuthorityReducer::new_receipt(
        scope(5, PathMetricDirection::ClientToServer),
        bps(10),
        receipt_generation,
    );

    let wrong_generation = ReceiptTermPublication {
        authority_key: receipt.authority_key.duplicate(),
        expected: reducer.stamp(),
        generation: generation(6),
        term: ReceiptAuthorityTerm {
            id: term_id(1),
            rate: receipt_rate(30),
        },
    };
    assert!(matches!(
        reducer.publish_receipt_term(wrong_generation),
        Err(CarrierRateAuthorityError::ReceiptGenerationMismatch)
    ));
    let low = receipt_publication(&reducer, &receipt, 1, 10);
    assert!(matches!(
        reducer.publish_receipt_term(low),
        Err(CarrierRateAuthorityError::ReceiptRateDoesNotExceedFallback)
    ));

    let first_publication = receipt_publication(&reducer, &receipt, 1, 30);
    let first = reducer
        .publish_receipt_term(first_publication)
        .expect("publish first term");
    assert_eq!(first.transition, CarrierRateAuthorityTransition::Applied);
    let first_retirement = first.retirement.expect("first retirement capability");
    let first_snapshot = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::Receipt,
        CarrierRateAuthorityBasis::ReceiptTerm(ReceiptTermKey {
            generation: receipt_generation,
            term_id: term_id(1),
        }),
        30,
    );

    let replay_publication = receipt_publication(&reducer, &receipt, 1, 30);
    let exact_replay = reducer
        .publish_receipt_term(replay_publication)
        .expect("exact replay is idempotent");
    assert_eq!(
        exact_replay.transition,
        CarrierRateAuthorityTransition::Unchanged
    );
    assert!(exact_replay.retirement.is_none());
    assert_eq!(reducer.snapshot(), Some(first_snapshot));
    let mutation = receipt_publication(&reducer, &receipt, 1, 40);
    assert!(matches!(
        reducer.publish_receipt_term(mutation),
        Err(CarrierRateAuthorityError::ReceiptTermMismatch)
    ));

    let second_publication = receipt_publication(&reducer, &receipt, 2, 30);
    let second = reducer
        .publish_receipt_term(second_publication)
        .expect("same numeric rate under a new term is semantic");
    assert_eq!(second.transition, CarrierRateAuthorityTransition::Applied);
    let second_retirement = second.retirement.expect("second retirement capability");
    let second_snapshot = reducer.snapshot().expect("second term");
    assert!(second_snapshot.stamp().revision() > first_snapshot.stamp().revision());
    let CarrierRateAuthorityBasis::ReceiptTerm(second_key) = second_snapshot.basis() else {
        panic!("second term basis")
    };
    assert_eq!(second_key.generation(), receipt_generation);
    assert_eq!(second_key.term_id(), term_id(2));

    assert_eq!(
        reducer.retire_receipt_term(first_retirement),
        Err(CarrierRateAuthorityError::StaleStamp),
        "a superseded term's expiry cannot retire its successor"
    );
    assert_eq!(
        reducer.retire_receipt_term(second_retirement),
        Ok(CarrierRateAuthorityTransition::Applied)
    );
    let fallback = assert_projection(
        &reducer,
        CarrierRateAuthorityMode::Receipt,
        CarrierRateAuthorityBasis::ReceiptFallback,
        10,
    );
    let reused_two = receipt_publication(&reducer, &receipt, 2, 30);
    assert!(matches!(
        reducer.publish_receipt_term(reused_two),
        Err(CarrierRateAuthorityError::ReceiptTermReused)
    ));
    let reused_one = receipt_publication(&reducer, &receipt, 1, 50);
    assert!(matches!(
        reducer.publish_receipt_term(reused_one),
        Err(CarrierRateAuthorityError::ReceiptTermReused)
    ));
    assert_eq!(reducer.snapshot(), Some(fallback));

    let third_publication = receipt_publication(&reducer, &receipt, 3, 30);
    let third = reducer
        .publish_receipt_term(third_publication)
        .expect("strictly newer term remains eligible");
    assert_eq!(third.transition, CarrierRateAuthorityTransition::Applied);
}

#[test]
fn receipt_capability_cannot_cross_reducer_instances() {
    let shared_scope = scope(6, PathMetricDirection::ServerToClient);
    let (first, first_receipt) =
        CarrierRateAuthorityReducer::new_receipt(shared_scope, bps(10), generation(1));
    let (mut second, _) =
        CarrierRateAuthorityReducer::new_receipt(shared_scope, bps(10), generation(1));
    let cross_instance = ReceiptTermPublication {
        authority_key: first_receipt.authority_key.duplicate(),
        expected: second.stamp(),
        generation: first_receipt.generation,
        term: ReceiptAuthorityTerm {
            id: term_id(1),
            rate: receipt_rate(20),
        },
    };
    let before = second.snapshot();
    assert!(matches!(
        second.publish_receipt_term(cross_instance),
        Err(CarrierRateAuthorityError::AuthorityInstanceMismatch)
    ));
    assert_eq!(second.snapshot(), before);
    assert!(first.snapshot().is_some());
}

#[test]
fn revision_exhaustion_and_explicit_terminal_are_absorbing() {
    let (mut exhausted, activation) = CarrierRateAuthorityReducer::new_native(
        scope(7, PathMetricDirection::ClientToServer),
        bps(8),
        fresh_activation(1, controller(1)),
    );
    exhausted.set_revision_for_test(u64::MAX - 1);
    let last_live_stamp = exhausted.stamp();
    assert_eq!(
        apply_current_native(&mut exhausted, &activation, operational(16)),
        Ok(CarrierRateAuthorityTransition::Terminal)
    );
    assert_eq!(exhausted.snapshot(), None);
    assert_eq!(exhausted.stamp().revision().as_u64(), u64::MAX);
    assert_eq!(exhausted.stamp().native_controller(), None);
    assert!(!exhausted.is_current(last_live_stamp));
    assert_eq!(
        apply_current_native(&mut exhausted, &activation, operational(24)),
        Err(CarrierRateAuthorityError::Terminal)
    );

    let (mut terminated, _) = CarrierRateAuthorityReducer::new_native(
        scope(8, PathMetricDirection::ServerToClient),
        bps(8),
        fresh_activation(1, controller(2)),
    );
    assert_eq!(
        terminated.terminate(),
        Ok(CarrierRateAuthorityTransition::Terminal)
    );
    assert_eq!(terminated.snapshot(), None);
    assert_eq!(
        terminated.terminate(),
        Err(CarrierRateAuthorityError::Terminal)
    );

    let exhaustion_scope = scope(15, PathMetricDirection::ClientToServer);
    let last_live = transport_activation(u64::MAX - 1);
    let (mut activation_exhausted, last_activation) = CarrierRateAuthorityReducer::new_native(
        exhaustion_scope,
        bps(8),
        NativeControllerActivationEvent {
            transport_activation: last_live,
            controller: controller(3),
            observation: NativeControllerObservation::Absent,
        },
    );
    assert_eq!(
        activation_exhausted.terminate_native_transport_exhaustion(
            &last_activation,
            NativeTransportActivationExhausted {
                scope: exhaustion_scope,
                last_live_activation: last_live,
            },
        ),
        Ok(CarrierRateAuthorityTransition::Terminal)
    );
    assert_eq!(activation_exhausted.snapshot(), None);
    assert_eq!(activation_exhausted.stamp().revision().as_u64(), u64::MAX);
}

fn facade_source(
    authority_scope: CarrierRateAuthorityScope,
    activation: u64,
    identity: u64,
    operational_rate: Option<u128>,
) -> NativeCarrierRateSourceSnapshot {
    NativeCarrierRateSourceSnapshot::checked_from_bits_per_second(
        authority_scope,
        activation,
        identity,
        operational_rate,
    )
    .expect("checked coherent Native source")
}

fn facade_current(
    authority_scope: CarrierRateAuthorityScope,
    activation: u64,
) -> NativeCarrierTransportCurrent {
    NativeCarrierTransportCurrent::checked_from_raw(authority_scope, activation)
        .expect("checked current transport activation")
}

#[test]
fn native_facade_checks_exact_input_and_orders_same_activation_publication() {
    let authority_scope = scope(21, PathMetricDirection::ClientToServer);
    assert_eq!(
        NativeCarrierRateSourceSnapshot::checked_from_bits_per_second(authority_scope, 0, 1, None)
            .expect_err("zero A"),
        NativeCarrierRateInputError::TransportActivationZero
    );
    assert_eq!(
        NativeCarrierRateSourceSnapshot::checked_from_bits_per_second(
            authority_scope,
            u64::MAX,
            1,
            None,
        )
        .expect_err("terminal A"),
        NativeCarrierRateInputError::TransportActivationExhausted
    );
    assert_eq!(
        NativeCarrierRateSourceSnapshot::checked_from_bits_per_second(authority_scope, 1, 0, None)
            .expect_err("zero I"),
        NativeCarrierRateInputError::ControllerIdentityZero
    );
    assert_eq!(
        NativeCarrierRateSourceSnapshot::checked_from_bits_per_second(
            authority_scope,
            1,
            1,
            Some(9),
        )
        .expect_err("non-byte-aligned B_op"),
        NativeCarrierRateInputError::OperationalRate(CarrierRateConversionError::NotByteAligned)
    );
    assert_eq!(
        NativeCarrierRateSourceSnapshot::checked_from_bits_per_second(
            authority_scope,
            1,
            1,
            Some(u128::from(u64::MAX) + 1),
        )
        .expect_err("out-of-range B_op"),
        NativeCarrierRateInputError::OperationalRate(CarrierRateConversionError::OutOfRange)
    );

    let mut authority = NativeCarrierRateAuthority::new(
        authority_scope,
        startup(authority_scope, 80),
        facade_source(authority_scope, 1, 7, Some(100_000_000)),
    )
    .expect("scope-bound authority");
    let initialized = authority.snapshot().expect("initialized authority");
    assert_eq!(initialized.finite_rate_bps(), Some(100_000_000));
    assert_eq!(
        initialized.basis(),
        CarrierRateAuthorityBasis::NativeOperational
    );

    let absence = authority
        .capture_publication_ticket()
        .expect("same-A absence ticket");
    assert_eq!(
        authority.compare_apply(
            absence,
            facade_source(authority_scope, 1, 7, None),
            facade_current(authority_scope, 1),
        ),
        Ok(CarrierRateAuthorityTransition::Unchanged)
    );
    assert_eq!(authority.snapshot(), Some(initialized));

    let delayed = authority
        .capture_publication_ticket()
        .expect("delayed same-A ticket");
    let accepted = authority
        .capture_publication_ticket()
        .expect("accepted same-A ticket");
    assert_eq!(
        authority.compare_apply(
            accepted,
            facade_source(authority_scope, 1, 7, Some(160)),
            facade_current(authority_scope, 1),
        ),
        Ok(CarrierRateAuthorityTransition::Applied)
    );
    assert_eq!(authority.snapshot().unwrap().finite_rate_bps(), Some(160));
    assert_eq!(
        authority.compare_apply(
            delayed,
            facade_source(authority_scope, 1, 7, Some(40)),
            facade_current(authority_scope, 1),
        ),
        Err(CarrierRateAuthorityError::StaleStamp)
    );
    assert_eq!(authority.snapshot().unwrap().finite_rate_bps(), Some(160));
}

#[test]
fn native_facade_coalesces_current_activation_without_inheriting_old_rate() {
    let authority_scope = scope(22, PathMetricDirection::ServerToClient);
    let mut authority = NativeCarrierRateAuthority::new(
        authority_scope,
        startup(authority_scope, 80),
        facade_source(authority_scope, 1, 9, Some(320)),
    )
    .expect("scope-bound authority");
    let a1 = authority.snapshot().expect("A1");
    let coalesced = authority
        .capture_publication_ticket()
        .expect("A1 expected-G ticket");
    assert_eq!(
        authority.compare_apply(
            coalesced,
            facade_source(authority_scope, 3, 9, None),
            facade_current(authority_scope, 3),
        ),
        Ok(CarrierRateAuthorityTransition::Applied),
        "an unobserved A2 does not prevent coherent catch-up to current A3"
    );
    let a3 = authority.snapshot().expect("A3");
    assert_eq!(
        a3.stamp().native_activation(),
        Some(transport_activation(3))
    );
    assert_eq!(a3.basis(), CarrierRateAuthorityBasis::StartupPrior);
    assert_eq!(a3.finite_rate_bps(), Some(80));
    assert!(a3.stamp().revision() > a1.stamp().revision());

    let before_stale = authority.snapshot();
    let stale_a2 = authority
        .capture_publication_ticket()
        .expect("current central ticket");
    assert_eq!(
        authority.compare_apply(
            stale_a2,
            facade_source(authority_scope, 2, 10, Some(800)),
            facade_current(authority_scope, 3),
        ),
        Err(CarrierRateAuthorityError::ActiveTransportMismatch),
        "a stale A2 source cannot be relabeled with current transport A3"
    );
    assert_eq!(authority.snapshot(), before_stale);
}

#[test]
fn native_facade_precommit_and_tickets_are_instance_fenced() {
    let shared_scope = scope(23, PathMetricDirection::ClientToServer);
    let mut first = NativeCarrierRateAuthority::new(
        shared_scope,
        startup(shared_scope, 80),
        facade_source(shared_scope, 1, 1, Some(160)),
    )
    .expect("first scope-bound authority");
    let mut second = NativeCarrierRateAuthority::new(
        shared_scope,
        startup(shared_scope, 80),
        facade_source(shared_scope, 1, 1, Some(160)),
    )
    .expect("second scope-bound authority");
    let cross_instance = first
        .capture_publication_ticket()
        .expect("first reducer ticket");
    assert_eq!(
        second.compare_apply(
            cross_instance,
            facade_source(shared_scope, 1, 1, Some(320)),
            facade_current(shared_scope, 1),
        ),
        Err(CarrierRateAuthorityError::AuthorityInstanceMismatch)
    );

    let decision = first.stamp();
    let mut ran = false;
    assert_eq!(
        first.commit_if_current(decision, facade_current(shared_scope, 2), || ran = true),
        Err(CarrierRateAuthorityError::ActiveTransportMismatch)
    );
    assert!(!ran, "a stale transport activation must fence the closure");
    assert_eq!(
        first.commit_if_current(decision, facade_current(shared_scope, 1), || ran = true),
        Ok(())
    );
    assert!(ran);

    let update = first
        .capture_publication_ticket()
        .expect("same-A update ticket");
    assert_eq!(
        first.compare_apply(
            update,
            facade_source(shared_scope, 1, 1, Some(320)),
            facade_current(shared_scope, 1),
        ),
        Ok(CarrierRateAuthorityTransition::Applied)
    );
    ran = false;
    assert_eq!(
        first.commit_if_current(decision, facade_current(shared_scope, 1), || ran = true),
        Err(CarrierRateAuthorityError::StaleStamp)
    );
    assert!(!ran, "a stale central G must also fence the closure");
}

#[test]
fn native_facade_rejects_cross_scope_source_and_current_proofs() {
    let authority_scope = scope(25, PathMetricDirection::ClientToServer);
    let foreign_scope = scope(26, PathMetricDirection::ClientToServer);
    assert!(matches!(
        NativeCarrierRateAuthority::new(
            authority_scope,
            startup(authority_scope, 80),
            facade_source(foreign_scope, 1, 1, Some(160)),
        ),
        Err(CarrierRateAuthorityError::AuthorityScopeMismatch)
    ));

    let mut authority = NativeCarrierRateAuthority::new(
        authority_scope,
        startup(authority_scope, 80),
        facade_source(authority_scope, 1, 1, Some(160)),
    )
    .expect("scope-bound authority");
    let before = authority.snapshot();
    let foreign_source = authority
        .capture_publication_ticket()
        .expect("foreign-source ticket");
    assert_eq!(
        authority.compare_apply(
            foreign_source,
            facade_source(foreign_scope, 1, 1, Some(320)),
            facade_current(foreign_scope, 1),
        ),
        Err(CarrierRateAuthorityError::AuthorityScopeMismatch)
    );
    assert_eq!(authority.snapshot(), before);

    let foreign_current = authority
        .capture_publication_ticket()
        .expect("foreign-current ticket");
    assert_eq!(
        authority.compare_apply(
            foreign_current,
            facade_source(authority_scope, 1, 1, Some(320)),
            facade_current(foreign_scope, 1),
        ),
        Err(CarrierRateAuthorityError::AuthorityScopeMismatch)
    );
    assert_eq!(authority.snapshot(), before);

    let decision = authority.stamp();
    let mut ran = false;
    assert_eq!(
        authority.commit_if_current(decision, facade_current(foreign_scope, 1), || ran = true),
        Err(CarrierRateAuthorityError::AuthorityScopeMismatch)
    );
    assert!(
        !ran,
        "equal raw A from another fence cannot authorize ownership"
    );
}

#[test]
fn native_facade_checked_exhaustion_is_absorbing() {
    let authority_scope = scope(24, PathMetricDirection::ServerToClient);
    assert_eq!(
        NativeCarrierTransportExhaustion::checked_after_last_live(authority_scope, u64::MAX - 2,)
            .expect_err("only MAX-1 is the last live activation"),
        NativeCarrierRateInputError::ExhaustionBeforeLastLiveActivation
    );
    let mut authority = NativeCarrierRateAuthority::new(
        authority_scope,
        startup(authority_scope, 8),
        facade_source(authority_scope, u64::MAX - 1, 1, None),
    )
    .expect("scope-bound authority");
    let ticket = authority
        .capture_publication_ticket()
        .expect("last-live ticket");
    let exhaustion =
        NativeCarrierTransportExhaustion::checked_after_last_live(authority_scope, u64::MAX - 1)
            .expect("checked exhaustion");
    assert_eq!(
        authority.terminate_transport_exhaustion(ticket, exhaustion),
        Ok(CarrierRateAuthorityTransition::Terminal)
    );
    assert_eq!(authority.snapshot(), None);
    assert_eq!(authority.stamp().revision().as_u64(), u64::MAX);
    assert!(matches!(
        authority.capture_publication_ticket(),
        Err(CarrierRateAuthorityError::Terminal)
    ));

    let lagging_scope = scope(27, PathMetricDirection::ServerToClient);
    let mut lagging = NativeCarrierRateAuthority::new(
        lagging_scope,
        startup(lagging_scope, 8),
        facade_source(lagging_scope, u64::MAX - 2, 2, Some(80)),
    )
    .expect("lagging scope-bound authority");
    let lagging_ticket = lagging
        .capture_publication_ticket()
        .expect("pre-exhaustion central ticket");
    let terminal_transport =
        NativeCarrierTransportExhaustion::checked_after_last_live(lagging_scope, u64::MAX - 1)
            .expect("transport reached its final live activation before exhaustion");
    assert_eq!(
        lagging.terminate_transport_exhaustion(lagging_ticket, terminal_transport),
        Ok(CarrierRateAuthorityTransition::Terminal),
        "terminal transport proof must close a coordinator that has not yet published MAX-1"
    );
    assert_eq!(lagging.snapshot(), None);
}
