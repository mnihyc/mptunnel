use super::*;
use crate::model::carrier_rate_authority::{
    CarrierRateAuthorityBasis, CarrierRateAuthorityTransition,
};
use crate::model::path::CarrierPathInstanceId;
use crate::protocol::PathMetricDirection;
use crate::runtime::path::commands::reliable_path_command_channels;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn scope(carrier: u64, direction: PathMetricDirection) -> CarrierRateAuthorityScope {
    CarrierRateAuthorityScope::new(CarrierPathInstanceId::from_raw(carrier), direction)
}

fn source(
    activation: u64,
    controller: u64,
    operational_rate_bps: Option<u128>,
) -> CoherentNativeCarrierSource {
    CoherentNativeCarrierSource::checked_for_test(activation, controller, operational_rate_bps)
        .expect("checked coherent test source")
}

fn shape(
    activation: u64,
    controller: u64,
    operational_rate_bps: Option<u128>,
    srtt_ms: u64,
    congestion_window: u64,
    bytes_in_flight: u64,
) -> CoherentNativeCarrierShape {
    CoherentNativeCarrierShape::checked_for_test(
        activation,
        controller,
        operational_rate_bps,
        Duration::from_millis(srtt_ms),
        Duration::from_millis(srtt_ms / 5),
        congestion_window,
        bytes_in_flight,
        1400,
        Some(240),
        false,
    )
    .expect("checked coherent test shape")
}

fn authority(
    authority_scope: CarrierRateAuthorityScope,
    initial: CoherentNativeCarrierSource,
) -> Arc<NativeCarrierRateAuthorityHandle> {
    NativeCarrierRateAuthorityHandle::new_for_test(authority_scope, 40, initial)
        .expect("checked native authority")
}

#[test]
fn binding_cell_is_single_across_clones_and_rejects_a_different_scope() {
    let binding = NativeCarrierRateAuthorityBinding::default();
    let cloned = binding.clone();
    let first_scope = scope(11, PathMetricDirection::ClientToServer);
    let first_candidate = authority(first_scope, source(1, 1, Some(80)));

    let (first, installed) = binding
        .install(first_scope, first_candidate)
        .expect("first binding");
    assert!(installed);

    let redundant_candidate = authority(first_scope, source(1, 1, Some(80)));
    let (from_clone, installed) = cloned
        .install(first_scope, redundant_candidate)
        .expect("same-scope binding through clone");
    assert!(!installed);
    assert!(Arc::ptr_eq(&first, &from_clone));
    assert!(Arc::ptr_eq(
        &first,
        &binding.get().expect("bound authority")
    ));

    let different_scope = scope(12, PathMetricDirection::ClientToServer);
    let error = cloned
        .install(
            different_scope,
            authority(different_scope, source(1, 1, Some(80))),
        )
        .expect_err("different physical scope must fail closed");
    assert_eq!(
        error,
        NativeCarrierRateAuthorityRuntimeError::BindingScopeMismatch {
            existing: first_scope,
            requested: different_scope,
        }
    );
}

#[test]
fn operational_rate_is_already_bits_per_second_and_is_not_converted_twice() {
    let handle = authority(
        scope(21, PathMetricDirection::ClientToServer),
        source(1, 3, Some(800)),
    );
    let snapshot = handle
        .snapshot()
        .expect("coordinator lock")
        .expect("live snapshot");

    assert_eq!(snapshot.rate().as_u64(), 800);
    assert_eq!(
        snapshot.basis(),
        CarrierRateAuthorityBasis::NativeOperational
    );
}

#[test]
fn scheduling_shape_binds_central_rate_to_one_exact_active_path_shape() {
    let authority_scope = scope(71, PathMetricDirection::ClientToServer);
    let handle = authority(authority_scope, source(1, 9, None));

    let snapshot = handle
        .scheduling_shape_for_test(authority_scope, shape(1, 9, None, 80, 64_000, 12_000))
        .expect("matching startup shape");

    assert_eq!(snapshot.rate_bps(), 40);
    assert_eq!(snapshot.basis(), CarrierRateAuthorityBasis::StartupPrior);
    assert_eq!(snapshot.srtt(), Duration::from_millis(80));
    assert_eq!(snapshot.rttvar(), Duration::from_millis(16));
    assert_eq!(snapshot.congestion_window(), 64_000);
    assert_eq!(snapshot.bytes_in_flight(), 12_000);
    assert_eq!(snapshot.pacing_rate_bps(), Some(240));
    assert!(!snapshot.app_limited());
    assert_eq!(snapshot.stamp(), handle.stamp().expect("central stamp"));
}

#[test]
fn scheduling_shape_uses_central_g_during_bounded_same_activation_publication_lag() {
    let authority_scope = scope(72, PathMetricDirection::ServerToClient);
    let handle = authority(authority_scope, source(1, 10, Some(80)));

    let snapshot = handle
        .scheduling_shape_for_test(authority_scope, shape(1, 10, Some(160), 50, 80_000, 20_000))
        .expect("same native controller lifetime remains coherent");

    assert_eq!(snapshot.rate_bps(), 80);
    assert_eq!(
        snapshot.basis(),
        CarrierRateAuthorityBasis::NativeOperational
    );
    assert_eq!(snapshot.congestion_window(), 80_000);
}

#[test]
fn scheduling_shape_rejects_same_lineage_from_a_different_activation() {
    let authority_scope = scope(73, PathMetricDirection::ClientToServer);
    let handle = authority(authority_scope, source(1, 11, Some(80)));
    let _ = handle
        .advance_transport_activation_for_test(2)
        .expect("transport installs A2");

    assert_eq!(
        handle
            .scheduling_shape_for_test(
                authority_scope,
                shape(2, 11, Some(160), 45, 90_000, 30_000),
            )
            .expect_err("same I cannot authorize a different A"),
        NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged,
    );
    assert_eq!(
        handle
            .scheduling_shape_for_test(authority_scope, shape(1, 11, Some(80), 55, 70_000, 10_000),)
            .expect_err("old A cannot pass the current transport fence"),
        NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged,
    );
}

#[test]
fn scheduling_shape_cache_fails_closed_across_g_until_cadence_refreshes_it() {
    let authority_scope = scope(74, PathMetricDirection::ServerToClient);
    let handle = authority(authority_scope, source(1, 12, Some(80)));
    let _ = handle
        .scheduling_shape_for_test(authority_scope, shape(1, 12, Some(80), 60, 64_000, 8_000))
        .expect("seed exact G1 shape");
    let first = handle
        .scheduling_shape_snapshot(authority_scope)
        .expect("cached G1 shape is current");
    assert_eq!(first.rate_bps(), 80);
    assert_eq!(first.srtt(), Duration::from_millis(60));

    handle
        .publish_observation_for_test(1, 12, Some(160))
        .expect("same-A Bop publication advances G");
    assert_eq!(
        handle
            .scheduling_shape_snapshot(authority_scope)
            .expect_err("G1 shape cannot combine with G2 rate"),
        NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged,
    );

    let _ = handle
        .scheduling_shape_for_test(
            authority_scope,
            shape(1, 12, Some(160), 45, 128_000, 16_000),
        )
        .expect("cadence refresh installs exact G2 shape");
    let second = handle
        .scheduling_shape_snapshot(authority_scope)
        .expect("cached G2 shape is current");
    assert_eq!(second.rate_bps(), 160);
    assert_eq!(second.srtt(), Duration::from_millis(45));
    assert_eq!(second.congestion_window(), 128_000);
}

#[test]
fn same_activation_absence_retains_the_accepted_native_rate() {
    let handle = authority(
        scope(22, PathMetricDirection::ServerToClient),
        source(1, 4, Some(320)),
    );
    let before = handle.stamp().expect("initial stamp");

    let publication = handle
        .refresh_at_activation_for_test(source(1, 4, None), 1)
        .expect("same-activation absence is valid");

    assert_eq!(
        publication.transition(),
        CarrierRateAuthorityTransition::Unchanged
    );
    let snapshot = publication.snapshot().expect("live snapshot");
    assert_eq!(snapshot.rate().as_u64(), 320);
    assert_eq!(snapshot.stamp(), before);
}

#[tokio::test]
async fn applied_authority_change_wakes_all_subscribers_with_the_new_stamp() {
    let authority_scope = scope(31, PathMetricDirection::ClientToServer);
    let handle = authority(authority_scope, source(1, 11, Some(80)));
    let mut first = handle.accepted_change_cursor();
    let mut second = handle.accepted_change_cursor();
    let initial = *first.borrow_and_update();
    let _ = second.borrow_and_update();

    let publication = handle
        .refresh_at_activation_for_test(source(1, 11, Some(160)), 1)
        .expect("accepted same-A rate change");
    assert_eq!(
        publication.transition(),
        CarrierRateAuthorityTransition::Applied
    );

    tokio::time::timeout(Duration::from_millis(100), first.changed())
        .await
        .expect("first subscriber wake")
        .expect("authority cursor remains open");
    tokio::time::timeout(Duration::from_millis(100), second.changed())
        .await
        .expect("second subscriber wake")
        .expect("authority cursor remains open");
    assert_ne!(publication.stamp(), initial);
    assert_eq!(*first.borrow_and_update(), publication.stamp());
    assert_eq!(*second.borrow_and_update(), publication.stamp());

    let decision = handle
        .decision_snapshot(authority_scope)
        .expect("subscriber rereads current authority");
    assert_eq!(decision.stamp(), publication.stamp());
    assert_eq!(decision.rate_bps(), 160);
}

#[tokio::test]
async fn unchanged_authority_publication_does_not_wake_cursor() {
    let handle = authority(
        scope(32, PathMetricDirection::ServerToClient),
        source(1, 12, Some(320)),
    );
    let mut cursor = handle.accepted_change_cursor();
    let initial = *cursor.borrow_and_update();

    let publication = handle
        .refresh_at_activation_for_test(source(1, 12, None), 1)
        .expect("same-A absence retains accepted rate");
    assert_eq!(
        publication.transition(),
        CarrierRateAuthorityTransition::Unchanged
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), cursor.changed())
            .await
            .is_err(),
        "Unchanged must not create a false authority-change wake"
    );
    assert_eq!(*cursor.borrow(), initial);
}

#[tokio::test]
async fn terminal_authority_change_wakes_cursor_and_decisions_fail_closed() {
    let authority_scope = scope(33, PathMetricDirection::ClientToServer);
    let handle = authority(authority_scope, source(u64::MAX - 2, 13, Some(80)));
    let mut cursor = handle.accepted_change_cursor();
    let _ = cursor.borrow_and_update();

    let publication = handle
        .terminate_exhaustion_for_test()
        .expect("terminal authority publication");
    tokio::time::timeout(Duration::from_millis(100), cursor.changed())
        .await
        .expect("terminal subscriber wake")
        .expect("authority cursor remains open");
    assert_eq!(
        publication.transition(),
        CarrierRateAuthorityTransition::Terminal
    );
    assert_eq!(*cursor.borrow_and_update(), publication.stamp());
    assert_eq!(publication.snapshot(), None);
    assert_eq!(
        handle
            .decision_snapshot(authority_scope)
            .expect_err("terminal authority cannot authorize a decision"),
        NativeCarrierRateAuthorityRuntimeError::Authority(CarrierRateAuthorityError::Terminal)
    );
}

#[test]
fn late_authority_change_subscriber_starts_at_the_latest_accepted_stamp() {
    let handle = authority(
        scope(34, PathMetricDirection::ClientToServer),
        source(1, 14, Some(80)),
    );
    let publication = handle
        .refresh_at_activation_for_test(source(1, 14, Some(240)), 1)
        .expect("accepted update before subscription");

    let cursor = handle.accepted_change_cursor();

    assert_eq!(*cursor.borrow(), publication.stamp());
}

#[test]
fn coalesced_a1_to_a3_publication_installs_only_the_current_source() {
    let handle = authority(
        scope(23, PathMetricDirection::ClientToServer),
        source(1, 5, Some(80)),
    );
    let initial_revision = handle.stamp().expect("initial stamp").revision().as_u64();

    let publication = handle
        .refresh_at_activation_for_test(source(3, 8, Some(240)), 3)
        .expect("coalesced current activation");
    let snapshot = publication.snapshot().expect("live snapshot");

    assert_eq!(
        publication.transition(),
        CarrierRateAuthorityTransition::Applied
    );
    assert_eq!(snapshot.rate().as_u64(), 240);
    assert_eq!(snapshot.stamp().revision().as_u64(), initial_revision + 1);
}

#[test]
fn source_older_than_the_fenced_transport_is_rejected_without_mutation() {
    let handle = authority(
        scope(24, PathMetricDirection::ServerToClient),
        source(1, 2, Some(80)),
    );
    handle
        .refresh_at_activation_for_test(source(3, 9, Some(240)), 3)
        .expect("advance central activation");
    let before = handle.snapshot().expect("snapshot lock");

    let error = handle
        .refresh_at_activation_for_test(source(2, 7, Some(160)), 3)
        .expect_err("stale source must not cross the current transport fence");

    assert_eq!(
        error,
        NativeCarrierRateAuthorityRuntimeError::TransportSourceChanged
    );
    assert_eq!(handle.snapshot().expect("snapshot lock"), before);
}

#[test]
fn source_from_another_connection_is_rejected_even_when_a_and_i_match() {
    let first = authority(
        scope(28, PathMetricDirection::ClientToServer),
        source(1, 7, Some(80)),
    );
    let second = authority(
        scope(29, PathMetricDirection::ClientToServer),
        source(1, 7, Some(80)),
    );
    let foreign = second.bind_source_for_test(source(1, 7, Some(160)));
    let before = first.snapshot().expect("snapshot lock");

    let error = first
        .refresh_bound_at_activation_for_test(foreign, 1)
        .expect_err("another connection's source capability must fail closed");

    assert_eq!(
        error,
        NativeCarrierRateAuthorityRuntimeError::TransportSourceBindingMismatch
    );
    assert_eq!(first.snapshot().expect("snapshot lock"), before);
}

#[test]
fn exhausted_fence_terminalizes_a_coordinator_lagging_at_max_minus_two() {
    let handle = authority(
        scope(27, PathMetricDirection::ClientToServer),
        source(u64::MAX - 2, 9, Some(80)),
    );

    let publication = handle
        .terminate_exhaustion_for_test()
        .expect("trusted exhausted fence proves the final live activation");

    assert_eq!(
        publication.transition(),
        CarrierRateAuthorityTransition::Terminal
    );
    assert_eq!(publication.snapshot(), None);
}

#[test]
fn precommit_closure_does_not_run_after_transport_activation_changes() {
    let handle = authority(
        scope(25, PathMetricDirection::ClientToServer),
        source(1, 2, Some(80)),
    );
    let decision = handle.stamp().expect("decision stamp");
    let calls = AtomicUsize::new(0);

    let error = handle
        .commit_under_live_activation(decision, 2, || {
            calls.fetch_add(1, Ordering::Relaxed);
        })
        .expect_err("a changed transport A must reject precommit");

    assert_eq!(
        error,
        NativeCarrierRateAuthorityRuntimeError::Authority(
            CarrierRateAuthorityError::ActiveTransportMismatch,
        )
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn precommit_closure_does_not_run_after_central_generation_changes() {
    let handle = authority(
        scope(26, PathMetricDirection::ServerToClient),
        source(1, 6, Some(80)),
    );
    let decision = handle.stamp().expect("decision stamp");
    handle
        .refresh_at_activation_for_test(source(1, 6, Some(160)), 1)
        .expect("same-source rate mutation");
    let calls = AtomicUsize::new(0);

    let error = handle
        .commit_under_live_activation(decision, 1, || {
            calls.fetch_add(1, Ordering::Relaxed);
        })
        .expect_err("a changed central G must reject precommit");

    assert_eq!(
        error,
        NativeCarrierRateAuthorityRuntimeError::Authority(CarrierRateAuthorityError::StaleStamp,)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn decision_snapshot_rejects_transport_activation_ahead_of_central_publication() {
    let authority_scope = scope(30, PathMetricDirection::ClientToServer);
    let handle = authority(authority_scope, source(1, 10, Some(80)));
    handle
        .advance_transport_activation_for_test(2)
        .expect("advance only the transport fence");

    let error = handle
        .decision_snapshot(authority_scope)
        .expect_err("a stale central A cannot authorize a scheduling decision");

    assert_eq!(
        error,
        NativeCarrierRateAuthorityRuntimeError::Authority(
            CarrierRateAuthorityError::ActiveTransportMismatch,
        )
    );
}

#[test]
fn generic_command_senders_default_to_no_native_authority() {
    let (commands, _receivers) = reliable_path_command_channels(2);

    assert!(commands.native_rate_authority().is_none());
    assert!(commands.clone().native_rate_authority().is_none());
}
