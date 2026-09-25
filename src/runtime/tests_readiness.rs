use super::*;
use std::time::Duration;

#[tokio::test]
async fn barrier_notifies_once_every_required_service_is_ready() {
    let generation = RuntimeGenerationControl::new();
    let barrier = RuntimeReadinessBarrier::new(generation.clone());
    let first = barrier.require("first listener");
    let second = barrier.require("second listener");
    barrier.seal();

    first.ready();
    assert_eq!(generation.status().phase, RuntimeGenerationPhase::Starting);

    let waiter_generation = generation.clone();
    let waiter = tokio::spawn(async move { waiter_generation.wait_until_ready().await });
    second.ready();
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("readiness notification")
        .expect("readiness task")
        .expect("ready generation");
    assert_eq!(generation.status().phase, RuntimeGenerationPhase::Ready);
}

#[tokio::test]
async fn dropped_required_service_fails_instead_of_reporting_ready() {
    let generation = RuntimeGenerationControl::new();
    let barrier = RuntimeReadinessBarrier::new(generation.clone());
    let required = barrier.require("failed listener");
    barrier.seal();
    drop(required);

    let error = generation
        .wait_until_ready()
        .await
        .expect_err("failed listener cannot satisfy readiness");
    assert!(matches!(
        error,
        RuntimeGenerationReadinessError::Failed(Some(_))
    ));
    let status = generation.status();
    assert_eq!(status.phase, RuntimeGenerationPhase::Failed);
    assert!(
        status
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("failed listener"))
    );
}

#[tokio::test]
async fn stopping_is_terminal_for_late_readiness() {
    let generation = RuntimeGenerationControl::new();
    let barrier = RuntimeReadinessBarrier::new(generation.clone());
    let required = barrier.require("listener");
    barrier.seal();
    generation.mark_stopping();
    required.ready();

    assert_eq!(
        generation
            .wait_until_ready()
            .await
            .expect_err("stopping generation is not ready"),
        RuntimeGenerationReadinessError::Stopping
    );
    assert_eq!(generation.status().phase, RuntimeGenerationPhase::Stopping);
}

#[tokio::test]
async fn shutdown_is_terminal_and_upgrades_a_pending_reload() {
    let generation = RuntimeGenerationControl::new();
    generation.request_reload();
    assert_eq!(
        generation.stop_reason(),
        Some(RuntimeGenerationStopReason::ReloadRequested)
    );

    generation.request_shutdown();
    assert_eq!(
        generation.wait_for_stop().await,
        RuntimeGenerationStopReason::ShutdownRequested
    );
    assert_eq!(generation.status().phase, RuntimeGenerationPhase::Stopping);
}

#[tokio::test]
async fn deferred_retirement_waits_for_explicit_authorization() {
    let generation = RuntimeGenerationControl::new();
    generation.defer_retirement();
    generation.request_shutdown();

    let waiter_generation = generation.clone();
    let waiter = tokio::spawn(async move {
        waiter_generation.wait_for_retirement_authorization().await;
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    generation.authorize_retirement();
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("retirement authorization")
        .expect("retirement waiter");
}

#[tokio::test]
async fn deferred_observer_activation_waits_for_configuration_commit() {
    let generation = RuntimeGenerationControl::new();
    generation.defer_activation();
    generation.mark_ready();
    assert!(generation.is_ready());
    assert!(!generation.is_activated());

    let observed = generation.clone();
    let waiter = tokio::spawn(async move { observed.wait_until_activated().await });
    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "readiness must not send candidate hooks"
    );
    assert!(generation.activate());
    assert!(!generation.activate(), "publication is once per generation");
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("activation notification")
        .expect("activation waiter")
        .expect("active generation");
}

#[tokio::test]
async fn rejected_candidate_never_activates_observers() {
    let generation = RuntimeGenerationControl::new();
    generation.defer_activation();
    generation.mark_ready();
    generation.request_shutdown();
    assert!(!generation.activate());
    assert!(!generation.is_activated());
    assert_eq!(
        generation.wait_until_activated().await,
        Err(RuntimeGenerationReadinessError::Stopping)
    );
}

#[tokio::test]
async fn standalone_observers_activate_at_readiness_and_failures_wake_waiters() {
    let ordinary = RuntimeGenerationControl::new();
    assert!(!ordinary.activate(), "starting is not ready");
    ordinary.mark_ready();
    ordinary
        .wait_until_activated()
        .await
        .expect("standalone publication");

    let failed = RuntimeGenerationControl::new();
    failed.defer_activation();
    failed.mark_failed("listener unavailable");
    assert!(matches!(
        failed.wait_until_activated().await,
        Err(RuntimeGenerationReadinessError::Failed(_))
    ));
    assert!(!failed.activate());
}
