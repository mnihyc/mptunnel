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
