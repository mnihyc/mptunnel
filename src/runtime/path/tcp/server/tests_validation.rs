use super::complete_before_optional_deadline;

#[tokio::test]
async fn absolute_validation_ceiling_preempts_ready_and_blocked_work() {
    let expired = tokio::time::Instant::now();
    assert!(
        complete_before_optional_deadline(expired.into(), std::future::pending::<()>())
            .await
            .is_none(),
        "an in-progress actor operation cannot outlive the absolute validation ceiling",
    );
    assert!(
        complete_before_optional_deadline(expired.into(), std::future::ready(()))
            .await
            .is_none(),
        "already-ready work cannot starve an expired absolute validation ceiling",
    );
}
