use super::*;
use crate::model::work::CarrierWorkKind;

#[test]
fn only_original_connection_data_owns_a_sequence_range() {
    assert!(CarrierWorkKind::OriginalData.is_original_transmission());
    assert!(
        !CarrierWorkKind::ReinjectedData.is_original_transmission(),
        "a reinjected copy must not create a second range owner or delivery sample"
    );
}

#[test]
fn optional_reinjection_accounting_target_is_percent_plus_startup_floor() {
    let budget = OptionalReinjectionBudget::new(
        1_000_000,
        49_999,
        1024,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );

    assert_eq!(budget.limit_bytes(), 51_024);
    assert_eq!(budget.remaining_bytes(), 1025);
}

#[test]
fn optional_reinjection_ledger_keeps_original_and_duplicate_bytes_separate() {
    let mut ledger = OptionalReinjectionLedger::default();
    ledger.record_delivered_data(1_000_000);
    ledger.record_reinjection(300);

    assert_eq!(ledger.delivered_data_bytes(), 1_000_000);
    assert_eq!(ledger.reinjected_bytes(), 300);
    assert_eq!(
        ledger
            .budget(
                1024,
                MppPerformanceConfig {
                    optional_reinjection_budget_percent: 5
                }
            )
            .limit_bytes(),
        51_024
    );
}

#[test]
fn live_owner_frontier_epoch_retains_one_non_accumulating_successor_observation() {
    let started = Instant::now();
    let interval = Duration::from_millis(200);
    let mut epoch = LiveOwnerFrontierFloorEpoch::default();

    assert!(epoch.attempt_ready(started));
    assert_eq!(epoch.next_attempt_at(), None);
    epoch.record_data_ack_progress_at_for_test(started + Duration::from_millis(10));
    assert_eq!(
        epoch.next_attempt_at(),
        None,
        "progress before the first accepted repair must not invent a timer",
    );

    epoch.record_accepted_attempt_at_for_test(started, interval);
    let first_deadline = started + interval;
    assert_eq!(epoch.next_attempt_at(), Some(first_deadline));
    assert!(!epoch.attempt_ready(started + Duration::from_millis(50)));

    // Queue removal, a target switch, or a tail-to-gap evidence transition all
    // merely reevaluate this same direction; none supplies an input capable of
    // moving its retained successor observation.
    epoch.record_accepted_attempt_at_for_test(started + Duration::from_millis(50), interval);
    assert_eq!(epoch.next_attempt_at(), Some(first_deadline));
    assert!(!epoch.attempt_ready(first_deadline - Duration::from_nanos(1)));
    assert!(epoch.attempt_ready(first_deadline));

    // Polling across many expired intervals does not accumulate successor
    // observations. The next accepted recovery starts exactly one interval
    // from its own acceptance time.
    let late_retry = started + Duration::from_secs(2);
    assert!(epoch.attempt_ready(late_retry));
    assert!(epoch.attempt_ready(late_retry));
    epoch.record_accepted_attempt_at_for_test(late_retry, interval);
    assert_eq!(epoch.next_attempt_at(), Some(late_retry + interval));

    let progress_at = late_retry + Duration::from_millis(50);
    epoch.record_data_ack_progress_at_for_test(progress_at);
    assert_eq!(
        epoch.next_attempt_at(),
        Some(progress_at + interval),
        "contiguous Data-ACK frontier progress restarts the full quiet interval",
    );
}

#[test]
fn live_owner_batch_uses_the_maximum_accepted_frame_interval() {
    let short = Duration::from_millis(20);
    let medium = Duration::from_millis(50);
    let long = Duration::from_millis(100);

    let forward = [short, long, medium]
        .into_iter()
        .fold(None, |current, interval| {
            Some(include_live_owner_recovery_interval(current, interval))
        });
    let reverse = [medium, long, short]
        .into_iter()
        .fold(None, |current, interval| {
            Some(include_live_owner_recovery_interval(current, interval))
        });

    assert_eq!(forward, Some(long));
    assert_eq!(reverse, Some(long));
}

#[test]
fn live_owner_fallback_epoch_ignores_scoring_extent_and_metric_churn() {
    let assigned_at = Instant::now();
    let owner = 7_u8;
    let first_deadline = assigned_at + Duration::from_millis(80);
    let later_metric_deadline = assigned_at + Duration::from_millis(400);
    let mut epoch = LiveOwnerFallbackEpoch::default();

    assert_eq!(
        epoch.observe(
            crate::protocol::OffsetRange { start: 10, end: 42 },
            &[owner],
            crate::model::timing::ReliableDataAckGapTiming {
                assignment_at: assigned_at,
                loss_at: None,
                fallback_at: first_deadline,
            },
        ),
        first_deadline,
    );
    assert_eq!(
        epoch.observe(
            crate::protocol::OffsetRange {
                start: 10,
                end: 4096,
            },
            &[owner],
            crate::model::timing::ReliableDataAckGapTiming {
                assignment_at: assigned_at,
                loss_at: None,
                fallback_at: later_metric_deadline,
            },
        ),
        first_deadline,
        "a Q/M size change and worsened metric cannot postpone the same lowest owner assignment",
    );

    let later_assignment = assigned_at + Duration::from_millis(10);
    assert_eq!(
        epoch.observe(
            crate::protocol::OffsetRange {
                start: 10,
                end: 4096,
            },
            &[owner],
            crate::model::timing::ReliableDataAckGapTiming {
                assignment_at: later_assignment,
                loss_at: None,
                fallback_at: later_metric_deadline,
            },
        ),
        later_metric_deadline,
        "a genuinely later assignment participating in M starts a new causal epoch",
    );
}

#[test]
fn live_owner_recovery_wake_preserves_the_cause_branch() {
    let observed_at = Instant::now();
    let due_cause = observed_at - Duration::from_millis(1);
    let future_cause = observed_at + Duration::from_millis(50);
    let later_epoch = observed_at + Duration::from_millis(100);

    assert_eq!(
        live_owner_recovery_wake(Some(due_cause), Some(later_epoch), observed_at),
        LiveOwnerRecoveryWake {
            due: true,
            deadline: Some(later_epoch),
        },
        "a due retained cause remains actionable while preserving the later successor observation",
    );
    assert_eq!(
        live_owner_recovery_wake(Some(future_cause), Some(later_epoch), observed_at),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: Some(future_cause),
        },
        "the retained cause is the first actionable deadline",
    );
    assert_eq!(
        live_owner_recovery_wake(None, Some(later_epoch), observed_at),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: None,
        },
        "an epoch without a retained cause cannot create recovery authority",
    );
}

#[test]
fn live_owner_gap_recovery_wake_preserves_candidate_then_fallback() {
    let observed_at = Instant::now();
    let due_candidate = observed_at - Duration::from_millis(1);
    let future_candidate = observed_at + Duration::from_millis(50);
    let fallback = observed_at + Duration::from_millis(100);
    let later_epoch = observed_at + Duration::from_millis(150);

    assert_eq!(
        live_owner_gap_recovery_wake(
            Some(due_candidate),
            Some(fallback),
            Some(later_epoch),
            observed_at,
        ),
        LiveOwnerRecoveryWake {
            due: true,
            deadline: Some(later_epoch),
        },
        "a due authoritative-gap candidate keeps the early cause branch",
    );
    assert_eq!(
        live_owner_gap_recovery_wake(
            Some(future_candidate),
            Some(fallback),
            Some(later_epoch),
            observed_at,
        ),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: Some(future_candidate),
        },
        "the exact candidate remains the first actionable deadline",
    );
    assert_eq!(
        live_owner_gap_recovery_wake(None, Some(fallback), Some(later_epoch), observed_at,),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: Some(fallback),
        },
        "the retained owner fallback is the cause when no candidate exists",
    );

    assert_eq!(
        live_owner_gap_recovery_wake(
            Some(due_candidate),
            Some(fallback),
            Some(later_epoch),
            later_epoch,
        ),
        LiveOwnerRecoveryWake {
            due: true,
            deadline: None,
        },
        "past cause and epoch clocks remain durable due state",
    );
}
