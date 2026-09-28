use super::*;
use crate::model::path::next_carrier_path_instance_id;
use crate::protocol::UnderlayProtocol;
use futures::FutureExt;

fn key(index: usize) -> RelayPathKey {
    RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index,
    }
}

fn submitted(owner: &InitialOpenAcquisition, ordinal: u8, successor: bool) -> InitialOpenBackend {
    let attempt = owner
        .launch_handle()
        .begin(
            ordinal,
            key(usize::from(ordinal)),
            Duration::from_secs(10),
            Duration::from_secs(10),
            successor,
            None,
        )
        .unwrap()
        .unwrap();
    let backend = attempt.begin_backend().unwrap();
    backend.bind(next_carrier_path_instance_id()).unwrap();
    backend.submitted().unwrap();
    backend
}

#[tokio::test(start_paused = true)]
async fn due_backend_waits_for_actual_successor_entry() {
    let deadline = Instant::now() + Duration::from_secs(30);
    let owner = InitialOpenAcquisition::new(SessionId(701), StreamId(801), deadline, 2);
    let first = submitted(&owner, 0, true);
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(first.deadline().unwrap(), deadline);
    assert!(!owner.has_admission());
    // Reserving/preparing a handle does not promote the original.
    let launch = owner.launch_handle();
    assert!(
        !owner.0.state.lock().unwrap().slots[0]
            .as_ref()
            .unwrap()
            .retained
    );
    let second = launch
        .begin(
            1,
            key(1),
            Duration::from_secs(10),
            Duration::from_secs(10),
            false,
            Some(0),
        )
        .unwrap()
        .unwrap();
    assert!(
        owner.0.state.lock().unwrap().slots[0]
            .as_ref()
            .unwrap()
            .retained
    );
    assert_eq!(first.admission().unwrap(), deadline);
    assert_eq!(
        second.timing().unwrap().0,
        Instant::now() + Duration::from_secs(10)
    );
}

#[tokio::test(start_paused = true)]
async fn late_max_is_admitted_and_fences_until_concrete_failure() {
    for publish_due_before_max in [false, true] {
        let deadline = Instant::now() + Duration::from_secs(30);
        let owner = InitialOpenAcquisition::new(SessionId(702), StreamId(802), deadline, 2);
        let first = submitted(&owner, 0, true);
        tokio::time::advance(Duration::from_secs(11)).await;
        if publish_due_before_max {
            assert_eq!(
                first.deadline().unwrap(),
                deadline,
                "coordinator may already have observed DecisionDue"
            );
        }
        let prepared = owner.launch_handle();
        assert_eq!(
            first.admission().unwrap(),
            deadline,
            "a first MAX after S but before T remains valid"
        );
        assert!(owner.has_admission());
        assert!(
            owner.0.state.lock().unwrap().slots[0]
                .as_ref()
                .unwrap()
                .retained
        );
        assert_eq!(first.deadline().unwrap(), deadline);
        assert!(
            prepared
                .begin(
                    1,
                    key(1),
                    Duration::from_secs(10),
                    Duration::from_secs(10),
                    false,
                    Some(0),
                )
                .unwrap()
                .is_none(),
            "the actual MAX fences this prepared successor"
        );
        assert_eq!(owner.started_ordinals(), [0]);

        owner.settle(0); // concrete native failure for this exact admitted owner
        assert!(!owner.has_admission());
        assert!(
            prepared
                .begin(
                    1,
                    key(1),
                    Duration::from_secs(10),
                    Duration::from_secs(10),
                    false,
                    Some(0),
                )
                .unwrap()
                .is_none(),
            "settling A cannot revive a launch prepared before its admission"
        );
        assert!(
            owner
                .launch_handle()
                .begin(
                    1,
                    key(1),
                    Duration::from_secs(10),
                    Duration::from_secs(10),
                    false,
                    None,
                )
                .unwrap()
                .is_some(),
            "a fresh decision can retry after the exact owner fails"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn admitted_zero_then_refusal_does_not_permanently_veto_retry() {
    let owner = InitialOpenAcquisition::new(
        SessionId(703),
        StreamId(803),
        Instant::now() + Duration::from_secs(30),
        2,
    );
    let first = submitted(&owner, 0, true);
    first.admission().unwrap(); // phase deliberately does not encode positive credit
    assert!(
        owner
            .launch_handle()
            .begin(
                1,
                key(1),
                Duration::from_secs(10),
                Duration::from_secs(10),
                false,
                Some(0),
            )
            .unwrap()
            .is_none()
    );
    owner.settle(0); // only the exact owner's actual failure settles admission
    assert!(
        owner
            .launch_handle()
            .begin(
                1,
                key(1),
                Duration::from_secs(10),
                Duration::from_secs(10),
                false,
                None,
            )
            .unwrap()
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn singleton_submission_survives_s_but_stale_setup_does_not() {
    let deadline = Instant::now() + Duration::from_secs(30);
    let owner = InitialOpenAcquisition::new(SessionId(704), StreamId(804), deadline, 2);
    let first = submitted(&owner, 0, false);
    let second = owner
        .launch_handle()
        .begin(
            1,
            key(1),
            Duration::from_secs(10),
            Duration::from_secs(10),
            true,
            None,
        )
        .unwrap()
        .unwrap();
    let stale = second.begin_backend().unwrap();
    let current = second.begin_backend().unwrap();
    assert!(matches!(
        stale.deadline(),
        Err(RuntimeError::ReliablePathRetired)
    ));
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        first.deadline().unwrap(),
        deadline,
        "fully submitted singleton survives its setup allowance"
    );
    assert!(
        current.deadline().unwrap() <= Instant::now(),
        "incomplete setup expires"
    );
    assert!(matches!(
        second.begin_backend(),
        Err(RuntimeError::PathOpenTimedOut)
    ));
    drop(owner);
    assert!(matches!(
        first.deadline(),
        Err(RuntimeError::ReliablePathRetired)
    ));
}

#[tokio::test(start_paused = true)]
async fn expired_backend_returns_one_stable_actor_stop() {
    let deadline = Instant::now() + Duration::from_secs(30);
    let owner = InitialOpenAcquisition::new(SessionId(705), StreamId(805), deadline, 1);
    let backend = submitted(&owner, 0, false);
    tokio::time::advance(Duration::from_secs(30)).await;
    let actor_now = Instant::now();
    let first = backend.deadline().unwrap();
    assert_eq!(first, actor_now);
    assert!(
        owner.exhaust_successors(0),
        "expiry at T must not be revived"
    );
    assert_eq!(backend.deadline().unwrap(), first);
    tokio::time::advance(Duration::from_secs(1)).await;
    let later = backend.deadline().unwrap();
    assert_eq!(
        later, first,
        "expiry does not move with each actor deadline query"
    );
    assert!(
        later <= actor_now,
        "the actor's captured expiry-before-routing comparison remains true"
    );
    owner.settle(0);
    assert!(
        !owner.exhaust_successors(0),
        "settled state cannot be revived"
    );
    assert_eq!(
        backend.deadline().unwrap(),
        first,
        "settlement cannot postpone an expired stop"
    );
}

#[tokio::test(start_paused = true)]
async fn retained_prior_admission_fences_prepared_nominal_successor_after_settlement() {
    let owner = InitialOpenAcquisition::new(
        SessionId(706),
        StreamId(806),
        Instant::now() + Duration::from_secs(40),
        3,
    );
    let first = submitted(&owner, 0, true);
    tokio::time::advance(Duration::from_secs(10)).await;
    first.deadline().unwrap();
    let second = owner
        .launch_handle()
        .begin(
            1,
            key(1),
            Duration::from_secs(10),
            Duration::from_secs(10),
            true,
            Some(0),
        )
        .unwrap()
        .unwrap();
    let second_backend = second.begin_backend().unwrap();
    second_backend
        .bind(next_carrier_path_instance_id())
        .unwrap();
    second_backend.submitted().unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    second_backend.deadline().unwrap();
    let prepared_third = owner.launch_handle();
    first.admission().unwrap();
    owner.settle(0); // actual retryable raw physical commit failure
    assert!(!owner.has_admission());
    assert!(
        prepared_third
            .begin(
                2,
                key(2),
                Duration::from_secs(10),
                Duration::from_secs(10),
                false,
                Some(1)
            )
            .unwrap()
            .is_none(),
        "retained A's MAX fences prepared C even though B is C's immediate predecessor"
    );
    assert_eq!(owner.unstarted_predecessor(2, Some(1)), Some(1));
    assert_eq!(owner.started_ordinals(), [0, 1]);
    assert!(
        owner
            .launch_handle()
            .begin(
                2,
                key(2),
                Duration::from_secs(10),
                Duration::from_secs(10),
                false,
                Some(1)
            )
            .unwrap()
            .is_some(),
        "a fresh decision after exact admission settlement is permitted"
    );
}

fn phase_budgets() -> (Duration, Duration) {
    let pto = crate::model::timing::default_transport_pto();
    let setup = pto.saturating_mul(crate::model::timing::path_open_serialized_exchanges(None));
    (setup, pto)
}

#[tokio::test(start_paused = true)]
async fn unsubmitted_setup_keeps_whole_allowance_then_full_submission_wakes_contraction() {
    let (setup, pto) = phase_budgets();
    let start = Instant::now();
    let owner = InitialOpenAcquisition::new(
        SessionId(710),
        StreamId(810),
        start + Duration::from_secs(30),
        2,
    );
    let attempt = owner
        .launch_handle()
        .begin(0, key(0), setup, pto, true, None)
        .unwrap()
        .unwrap();
    let backend = attempt.begin_backend().unwrap();
    assert_eq!(backend.deadline().unwrap(), start + setup);
    backend.bind(next_carrier_path_instance_id()).unwrap();
    // Binding/setup/partial local writes do not publish full submission.
    tokio::time::advance(pto / 2).await;
    assert_eq!(owner.decision_deadline(0), Some(start + setup));
    assert_eq!(backend.deadline().unwrap(), start + setup);
    let changed = owner.changed();
    tokio::pin!(changed);
    changed.as_mut().enable();
    backend.submitted().unwrap();
    assert!(
        changed.now_or_never().is_some(),
        "existing coordinator wake"
    );
    let decision = Instant::now() + pto;
    assert_eq!(owner.decision_deadline(0), Some(decision));
    assert_eq!(backend.deadline().unwrap(), decision);
    assert!(decision < start + setup);
    assert_eq!(attempt.timing().unwrap(), (start + setup, setup));
    // Another callback cannot refresh that absolute decision.
    tokio::time::advance(pto / 4).await;
    assert!(matches!(
        backend.submitted(),
        Err(RuntimeError::PathOpenTimedOut)
    ));
    assert_eq!(backend.deadline().unwrap(), decision);
}

#[tokio::test(start_paused = true)]
async fn unsubmitted_backend_still_expires_only_at_whole_setup_bound() {
    let (setup, pto) = phase_budgets();
    let start = Instant::now();
    let owner = InitialOpenAcquisition::new(
        SessionId(711),
        StreamId(811),
        start + Duration::from_secs(30),
        2,
    );
    let attempt = owner
        .launch_handle()
        .begin(0, key(0), setup, pto, false, None)
        .unwrap()
        .unwrap();
    let backend = attempt.begin_backend().unwrap();
    backend.bind(next_carrier_path_instance_id()).unwrap();
    tokio::time::advance(pto).await;
    assert_eq!(backend.deadline().unwrap(), start + setup);
    assert!(
        !owner.expire_unsubmitted(1),
        "unstarted alternative has no owner"
    );
    tokio::time::advance(setup - pto).await;
    assert_eq!(backend.deadline().unwrap(), Instant::now());
    assert!(matches!(
        backend.submitted(),
        Err(RuntimeError::PathOpenTimedOut)
    ));
    assert!(
        !owner.0.state.lock().unwrap().slots[0]
            .as_ref()
            .unwrap()
            .retained
    );
}

#[tokio::test(start_paused = true)]
async fn submitted_clock_cannot_slide_through_stale_carriers_or_backend_retries() {
    let (setup, pto) = phase_budgets();
    let start = Instant::now();
    let owner = InitialOpenAcquisition::new(
        SessionId(712),
        StreamId(812),
        start + Duration::from_secs(30),
        2,
    );
    let attempt = owner
        .launch_handle()
        .begin(0, key(0), setup, pto, true, None)
        .unwrap()
        .unwrap();
    let stale = attempt.begin_backend().unwrap();
    stale.bind(next_carrier_path_instance_id()).unwrap();
    let current = attempt.begin_backend().unwrap();
    let current_carrier = next_carrier_path_instance_id();
    current.bind(current_carrier).unwrap();
    assert!(matches!(
        stale.submitted(),
        Err(RuntimeError::ReliablePathRetired)
    ));
    assert_eq!(current.deadline().unwrap(), start + setup);
    tokio::time::advance(pto / 4).await;
    current.submitted().unwrap();
    let decision = Instant::now() + pto;
    assert!(matches!(
        current.bind(next_carrier_path_instance_id()),
        Err(RuntimeError::ReliablePathRetired)
    ));
    assert_eq!(current.deadline().unwrap(), decision);
    tokio::time::advance(pto / 4).await;
    let retry = attempt.begin_backend().unwrap();
    retry.bind(next_carrier_path_instance_id()).unwrap();
    assert_eq!(
        retry.deadline().unwrap(),
        start + setup,
        "retry setup retains immutable S"
    );
    assert_eq!(
        owner.decision_deadline(0),
        Some(start + setup),
        "Setup masks earlier D"
    );
    assert!(matches!(
        current.submitted(),
        Err(RuntimeError::ReliablePathRetired)
    ));
    retry.submitted().unwrap();
    assert_eq!(
        retry.deadline().unwrap(),
        decision,
        "new generation cannot restart the admission PTO"
    );
    tokio::time::advance(start + setup - Instant::now()).await;
    assert!(matches!(
        attempt.begin_backend(),
        Err(RuntimeError::PathOpenTimedOut)
    ));
    drop(owner);
    assert!(matches!(
        retry.submitted(),
        Err(RuntimeError::ReliablePathRetired)
    ));
}

#[tokio::test(start_paused = true)]
async fn late_full_submission_and_logical_clipping_never_extend_existing_bound() {
    let (setup, pto) = phase_budgets();
    for (setup_budget, logical_budget) in [(setup, setup + pto), (setup + pto, setup)] {
        let start = Instant::now();
        let logical = start + logical_budget;
        let owner = InitialOpenAcquisition::new(SessionId(713), StreamId(813), logical, 2);
        let attempt = owner
            .launch_handle()
            .begin(0, key(0), setup_budget, pto, true, None)
            .unwrap()
            .unwrap();
        let backend = attempt.begin_backend().unwrap();
        backend.bind(next_carrier_path_instance_id()).unwrap();
        let original = start + setup_budget.min(logical_budget);
        tokio::time::advance(original - Instant::now() - pto / 2).await;
        backend.submitted().unwrap();
        assert_eq!(owner.decision_deadline(0), Some(original));
        assert_eq!(attempt.timing().unwrap().0, original);
        tokio::time::advance(pto / 2).await;
        let logical_deadline_was_clipped = logical <= original;
        assert_eq!(
            owner.exhaust_successors(0),
            logical_deadline_was_clipped,
            "a submitted candidate keeps T unless T itself already expired"
        );
        if logical_deadline_was_clipped {
            assert_eq!(backend.deadline().unwrap(), original);
            assert!(matches!(
                backend.admission(),
                Err(RuntimeError::PathOpenTimedOut)
            ));
        } else {
            assert_eq!(backend.deadline().unwrap(), logical);
            assert_eq!(backend.admission().unwrap(), logical);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn singleton_and_final_submission_retain_to_logical_deadline() {
    let (_, pto) = phase_budgets();
    let terminal_budget = pto.saturating_mul(crate::model::timing::path_open_pto_multiplier(None));
    for (total, ordinal) in [(1, 0), (2, 1)] {
        let start = Instant::now();
        let logical = start + Duration::from_secs(30);
        let owner = InitialOpenAcquisition::new(SessionId(714), StreamId(814), logical, total);
        let attempt = owner
            .launch_handle()
            .begin(
                ordinal,
                key(usize::from(ordinal)),
                terminal_budget,
                pto,
                false,
                None,
            )
            .unwrap()
            .unwrap();
        let backend = attempt.begin_backend().unwrap();
        backend.bind(next_carrier_path_instance_id()).unwrap();
        backend.submitted().unwrap();
        assert_eq!(backend.deadline().unwrap(), logical);
        assert_eq!(owner.decision_deadline(ordinal), None);
        tokio::time::advance(pto).await;
        assert_eq!(backend.deadline().unwrap(), logical);
        tokio::time::advance(terminal_budget - pto).await;
        assert_eq!(backend.deadline().unwrap(), logical);
        assert_eq!(backend.admission().unwrap(), logical);
    }
}

#[tokio::test(start_paused = true)]
async fn early_due_preserves_first_max_authority_before_actual_successor_retention() {
    let (setup, pto) = phase_budgets();
    for max_first in [true, false] {
        let deadline = Instant::now() + Duration::from_secs(30);
        let owner = InitialOpenAcquisition::new(SessionId(715), StreamId(815), deadline, 2);
        let attempt = owner
            .launch_handle()
            .begin(0, key(0), setup, pto, true, None)
            .unwrap()
            .unwrap();
        let backend = attempt.begin_backend().unwrap();
        backend.bind(next_carrier_path_instance_id()).unwrap();
        backend.submitted().unwrap();
        tokio::time::advance(pto + Duration::from_nanos(1)).await;
        assert_eq!(
            backend.deadline().unwrap(),
            deadline,
            "Due awaits coordinator decision"
        );
        let prepared = owner.launch_handle();
        assert!(
            !owner.0.state.lock().unwrap().slots[0]
                .as_ref()
                .unwrap()
                .retained
        );
        if max_first {
            // Admission represents actual first MAX even when its value is zero.
            assert_eq!(backend.admission().unwrap(), deadline);
            assert!(owner.has_admission());
            assert!(
                prepared
                    .begin(1, key(1), setup, pto, false, Some(0))
                    .unwrap()
                    .is_none()
            );
            owner.settle(0);
            assert!(
                prepared
                    .begin(1, key(1), setup, pto, false, Some(0))
                    .unwrap()
                    .is_none()
            );
            assert!(
                owner
                    .launch_handle()
                    .begin(1, key(1), setup, pto, false, None)
                    .unwrap()
                    .is_some()
            );
        } else {
            assert!(
                prepared
                    .begin(1, key(1), setup, pto, false, Some(0))
                    .unwrap()
                    .is_some()
            );
            assert!(
                owner.0.state.lock().unwrap().slots[0]
                    .as_ref()
                    .unwrap()
                    .retained
            );
            assert_eq!(backend.admission().unwrap(), deadline);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn exhausted_early_decision_retains_full_submission_until_logical_deadline() {
    let (setup, pto) = phase_budgets();
    for receive_max in [false, true] {
        let start = Instant::now();
        let logical = start + Duration::from_secs(30);
        let owner = InitialOpenAcquisition::new(SessionId(717), StreamId(817), logical, 2);
        let attempt = owner
            .launch_handle()
            .begin(0, key(0), setup, pto, true, None)
            .unwrap()
            .unwrap();
        let backend = attempt.begin_backend().unwrap();
        backend.bind(next_carrier_path_instance_id()).unwrap();
        backend.submitted().unwrap();
        tokio::time::advance(pto).await;
        assert!(
            backend.deadline().unwrap() > start + setup,
            "Due custody is not retention"
        );
        assert!(
            !owner.exhaust_successors(0),
            "no successor cannot cancel at D"
        );
        assert_eq!(backend.deadline().unwrap(), logical);
        assert_eq!(
            owner.decision_deadline(0),
            None,
            "consumed D cannot busy-loop"
        );
        assert!(
            owner.0.state.lock().unwrap().slots[0]
                .as_ref()
                .unwrap()
                .retained
        );
        tokio::time::advance(start + setup + Duration::from_millis(1) - Instant::now()).await;
        assert_eq!(backend.deadline().unwrap(), logical);
        if receive_max {
            assert_eq!(backend.admission().unwrap(), logical);
            assert!(owner.has_admission());
            assert_eq!(owner.decision_deadline(0), None);
        } else {
            let pending = std::future::pending::<Result<(), RuntimeError>>();
            tokio::pin!(pending);
            let operation = backend.complete(pending.as_mut());
            tokio::pin!(operation);
            assert!(operation.as_mut().now_or_never().is_none());
            tokio::time::advance(logical - Instant::now()).await;
            assert!(matches!(
                operation.await,
                Err(RuntimeError::PathOpenTimedOut)
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn renewed_setup_fences_prepared_successor_and_preserves_its_retryability() {
    let (setup, pto) = phase_budgets();
    let start = Instant::now();
    let logical = start + Duration::from_secs(30);
    let owner = InitialOpenAcquisition::new(SessionId(718), StreamId(818), logical, 2);
    let attempt = owner
        .launch_handle()
        .begin(0, key(0), setup, pto, true, None)
        .unwrap()
        .unwrap();
    let first = attempt.begin_backend().unwrap();
    first.bind(next_carrier_path_instance_id()).unwrap();
    first.submitted().unwrap();
    tokio::time::advance(pto).await;
    first.deadline().unwrap();
    let prepared = owner.launch_handle();
    let retry = attempt.begin_backend().unwrap();
    assert!(
        !owner.expire_unsubmitted(0),
        "a decision sampled at D cannot expire new Setup before S"
    );
    assert!(
        prepared
            .begin(1, key(1), setup, pto, false, Some(0))
            .unwrap()
            .is_none()
    );
    assert_eq!(owner.started_ordinals(), [0]);
    assert_eq!(owner.unstarted_predecessor(1, Some(0)), Some(0));
    assert_eq!(
        owner.decision_deadline(0),
        Some(start + setup),
        "coordinator parks on S while Setup masks D"
    );
    assert_eq!(retry.deadline().unwrap(), start + setup);
    retry.bind(next_carrier_path_instance_id()).unwrap();
    tokio::time::advance(pto / 2).await;
    retry.submitted().unwrap();
    assert_eq!(
        owner.decision_deadline(0),
        Some(start + pto),
        "retry cannot slide D"
    );
    assert!(
        owner
            .launch_handle()
            .begin(1, key(1), setup, pto, false, Some(0))
            .unwrap()
            .is_some()
    );
    assert_eq!(owner.started_ordinals(), [0, 1]);
    assert_eq!(retry.admission().unwrap(), logical);
}

#[tokio::test(start_paused = true)]
async fn early_retained_retry_keeps_partial_setup_on_original_bound() {
    let (setup, pto) = phase_budgets();
    for resubmit in [false, true] {
        let start = Instant::now();
        let logical = start + Duration::from_secs(30);
        let owner = InitialOpenAcquisition::new(SessionId(719), StreamId(819), logical, 2);
        let attempt = owner
            .launch_handle()
            .begin(0, key(0), setup, pto, true, None)
            .unwrap()
            .unwrap();
        let first = attempt.begin_backend().unwrap();
        first.bind(next_carrier_path_instance_id()).unwrap();
        first.submitted().unwrap();
        tokio::time::advance(pto).await;
        assert!(
            owner
                .launch_handle()
                .begin(1, key(1), setup, pto, false, Some(0))
                .unwrap()
                .is_some()
        );
        assert_eq!(first.deadline().unwrap(), logical);
        let changed = owner.changed();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let retry = attempt.begin_backend().unwrap();
        assert!(
            changed.now_or_never().is_some(),
            "new Setup wakes old retained deadline users"
        );
        assert_eq!(retry.deadline().unwrap(), start + setup);
        assert_eq!(attempt.deadline().unwrap(), start + setup);
        assert!(matches!(
            first.submitted(),
            Err(RuntimeError::ReliablePathRetired)
        ));
        retry.bind(next_carrier_path_instance_id()).unwrap();
        tokio::time::advance(pto / 2).await;
        if resubmit {
            retry.submitted().unwrap();
            assert_eq!(retry.deadline().unwrap(), logical);
            assert_eq!(retry.admission().unwrap(), logical);
        } else {
            let pending = std::future::pending::<Result<(), RuntimeError>>();
            tokio::pin!(pending);
            let operation = retry.complete(pending.as_mut());
            tokio::pin!(operation);
            assert!(operation.as_mut().now_or_never().is_none());
            tokio::time::advance(start + setup - Instant::now()).await;
            assert!(
                matches!(operation.await, Err(RuntimeError::PathOpenTimedOut)),
                "partial native work cannot inherit retained T"
            );
            assert!(matches!(
                retry.submitted(),
                Err(RuntimeError::PathOpenTimedOut)
            ));
            assert!(matches!(
                attempt.begin_backend(),
                Err(RuntimeError::PathOpenTimedOut)
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn fully_submitted_final_candidate_keeps_logical_deadline() {
    let (setup, pto) = phase_budgets();
    let terminal_budget = setup.max(pto);
    for (total, ordinal) in [(1, 0), (2, 1)] {
        for receive_max_after_setup in [false, true] {
            let start = Instant::now();
            let logical = start + Duration::from_secs(30);
            let owner = InitialOpenAcquisition::new(SessionId(720), StreamId(820), logical, total);
            let attempt = owner
                .launch_handle()
                .begin(
                    ordinal,
                    key(usize::from(ordinal)),
                    terminal_budget,
                    pto,
                    false,
                    None,
                )
                .unwrap()
                .unwrap();
            let backend = attempt.begin_backend().unwrap();
            backend.bind(next_carrier_path_instance_id()).unwrap();
            backend.submitted().unwrap();

            assert_eq!(backend.deadline().unwrap(), logical);
            assert_eq!(owner.decision_deadline(ordinal), None);
            tokio::time::advance(terminal_budget + Duration::from_millis(1)).await;
            assert_eq!(
                backend.deadline().unwrap(),
                logical,
                "full submission has no fallback, so setup S no longer cancels it"
            );

            if receive_max_after_setup {
                assert_eq!(backend.admission().unwrap(), logical);
                assert!(owner.has_admission());
            } else {
                let pending = std::future::pending::<Result<(), RuntimeError>>();
                tokio::pin!(pending);
                let operation = backend.complete(pending.as_mut());
                tokio::pin!(operation);
                assert!(operation.as_mut().now_or_never().is_none());
                tokio::time::advance(logical - Instant::now()).await;
                assert!(matches!(
                    operation.await,
                    Err(RuntimeError::PathOpenTimedOut)
                ));
                assert!(matches!(
                    backend.admission(),
                    Err(RuntimeError::PathOpenTimedOut)
                ));
                assert!(!owner.has_admission(), "MAX at T cannot revive the open");
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn exhausted_successor_search_retains_submitted_candidate_until_logical_deadline() {
    let (setup, pto) = phase_budgets();
    for receive_max_after_setup in [false, true] {
        let start = Instant::now();
        let logical = start + Duration::from_secs(30);
        let owner = InitialOpenAcquisition::new(SessionId(721), StreamId(821), logical, 2);
        let attempt = owner
            .launch_handle()
            .begin(0, key(0), setup, pto, true, None)
            .unwrap()
            .unwrap();
        let backend = attempt.begin_backend().unwrap();
        backend.bind(next_carrier_path_instance_id()).unwrap();
        backend.submitted().unwrap();

        tokio::time::advance(pto).await;
        assert_eq!(backend.deadline().unwrap(), logical, "D is not terminal");
        tokio::time::advance(start + setup + Duration::from_millis(1) - Instant::now()).await;
        assert!(
            !owner.exhaust_successors(0),
            "exhausting candidates must not cancel a submitted attempt"
        );
        assert_eq!(backend.deadline().unwrap(), logical);
        assert_eq!(owner.decision_deadline(0), None, "consumed D cannot spin");
        assert!(
            owner.0.state.lock().unwrap().slots[0]
                .as_ref()
                .unwrap()
                .retained
        );

        if receive_max_after_setup {
            assert_eq!(backend.admission().unwrap(), logical);
            assert!(owner.has_admission());
        } else {
            let pending = std::future::pending::<Result<(), RuntimeError>>();
            tokio::pin!(pending);
            let operation = backend.complete(pending.as_mut());
            tokio::pin!(operation);
            assert!(operation.as_mut().now_or_never().is_none());
            tokio::time::advance(logical - Instant::now()).await;
            assert!(matches!(
                operation.await,
                Err(RuntimeError::PathOpenTimedOut)
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn contracted_admission_cannot_override_ready_logical_reset() {
    use crate::protocol::ResetReason;
    use crate::runtime::path::ClientStreamTerminalScope;

    let (setup, pto) = phase_budgets();
    let session = SessionId(716);
    let stream = StreamId(816);
    let owner =
        InitialOpenAcquisition::new(session, stream, Instant::now() + Duration::from_secs(30), 2);
    let attempt = owner
        .launch_handle()
        .begin(0, key(0), setup, pto, true, None)
        .unwrap()
        .unwrap();
    let backend = attempt.begin_backend().unwrap();
    backend.bind(next_carrier_path_instance_id()).unwrap();
    backend.submitted().unwrap();
    let (scope, _terminal_owner) =
        ClientStreamTerminalScope::for_open(None, session, stream).unwrap();
    let publisher = scope.pending_input();
    tokio::time::advance(pto / 2).await;
    {
        let operation = async {
            backend.admission()?;
            publisher.publish_reset(stream, ResetReason::RemoteClosed);
            Ok(())
        };
        tokio::pin!(operation);
        assert!(matches!(
            scope.complete(operation.as_mut()).await,
            Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
        ));
    }
    assert!(owner.has_admission());
    assert!(matches!(
        scope.ensure_active(),
        Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
    ));
    drop(owner);
    assert!(matches!(
        backend.deadline(),
        Err(RuntimeError::ReliablePathRetired)
    ));
}
