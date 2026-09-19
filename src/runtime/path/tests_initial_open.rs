use super::*;
use crate::model::path::next_carrier_path_instance_id;
use crate::protocol::UnderlayProtocol;

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
        .begin(1, key(1), Duration::from_secs(10), false, Some(0))
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
async fn late_max_fences_nominal_start_but_settlement_allows_real_retry() {
    let owner = InitialOpenAcquisition::new(
        SessionId(702),
        StreamId(802),
        Instant::now() + Duration::from_secs(30),
        2,
    );
    let first = submitted(&owner, 0, true);
    tokio::time::advance(Duration::from_secs(11)).await;
    first.deadline().unwrap(); // actor's nominal-expiry path publishes Due
    let prepared = owner.launch_handle();
    assert!(matches!(
        first.admission(),
        Err(RuntimeError::PathOpenTimedOut)
    ));
    assert!(owner.has_admission(), "actual late MAX is still observed");
    assert!(
        prepared
            .begin(1, key(1), Duration::from_secs(10), false, Some(0),)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        owner.started_ordinals(),
        [0],
        "prepared ordinal remains unstarted"
    );
    owner.settle(0);
    assert!(!owner.has_admission());
    assert!(
        prepared
            .begin(1, key(1), Duration::from_secs(10), false, Some(0),)
            .unwrap()
            .is_none(),
        "settling A cannot revive a previously fenced nominal launch",
    );
    assert!(
        owner
            .launch_handle()
            .begin(1, key(1), Duration::from_secs(10), false, None,)
            .unwrap()
            .is_some(),
        "settled late MAX cannot veto concrete-error retry"
    );
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
            .begin(1, key(1), Duration::from_secs(10), false, Some(0),)
            .unwrap()
            .is_none()
    );
    owner.settle(0); // only the exact owner's actual failure settles admission
    assert!(
        owner
            .launch_handle()
            .begin(1, key(1), Duration::from_secs(10), false, None,)
            .unwrap()
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn singleton_setup_and_stale_backends_never_gain_retention() {
    let deadline = Instant::now() + Duration::from_secs(30);
    let owner = InitialOpenAcquisition::new(SessionId(704), StreamId(804), deadline, 2);
    let first = submitted(&owner, 0, false);
    let second = owner
        .launch_handle()
        .begin(1, key(1), Duration::from_secs(10), true, None)
        .unwrap()
        .unwrap();
    let stale = second.begin_backend().unwrap();
    let current = second.begin_backend().unwrap();
    assert!(matches!(
        stale.deadline(),
        Err(RuntimeError::ReliablePathRetired)
    ));
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(
        first.deadline().unwrap() <= Instant::now(),
        "singleton expires"
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
    tokio::time::advance(Duration::from_secs(10)).await;
    let actor_now = Instant::now();
    let first = backend.deadline().unwrap();
    assert_eq!(first, actor_now);
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
        .begin(1, key(1), Duration::from_secs(10), true, Some(0))
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
            .begin(2, key(2), Duration::from_secs(10), false, Some(1))
            .unwrap()
            .is_none(),
        "retained A's MAX fences prepared C even though B is C's immediate predecessor"
    );
    assert_eq!(owner.unstarted_predecessor(2, Some(1)), Some(1));
    assert_eq!(owner.started_ordinals(), [0, 1]);
    assert!(
        owner
            .launch_handle()
            .begin(2, key(2), Duration::from_secs(10), false, Some(1))
            .unwrap()
            .is_some(),
        "a fresh decision after exact admission settlement is permitted"
    );
}
