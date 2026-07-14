use super::*;

fn instance(index: usize, id: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        },
        id,
    }
}

#[test]
fn planning_startup_admission_does_not_mutate_state() {
    let state = RequestStartupState::default();
    let service = instance(0, 10);
    let candidate = instance(1, 11);

    let admission = state
        .plan_admission(MuxLimits::default(), service, candidate, 4096)
        .expect("valid same-family startup plan");

    assert!(state.epoch.is_none());
    assert!(!state.attempted_subflows.contains(&candidate));
    drop(admission);
    assert!(state.epoch.is_none());
}

#[test]
fn committing_startup_admission_installs_epoch_and_attempt_atomically() {
    let mut state = RequestStartupState::default();
    let service = instance(0, 20);
    let candidate = instance(1, 21);
    let admission = state
        .plan_admission(MuxLimits::default(), service, candidate, 4096)
        .expect("valid same-family startup plan");

    state.commit_admission(admission);

    assert_eq!(
        state
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        Some(candidate)
    );
    assert!(state.attempted_subflows.contains(&candidate));
}
