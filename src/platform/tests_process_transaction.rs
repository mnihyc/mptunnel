use super::*;
use crate::platform::{
    AddressFamily, DnsCaptureConfig, ManagedVpnConfig, ProcessNativeRoute, ProcessVpnEnvironment,
    RouteMode,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MockError(usize);

impl fmt::Display for MockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "mock failure {}", self.0)
    }
}

impl std::error::Error for MockError {}

#[derive(Default)]
struct MockBackend {
    sequence: usize,
    fail_apply: Option<usize>,
    fail_rollbacks: BTreeSet<usize>,
    applied: Vec<ProcessHostOperation>,
    reversed: Vec<ProcessHostOperation>,
}

impl ProcessHostMutationBackend for MockBackend {
    type RollbackToken = usize;
    type Error = MockError;

    fn apply(
        &mut self,
        operation: &ProcessHostOperation,
    ) -> Result<Self::RollbackToken, Self::Error> {
        let sequence = self.sequence;
        self.sequence += 1;
        if self.fail_apply == Some(sequence) {
            return Err(MockError(sequence));
        }
        self.applied.push(operation.clone());
        Ok(sequence)
    }

    fn rollback(
        &mut self,
        operation: &ProcessHostOperation,
        token: &Self::RollbackToken,
    ) -> Result<(), Self::Error> {
        if self.fail_rollbacks.remove(token) {
            return Err(MockError(*token));
        }
        self.reversed.push(operation.clone());
        Ok(())
    }
}

fn plan() -> ProcessVpnPlan {
    let config = ManagedVpnConfig::new(
        vec!["10.88.0.1/24".parse().expect("address")],
        1400,
        RouteMode::Full,
    )
    .expect("config")
    .with_dns(DnsCaptureConfig::new(vec!["10.88.0.53".parse().expect("DNS")]).expect("DNS"))
    .expect("config DNS");
    let default = ProcessNativeRoute::new(
        AddressFamily::Ipv4,
        7,
        Some("192.0.2.1".parse().unwrap()),
        10,
    )
    .expect("route");
    ProcessVpnPlan::build(
        &config,
        &ProcessVpnEnvironment::new([default], vec![]).expect("environment"),
        42,
        ["198.51.100.7".parse().expect("carrier")],
        [],
    )
    .expect("plan")
}

#[test]
fn normal_lifecycle_preserves_two_phase_order_and_reverse_order() {
    let expected = plan();
    let mut controller = TransactionalProcessVpnController::new(MockBackend::default());
    controller.prepare(expected.clone()).expect("prepare");
    assert_eq!(controller.state(), ProcessControllerState::Prepared);
    assert_eq!(
        controller.backend().applied,
        expected.prepare_operations(),
        "prepare cannot publish capture"
    );

    controller.publish().expect("publish");
    assert_eq!(controller.state(), ProcessControllerState::Active);
    controller.unpublish().expect("unpublish");
    assert_eq!(controller.state(), ProcessControllerState::Prepared);
    controller.cleanup().expect("cleanup");
    assert_eq!(controller.state(), ProcessControllerState::Idle);

    let mut expected_reverse = expected.publish_operations().to_vec();
    expected_reverse.reverse();
    let mut prepare_reverse = expected.prepare_operations().to_vec();
    prepare_reverse.reverse();
    expected_reverse.extend(prepare_reverse);
    assert_eq!(controller.backend().reversed, expected_reverse);
}

#[test]
fn every_prepare_failure_reverses_completed_prefix() {
    let plan = plan();
    for failure in 0..plan.prepare_operations().len() {
        let backend = MockBackend {
            fail_apply: Some(failure),
            ..MockBackend::default()
        };
        let mut controller = TransactionalProcessVpnController::new(backend);
        assert!(matches!(
            controller.prepare(plan.clone()),
            Err(ProcessPrepareError::Apply {
                operation_index,
                ..
            }) if operation_index == failure
        ));
        assert_eq!(controller.state(), ProcessControllerState::Idle);
        assert_eq!(controller.pending_prepare_steps(), 0);
    }
}

#[test]
fn publish_failure_reverses_only_publish_prefix() {
    let plan = plan();
    let prepare_count = plan.prepare_operations().len();
    let backend = MockBackend {
        fail_apply: Some(prepare_count + 1),
        ..MockBackend::default()
    };
    let mut controller = TransactionalProcessVpnController::new(backend);
    controller.prepare(plan).expect("prepare");
    assert!(matches!(
        controller.publish(),
        Err(ProcessPublishError::Apply {
            operation_index: 1,
            ..
        })
    ));
    assert_eq!(controller.state(), ProcessControllerState::Prepared);
    assert_eq!(controller.pending_prepare_steps(), prepare_count);
    assert_eq!(controller.pending_publish_steps(), 0);
}

#[test]
fn failed_cleanup_is_retryable_without_losing_ownership() {
    let mut controller = TransactionalProcessVpnController::new(MockBackend::default());
    controller.prepare(plan()).expect("prepare");
    controller.publish().expect("publish");
    controller
        .backend
        .fail_rollbacks
        .insert(controller.published.last().expect("published").token);

    assert!(matches!(
        controller.unpublish(),
        Err(ProcessCleanupError::Rollback { failures, .. }) if failures.len() == 1
    ));
    assert_eq!(controller.state(), ProcessControllerState::CleanupPending);
    assert_eq!(controller.pending_publish_steps(), 1);
    controller.unpublish().expect("retry unpublish");
    controller.cleanup().expect("retry cleanup");
    assert_eq!(controller.state(), ProcessControllerState::Idle);
}
