use super::*;
use crate::platform::{
    AddressFamily, DnsCaptureConfig, LinuxInterfaceName, LinuxNativeRoute, LinuxVpnConfig,
    LinuxVpnEnvironment, RouteMode,
};
use std::collections::BTreeMap;
use std::net::IpAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Apply(usize),
    TakeDevice,
    Rollback(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MockError(String);

impl fmt::Display for MockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for MockError {}

#[derive(Default)]
struct MockBackend {
    events: Vec<Event>,
    next_token: usize,
    fail_apply_at: Option<usize>,
    rollback_failures_remaining: BTreeMap<usize, usize>,
    take_failures_remaining: usize,
    device_available: bool,
}

impl MockBackend {
    fn failing_apply(index: usize) -> Self {
        Self {
            fail_apply_at: Some(index),
            ..Self::default()
        }
    }

    fn fail_rollback(&mut self, token: usize, times: usize) {
        self.rollback_failures_remaining.insert(token, times);
    }
}

impl LinuxHostMutationBackend for MockBackend {
    type RollbackToken = usize;
    type PreparedDevice = usize;
    type Error = MockError;

    fn apply(
        &mut self,
        operation: &LinuxHostOperation,
    ) -> Result<Self::RollbackToken, Self::Error> {
        let token = self.next_token;
        self.next_token += 1;
        self.events.push(Event::Apply(token));
        if self.fail_apply_at == Some(token) {
            return Err(MockError(format!("apply {token}")));
        }
        if matches!(operation, LinuxHostOperation::CreateTun { .. }) {
            self.device_available = true;
        }
        Ok(token)
    }

    fn rollback(
        &mut self,
        operation: &LinuxHostOperation,
        token: &Self::RollbackToken,
    ) -> Result<(), Self::Error> {
        self.events.push(Event::Rollback(*token));
        if let Some(remaining) = self.rollback_failures_remaining.get_mut(token)
            && *remaining > 0
        {
            *remaining -= 1;
            return Err(MockError(format!("rollback {token}")));
        }
        if matches!(operation, LinuxHostOperation::CreateTun { .. }) {
            self.device_available = false;
        }
        Ok(())
    }

    fn take_prepared_device(&mut self) -> Result<Self::PreparedDevice, Self::Error> {
        self.events.push(Event::TakeDevice);
        if self.take_failures_remaining > 0 {
            self.take_failures_remaining -= 1;
            return Err(MockError("take device".to_string()));
        }
        if !self.device_available {
            return Err(MockError("device unavailable".to_string()));
        }
        self.device_available = false;
        Ok(7)
    }
}

fn interface(name: &str) -> LinuxInterfaceName {
    LinuxInterfaceName::parse(name).expect("interface")
}

fn test_plan() -> LinuxVpnPlan {
    let config = LinuxVpnConfig::new(
        interface("mptun0"),
        vec!["10.88.0.1/24".parse().expect("address")],
        1500,
        RouteMode::Full,
    )
    .expect("config")
    .with_dns(DnsCaptureConfig::new(vec!["1.1.1.1".parse().expect("DNS")]).expect("DNS"))
    .expect("DNS");
    let environment = LinuxVpnEnvironment::new(
        vec![
            LinuxNativeRoute::new(
                AddressFamily::Ipv4,
                interface("eth0"),
                Some("192.0.2.1".parse().expect("gateway")),
                None,
                100,
            )
            .expect("route"),
        ],
        vec![],
    )
    .expect("environment");
    LinuxVpnPlan::build(
        &config,
        &environment,
        ["203.0.113.9".parse::<IpAddr>().expect("carrier")],
        [],
    )
    .expect("plan")
}

fn prepared_controller() -> TransactionalLinuxVpnController<MockBackend> {
    let mut controller = TransactionalLinuxVpnController::new(MockBackend::default());
    controller.prepare(test_plan()).expect("prepare");
    controller
}

fn active_controller() -> TransactionalLinuxVpnController<MockBackend> {
    let mut controller = prepared_controller();
    assert_eq!(controller.take_prepared_device().expect("device"), 7);
    controller.publish().expect("publish");
    controller
}

#[test]
fn plan_phases_never_publish_during_prepare() {
    let plan = test_plan();
    assert!(plan.prepare_operations().iter().all(|operation| !matches!(
        operation,
        LinuxHostOperation::ActivateNativeEgressRule { .. }
            | LinuxHostOperation::ActivateCaptureRule { .. }
            | LinuxHostOperation::ConfigureDns { .. }
    )));
    assert!(plan.publish_operations().iter().all(|operation| matches!(
        operation,
        LinuxHostOperation::ActivateNativeEgressRule { .. }
            | LinuxHostOperation::ActivateCaptureRule { .. }
            | LinuxHostOperation::ConfigureDns { .. }
    )));

    let prepare_count = plan.prepare_operations().len();
    let mut controller = TransactionalLinuxVpnController::new(MockBackend::default());
    controller.prepare(plan).expect("prepare");
    assert_eq!(controller.state(), LinuxControllerState::Prepared);
    assert_eq!(controller.pending_prepare_steps(), prepare_count);
    assert_eq!(controller.pending_publish_steps(), 0);
    assert_eq!(
        controller.backend().events,
        (0..prepare_count).map(Event::Apply).collect::<Vec<_>>()
    );
}

#[test]
fn publish_requires_successful_device_handoff() {
    let mut controller = prepared_controller();
    assert_eq!(
        controller.publish(),
        Err(LinuxPublishError::PacketDeviceNotTaken)
    );
    assert_eq!(controller.take_prepared_device().expect("device"), 7);
    assert!(controller.packet_device_taken());
    assert_eq!(
        controller.take_prepared_device(),
        Err(LinuxPreparedDeviceError::AlreadyTaken)
    );
    controller.publish().expect("publish");
    assert_eq!(controller.state(), LinuxControllerState::Active);
}

#[test]
fn backend_device_handoff_error_is_retryable_without_losing_prepare() {
    let mut controller = prepared_controller();
    controller.backend_mut().take_failures_remaining = 1;
    assert_eq!(
        controller.take_prepared_device(),
        Err(LinuxPreparedDeviceError::Backend(MockError(
            "take device".to_string()
        )))
    );
    assert_eq!(controller.state(), LinuxControllerState::Prepared);
    assert!(!controller.packet_device_taken());
    assert_eq!(controller.take_prepared_device().expect("retry"), 7);
}

#[test]
fn active_cleanup_reverses_publish_then_prepare_exactly_once() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    let publish_count = plan.publish_operations().len();
    let total = prepare_count + publish_count;
    let mut controller = active_controller();

    assert_eq!(
        controller.cleanup().expect("cleanup"),
        LinuxCleanupOutcome {
            attempted: total,
            completed: total,
        }
    );
    assert_eq!(controller.state(), LinuxControllerState::Idle);
    let expected = (0..prepare_count)
        .map(Event::Apply)
        .chain([Event::TakeDevice])
        .chain((prepare_count..total).map(Event::Apply))
        .chain((0..total).rev().map(Event::Rollback))
        .collect::<Vec<_>>();
    assert_eq!(controller.backend().events, expected);
    assert_eq!(
        controller.cleanup().expect("idempotent"),
        LinuxCleanupOutcome {
            attempted: 0,
            completed: 0,
        }
    );
}

#[test]
fn unpublish_reverses_only_publication_and_leaves_prepared_state() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    let publish_count = plan.publish_operations().len();
    let mut controller = active_controller();

    assert_eq!(
        controller.unpublish().expect("unpublish"),
        LinuxCleanupOutcome {
            attempted: publish_count,
            completed: publish_count,
        }
    );
    assert_eq!(controller.state(), LinuxControllerState::Prepared);
    assert_eq!(controller.pending_prepare_steps(), prepare_count);
    assert_eq!(controller.pending_publish_steps(), 0);

    assert_eq!(
        controller.cleanup().expect("prepared cleanup"),
        LinuxCleanupOutcome {
            attempted: prepare_count,
            completed: prepare_count,
        }
    );
    assert_eq!(controller.state(), LinuxControllerState::Idle);
}

#[test]
fn every_prepare_failure_reverses_only_its_completed_prefix() {
    let prepare_count = test_plan().prepare_operations().len();
    for failure_index in 0..prepare_count {
        let mut controller =
            TransactionalLinuxVpnController::new(MockBackend::failing_apply(failure_index));
        let error = controller.prepare(test_plan()).expect_err("prepare fails");
        assert!(matches!(
            error,
            LinuxPrepareError::Apply {
                operation_index,
                rollback_failures,
                ..
            } if operation_index == failure_index && rollback_failures.is_empty()
        ));
        assert_eq!(controller.state(), LinuxControllerState::Idle);
        let expected = (0..=failure_index)
            .map(Event::Apply)
            .chain((0..failure_index).rev().map(Event::Rollback))
            .collect::<Vec<_>>();
        assert_eq!(controller.backend().events, expected);
    }
}

#[test]
fn prepare_failure_retains_reverse_failures_for_cleanup_retry() {
    let mut backend = MockBackend::failing_apply(4);
    backend.fail_rollback(2, 1);
    backend.fail_rollback(0, 1);
    let mut controller = TransactionalLinuxVpnController::new(backend);
    let error = controller.prepare(test_plan()).expect_err("prepare fails");
    let LinuxPrepareError::Apply {
        rollback_failures, ..
    } = error
    else {
        panic!("expected apply error");
    };
    assert_eq!(
        rollback_failures
            .iter()
            .map(|failure| (failure.phase, failure.operation_index))
            .collect::<Vec<_>>(),
        vec![
            (LinuxOperationPhase::Prepare, 2),
            (LinuxOperationPhase::Prepare, 0)
        ]
    );
    assert_eq!(controller.state(), LinuxControllerState::CleanupPending);
    assert_eq!(controller.pending_prepare_steps(), 2);
    controller.cleanup().expect("retry residual");
    assert_eq!(controller.state(), LinuxControllerState::Idle);
}

#[test]
fn every_publish_failure_reverses_publish_prefix_but_keeps_prepare() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    let publish_count = plan.publish_operations().len();
    for failure_index in 0..publish_count {
        let mut controller = prepared_controller();
        controller.take_prepared_device().expect("device");
        controller.backend_mut().fail_apply_at = Some(prepare_count + failure_index);
        let event_start = controller.backend().events.len();
        let error = controller.publish().expect_err("publish fails");
        assert!(matches!(
            error,
            LinuxPublishError::Apply {
                operation_index,
                rollback_failures,
                ..
            } if operation_index == failure_index && rollback_failures.is_empty()
        ));
        assert_eq!(controller.state(), LinuxControllerState::Prepared);
        assert_eq!(controller.pending_prepare_steps(), prepare_count);
        assert_eq!(controller.pending_publish_steps(), 0);
        let expected = (prepare_count..=prepare_count + failure_index)
            .map(Event::Apply)
            .chain(
                (prepare_count..prepare_count + failure_index)
                    .rev()
                    .map(Event::Rollback),
            )
            .collect::<Vec<_>>();
        assert_eq!(&controller.backend().events[event_start..], expected);
        controller.cleanup().expect("prepared cleanup");
    }
}

#[test]
fn failed_publish_rollback_can_be_retried_without_removing_prepare() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    assert!(plan.publish_operations().len() >= 2);
    let mut controller = prepared_controller();
    controller.take_prepared_device().expect("device");
    controller.backend_mut().fail_apply_at = Some(prepare_count + 1);
    controller.backend_mut().fail_rollback(prepare_count, 1);

    let error = controller.publish().expect_err("publish fails");
    assert!(matches!(
        error,
        LinuxPublishError::Apply {
            rollback_failures,
            ..
        } if rollback_failures.len() == 1
            && rollback_failures[0].phase == LinuxOperationPhase::Publish
    ));
    assert_eq!(controller.state(), LinuxControllerState::CleanupPending);
    assert_eq!(controller.pending_prepare_steps(), prepare_count);
    assert_eq!(controller.pending_publish_steps(), 1);

    assert_eq!(
        controller.unpublish().expect("retry unpublish"),
        LinuxCleanupOutcome {
            attempted: 1,
            completed: 1,
        }
    );
    assert_eq!(controller.state(), LinuxControllerState::Prepared);
    controller.cleanup().expect("prepare cleanup");
}

#[test]
fn full_cleanup_aggregates_publish_and_prepare_failures() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    let publish_count = plan.publish_operations().len();
    let total = prepare_count + publish_count;
    let mut controller = active_controller();
    let publish_token = total - 1;
    let prepare_token = 1;
    controller.backend_mut().fail_rollback(publish_token, 1);
    controller.backend_mut().fail_rollback(prepare_token, 1);

    let error = controller.cleanup().expect_err("cleanup fails");
    let LinuxCleanupError::Rollback {
        attempted,
        completed,
        failures,
    } = error
    else {
        panic!("expected rollback error");
    };
    assert_eq!(attempted, total);
    assert_eq!(completed, total - 2);
    assert_eq!(
        failures
            .iter()
            .map(|failure| failure.phase)
            .collect::<Vec<_>>(),
        vec![LinuxOperationPhase::Publish, LinuxOperationPhase::Prepare]
    );
    assert_eq!(controller.state(), LinuxControllerState::CleanupPending);
    assert_eq!(controller.pending_publish_steps(), 1);
    assert_eq!(controller.pending_prepare_steps(), 1);

    assert_eq!(
        controller.cleanup().expect("retry"),
        LinuxCleanupOutcome {
            attempted: 2,
            completed: 2,
        }
    );
    assert_eq!(controller.state(), LinuxControllerState::Idle);
}

#[test]
fn controller_fences_overlapping_phase_transitions() {
    let mut controller = TransactionalLinuxVpnController::new(MockBackend::default());
    assert_eq!(
        controller.take_prepared_device(),
        Err(LinuxPreparedDeviceError::WrongState {
            state: LinuxControllerState::Idle
        })
    );
    assert_eq!(
        controller.publish(),
        Err(LinuxPublishError::WrongState {
            state: LinuxControllerState::Idle
        })
    );
    controller.prepare(test_plan()).expect("prepare");
    assert_eq!(
        controller.prepare(test_plan()),
        Err(LinuxPrepareError::Busy {
            state: LinuxControllerState::Prepared
        })
    );
    assert!(matches!(
        controller.unpublish(),
        Err(LinuxCleanupError::WrongState {
            state: LinuxControllerState::Prepared
        })
    ));
    controller.take_prepared_device().expect("device");
    controller.publish().expect("publish");
    assert_eq!(
        controller.publish(),
        Err(LinuxPublishError::WrongState {
            state: LinuxControllerState::Active
        })
    );
}
