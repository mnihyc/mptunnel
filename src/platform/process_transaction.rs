//! Transaction engine for process-managed desktop route/DNS publication.

use crate::platform::{ProcessHostOperation, ProcessVpnPlan};
use std::fmt;

/// Backend for exact Windows or privileged-process macOS host mutations.
///
/// A successful `apply` returns an ownership token. The token must distinguish
/// an entry created by MPTUNNEL from an identical pre-existing entry so
/// rollback never deletes foreign state. Each individual operation must be
/// atomic: a backend that partially mutates an operation must reverse that
/// partial effect before returning an error.
pub trait ProcessHostMutationBackend {
    type RollbackToken;
    type Error;

    fn apply(
        &mut self,
        operation: &ProcessHostOperation,
    ) -> Result<Self::RollbackToken, Self::Error>;

    /// Reversal must be safe to retry after an error.
    fn rollback(
        &mut self,
        operation: &ProcessHostOperation,
        token: &Self::RollbackToken,
    ) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOperationPhase {
    Prepare,
    Publish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessControllerState {
    Idle,
    /// Native bypasses exist; no capture route or DNS has been published.
    Prepared,
    /// Capture routes and optional DNS are active.
    Active,
    /// One or more reverse operations must be retried.
    CleanupPending,
}

struct AppliedStep<Token> {
    phase: ProcessOperationPhase,
    operation_index: usize,
    operation: ProcessHostOperation,
    token: Token,
}

/// Reusable two-phase transaction for Windows and privileged-process macOS.
///
/// Device creation and address assignment occur before `prepare`; their
/// adapter owns that rollback. This controller then prepares exact native
/// bypasses, publishes capture routes and DNS after worker readiness,
/// unpublishes before worker shutdown, and finally removes bypasses.
pub struct TransactionalProcessVpnController<Backend>
where
    Backend: ProcessHostMutationBackend,
{
    backend: Backend,
    prepared: Vec<AppliedStep<Backend::RollbackToken>>,
    published: Vec<AppliedStep<Backend::RollbackToken>>,
    publish_operations: Option<Vec<ProcessHostOperation>>,
    prepare_complete: bool,
    active: bool,
}

impl<Backend> TransactionalProcessVpnController<Backend>
where
    Backend: ProcessHostMutationBackend,
{
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            prepared: Vec::new(),
            published: Vec::new(),
            publish_operations: None,
            prepare_complete: false,
            active: false,
        }
    }

    pub fn state(&self) -> ProcessControllerState {
        if self.active {
            ProcessControllerState::Active
        } else if self.prepare_complete && self.published.is_empty() {
            ProcessControllerState::Prepared
        } else if self.publish_operations.is_none()
            && self.prepared.is_empty()
            && self.published.is_empty()
        {
            ProcessControllerState::Idle
        } else {
            ProcessControllerState::CleanupPending
        }
    }

    pub fn pending_prepare_steps(&self) -> usize {
        self.prepared.len()
    }

    pub fn pending_publish_steps(&self) -> usize {
        self.published.len()
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    pub fn prepare(
        &mut self,
        plan: ProcessVpnPlan,
    ) -> Result<(), ProcessPrepareError<Backend::Error>> {
        let state = self.state();
        if state != ProcessControllerState::Idle {
            return Err(ProcessPrepareError::Busy { state });
        }

        let (prepare_operations, publish_operations) = plan.into_phases();
        self.publish_operations = Some(publish_operations);
        for (operation_index, operation) in prepare_operations.into_iter().enumerate() {
            match self.backend.apply(&operation) {
                Ok(token) => self.prepared.push(AppliedStep {
                    phase: ProcessOperationPhase::Prepare,
                    operation_index,
                    operation,
                    token,
                }),
                Err(error) => {
                    let failed_operation = operation;
                    let (_, rollback_failures) =
                        rollback_steps(&mut self.backend, &mut self.prepared);
                    self.prepare_complete = false;
                    if self.prepared.is_empty() {
                        self.reset_idle();
                    }
                    return Err(ProcessPrepareError::Apply {
                        operation_index,
                        operation: failed_operation,
                        error,
                        rollback_failures,
                    });
                }
            }
        }
        self.prepare_complete = true;
        Ok(())
    }

    pub fn publish(&mut self) -> Result<(), ProcessPublishError<Backend::Error>> {
        let state = self.state();
        if state != ProcessControllerState::Prepared {
            return Err(ProcessPublishError::WrongState { state });
        }

        let operation_count = self.publish_operations.as_ref().map_or(0, Vec::len);
        for operation_index in 0..operation_count {
            let operation = self
                .publish_operations
                .as_ref()
                .and_then(|operations| operations.get(operation_index))
                .cloned()
                .expect("validated publish operation index");
            match self.backend.apply(&operation) {
                Ok(token) => self.published.push(AppliedStep {
                    phase: ProcessOperationPhase::Publish,
                    operation_index,
                    operation,
                    token,
                }),
                Err(error) => {
                    let failed_operation = operation;
                    let (_, rollback_failures) =
                        rollback_steps(&mut self.backend, &mut self.published);
                    self.active = false;
                    return Err(ProcessPublishError::Apply {
                        operation_index,
                        operation: failed_operation,
                        error,
                        rollback_failures,
                    });
                }
            }
        }
        self.active = true;
        Ok(())
    }

    /// Reverses DNS then capture routes while the packet worker still exists.
    pub fn unpublish(
        &mut self,
    ) -> Result<ProcessCleanupOutcome, ProcessCleanupError<Backend::Error>> {
        if !self.prepare_complete || self.published.is_empty() {
            return Err(ProcessCleanupError::WrongState {
                state: self.state(),
            });
        }
        self.active = false;
        let (attempted, failures) = rollback_steps(&mut self.backend, &mut self.published);
        cleanup_result(attempted, failures)
    }

    /// Best-effort reversal in global reverse activation order.
    pub fn cleanup(
        &mut self,
    ) -> Result<ProcessCleanupOutcome, ProcessCleanupError<Backend::Error>> {
        self.active = false;
        self.prepare_complete = false;
        let (publish_attempted, mut failures) =
            rollback_steps(&mut self.backend, &mut self.published);
        let (prepare_attempted, prepare_failures) =
            rollback_steps(&mut self.backend, &mut self.prepared);
        failures.extend(prepare_failures);
        let attempted = publish_attempted + prepare_attempted;
        if failures.is_empty() {
            self.reset_idle();
            Ok(ProcessCleanupOutcome {
                attempted,
                completed: attempted,
            })
        } else {
            Err(ProcessCleanupError::Rollback {
                attempted,
                completed: attempted.saturating_sub(failures.len()),
                failures,
            })
        }
    }

    fn reset_idle(&mut self) {
        debug_assert!(self.prepared.is_empty());
        debug_assert!(self.published.is_empty());
        self.publish_operations = None;
        self.prepare_complete = false;
        self.active = false;
    }
}

fn rollback_steps<Backend>(
    backend: &mut Backend,
    applied: &mut Vec<AppliedStep<Backend::RollbackToken>>,
) -> (usize, Vec<ProcessRollbackFailure<Backend::Error>>)
where
    Backend: ProcessHostMutationBackend,
{
    let attempted = applied.len();
    let mut residual = Vec::new();
    let mut failures = Vec::new();
    while let Some(step) = applied.pop() {
        match backend.rollback(&step.operation, &step.token) {
            Ok(()) => {}
            Err(error) => {
                failures.push(ProcessRollbackFailure {
                    phase: step.phase,
                    operation_index: step.operation_index,
                    operation: step.operation.clone(),
                    error,
                });
                residual.push(step);
            }
        }
    }
    residual.reverse();
    *applied = residual;
    (attempted, failures)
}

fn cleanup_result<Error>(
    attempted: usize,
    failures: Vec<ProcessRollbackFailure<Error>>,
) -> Result<ProcessCleanupOutcome, ProcessCleanupError<Error>> {
    if failures.is_empty() {
        Ok(ProcessCleanupOutcome {
            attempted,
            completed: attempted,
        })
    } else {
        Err(ProcessCleanupError::Rollback {
            attempted,
            completed: attempted.saturating_sub(failures.len()),
            failures,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProcessRollbackFailure<Error> {
    pub phase: ProcessOperationPhase,
    pub operation_index: usize,
    pub operation: ProcessHostOperation,
    pub error: Error,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessPrepareError<Error> {
    Busy {
        state: ProcessControllerState,
    },
    Apply {
        operation_index: usize,
        operation: ProcessHostOperation,
        error: Error,
        rollback_failures: Vec<ProcessRollbackFailure<Error>>,
    },
}

impl<Error: fmt::Display> fmt::Display for ProcessPrepareError<Error> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy { state } => {
                write!(formatter, "desktop VPN cannot prepare from {state:?}")
            }
            Self::Apply {
                operation_index,
                error,
                rollback_failures,
                ..
            } => {
                write!(
                    formatter,
                    "desktop VPN prepare operation {operation_index} failed: {error}"
                )?;
                write_rollback_suffix(formatter, rollback_failures)
            }
        }
    }
}

impl<Error> std::error::Error for ProcessPrepareError<Error> where Error: std::error::Error + 'static
{}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessPublishError<Error> {
    WrongState {
        state: ProcessControllerState,
    },
    Apply {
        operation_index: usize,
        operation: ProcessHostOperation,
        error: Error,
        rollback_failures: Vec<ProcessRollbackFailure<Error>>,
    },
}

impl<Error: fmt::Display> fmt::Display for ProcessPublishError<Error> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongState { state } => {
                write!(formatter, "desktop VPN cannot publish from {state:?}")
            }
            Self::Apply {
                operation_index,
                error,
                rollback_failures,
                ..
            } => {
                write!(
                    formatter,
                    "desktop VPN publish operation {operation_index} failed: {error}"
                )?;
                write_rollback_suffix(formatter, rollback_failures)
            }
        }
    }
}

impl<Error> std::error::Error for ProcessPublishError<Error> where Error: std::error::Error + 'static
{}

fn write_rollback_suffix<Error>(
    formatter: &mut fmt::Formatter<'_>,
    failures: &[ProcessRollbackFailure<Error>],
) -> fmt::Result {
    if !failures.is_empty() {
        write!(
            formatter,
            "; {} reverse operations also failed",
            failures.len()
        )?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessCleanupOutcome {
    pub attempted: usize,
    pub completed: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessCleanupError<Error> {
    WrongState {
        state: ProcessControllerState,
    },
    Rollback {
        attempted: usize,
        completed: usize,
        failures: Vec<ProcessRollbackFailure<Error>>,
    },
}

impl<Error: fmt::Display> fmt::Display for ProcessCleanupError<Error> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongState { state } => {
                write!(formatter, "desktop VPN cannot unpublish from {state:?}")
            }
            Self::Rollback {
                attempted,
                completed,
                failures,
            } => write!(
                formatter,
                "desktop VPN cleanup reverted {completed} of {attempted} operations; {} failed",
                failures.len()
            ),
        }
    }
}

impl<Error> std::error::Error for ProcessCleanupError<Error> where Error: std::error::Error + 'static
{}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::{
        AddressFamily, DnsCaptureConfig, ManagedVpnConfig, ProcessNativeRoute,
        ProcessVpnEnvironment, RouteMode,
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
}
