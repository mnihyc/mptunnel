use crate::platform::{LinuxHostOperation, LinuxVpnPlan};
use std::fmt;

/// Backend for concrete host-network mutations and packet-device handoff.
///
/// `apply` must be atomic at the granularity of one [`LinuxHostOperation`]: before
/// returning an error, a backend must undo any partial effect of that one
/// operation. The controller owns ordering and rollback across completed
/// operations. `rollback` must be safe to retry after an error.
///
/// A successful `CreateTun` prepare operation leaves the concrete packet
/// device owned by the backend. [`Self::take_prepared_device`] transfers that
/// device exactly once to the runtime without exposing tun-rs (or any other
/// platform type) to the planner. On error, the backend must retain the device
/// so the caller can retry the handoff or clean up the prepared transaction.
pub trait LinuxHostMutationBackend {
    type RollbackToken;
    type PreparedDevice;
    type Error;

    fn apply(&mut self, operation: &LinuxHostOperation)
    -> Result<Self::RollbackToken, Self::Error>;

    fn rollback(
        &mut self,
        operation: &LinuxHostOperation,
        token: &Self::RollbackToken,
    ) -> Result<(), Self::Error>;

    fn take_prepared_device(&mut self) -> Result<Self::PreparedDevice, Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxOperationPhase {
    Prepare,
    Publish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxControllerState {
    Idle,
    /// TUN and private-table routes exist, but no host traffic is captured.
    Prepared,
    /// Policy rules and optional link DNS publish traffic to the ready worker.
    Active,
    /// A failed reverse operation is retained for explicit retry.
    CleanupPending,
}

struct AppliedStep<Token> {
    phase: LinuxOperationPhase,
    operation_index: usize,
    operation: LinuxHostOperation,
    token: Token,
}

/// Owns one two-phase VPN host transaction.
///
/// The safe startup contract is:
///
/// 1. [`Self::prepare`] creates/configures the TUN and installs bypass plus
///    inert private-table capture routes.
/// 2. [`Self::take_prepared_device`] transfers the backend-specific device.
/// 3. The caller starts the packet worker and waits for an explicit ready
///    signal.
/// 4. Only then may the caller invoke [`Self::publish`].
///
/// `publish` never rolls back prepare work on failure because the packet worker
/// already owns the device. It reverses only the completed publish prefix,
/// leaving a retryable `Prepared` state when that rollback succeeds.
///
/// The safe shutdown contract is:
///
/// 1. [`Self::unpublish`] restores DNS and removes policy rules while the
///    packet worker can still drain.
/// 2. Stop the worker and drop the transferred packet device.
/// 3. Call [`Self::cleanup`] to reverse prepared routes, addresses, and TUN
///    creation.
///
/// `cleanup` can also perform a best-effort full reverse transaction after an
/// error and aggregates failures from both phases. The caller must stop/drop a
/// transferred packet device before full cleanup; the controller deliberately
/// does not depend on its concrete type and therefore cannot observe that
/// external lifetime.
pub struct TransactionalLinuxVpnController<Backend>
where
    Backend: LinuxHostMutationBackend,
{
    backend: Backend,
    prepared: Vec<AppliedStep<Backend::RollbackToken>>,
    published: Vec<AppliedStep<Backend::RollbackToken>>,
    publish_operations: Option<Vec<LinuxHostOperation>>,
    prepare_complete: bool,
    device_taken: bool,
    active: bool,
}

impl<Backend> TransactionalLinuxVpnController<Backend>
where
    Backend: LinuxHostMutationBackend,
{
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            prepared: Vec::new(),
            published: Vec::new(),
            publish_operations: None,
            prepare_complete: false,
            device_taken: false,
            active: false,
        }
    }

    pub fn state(&self) -> LinuxControllerState {
        if self.active {
            LinuxControllerState::Active
        } else if self.prepare_complete && self.published.is_empty() {
            LinuxControllerState::Prepared
        } else if self.publish_operations.is_none()
            && self.prepared.is_empty()
            && self.published.is_empty()
        {
            LinuxControllerState::Idle
        } else {
            LinuxControllerState::CleanupPending
        }
    }

    pub fn pending_prepare_steps(&self) -> usize {
        self.prepared.len()
    }

    pub fn pending_publish_steps(&self) -> usize {
        self.published.len()
    }

    pub fn packet_device_taken(&self) -> bool {
        self.device_taken
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    #[cfg(test)]
    fn backend_mut(&mut self) -> &mut Backend {
        &mut self.backend
    }

    /// Applies only the non-publishing half of a plan.
    pub fn prepare(&mut self, plan: LinuxVpnPlan) -> Result<(), LinuxPrepareError<Backend::Error>> {
        let state = self.state();
        if state != LinuxControllerState::Idle {
            return Err(LinuxPrepareError::Busy { state });
        }

        let (prepare_operations, publish_operations) = plan.into_phases();
        self.publish_operations = Some(publish_operations);
        for (operation_index, operation) in prepare_operations.into_iter().enumerate() {
            match self.backend.apply(&operation) {
                Ok(token) => self.prepared.push(AppliedStep {
                    phase: LinuxOperationPhase::Prepare,
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
                    return Err(LinuxPrepareError::Apply {
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

    /// Transfers the concrete prepared packet device to the future worker.
    ///
    /// This is the Linux planner's type-erased device handoff. The associated
    /// device type belongs to the backend, not to `LinuxVpnPlan` or host operations.
    pub fn take_prepared_device(
        &mut self,
    ) -> Result<Backend::PreparedDevice, LinuxPreparedDeviceError<Backend::Error>> {
        let state = self.state();
        if state != LinuxControllerState::Prepared {
            return Err(LinuxPreparedDeviceError::WrongState { state });
        }
        if self.device_taken {
            return Err(LinuxPreparedDeviceError::AlreadyTaken);
        }
        let device = self
            .backend
            .take_prepared_device()
            .map_err(LinuxPreparedDeviceError::Backend)?;
        self.device_taken = true;
        Ok(device)
    }

    /// Publishes policy rules and host DNS after the packet worker is ready.
    ///
    /// The controller verifies that device ownership was transferred, while
    /// the caller is responsible for waiting for the worker's ready signal.
    pub fn publish(&mut self) -> Result<(), LinuxPublishError<Backend::Error>> {
        let state = self.state();
        if state != LinuxControllerState::Prepared {
            return Err(LinuxPublishError::WrongState { state });
        }
        if !self.device_taken {
            return Err(LinuxPublishError::PacketDeviceNotTaken);
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
                    phase: LinuxOperationPhase::Publish,
                    operation_index,
                    operation,
                    token,
                }),
                Err(error) => {
                    let failed_operation = operation;
                    let (_, rollback_failures) =
                        rollback_steps(&mut self.backend, &mut self.published);
                    self.active = false;
                    return Err(LinuxPublishError::Apply {
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

    /// Reverses only published DNS and policy rules.
    ///
    /// Prepared routes and the TUN remain intact so the packet worker may be
    /// stopped after new host traffic is no longer captured.
    pub fn unpublish(&mut self) -> Result<LinuxCleanupOutcome, LinuxCleanupError<Backend::Error>> {
        if !self.prepare_complete || self.published.is_empty() {
            return Err(LinuxCleanupError::WrongState {
                state: self.state(),
            });
        }
        self.active = false;
        let (attempted, failures) = rollback_steps(&mut self.backend, &mut self.published);
        cleanup_result(attempted, failures)
    }

    /// Best-effort reversal of publish and prepare phases in global reverse
    /// activation order.
    ///
    /// Callers following normal shutdown should invoke `unpublish`, stop and
    /// drop the packet worker/device, then invoke this method. After abnormal
    /// failure this method also retries residual publish reversals before
    /// prepared operations and reports every failure.
    pub fn cleanup(&mut self) -> Result<LinuxCleanupOutcome, LinuxCleanupError<Backend::Error>> {
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
            Ok(LinuxCleanupOutcome {
                attempted,
                completed: attempted,
            })
        } else {
            Err(LinuxCleanupError::Rollback {
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
        self.device_taken = false;
        self.active = false;
    }
}

fn rollback_steps<Backend>(
    backend: &mut Backend,
    applied: &mut Vec<AppliedStep<Backend::RollbackToken>>,
) -> (usize, Vec<LinuxRollbackFailure<Backend::Error>>)
where
    Backend: LinuxHostMutationBackend,
{
    let attempted = applied.len();
    let mut residual = Vec::new();
    let mut failures = Vec::new();
    while let Some(step) = applied.pop() {
        match backend.rollback(&step.operation, &step.token) {
            Ok(()) => {}
            Err(error) => {
                failures.push(LinuxRollbackFailure {
                    phase: step.phase,
                    operation_index: step.operation_index,
                    operation: step.operation.clone(),
                    error,
                });
                residual.push(step);
            }
        }
    }
    // Steps are stored in activation order so a retry again pops them in exact
    // reverse activation order.
    residual.reverse();
    *applied = residual;
    (attempted, failures)
}

fn cleanup_result<Error>(
    attempted: usize,
    failures: Vec<LinuxRollbackFailure<Error>>,
) -> Result<LinuxCleanupOutcome, LinuxCleanupError<Error>> {
    if failures.is_empty() {
        Ok(LinuxCleanupOutcome {
            attempted,
            completed: attempted,
        })
    } else {
        Err(LinuxCleanupError::Rollback {
            attempted,
            completed: attempted.saturating_sub(failures.len()),
            failures,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct LinuxRollbackFailure<Error> {
    pub phase: LinuxOperationPhase,
    pub operation_index: usize,
    pub operation: LinuxHostOperation,
    pub error: Error,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LinuxPrepareError<Error> {
    Busy {
        state: LinuxControllerState,
    },
    Apply {
        operation_index: usize,
        operation: LinuxHostOperation,
        error: Error,
        rollback_failures: Vec<LinuxRollbackFailure<Error>>,
    },
}

impl<Error: fmt::Display> fmt::Display for LinuxPrepareError<Error> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy { state } => {
                write!(formatter, "VPN controller cannot prepare from {state:?}")
            }
            Self::Apply {
                operation_index,
                error,
                rollback_failures,
                ..
            } => {
                write!(
                    formatter,
                    "VPN prepare operation {operation_index} failed: {error}"
                )?;
                write_rollback_suffix(formatter, rollback_failures)
            }
        }
    }
}

impl<Error> std::error::Error for LinuxPrepareError<Error> where Error: std::error::Error + 'static {}

#[derive(Debug, PartialEq, Eq)]
pub enum LinuxPreparedDeviceError<Error> {
    WrongState { state: LinuxControllerState },
    AlreadyTaken,
    Backend(Error),
}

impl<Error: fmt::Display> fmt::Display for LinuxPreparedDeviceError<Error> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongState { state } => {
                write!(
                    formatter,
                    "prepared packet device is unavailable in {state:?}"
                )
            }
            Self::AlreadyTaken => formatter.write_str("prepared packet device was already taken"),
            Self::Backend(error) => {
                write!(formatter, "prepared packet-device handoff failed: {error}")
            }
        }
    }
}

impl<Error> std::error::Error for LinuxPreparedDeviceError<Error> where
    Error: std::error::Error + 'static
{
}

#[derive(Debug, PartialEq, Eq)]
pub enum LinuxPublishError<Error> {
    WrongState {
        state: LinuxControllerState,
    },
    PacketDeviceNotTaken,
    Apply {
        operation_index: usize,
        operation: LinuxHostOperation,
        error: Error,
        rollback_failures: Vec<LinuxRollbackFailure<Error>>,
    },
}

impl<Error: fmt::Display> fmt::Display for LinuxPublishError<Error> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongState { state } => {
                write!(formatter, "VPN controller cannot publish from {state:?}")
            }
            Self::PacketDeviceNotTaken => {
                formatter.write_str("VPN cannot publish before packet-device handoff")
            }
            Self::Apply {
                operation_index,
                error,
                rollback_failures,
                ..
            } => {
                write!(
                    formatter,
                    "VPN publish operation {operation_index} failed: {error}"
                )?;
                write_rollback_suffix(formatter, rollback_failures)
            }
        }
    }
}

impl<Error> std::error::Error for LinuxPublishError<Error> where Error: std::error::Error + 'static {}

fn write_rollback_suffix<Error>(
    formatter: &mut fmt::Formatter<'_>,
    failures: &[LinuxRollbackFailure<Error>],
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
pub struct LinuxCleanupOutcome {
    pub attempted: usize,
    pub completed: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LinuxCleanupError<Error> {
    WrongState {
        state: LinuxControllerState,
    },
    Rollback {
        attempted: usize,
        completed: usize,
        failures: Vec<LinuxRollbackFailure<Error>>,
    },
}

impl<Error: fmt::Display> fmt::Display for LinuxCleanupError<Error> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongState { state } => {
                write!(
                    formatter,
                    "VPN publish state cannot be reversed from {state:?}"
                )
            }
            Self::Rollback {
                attempted,
                completed,
                failures,
            } => write!(
                formatter,
                "VPN cleanup reverted {completed} of {attempted} operations; {} failed",
                failures.len()
            ),
        }
    }
}

impl<Error> std::error::Error for LinuxCleanupError<Error> where Error: std::error::Error + 'static {}

#[cfg(test)]
#[path = "tests_linux_transaction.rs"]
mod tests;
