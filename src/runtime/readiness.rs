//! Event-driven runtime-generation lifecycle and startup barriers.
//!
//! Readiness is a composition concern. Data-plane actors never receive these
//! handles, and no packet or stream loop polls generation state.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeGenerationPhase {
    Starting,
    Ready,
    Stopping,
    Failed,
}

impl RuntimeGenerationPhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Stopping => "stopping",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeGenerationStatus {
    pub(crate) phase: RuntimeGenerationPhase,
    pub(crate) failure: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeGenerationControl {
    status: watch::Sender<RuntimeGenerationStatus>,
    stop: watch::Sender<Option<RuntimeGenerationStopReason>>,
    retirement_authorized: watch::Sender<bool>,
}

impl RuntimeGenerationControl {
    pub(crate) fn new() -> Self {
        let (status, _) = watch::channel(RuntimeGenerationStatus {
            phase: RuntimeGenerationPhase::Starting,
            failure: None,
        });
        let (stop, _) = watch::channel(None);
        let (retirement_authorized, _) = watch::channel(true);
        Self {
            status,
            stop,
            retirement_authorized,
        }
    }

    pub(crate) fn status(&self) -> RuntimeGenerationStatus {
        self.status.borrow().clone()
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.status.borrow().phase == RuntimeGenerationPhase::Ready
    }

    pub(crate) async fn wait_until_ready(&self) -> Result<(), RuntimeGenerationReadinessError> {
        let mut status = self.status.subscribe();
        loop {
            let current = status.borrow_and_update().clone();
            match current.phase {
                RuntimeGenerationPhase::Starting => {}
                RuntimeGenerationPhase::Ready => return Ok(()),
                RuntimeGenerationPhase::Stopping => {
                    return Err(RuntimeGenerationReadinessError::Stopping);
                }
                RuntimeGenerationPhase::Failed => {
                    return Err(RuntimeGenerationReadinessError::Failed(current.failure));
                }
            }
            status
                .changed()
                .await
                .map_err(|_| RuntimeGenerationReadinessError::Closed)?;
        }
    }

    pub(crate) fn mark_stopping(&self) {
        self.status.send_if_modified(|status| {
            if matches!(
                status.phase,
                RuntimeGenerationPhase::Starting | RuntimeGenerationPhase::Ready
            ) {
                status.phase = RuntimeGenerationPhase::Stopping;
                status.failure = None;
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn request_reload(&self) {
        self.mark_stopping();
        self.stop.send_if_modified(|reason| {
            if reason.is_none() {
                *reason = Some(RuntimeGenerationStopReason::ReloadRequested);
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn request_shutdown(&self) {
        self.mark_stopping();
        self.stop.send_if_modified(|reason| {
            if *reason != Some(RuntimeGenerationStopReason::ShutdownRequested) {
                *reason = Some(RuntimeGenerationStopReason::ShutdownRequested);
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn stop_reason(&self) -> Option<RuntimeGenerationStopReason> {
        *self.stop.borrow()
    }

    pub(crate) async fn wait_for_stop(&self) -> RuntimeGenerationStopReason {
        let mut stop = self.stop.subscribe();
        loop {
            if let Some(reason) = *stop.borrow_and_update() {
                return reason;
            }
            stop.changed()
                .await
                .expect("runtime generation retains its stop sender");
        }
    }

    pub(crate) fn defer_retirement(&self) {
        if self.stop_reason().is_none() {
            self.retirement_authorized.send_replace(false);
        }
    }

    pub(crate) fn authorize_retirement(&self) {
        self.retirement_authorized.send_replace(true);
    }

    pub(crate) async fn wait_for_retirement_authorization(&self) {
        let mut authorized = self.retirement_authorized.subscribe();
        loop {
            if *authorized.borrow_and_update() {
                return;
            }
            authorized
                .changed()
                .await
                .expect("runtime generation retains its retirement sender");
        }
    }

    pub(crate) fn mark_failed(&self, failure: impl Into<Arc<str>>) {
        let failure = failure.into();
        self.status.send_if_modified(|status| {
            if matches!(
                status.phase,
                RuntimeGenerationPhase::Starting | RuntimeGenerationPhase::Ready
            ) {
                status.phase = RuntimeGenerationPhase::Failed;
                status.failure = Some(failure);
                true
            } else {
                false
            }
        });
    }

    pub(super) fn mark_ready(&self) {
        self.status.send_if_modified(|status| {
            if status.phase == RuntimeGenerationPhase::Starting {
                status.phase = RuntimeGenerationPhase::Ready;
                status.failure = None;
                true
            } else {
                false
            }
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeGenerationStopReason {
    ReloadRequested,
    ShutdownRequested,
}

impl Default for RuntimeGenerationControl {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuntimeGenerationReadinessError {
    Stopping,
    Failed(Option<Arc<str>>),
    Closed,
}

impl fmt::Display for RuntimeGenerationReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopping => formatter.write_str("runtime generation is stopping"),
            Self::Failed(Some(failure)) => {
                write!(formatter, "runtime generation failed: {failure}")
            }
            Self::Failed(None) => formatter.write_str("runtime generation failed"),
            Self::Closed => formatter.write_str("runtime generation readiness channel closed"),
        }
    }
}

impl std::error::Error for RuntimeGenerationReadinessError {}

#[derive(Debug, Clone)]
pub(super) struct RuntimeReadinessBarrier {
    inner: Arc<RuntimeReadinessBarrierInner>,
}

#[derive(Debug)]
struct RuntimeReadinessBarrierInner {
    generation: RuntimeGenerationControl,
    state: Mutex<RuntimeReadinessBarrierState>,
}

#[derive(Debug, Default)]
struct RuntimeReadinessBarrierState {
    next_id: u64,
    sealed: bool,
    pending: BTreeMap<u64, &'static str>,
}

impl RuntimeReadinessBarrier {
    pub(super) fn new(generation: RuntimeGenerationControl) -> Self {
        Self {
            inner: Arc::new(RuntimeReadinessBarrierInner {
                generation,
                state: Mutex::new(RuntimeReadinessBarrierState::default()),
            }),
        }
    }

    pub(super) fn require(&self, service: &'static str) -> RequiredServiceReadiness {
        let id = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.sealed {
                drop(state);
                self.inner.generation.mark_failed(format!(
                    "required service `{service}` registered after startup barrier was sealed"
                ));
                return RequiredServiceReadiness {
                    barrier: self.inner.clone(),
                    id: None,
                    service,
                };
            }
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.pending.insert(id, service);
            id
        };
        RequiredServiceReadiness {
            barrier: self.inner.clone(),
            id: Some(id),
            service,
        }
    }

    pub(super) fn seal(&self) {
        let ready = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.sealed {
                false
            } else {
                state.sealed = true;
                state.pending.is_empty()
            }
        };
        if ready {
            self.inner.generation.mark_ready();
        }
    }
}

#[derive(Debug)]
pub(super) struct RequiredServiceReadiness {
    barrier: Arc<RuntimeReadinessBarrierInner>,
    id: Option<u64>,
    service: &'static str,
}

impl RequiredServiceReadiness {
    pub(super) fn ready(mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        let ready = {
            let mut state = self
                .barrier
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pending.remove(&id);
            state.sealed && state.pending.is_empty()
        };
        if ready {
            self.barrier.generation.mark_ready();
        }
    }
}

impl Drop for RequiredServiceReadiness {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        let was_pending = self
            .barrier
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .remove(&id)
            .is_some();
        if was_pending {
            self.barrier.generation.mark_failed(format!(
                "required service `{}` exited before readiness",
                self.service
            ));
        }
    }
}

#[cfg(test)]
#[path = "tests_readiness.rs"]
mod tests;
