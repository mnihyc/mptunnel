//! Sticky client MPP-session lifetime.
//!
//! Native carrier loss is deliberately absent from this owner. Only an
//! explicit `SESSION_CLOSE` (received, or locally committed before a future
//! sender writes one) makes the complete `SessionId` terminal.

use crate::protocol::CloseReason;
use crate::runtime::error::RuntimeError;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Clone)]
pub(in crate::runtime) struct ClientSessionLifecycle {
    inner: Arc<ClientSessionLifecycleInner>,
}

struct ClientSessionLifecycleInner {
    /// Serializes terminal publication with readiness/Product admission. The
    /// protected closures must stay synchronous and bounded.
    commitment: Mutex<()>,
    retirement: watch::Sender<Option<CloseReason>>,
}

impl ClientSessionLifecycle {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            inner: Arc::new(ClientSessionLifecycleInner {
                commitment: Mutex::new(()),
                retirement: watch::channel(None).0,
            }),
        }
    }

    pub(in crate::runtime) fn retirement(&self) -> ClientSessionRetirement {
        ClientSessionRetirement {
            reason: self.inner.retirement.subscribe(),
        }
    }

    pub(in crate::runtime) fn reason(&self) -> Option<CloseReason> {
        *self.inner.retirement.borrow()
    }

    pub(in crate::runtime) fn ensure_active(&self) -> Result<(), RuntimeError> {
        self.reason()
            .map_or(Ok(()), |reason| Err(RuntimeError::RemoteClosed(reason)))
    }

    /// Publishes one irreversible session terminal. The first reason is the
    /// wire-authoritative reason observed by every sibling owner.
    pub(in crate::runtime) fn retire(&self, reason: CloseReason) -> CloseReason {
        let _commitment = self
            .inner
            .commitment
            .lock()
            .expect("client session lifecycle commitment lock");
        if let Some(existing) = *self.inner.retirement.borrow() {
            return existing;
        }
        self.inner.retirement.send_replace(Some(reason));
        reason
    }

    /// Linearizes a readiness or Product-admission publication against the
    /// complete-session terminal. This is intentionally not a data hot-path
    /// primitive.
    pub(in crate::runtime) fn commit_if_active<T>(
        &self,
        commit: impl FnOnce() -> T,
    ) -> Result<T, CloseReason> {
        let _commitment = self
            .inner
            .commitment
            .lock()
            .expect("client session lifecycle commitment lock");
        if let Some(reason) = *self.inner.retirement.borrow() {
            return Err(reason);
        }
        Ok(commit())
    }
}

impl Default for ClientSessionLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ClientSessionLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientSessionLifecycle")
            .field("reason", &self.reason())
            .finish()
    }
}

/// Cloneable sticky terminal observation for one client `SessionId`.
#[derive(Clone)]
pub(in crate::runtime) struct ClientSessionRetirement {
    reason: watch::Receiver<Option<CloseReason>>,
}

impl ClientSessionRetirement {
    pub(in crate::runtime) fn reason(&self) -> Option<CloseReason> {
        *self.reason.borrow()
    }

    pub(in crate::runtime) async fn wait(mut self) -> CloseReason {
        loop {
            if let Some(reason) = *self.reason.borrow_and_update() {
                return reason;
            }
            if self.reason.changed().await.is_err() {
                std::future::pending::<CloseReason>().await;
            }
        }
    }
}

impl std::fmt::Debug for ClientSessionRetirement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientSessionRetirement")
            .field("reason", &self.reason())
            .finish()
    }
}

#[cfg(test)]
#[path = "tests_client_session.rs"]
mod tests;
