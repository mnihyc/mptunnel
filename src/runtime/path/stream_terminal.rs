//! Logical RESET custody before an attachment has a committed input consumer.

use crate::protocol::{ResetReason, SessionId, StreamId};
use crate::runtime::error::RuntimeError;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Notify;

#[derive(Clone)]
pub(in crate::runtime) struct ClientStreamTerminalScope(Arc<StreamTerminalInner>);

struct StreamTerminalInner {
    session_id: SessionId,
    stream_id: StreamId,
    state: Mutex<StreamTerminalState>,
    changed: Notify,
}

#[derive(Default)]
struct StreamTerminalState {
    reason: Option<ResetReason>,
    closed: bool,
}

/// One logical owner, moved from initial acquisition to the active Product.
pub(in crate::runtime) struct ClientStreamTerminalOwner(ClientStreamTerminalScope);

impl ClientStreamTerminalScope {
    pub(in crate::runtime) fn for_open(
        scope: Option<Self>,
        session_id: SessionId,
        stream_id: StreamId,
    ) -> Result<(Self, Option<ClientStreamTerminalOwner>), RuntimeError> {
        if let Some(scope) = scope {
            if scope.0.session_id != session_id || scope.0.stream_id != stream_id {
                return Err(RuntimeError::Protocol("logical terminal scope mismatch"));
            }
            scope.ensure_active()?;
            return Ok((scope, None));
        }
        let scope = Self(Arc::new(StreamTerminalInner {
            session_id,
            stream_id,
            state: Mutex::new(StreamTerminalState::default()),
            changed: Notify::new(),
        }));
        Ok((scope.clone(), Some(ClientStreamTerminalOwner(scope))))
    }

    pub(in crate::runtime) fn pending_input(&self) -> PendingStreamTerminal {
        PendingStreamTerminal(Arc::new(PendingStreamTerminalInner {
            scope: Arc::downgrade(&self.0),
            committed: AtomicBool::new(false),
        }))
    }

    pub(in crate::runtime) fn reset_error(&self) -> Option<RuntimeError> {
        self.0
            .state
            .lock()
            .expect("stream terminal lock")
            .reason
            .map(RuntimeError::RemoteReset)
    }

    pub(in crate::runtime) fn ensure_active(&self) -> Result<(), RuntimeError> {
        let state = self.0.state.lock().expect("stream terminal lock");
        if let Some(reason) = state.reason {
            Err(RuntimeError::RemoteReset(reason))
        } else if state.closed {
            Err(RuntimeError::ReliablePathRetired)
        } else {
            Ok(())
        }
    }

    pub(in crate::runtime) async fn wait(&self) -> RuntimeError {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Err(error) = self.ensure_active() {
                return error;
            }
            changed.await;
        }
    }

    pub(in crate::runtime) async fn complete<T>(
        &self,
        operation: impl Future<Output = Result<T, RuntimeError>>,
    ) -> Result<T, RuntimeError> {
        let terminal = self.wait();
        tokio::pin!(terminal);
        tokio::pin!(operation);
        tokio::select! {
            biased;
            error = &mut terminal => Err(error),
            result = &mut operation => {
                self.ensure_active()?;
                result
            },
        }
    }
}

impl Drop for ClientStreamTerminalOwner {
    fn drop(&mut self) {
        self.0.0.state.lock().expect("stream terminal lock").closed = true;
        self.0.0.changed.notify_waiters();
    }
}

/// Exact pending input capability. Native publishers retain no logical owner.
#[derive(Clone)]
pub(in crate::runtime) struct PendingStreamTerminal(Arc<PendingStreamTerminalInner>);

struct PendingStreamTerminalInner {
    scope: Weak<StreamTerminalInner>,
    // Physical acceptance alone never commits input. Only installing the
    // Product's ordered consumer ends precommit publication for this attempt.
    // Access is serialized by the upgraded logical scope's state mutex.
    committed: AtomicBool,
}

impl PendingStreamTerminal {
    pub(in crate::runtime) fn scope(&self) -> Option<ClientStreamTerminalScope> {
        self.0.scope.upgrade().map(ClientStreamTerminalScope)
    }

    pub(in crate::runtime) fn publish_reset(&self, stream_id: StreamId, reason: ResetReason) {
        let Some(scope) = self.0.scope.upgrade() else {
            return;
        };
        if scope.stream_id != stream_id {
            return;
        }
        let publish = {
            let mut state = scope.state.lock().expect("stream terminal lock");
            if state.closed || self.0.committed.load(Ordering::Relaxed) || state.reason.is_some() {
                false
            } else {
                state.reason = Some(reason);
                true
            }
        };
        if publish {
            scope.changed.notify_waiters();
        }
    }

    /// Install the ordinary ordered input owner while publication is fenced.
    /// The closure must be synchronous and must not access this scope again.
    pub(in crate::runtime) fn commit<T>(
        &self,
        install: impl FnOnce() -> T,
    ) -> Result<T, RuntimeError> {
        let scope = self
            .0
            .scope
            .upgrade()
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let state = scope.state.lock().expect("stream terminal lock");
        if let Some(reason) = state.reason {
            return Err(RuntimeError::RemoteReset(reason));
        }
        if state.closed {
            return Err(RuntimeError::ReliablePathRetired);
        }
        self.0.committed.store(true, Ordering::Relaxed);
        Ok(install())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[test]
    fn pending_terminal_is_exact_and_keeps_the_first_reason() {
        let (scope, _owner) =
            ClientStreamTerminalScope::for_open(None, SessionId(310), StreamId(710)).unwrap();
        for (session, stream) in [
            (SessionId(311), StreamId(710)),
            (SessionId(310), StreamId(711)),
        ] {
            assert!(matches!(
                ClientStreamTerminalScope::for_open(Some(scope.clone()), session, stream),
                Err(RuntimeError::Protocol("logical terminal scope mismatch")),
            ));
        }
        let publisher = scope.pending_input();
        publisher.publish_reset(StreamId(711), ResetReason::TimedOut);
        assert!(scope.ensure_active().is_ok());
        publisher.publish_reset(StreamId(710), ResetReason::RemoteClosed);
        publisher.publish_reset(StreamId(710), ResetReason::TimedOut);
        assert!(matches!(
            scope.reset_error(),
            Some(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
        ));
        let mut installed = false;
        assert!(matches!(
            publisher.commit(|| installed = true),
            Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
        ));
        assert!(
            !installed,
            "a routed terminal forbids later input installation"
        );
    }

    #[test]
    fn native_publisher_neither_owns_nor_revives_the_logical_stream() {
        let (scope, owner) =
            ClientStreamTerminalScope::for_open(None, SessionId(312), StreamId(712)).unwrap();
        let publisher = scope.pending_input();
        drop(owner);
        publisher.publish_reset(StreamId(712), ResetReason::RemoteClosed);
        assert!(scope.reset_error().is_none());
        assert!(matches!(
            scope.ensure_active(),
            Err(RuntimeError::ReliablePathRetired)
        ));
        assert!(matches!(
            publisher.commit(|| ()),
            Err(RuntimeError::ReliablePathRetired)
        ));
        drop(scope);
        assert!(
            publisher.scope().is_none(),
            "a surviving native actor retains only weak custody"
        );
    }

    #[test]
    fn committed_attachment_does_not_silence_a_pending_sibling() {
        let (scope, _owner) =
            ClientStreamTerminalScope::for_open(None, SessionId(314), StreamId(714)).unwrap();
        let attached = scope.pending_input();
        let pending = scope.pending_input();
        attached.commit(|| ()).unwrap();
        attached.publish_reset(StreamId(714), ResetReason::TimedOut);
        assert!(
            scope.reset_error().is_none(),
            "committed input retains FIFO authority"
        );
        pending.publish_reset(StreamId(714), ResetReason::RemoteClosed);
        assert!(matches!(
            scope.reset_error(),
            Some(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
        ));
    }

    #[tokio::test]
    async fn pending_terminal_wakes_an_existing_owner_and_overrides_ready_timeout() {
        let (scope, _owner) =
            ClientStreamTerminalScope::for_open(None, SessionId(313), StreamId(713)).unwrap();
        let publisher = scope.pending_input();
        let waiting = scope.complete(std::future::pending::<Result<(), RuntimeError>>());
        tokio::pin!(waiting);
        assert!(waiting.as_mut().now_or_never().is_none());
        publisher.publish_reset(StreamId(713), ResetReason::RemoteClosed);
        assert!(matches!(
            waiting.await,
            Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
        ));
        assert!(matches!(
            scope
                .complete(async { Err::<(), _>(RuntimeError::PathOpenTimedOut) })
                .await,
            Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed)),
        ));
    }
}
