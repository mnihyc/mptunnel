//! One-time handoff of a running, initially unauthenticated connection driver.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
};

use crate::{DomainFuture, ExecutionDomain};

#[cfg(test)]
#[path = "tests_execution_binding.rs"]
mod tests;

/// A live connection was already assigned to another execution domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("connection is already bound to a different execution domain")]
pub struct ExecutionDomainConflict;

#[derive(Debug, Default)]
pub(crate) struct ExecutionBinding {
    domain: OnceLock<ExecutionDomain>,
    // Only unbound polls hold this fence. Native deliberately unlocks its own
    // state while polling a source, so Native's mutex is not a handoff fence.
    unbound_poll: Mutex<()>,
}

impl ExecutionBinding {
    pub(crate) fn bind(&self, domain: ExecutionDomain) -> Result<(), ExecutionDomainConflict> {
        if let Some(bound) = self.domain.get() {
            return same_binding(bound, &domain);
        }
        let _fence = self.unbound_poll.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(bound) = self.domain.get() {
            return same_binding(bound, &domain);
        }
        // No unbound poll or destructor remains in progress when this returns.
        self.domain.set(domain).expect("binding fence held");
        Ok(())
    }

    pub(crate) fn domain(&self) -> Option<ExecutionDomain> {
        self.domain.get().cloned()
    }

    pub(crate) fn wrap<F: Future>(self: &Arc<Self>, future: F) -> BoundDriver<F> {
        BoundDriver {
            binding: self.clone(),
            unbound: Some(Box::pin(future)),
            bound: None,
        }
    }
}

fn same_binding(a: &ExecutionDomain, b: &ExecutionDomain) -> Result<(), ExecutionDomainConflict> {
    if a.same_domain(b) {
        Ok(())
    } else {
        Err(ExecutionDomainConflict)
    }
}

pub(crate) struct BoundDriver<F: Future> {
    binding: Arc<ExecutionBinding>,
    unbound: Option<Pin<Box<F>>>,
    bound: Option<DomainFuture<Pin<Box<F>>>>,
}

impl<F: Future> Future for BoundDriver<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(bound) = this.bound.as_mut() {
            return Pin::new(bound).poll(cx);
        }
        let fence = this
            .binding
            .unbound_poll
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(domain) = this.binding.domain() {
            let future = this.unbound.take().expect("driver polled after completion");
            this.bound = Some(domain.wrap(future));
            drop(fence);
            return Pin::new(this.bound.as_mut().unwrap()).poll(cx);
        }
        let result = this
            .unbound
            .as_mut()
            .expect("driver polled after completion")
            .as_mut()
            .poll(cx);
        if result.is_ready() {
            // Completion destruction is part of the old unbound turn too.
            drop(this.unbound.take());
        }
        drop(fence);
        result
    }
}

impl<F: Future> Drop for BoundDriver<F> {
    fn drop(&mut self) {
        let Some(future) = self.unbound.take() else {
            return;
        };
        let fence = self
            .binding
            .unbound_poll
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(domain) = self.binding.domain() {
            // Binding may complete before this driver gets its next poll.
            drop(fence);
            domain.with_exclusive(|| drop(future));
        } else {
            drop(future);
            drop(fence);
        }
        // A transitioned driver's DomainFuture owns its own destruction fence.
    }
}
