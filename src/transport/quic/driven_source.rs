//! Typed ownership bridge for an actor polled by the native connection driver.

use std::{
    any::Any,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

/// One connection-bound registration, obtained before moving its send half.
/// This capability and the resulting owning handle are deliberately not Clone.
#[derive(Debug)]
pub struct NativeSourceRegistration {
    connection: quinn::Connection,
}

impl NativeSourceRegistration {
    pub(super) fn new(connection: quinn::Connection) -> Self {
        Self { connection }
    }

    /// Move an actor into native-driven polling without changing its result.
    ///
    /// The actor must do finite cooperative work per poll. The caller must not
    /// hold Native or Product ownership while polling/dropping the result handle:
    /// cancellation synchronously waits for a current actor poll and destructor.
    /// The handle is created after the actor, so that actor cannot own its own
    /// cancellation handle. No stream-specific STOP policy is added here.
    pub fn register<F, R>(self, future: F) -> Result<DrivenSource<R>, quinn::ConnectionError>
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let (mut handle, mut proxy) = source_pair(future);
        // Product registers only after authenticated execution binding. Keep
        // the lifetime capability even if the carrier registry later detaches.
        handle.domain = self.connection.execution_domain();
        proxy.connection = Some(self.connection.clone());
        self.connection.register_transmit_source(Box::pin(proxy))?;
        Ok(handle)
    }
}

/// Native terminated the registered actor before it returned a result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeSourceStopped {
    /// Exact recorded connection failure, unavailable for an unbound proxy.
    pub cause: Option<quinn::ConnectionError>,
}

impl fmt::Display for NativeSourceStopped {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            Some(cause) => write!(
                formatter,
                "native connection stopped the registered source: {cause}"
            ),
            None => formatter.write_str("native driver stopped the registered source"),
        }
    }
}

impl std::error::Error for NativeSourceStopped {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(|cause| cause as _)
    }
}

type Actor<R> = Pin<Box<dyn Future<Output = R> + Send + 'static>>;
type PanicPayload = Box<dyn Any + Send + 'static>;

enum Outcome<R> {
    Completed(R),
    Panicked(PanicPayload),
    Stopped(NativeSourceStopped),
}

struct SourceState<R> {
    actor: Option<Actor<R>>,
    outcome: Option<Outcome<R>>,
    result_waker: Option<Waker>,
    proxy_waker: Option<Waker>,
}

impl<R> SourceState<R> {
    /// The caller retains the source mutex until this actual destruction ends.
    /// Taking the Option alone is not a cancellation/completion fence.
    fn destroy_actor(&mut self) -> Option<PanicPayload> {
        self.actor
            .take()
            .and_then(|actor| catch_unwind(AssertUnwindSafe(|| drop(actor))).err())
    }
}

/// Owning result/cancellation handle for exactly one registered actor.
///
/// Dropping this handle synchronously destroys that actor after any concurrent
/// finite poll. A poll or destructor panic is resumed on this handle's original
/// parent, not on the shared native driver. As with ordinary Rust unwinding,
/// aborting panics cannot be contained by this bridge.
#[must_use = "dropping a driven source synchronously cancels its actor"]
pub struct DrivenSource<R> {
    state: Arc<Mutex<SourceState<R>>>,
    domain: Option<quinn::ExecutionDomain>,
    completed: bool,
}

impl<R> fmt::Debug for DrivenSource<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DrivenSource")
            .field("completed", &self.completed)
            .finish_non_exhaustive()
    }
}

impl<R> Future for DrivenSource<R> {
    type Output = Result<R, NativeSourceStopped>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.completed, "driven source polled after completion");
        let outcome = {
            let mut state = this.state.lock().expect("driven source ownership");
            match state.outcome.take() {
                Some(outcome) => {
                    state.result_waker = None;
                    Some(outcome)
                }
                None => {
                    if state
                        .result_waker
                        .as_ref()
                        .is_none_or(|old| !old.will_wake(cx.waker()))
                    {
                        state.result_waker = Some(cx.waker().clone());
                    }
                    None
                }
            }
        };
        let Some(outcome) = outcome else {
            return Poll::Pending;
        };
        this.completed = true;
        match outcome {
            Outcome::Completed(result) => Poll::Ready(Ok(result)),
            Outcome::Stopped(stopped) => Poll::Ready(Err(stopped)),
            Outcome::Panicked(panic) => resume_unwind(panic),
        }
    }
}

impl<R> Drop for DrivenSource<R> {
    fn drop(&mut self) {
        if let Some(domain) = self.domain.clone() {
            // Domain must precede SourceState, including when the original
            // parent is cancelled from a different runtime worker.
            domain.with_exclusive(|| self.cancel());
        } else {
            self.cancel();
        }
    }
}

impl<R> DrivenSource<R> {
    fn cancel(&mut self) {
        if self.completed {
            return;
        }
        let (outcome, cancelled_panic, proxy_waker) = {
            // This guard stays outside the catch boundary and outlives actual
            // actor destruction. Concurrent native polling cannot overlap it.
            let mut state = self.state.lock().expect("driven source cancellation");
            let cancelled_panic = state.destroy_actor();
            let outcome = state.outcome.take();
            state.result_waker = None;
            (outcome, cancelled_panic, state.proxy_waker.take())
        };
        if let Some(waker) = proxy_waker {
            waker.wake();
        }
        if let Some(panic) = cancelled_panic {
            resume_parent_panic(panic);
        }
        match outcome {
            Some(Outcome::Panicked(panic)) => resume_parent_panic(panic),
            // The unconsumed result is destroyed on the parent, never Native.
            Some(Outcome::Completed(result)) => drop(result),
            Some(Outcome::Stopped(_)) | None => {}
        }
    }
}

/// Never replace an already propagating parent panic with a secondary panic.
/// Forgetting that secondary payload also avoids a panic in its own destructor.
fn resume_parent_panic(panic: PanicPayload) {
    if std::thread::panicking() {
        std::mem::forget(panic);
    } else {
        resume_unwind(panic);
    }
}

struct NativeSourceProxy<R> {
    state: Arc<Mutex<SourceState<R>>>,
    connection: Option<quinn::Connection>,
}

impl<R> Future for NativeSourceProxy<R> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.state.lock().expect("driven source poll");
        if state.actor.is_none() {
            return Poll::Ready(());
        }
        state.proxy_waker = Some(cx.waker().clone());
        // Native is unlocked by the registering driver. Keep the source guard
        // outside catch_unwind so an actor panic does not poison this mutex.
        let polled = catch_unwind(AssertUnwindSafe(|| {
            state.actor.as_mut().expect("live actor").as_mut().poll(cx)
        }));
        let outcome = match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(result)) => match state.destroy_actor() {
                None => Outcome::Completed(result),
                Some(panic) => {
                    // The actor destructor already failed. Dispose of its
                    // returned value without allowing a second unwind into
                    // Native, retaining the first failure for the parent.
                    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(result))) {
                        std::mem::forget(secondary);
                    }
                    Outcome::Panicked(panic)
                }
            },
            Err(panic) => {
                if let Some(secondary) = state.destroy_actor() {
                    std::mem::forget(secondary);
                }
                Outcome::Panicked(panic)
            }
        };
        // Destruction completed while exclusively owned before publication.
        state.outcome = Some(outcome);
        let result_waker = state.result_waker.take();
        drop(state);
        if let Some(waker) = result_waker {
            waker.wake();
        }
        Poll::Ready(())
    }
}

impl<R> Drop for NativeSourceProxy<R> {
    fn drop(&mut self) {
        if let Some(domain) = self
            .connection
            .as_ref()
            .and_then(quinn::Connection::execution_domain)
        {
            domain.with_exclusive(|| self.stop());
        } else {
            self.stop();
        }
    }
}

impl<R> NativeSourceProxy<R> {
    fn stop(&mut self) {
        // The registering driver has released Native. Read its recorded cause
        // before taking source ownership; no Native guard spans actor cleanup.
        let cause = self
            .connection
            .as_ref()
            .and_then(quinn::Connection::close_reason);
        let mut state = self.state.lock().expect("driven source native stop");
        if state.actor.is_none() {
            return;
        }
        let outcome = match state.destroy_actor() {
            Some(panic) => Outcome::Panicked(panic),
            None => Outcome::Stopped(NativeSourceStopped { cause }),
        };
        state.outcome = Some(outcome);
        let result_waker = state.result_waker.take();
        drop(state);
        if let Some(waker) = result_waker {
            waker.wake();
        }
    }
}

fn source_pair<F, R>(future: F) -> (DrivenSource<R>, NativeSourceProxy<R>)
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    let state = Arc::new(Mutex::new(SourceState {
        actor: Some(Box::pin(future)),
        outcome: None,
        result_waker: None,
        proxy_waker: None,
    }));
    (
        DrivenSource {
            state: state.clone(),
            domain: None,
            completed: false,
        },
        NativeSourceProxy {
            state,
            connection: None,
        },
    )
}

#[cfg(test)]
#[path = "tests_driven_source.rs"]
mod tests;
