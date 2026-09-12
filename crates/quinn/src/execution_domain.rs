//! Explicit, cooperative serialization of related actor polls and destruction.
//!
//! A domain covers only members explicitly wrapped by its owner. It is neither
//! inherited by spawned tasks nor a process-wide executor. Each poll/destructor
//! must finish in finite time. Enter before taking Native, Product, or source
//! ownership; never enter while retaining those locks. Synchronous entry into a
//! different domain while already entered is outside this contract: reciprocal
//! cross-domain destruction could deadlock. Ordinary channel I/O between domain
//! members does not acquire the peer's domain.

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Waker},
    thread::{self, ThreadId},
};

/// An explicit serialization owner for finite actor polls and destruction.
///
/// Asynchronous members enter FIFO, with one wake for the queued head when the
/// current owner leaves. Synchronous cleanup takes priority over queued polls:
/// making every executor worker wait for a queued asynchronous owner would
/// prevent that owner from ever running. Cleanup waits only for current finite
/// work (and other synchronous cleanup), never for the asynchronous queue.
#[derive(Clone, Default)]
pub struct ExecutionDomain {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    released: Condvar,
}

#[derive(Default)]
struct State {
    owner: Option<ThreadId>,
    depth: usize,
    blocking_waiters: usize,
    queue: VecDeque<Arc<Waiter>>,
}

struct Waiter {
    // The member retains this cell, so replacing its waker is O(1), even when
    // it is not the queue head. Only scheduler state is protected by this lock.
    waker: Mutex<Waker>,
}

impl fmt::Debug for ExecutionDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionDomain")
            .field("identity", &Arc::as_ptr(&self.inner))
            .finish_non_exhaustive()
    }
}

impl ExecutionDomain {
    /// Wrap one actor without imposing a `Send` or `'static` requirement.
    ///
    /// The actual future is pinned separately and destroyed under the domain
    /// on completion or cancellation. Its returned value is owned by the caller;
    /// resources requiring domain-bound destruction must not escape unwrapped
    /// through that value.
    pub fn wrap<F: Future>(&self, future: F) -> DomainFuture<F> {
        DomainFuture {
            domain: self.clone(),
            future: Some(Box::pin(future)),
            waiter: None,
        }
    }

    /// Run finite synchronous ownership cleanup under this domain.
    ///
    /// This may wait for a current poll on another thread. Callers must retain
    /// no Native, Product, or source lock while entering. Same-domain nested
    /// cleanup is allowed and remains inside the outer exclusive interval.
    pub fn with_exclusive<R>(&self, action: impl FnOnce() -> R) -> R {
        let _guard = self.enter_blocking(&mut None);
        action()
    }

    /// Whether two handles name the same live execution owner.
    pub fn same_domain(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    fn poll_enter(&self, waiter: &mut Option<Arc<Waiter>>, cx: &Context<'_>) -> Option<Guard> {
        let current = thread::current().id();
        let mut state = self.inner.state.lock().expect("execution domain state");
        if state.owner == Some(current) {
            // A previously queued child may be canceled or polled from its
            // explicitly entered parent. This exceptional removal is O(N).
            let canceled = remove_waiter(&mut state, waiter);
            state.depth += 1;
            let guard = self.guard();
            drop(state);
            drop(canceled);
            return Some(guard);
        }

        let first = match waiter.as_ref() {
            Some(waiter) => state
                .queue
                .front()
                .is_some_and(|head| Arc::ptr_eq(head, waiter)),
            None => state.queue.is_empty(),
        };
        if state.owner.is_none() && state.blocking_waiters == 0 && first {
            let granted = waiter.take();
            if granted.is_some() {
                state.queue.pop_front().expect("queued domain head");
            }
            state.owner = Some(current);
            state.depth = 1;
            let guard = self.guard();
            drop(state);
            drop(granted);
            return Some(guard);
        }

        let retired_waker = match waiter {
            Some(waiter) => {
                let mut old = waiter.waker.lock().expect("execution domain waiter");
                if !old.will_wake(cx.waker()) {
                    Some(std::mem::replace(&mut *old, cx.waker().clone()))
                } else {
                    None
                }
            }
            None => {
                let queued = Arc::new(Waiter {
                    waker: Mutex::new(cx.waker().clone()),
                });
                state.queue.push_back(queued.clone());
                *waiter = Some(queued);
                None
            }
        };
        drop(state);
        // Releasing the previous waker may run its implementation's drop hook.
        drop(retired_waker);
        None
    }

    fn enter_blocking(&self, waiter: &mut Option<Arc<Waiter>>) -> Guard {
        let current = thread::current().id();
        let mut state = self.inner.state.lock().expect("execution domain state");
        // Cancel this member's pending asynchronous claim before waiting.
        // Cleanup cannot depend on its own canceled task being scheduled.
        let canceled = remove_waiter(&mut state, waiter);
        if state.owner == Some(current) {
            state.depth += 1;
            let guard = self.guard();
            drop(state);
            drop(canceled);
            return guard;
        }
        state.blocking_waiters += 1;
        while state.owner.is_some() {
            state = self
                .inner
                .released
                .wait(state)
                .expect("execution domain cleanup wait");
        }
        state.blocking_waiters -= 1;
        state.owner = Some(current);
        state.depth = 1;
        let guard = self.guard();
        drop(state);
        drop(canceled);
        guard
    }

    fn guard(&self) -> Guard {
        Guard {
            inner: self.inner.clone(),
            not_send: PhantomData,
        }
    }
}

fn remove_waiter(state: &mut State, waiter: &mut Option<Arc<Waiter>>) -> Option<Arc<Waiter>> {
    let Some(waiter) = waiter.take() else {
        return None;
    };
    // Cancellation is uncommon and can target any member. Ordinary polling
    // and granting inspect only the queue head and never scan the queue.
    let position = state
        .queue
        .iter()
        .position(|queued| Arc::ptr_eq(queued, &waiter))
        .expect("registered execution domain waiter");
    state.queue.remove(position);
    // Keep the last cell (and its waker) alive until the scheduler unlocks.
    Some(waiter)
}

struct Guard {
    inner: Arc<Inner>,
    // ThreadId reentrancy is valid only while this guard stays on its thread.
    not_send: PhantomData<Rc<()>>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.inner.state.lock().expect("execution domain release");
            debug_assert_eq!(state.owner, Some(thread::current().id()));
            state.depth -= 1;
            if state.depth != 0 {
                return;
            }
            state.owner = None;
            if state.blocking_waiters != 0 {
                self.inner.released.notify_one();
                None
            } else {
                state.queue.front().cloned()
            }
        };
        // Neither actor code nor a potentially reentrant waker runs under the
        // scheduler mutex. Actor panics therefore do not poison that mutex.
        if let Some(waiter) = wake {
            let waker = waiter
                .waker
                .lock()
                .expect("execution domain waiter wake")
                .clone();
            waker.wake();
        }
    }
}

/// An actor whose actual polls and destruction use one explicit domain.
#[must_use = "an execution-domain future must be polled or spawned"]
pub struct DomainFuture<F: Future> {
    domain: ExecutionDomain,
    future: Option<Pin<Box<F>>>,
    waiter: Option<Arc<Waiter>>,
}

impl<F: Future> fmt::Debug for DomainFuture<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DomainFuture")
            .field("domain", &self.domain)
            .field("active", &self.future.is_some())
            .field("queued", &self.waiter.is_some())
            .finish()
    }
}

impl<F: Future> Future for DomainFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Moving this wrapper never moves F out of its separate pinned box.
        let this = self.get_mut();
        assert!(
            this.future.is_some(),
            "domain future polled after completion"
        );
        let Some(_guard) = this.domain.poll_enter(&mut this.waiter, cx) else {
            return Poll::Pending;
        };
        match this
            .future
            .as_mut()
            .expect("live domain future")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(output) => {
                drop(this.future.take());
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F: Future> Drop for DomainFuture<F> {
    fn drop(&mut self) {
        if self.future.is_none() {
            return;
        }
        let _guard = self.domain.enter_blocking(&mut self.waiter);
        drop(self.future.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{pending, poll_fn},
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        task::Wake,
        time::{Duration, Instant},
    };

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll_once<F: Future>(
        future: &mut DomainFuture<F>,
        count: &Arc<WakeCount>,
    ) -> Poll<F::Output> {
        let waker = Waker::from(count.clone());
        Pin::new(future).poll(&mut Context::from_waker(&waker))
    }

    fn hold_on_thread(domain: &ExecutionDomain) -> (mpsc::Sender<()>, thread::JoinHandle<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let domain = domain.clone();
        let owner = thread::spawn(move || {
            let mut future = domain.wrap(poll_fn(|_| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Poll::Ready(())
            }));
            assert!(poll_once(&mut future, &Arc::default()).is_ready());
        });
        entered_rx.recv().unwrap();
        (release_tx, owner)
    }

    fn wait_for_cleanup(domain: &ExecutionDomain) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if domain.inner.state.lock().unwrap().blocking_waiters != 0 {
                return;
            }
            assert!(Instant::now() < deadline, "cleanup never entered its fence");
            thread::yield_now();
        }
    }

    #[test]
    fn polls_and_actual_cancellation_wait_for_current_owner() {
        struct OnDrop(Arc<AtomicBool>);
        impl Future for OnDrop {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                panic!("contending actor must not be polled");
            }
        }
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let domain = ExecutionDomain::default();
        let (release, owner) = hold_on_thread(&domain);
        let dropped = Arc::new(AtomicBool::new(false));
        let mut canceled = domain.wrap(OnDrop(dropped.clone()));
        assert!(poll_once(&mut canceled, &Arc::default()).is_pending());
        let cleanup = thread::spawn(move || drop(canceled));
        wait_for_cleanup(&domain);
        assert!(!dropped.load(Ordering::SeqCst));
        let mut queued = domain.wrap(pending::<()>());
        assert!(poll_once(&mut queued, &Arc::default()).is_pending());
        release.send(()).unwrap();
        owner.join().unwrap();
        cleanup.join().unwrap();
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn canceled_head_releases_next_and_only_head_is_woken() {
        let domain = ExecutionDomain::default();
        let (release, owner) = hold_on_thread(&domain);
        let mut first = domain.wrap(pending::<()>());
        let mut second = domain.wrap(pending::<()>());
        let first_wake = Arc::<WakeCount>::default();
        let second_wake = Arc::<WakeCount>::default();
        assert!(poll_once(&mut first, &first_wake).is_pending());
        assert!(poll_once(&mut second, &second_wake).is_pending());
        release.send(()).unwrap();
        owner.join().unwrap();
        assert_eq!(first_wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(second_wake.0.load(Ordering::SeqCst), 0);
        // Cleanup bypasses a queued async head, including itself.
        drop(first);
        assert_eq!(second_wake.0.load(Ordering::SeqCst), 1);
        assert!(poll_once(&mut second, &second_wake).is_pending());
        assert!(second.waiter.is_none());
    }

    #[test]
    fn queued_member_retains_its_latest_parent_waker() {
        let domain = ExecutionDomain::default();
        let (release, owner) = hold_on_thread(&domain);
        let mut member = domain.wrap(pending::<()>());
        let old = Arc::<WakeCount>::default();
        let current = Arc::<WakeCount>::default();
        assert!(poll_once(&mut member, &old).is_pending());
        assert!(poll_once(&mut member, &current).is_pending());
        release.send(()).unwrap();
        owner.join().unwrap();
        assert_eq!(old.0.load(Ordering::SeqCst), 0);
        assert_eq!(current.0.load(Ordering::SeqCst), 1);
        assert!(poll_once(&mut member, &current).is_pending());
        assert!(member.waiter.is_none());
    }

    #[test]
    fn fifo_members_make_progress_despite_out_of_order_repolls() {
        let domain = ExecutionDomain::default();
        let (release, owner) = hold_on_thread(&domain);
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut members = (0..32)
            .map(|index| {
                let order = order.clone();
                domain.wrap(poll_fn(move |_| {
                    order.lock().unwrap().push(index);
                    Poll::Ready(())
                }))
            })
            .collect::<Vec<_>>();
        let wakes = (0..members.len())
            .map(|_| Arc::<WakeCount>::default())
            .collect::<Vec<_>>();
        for (member, wake) in members.iter_mut().zip(&wakes) {
            assert!(poll_once(member, wake).is_pending());
        }
        release.send(()).unwrap();
        owner.join().unwrap();
        for head in 0..members.len() {
            // Later members are deliberately offered execution first. None
            // may bypass the retained FIFO head or wake the entire queue.
            for later in (head + 1..members.len()).rev() {
                assert!(poll_once(&mut members[later], &wakes[later]).is_pending());
            }
            assert_eq!(wakes[head].0.load(Ordering::SeqCst), 1);
            assert!(poll_once(&mut members[head], &wakes[head]).is_ready());
        }
        assert_eq!(*order.lock().unwrap(), (0..32).collect::<Vec<_>>());
    }

    #[test]
    fn independent_domains_progress_while_another_poll_is_running() {
        let first = ExecutionDomain::default();
        let second = ExecutionDomain::default();
        assert!(first.same_domain(&first.clone()));
        assert!(!first.same_domain(&second));
        let (release, owner) = hold_on_thread(&first);
        let mut independent = second.wrap(std::future::ready(17));
        assert_eq!(
            poll_once(&mut independent, &Arc::default()),
            Poll::Ready(17)
        );
        release.send(()).unwrap();
        owner.join().unwrap();
    }

    #[test]
    fn same_domain_nested_cancellation_stays_exclusive() {
        struct NestedDrop {
            domain: ExecutionDomain,
            observed: Arc<AtomicUsize>,
        }
        impl Future for NestedDrop {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                Poll::Pending
            }
        }
        impl Drop for NestedDrop {
            fn drop(&mut self) {
                self.domain.with_exclusive(|| {
                    let state = self.domain.inner.state.lock().unwrap();
                    assert_eq!(state.owner, Some(thread::current().id()));
                    self.observed.store(state.depth, Ordering::SeqCst);
                });
            }
        }
        let domain = ExecutionDomain::default();
        let observed = Arc::new(AtomicUsize::new(0));
        let child = domain.wrap(NestedDrop {
            domain: domain.clone(),
            observed: observed.clone(),
        });
        domain.with_exclusive(|| drop(child));
        assert_eq!(observed.load(Ordering::SeqCst), 3);
        assert!(domain.inner.state.lock().unwrap().owner.is_none());
    }

    #[test]
    fn completion_drops_future_under_domain_and_panic_does_not_poison_it() {
        struct Completed {
            domain: ExecutionDomain,
            destroyed: Arc<AtomicBool>,
        }
        impl Future for Completed {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                Poll::Ready(())
            }
        }
        impl Drop for Completed {
            fn drop(&mut self) {
                assert_eq!(
                    self.domain.inner.state.lock().unwrap().owner,
                    Some(thread::current().id())
                );
                self.destroyed.store(true, Ordering::SeqCst);
            }
        }
        let domain = ExecutionDomain::default();
        let destroyed = Arc::new(AtomicBool::new(false));
        let mut future = domain.wrap(Completed {
            domain: domain.clone(),
            destroyed: destroyed.clone(),
        });
        assert!(poll_once(&mut future, &Arc::default()).is_ready());
        assert!(destroyed.load(Ordering::SeqCst));
        let mut panicker = domain.wrap(poll_fn(|_| -> Poll<()> { panic!("actor poll failure") }));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                poll_once(&mut panicker, &Arc::default())
            }))
            .is_err()
        );
        drop(panicker);
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                domain.with_exclusive(|| panic!("actor failure"));
            }))
            .is_err()
        );
        domain.with_exclusive(|| {});
    }

    #[test]
    fn movable_wrapper_accepts_borrowed_non_send_non_unpin_actor() {
        struct LocalActor<'a> {
            completed: &'a Rc<std::cell::Cell<bool>>,
            _pinned: std::marker::PhantomPinned,
        }
        impl Future for LocalActor<'_> {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                self.as_ref().get_ref().completed.set(true);
                Poll::Ready(())
            }
        }
        let domain = ExecutionDomain::default();
        let completed = Rc::new(std::cell::Cell::new(false));
        let wrapped = domain.wrap(LocalActor {
            completed: &completed,
            _pinned: std::marker::PhantomPinned,
        });
        let mut moved = wrapped;
        assert!(poll_once(&mut moved, &Arc::default()).is_ready());
        assert!(completed.get());
    }
}
