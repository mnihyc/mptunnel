//! Synchronous ownership of response source, separate from received requests.

use super::prepared::ResponseProductState;
use crate::runtime::stream::response::ResponseStreamBinding;
use std::ops::{Deref, DerefMut};
use std::panic::Location;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

pub(in crate::runtime) type ResponseProductLockWait = Pin<Box<OwnedNotified>>;
static NEXT_OBSERVED_GUARD: AtomicU64 = AtomicU64::new(1);

struct Inner {
    state: Mutex<ResponseProductState>,
    binding: Arc<ResponseStreamBinding>,
    unlocked: Arc<Notify>,
}

#[derive(Clone)]
pub(in crate::runtime) struct SharedResponseProduct(Arc<Inner>);

#[derive(Clone)]
pub(in crate::runtime) struct WeakSharedResponseProduct(Weak<Inner>);

impl WeakSharedResponseProduct {
    pub(in crate::runtime) fn upgrade(&self) -> Option<SharedResponseProduct> {
        self.0.upgrade().map(SharedResponseProduct)
    }
}

impl SharedResponseProduct {
    pub(in crate::runtime) fn new(
        state: ResponseProductState,
        binding: Arc<ResponseStreamBinding>,
    ) -> Self {
        Self(Arc::new(Inner {
            state: Mutex::new(state),
            binding,
            unlocked: Arc::new(Notify::new()),
        }))
    }

    pub(in crate::runtime) fn binding(&self) -> &Arc<ResponseStreamBinding> {
        &self.0.binding
    }

    pub(in crate::runtime) fn downgrade(&self) -> WeakSharedResponseProduct {
        WeakSharedResponseProduct(Arc::downgrade(&self.0))
    }

    pub(in crate::runtime) fn actor_lifetime(&self) -> ResponseProductActorLifetime {
        ResponseProductActorLifetime(self.clone())
    }

    pub(in crate::runtime) fn observation_identity(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    /// Actor-only synchronous access. The guard must end lexically before I/O.
    #[track_caller]
    pub(in crate::runtime) fn lock(&self) -> ResponseProductGuard<'_> {
        let before = Instant::now();
        let state = self
            .0
            .state
            .lock()
            .expect("response Product owner poisoned");
        let after = Instant::now();
        ResponseProductGuard::observed(state, self, "actor", before, after)
    }

    /// Every native writer access, including advisory reads, is nonblocking.
    pub(in crate::runtime) fn arm_claim(&self) -> ResponseProductClaimAttempt<'_> {
        let mut wait = Box::pin(self.0.unlocked.clone().notified_owned());
        wait.as_mut().enable();
        ResponseProductClaimAttempt { owner: self, wait }
    }
}

pub(in crate::runtime) struct ResponseProductActorLifetime(SharedResponseProduct);

impl Drop for ResponseProductActorLifetime {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.prepared.claims_active = false;
        state.prepared.registrations.clear();
        state.prepared.work_changed.notify_waiters();
    }
}

pub(in crate::runtime) struct ResponseProductClaimAttempt<'a> {
    owner: &'a SharedResponseProduct,
    wait: ResponseProductLockWait,
}

impl<'a> ResponseProductClaimAttempt<'a> {
    #[track_caller]
    pub(in crate::runtime) fn try_lock(
        self,
    ) -> Result<ResponseProductGuard<'a>, ResponseProductLockWait> {
        let before = Instant::now();
        match self.owner.0.state.try_lock() {
            Ok(state) => Ok(ResponseProductGuard::observed(
                state,
                self.owner,
                "claim",
                before,
                Instant::now(),
            )),
            Err(TryLockError::WouldBlock) => Err(self.wait),
            Err(TryLockError::Poisoned(_)) => panic!("response Product owner poisoned"),
        }
    }
}

pub(in crate::runtime) struct ResponseProductGuard<'a> {
    state: Option<MutexGuard<'a, ResponseProductState>>,
    unlocked: &'a Notify,
    owner_id: usize,
    guard_id: u64,
    acquired_before: Option<Duration>,
    acquired_after: Option<Duration>,
    caller: &'static Location<'static>,
    kind: &'static str,
}

impl<'a> ResponseProductGuard<'a> {
    #[track_caller]
    fn observed(
        state: MutexGuard<'a, ResponseProductState>,
        owner: &'a SharedResponseProduct,
        kind: &'static str,
        before: Instant,
        after: Instant,
    ) -> Self {
        let acquired_before = quinn::native_source_window_at(before);
        let acquired_after = quinn::native_source_window_at(after);
        let traced = acquired_before.is_some() || acquired_after.is_some();
        let guard_id = if traced {
            NEXT_OBSERVED_GUARD.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        };
        let caller = Location::caller();
        let owner_id = owner.observation_identity();
        if traced {
            quinn::emit_native_trace(format_args!(
                "product_owner_hold role=server window=first_native_poll_10_11 edge=acquired owner_id={} guard_id={} kind={} caller={}:{}:{} acquired_before_ns={:?} acquired_after_ns={:?} censored={} stream_id={} queued_data_bytes={} next_offset={}",
                owner_id,
                guard_id,
                kind,
                caller.file(),
                caller.line(),
                caller.column(),
                acquired_before.map(|at| at.as_nanos()),
                acquired_after.map(|at| at.as_nanos()),
                acquired_before.is_none() || acquired_after.is_none(),
                state.sender.stream_id().0,
                state.sender.data_bytes(),
                state.send_stream.next_offset(),
            ));
        }
        Self {
            state: Some(state),
            unlocked: &owner.0.unlocked,
            owner_id,
            guard_id,
            acquired_before,
            acquired_after,
            caller,
            kind,
        }
    }
}

impl Deref for ResponseProductGuard<'_> {
    type Target = ResponseProductState;
    fn deref(&self) -> &Self::Target {
        self.state.as_deref().expect("live response guard")
    }
}

impl DerefMut for ResponseProductGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state.as_deref_mut().expect("live response guard")
    }
}

impl Drop for ResponseProductGuard<'_> {
    fn drop(&mut self) {
        let trace = self.guard_id != 0 || quinn::native_source_window_at(Instant::now()).is_some();
        let exit = trace.then(|| {
            let state = self.state.as_ref().expect("live response guard");
            (
                state.sender.stream_id().0,
                state.sender.data_bytes(),
                state.send_stream.next_offset(),
            )
        });
        let before = trace.then(Instant::now);
        drop(self.state.take());
        let after = trace.then(Instant::now);
        self.unlocked.notify_waiters();
        if let Some((stream_id, queued_data_bytes, next_offset)) = exit {
            let release_before = before.and_then(quinn::native_source_window_at);
            let release_after = after.and_then(quinn::native_source_window_at);
            let guard_id = if self.guard_id == 0 {
                NEXT_OBSERVED_GUARD.fetch_add(1, Ordering::Relaxed)
            } else {
                self.guard_id
            };
            // A boundary-crossing guard has a missing endpoint, never a fabricated timestamp.
            let censored = self.acquired_before.is_none()
                || self.acquired_after.is_none()
                || release_before.is_none()
                || release_after.is_none();
            quinn::emit_native_trace(format_args!(
                "product_owner_hold role=server window=first_native_poll_10_11 edge=released owner_id={} guard_id={} kind={} caller={}:{}:{} acquired_before_ns={:?} acquired_after_ns={:?} release_before_ns={:?} release_after_ns={:?} censored={} stream_id={} queued_data_bytes={} next_offset={}",
                self.owner_id,
                guard_id,
                self.kind,
                self.caller.file(),
                self.caller.line(),
                self.caller.column(),
                self.acquired_before.map(|at| at.as_nanos()),
                self.acquired_after.map(|at| at.as_nanos()),
                release_before.map(|at| at.as_nanos()),
                release_after.map(|at| at.as_nanos()),
                censored,
                stream_id,
                queued_data_bytes,
                next_offset,
            ));
        }
    }
}
