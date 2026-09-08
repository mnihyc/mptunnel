//! Synchronous ownership of response source, separate from received requests.

use super::prepared::ResponseProductState;
use crate::runtime::stream::response::ResponseStreamBinding;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

pub(in crate::runtime) type ResponseProductLockWait = Pin<Box<OwnedNotified>>;

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

    /// Actor-only synchronous access. The guard must end lexically before I/O.
    pub(in crate::runtime) fn lock(&self) -> ResponseProductGuard<'_> {
        ResponseProductGuard {
            state: Some(
                self.0
                    .state
                    .lock()
                    .expect("response Product owner poisoned"),
            ),
            unlocked: &self.0.unlocked,
        }
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
    pub(in crate::runtime) fn try_lock(
        self,
    ) -> Result<ResponseProductGuard<'a>, ResponseProductLockWait> {
        match self.owner.0.state.try_lock() {
            Ok(state) => Ok(ResponseProductGuard {
                state: Some(state),
                unlocked: &self.owner.0.unlocked,
            }),
            Err(TryLockError::WouldBlock) => Err(self.wait),
            Err(TryLockError::Poisoned(_)) => panic!("response Product owner poisoned"),
        }
    }
}

pub(in crate::runtime) struct ResponseProductGuard<'a> {
    state: Option<MutexGuard<'a, ResponseProductState>>,
    unlocked: &'a Notify,
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
        drop(self.state.take());
        self.unlocked.notify_waiters();
    }
}
