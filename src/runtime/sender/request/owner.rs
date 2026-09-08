//! Shared ownership of one request direction's existing Product state.

use super::RequestProductState;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

pub(in crate::runtime) type RequestProductLockWait = Pin<Box<OwnedNotified>>;

struct SharedRequestProductInner {
    state: Mutex<RequestProductState>,
    unlocked: Arc<Notify>,
    #[cfg(test)]
    before_prepared_native_resolve: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

#[derive(Clone)]
pub(in crate::runtime) struct SharedRequestProduct {
    inner: Arc<SharedRequestProductInner>,
}

#[derive(Clone)]
pub(in crate::runtime) struct WeakSharedRequestProduct {
    inner: Weak<SharedRequestProductInner>,
}

impl WeakSharedRequestProduct {
    pub(in crate::runtime) fn upgrade(&self) -> Option<SharedRequestProduct> {
        self.inner
            .upgrade()
            .map(|inner| SharedRequestProduct { inner })
    }
}

impl SharedRequestProduct {
    /// Declare this before the actor's first Product guard, so unwinding drops
    /// any synchronous guard before revoking the actor's claim authority.
    pub(in crate::runtime) fn actor_lifetime(&self) -> RequestProductActorLifetime {
        RequestProductActorLifetime {
            owner: self.clone(),
        }
    }

    pub(in crate::runtime) fn downgrade(&self) -> WeakSharedRequestProduct {
        WeakSharedRequestProduct {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub(in crate::runtime) fn new(state: RequestProductState) -> Self {
        Self {
            inner: Arc::new(SharedRequestProductInner {
                state: Mutex::new(state),
                unlocked: Arc::new(Notify::new()),
                #[cfg(test)]
                before_prepared_native_resolve: Mutex::new(None),
            }),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn before_prepared_native_resolve_once_for_test(
        &self,
        hook: impl FnOnce() + Send + 'static,
    ) {
        let previous = self
            .inner
            .before_prepared_native_resolve
            .lock()
            .expect("prepared claim test hook lock")
            .replace(Box::new(hook));
        assert!(previous.is_none(), "one unconsumed claim hook per owner");
    }

    #[cfg(test)]
    pub(super) fn run_before_prepared_native_resolve_for_test(&self) {
        let hook = self
            .inner
            .before_prepared_native_resolve
            .lock()
            .expect("prepared claim test hook lock")
            .take();
        // Neither Product nor hook synchronization is retained while the
        // one-shot callback runs a competing actual claim on this owner.
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Actor-side synchronous ownership. No guard may cross an actual await.
    /// Native writer claims use arm_claim/try_lock, including advisory reads.
    pub(in crate::runtime) fn lock(&self) -> RequestProductGuard<'_> {
        RequestProductGuard {
            state: Some(
                self.inner
                    .state
                    .lock()
                    .expect("request Product owner poisoned"),
            ),
            unlocked: &self.inner.unlocked,
        }
    }

    /// Register before each writer attempt (before entering a Native fence,
    /// if any). A failed try-lock then retains the
    /// exact notification, including an unlock before its first future poll.
    pub(in crate::runtime) fn arm_claim(&self) -> RequestProductClaimAttempt<'_> {
        let mut unlocked = Box::pin(self.inner.unlocked.clone().notified_owned());
        unlocked.as_mut().enable();
        RequestProductClaimAttempt {
            owner: self,
            unlocked,
        }
    }
}

/// Temporary writer upgrades may outlive an aborted actor future. Only this
/// actor-owned lifetime authorizes claims; Arc reachability alone does not.
pub(in crate::runtime) struct RequestProductActorLifetime {
    owner: SharedRequestProduct,
}

impl Drop for RequestProductActorLifetime {
    fn drop(&mut self) {
        let mut state = self.owner.lock();
        state.prepared.claims_active = false;
        state.prepared.registrations.clear();
        state.prepared.work_changed.notify_waiters();
    }
}

/// One nonblocking writer -> Product attempt, not a payload reservation or a
/// policy generation. On Busy, leave any Native fence and defer the notice;
/// do not wait inline on the native writer's service path.
pub(in crate::runtime) struct RequestProductClaimAttempt<'a> {
    owner: &'a SharedRequestProduct,
    unlocked: RequestProductLockWait,
}

impl<'a> RequestProductClaimAttempt<'a> {
    pub(in crate::runtime) fn try_lock(
        self,
    ) -> Result<RequestProductGuard<'a>, RequestProductLockWait> {
        match self.owner.inner.state.try_lock() {
            Ok(state) => Ok(RequestProductGuard {
                state: Some(state),
                unlocked: &self.owner.inner.unlocked,
            }),
            Err(TryLockError::WouldBlock) => Err(self.unlocked),
            Err(TryLockError::Poisoned(_)) => panic!("request Product owner poisoned"),
        }
    }
}

/// MutexGuard makes this guard !Send. Even advisory reads must notify after
/// release: an otherwise unchanged owner can have a writer waiting on its lock.
pub(in crate::runtime) struct RequestProductGuard<'a> {
    state: Option<MutexGuard<'a, RequestProductState>>,
    unlocked: &'a Notify,
}

impl Deref for RequestProductGuard<'_> {
    type Target = RequestProductState;

    fn deref(&self) -> &Self::Target {
        self.state.as_deref().expect("live request Product guard")
    }
}

impl DerefMut for RequestProductGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state
            .as_deref_mut()
            .expect("live request Product guard")
    }
}

impl Drop for RequestProductGuard<'_> {
    fn drop(&mut self) {
        drop(self.state.take());
        self.unlocked.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
    use crate::mux::MuxLimits;
    use crate::mux::stream::ReliableSendStream;
    use crate::protocol::{PathMetricDirection, StreamId, UnderlayProtocol};
    use crate::runtime::path::authority::NativeCarrierRateAuthorityHandle;
    use crate::runtime::path::commands::reliable_path_command_channels;
    use crate::runtime::relay::io::AuthoritativeStreamAckSnapshot;
    use crate::runtime::sender::queue::ReliableRelaySenderQueue;
    use crate::runtime::sender::request::test_support::opened_test_relay_stream_with_native_source;
    use crate::runtime::sender::request::{RequestPreparedSource, RequestSenderService};
    use crate::runtime::stream::ReliableRelayRemoteSet;
    use crate::scheduler::TrafficClass;
    use crate::transport::RateHint;
    use futures::FutureExt;
    use std::sync::Barrier;

    fn shared_product() -> (
        SharedRequestProduct,
        Arc<NativeCarrierRateAuthorityHandle>,
        CarrierRateAuthorityScope,
    ) {
        let stream_id = StreamId(720);
        let (commands, _receivers) = reliable_path_command_channels(8);
        let (opened, native) = opened_test_relay_stream_with_native_source(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            commands,
            RateHint::BitsPerSecond(25_000_000),
            7,
            Some(100_000_000),
        );
        let scope = CarrierRateAuthorityScope::new(
            opened.path_instance_id(),
            PathMetricDirection::ClientToServer,
        );
        let (remotes, _input) = ReliableRelayRemoteSet::new(opened, 8);
        (
            SharedRequestProduct::new(RequestProductState {
                sender_queue: ReliableRelaySenderQueue::default(),
                sender: RequestSenderService::new(stream_id),
                send_stream: ReliableSendStream::new(stream_id, MuxLimits::default()),
                last_send_ack: AuthoritativeStreamAckSnapshot::default(),
                remotes,
                prepared: RequestPreparedSource::new(TrafficClass::Throughput, 65_536),
            }),
            native.expect("actual QUIC attachment authority"),
            scope,
        )
    }

    #[tokio::test]
    async fn native_fenced_busy_claim_releases_native_before_waiting_for_product() {
        let (owner, native, scope) = shared_product();
        let stamp = native.scheduling_shape_snapshot(scope).unwrap().stamp();
        let actor_guard = owner.lock();
        let entered_native = Arc::new(Barrier::new(2));
        let try_product = Arc::new(Barrier::new(2));
        let writer_owner = owner.clone();
        let writer_native = native.clone();
        let writer_entered = entered_native.clone();
        let writer_try = try_product.clone();
        let writer = std::thread::spawn(move || {
            let attempt = writer_owner.arm_claim();
            writer_native
                .commit_with_current_scheduling_shape(stamp, |_| {
                    writer_entered.wait();
                    writer_try.wait();
                    match attempt.try_lock() {
                        Ok(_) => panic!("actor still owns Product"),
                        Err(wait) => wait,
                    }
                })
                .expect("current Native fence")
        });
        entered_native.wait();
        // Native is held on the writer while Product is held here. Only the
        // nonblocking reverse acquisition lets this actor-side Native read
        // finish without releasing Product first.
        try_product.wait();
        assert_eq!(
            native.scheduling_shape_snapshot(scope).unwrap().stamp(),
            stamp
        );
        let mut wait = writer.join().expect("writer exits Native on Busy");
        assert!(wait.as_mut().now_or_never().is_none());
        assert_eq!(actor_guard.send_stream.next_offset(), 0);
        drop(actor_guard);
        assert_eq!(wait.now_or_never(), Some(()));
        assert!(owner.arm_claim().try_lock().is_ok());
    }

    #[tokio::test]
    async fn advisory_owner_unlock_wakes_every_prearmed_claim_before_first_poll() {
        let (owner, _native, _scope) = shared_product();
        let read_guard = owner.lock();
        assert_eq!(read_guard.send_stream.next_offset(), 0);
        let first = match owner.arm_claim().try_lock() {
            Ok(_) => panic!("read guard retains ownership"),
            Err(wait) => wait,
        };
        let second = match owner.arm_claim().try_lock() {
            Ok(_) => panic!("read guard retains ownership"),
            Err(wait) => wait,
        };
        drop(read_guard);
        assert_eq!(first.now_or_never(), Some(()));
        assert_eq!(second.now_or_never(), Some(()));
        // A previous unlock is not a reusable credit for a fresh Busy wait.
        let next_guard = owner.lock();
        let mut next = match owner.arm_claim().try_lock() {
            Ok(_) => panic!("new guard retains ownership"),
            Err(wait) => wait,
        };
        assert!(next.as_mut().now_or_never().is_none());
        drop(next_guard);
        assert_eq!(next.now_or_never(), Some(()));
    }

    #[tokio::test]
    async fn poisoned_product_is_not_reported_as_a_busy_claim() {
        let (owner, _native, _scope) = shared_product();
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = owner.lock();
            panic!("poison the exact Product owner");
        }));
        assert!(poisoned.is_err());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = owner.arm_claim().try_lock();
            }))
            .is_err()
        );
    }

    #[tokio::test]
    async fn lexical_actor_field_borrows_end_before_owned_io() {
        fn require_send<F: std::future::Future<Output = ()> + Send>(future: F) -> F {
            future
        }
        fn fields(
            state: &mut RequestProductState,
        ) -> (
            &mut ReliableRelaySenderQueue,
            &mut RequestSenderService,
            &mut ReliableSendStream,
            &mut AuthoritativeStreamAckSnapshot,
            &mut ReliableRelayRemoteSet,
            &mut RequestPreparedSource,
        ) {
            (
                &mut state.sender_queue,
                &mut state.sender,
                &mut state.send_stream,
                &mut state.last_send_ack,
                &mut state.remotes,
                &mut state.prepared,
            )
        }
        let (owner, _, _) = shared_product();
        require_send(async move {
            {
                let mut guard = owner.lock();
                let (queue, sender, mux, ack, remotes, prepared) = fields(&mut guard);
                let _ = (queue, sender, mux, ack, remotes, prepared);
            }
            tokio::task::yield_now().await;
            {
                let mut guard = owner.lock();
                let (queue, sender, mux, ack, remotes, prepared) = fields(&mut guard);
                let _ = (queue, sender, mux, ack, remotes, prepared);
            }
        })
        .await;
    }

    #[tokio::test]
    async fn actor_lifetime_revokes_claims_despite_a_writer_owner_upgrade() {
        let (owner, _, _) = shared_product();
        let actor = owner.actor_lifetime();
        let writer = owner.downgrade().upgrade().expect("writer upgrade");
        drop(owner);
        assert!(writer.lock().prepared.claims_active);
        drop(actor);
        assert!(!writer.lock().prepared.claims_active);
    }
}
