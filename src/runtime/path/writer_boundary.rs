//! Exact physical-writer availability, not queue capacity or path health.

use crate::model::path::CarrierPathInstanceId;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::Notify;

/// One ordered writer owns publication. Cloned command senders share only an
/// observation handle; they cannot manufacture a ready guard.
#[derive(Debug, Default)]
pub(in crate::runtime) struct ReliableWriterBoundary {
    // Even generations are occupied; odd generations name one ready epoch.
    generation: AtomicU64,
    instance: AtomicU64,
    changed: Arc<Notify>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct ReliableWriterReadyReceipt {
    boundary: Arc<ReliableWriterBoundary>,
    generation: u64,
    instance: CarrierPathInstanceId,
}

/// Owns one ready epoch without borrowing the command receiver. A selected
/// handler withdraws it before any occupying work or awaited input routing.
#[derive(Debug)]
pub(in crate::runtime) struct ReliableWriterReadyGuard {
    receipt: ReliableWriterReadyReceipt,
}

impl ReliableWriterBoundary {
    pub(in crate::runtime::path) fn publish(
        self: &Arc<Self>,
        instance: CarrierPathInstanceId,
    ) -> Option<ReliableWriterReadyGuard> {
        let current = self.generation.load(Ordering::Acquire);
        if current & 1 != 0 {
            return None;
        }
        // Keep a successor occupied generation available. Exhaustion cannot
        // wrap an old receipt into current authority.
        let ready = current
            .checked_add(1)
            .filter(|ready| ready.checked_add(1).is_some())?;
        self.instance.store(instance.as_u64(), Ordering::Release);
        self.generation
            .compare_exchange(current, ready, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        self.changed.notify_waiters();
        Some(ReliableWriterReadyGuard {
            receipt: ReliableWriterReadyReceipt {
                boundary: self.clone(),
                generation: ready,
                instance,
            },
        })
    }

    pub(in crate::runtime) fn snapshot(self: &Arc<Self>) -> Option<ReliableWriterReadyReceipt> {
        let generation = self.generation.load(Ordering::Acquire);
        if generation & 1 == 0 {
            return None;
        }
        let instance = CarrierPathInstanceId::from_raw(self.instance.load(Ordering::Acquire));
        (self.generation.load(Ordering::Acquire) == generation).then(|| {
            ReliableWriterReadyReceipt {
                boundary: self.clone(),
                generation,
                instance,
            }
        })
    }

    pub(in crate::runtime) fn change_notify(&self) -> Arc<Notify> {
        self.changed.clone()
    }

    pub(in crate::runtime::path) fn invalidate(&self) {
        if self
            .generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current & 1 != 0).then(|| current + 1)
            })
            .is_ok()
        {
            self.changed.notify_waiters();
        }
    }
}

impl ReliableWriterReadyReceipt {
    pub(in crate::runtime) fn instance(&self) -> CarrierPathInstanceId {
        self.instance
    }

    pub(in crate::runtime) fn is_current(&self) -> bool {
        self.boundary.generation.load(Ordering::Acquire) == self.generation
            && self.boundary.instance.load(Ordering::Acquire) == self.instance.as_u64()
    }
}

impl PartialEq for ReliableWriterReadyReceipt {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.boundary, &other.boundary)
            && self.generation == other.generation
            && self.instance == other.instance
    }
}

impl Eq for ReliableWriterReadyReceipt {}

impl ReliableWriterReadyGuard {
    pub(in crate::runtime) fn receipt(&self) -> ReliableWriterReadyReceipt {
        self.receipt.clone()
    }

    /// Only the actual writer consumes its epoch, immediately before the
    /// synchronous claim transaction. This grants no Product/Native authority.
    pub(in crate::runtime) fn try_consume(&self) -> bool {
        let consumed = self
            .receipt
            .boundary
            .generation
            .compare_exchange(
                self.receipt.generation,
                self.receipt.generation + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if consumed {
            self.receipt.boundary.changed.notify_waiters();
        }
        consumed
    }
}

impl Drop for ReliableWriterReadyGuard {
    fn drop(&mut self) {
        self.try_consume();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_receipts_fence_occupation_replacement_and_old_guard_drop() {
        let boundary = Arc::new(ReliableWriterBoundary::default());
        let instance = CarrierPathInstanceId::from_raw(1);
        assert!(boundary.snapshot().is_none());
        let first = boundary.publish(instance).unwrap();
        let receipt = first.receipt();
        assert_eq!(boundary.snapshot(), Some(receipt.clone()));
        assert!(boundary.publish(instance).is_none());
        assert!(first.try_consume());
        assert!(!receipt.is_current());
        assert!(!first.try_consume());
        let second = boundary.publish(instance).unwrap();
        assert_ne!(receipt, second.receipt());
        drop(first);
        assert!(second.receipt().is_current());
        boundary.invalidate();
        let replacement = boundary
            .publish(CarrierPathInstanceId::from_raw(2))
            .unwrap();
        drop(second);
        assert!(replacement.receipt().is_current());
        assert_eq!(
            replacement.receipt().instance(),
            CarrierPathInstanceId::from_raw(2)
        );
        drop(replacement);
        assert!(boundary.snapshot().is_none());
    }

    #[tokio::test]
    async fn ready_publication_wakes_a_prearmed_waiter() {
        let boundary = Arc::new(ReliableWriterBoundary::default());
        let mut changed = Box::pin(boundary.change_notify().notified_owned());
        changed.as_mut().enable();
        let _ready = boundary
            .publish(CarrierPathInstanceId::from_raw(1))
            .unwrap();
        assert!(futures::poll!(&mut changed).is_ready());
    }
}
