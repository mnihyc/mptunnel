//! Exact physical-writer availability, not queue capacity or path health.

use crate::model::path::CarrierPathInstanceId;
use std::sync::{
    Arc, Mutex,
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
    // Serializes foreground publication with an opportunistic claim's final
    // consume. This counts ownership, not byte capacity or native idleness.
    foreground: Mutex<usize>,
    // Foreground arbitration becoming clear is not a physical Ready change.
    // Only repair work subscribes; Original refusals must not wake siblings
    // merely by dropping their payload-free queue/notification ownership.
    foreground_released: Arc<Notify>,
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
    background: bool,
    loan: bool,
}

#[derive(Debug)]
pub(in crate::runtime::path) struct ReliableWriterForegroundGuard {
    boundary: Arc<ReliableWriterBoundary>,
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
            background: false,
            loan: false,
        })
    }

    pub(in crate::runtime::path) fn register_foreground(
        self: &Arc<Self>,
    ) -> ReliableWriterForegroundGuard {
        let mut pending = self.foreground.lock().expect("writer foreground lock");
        *pending = pending.checked_add(1).expect("writer foreground overflow");
        ReliableWriterForegroundGuard {
            boundary: self.clone(),
        }
    }

    pub(in crate::runtime::path) fn has_foreground(&self) -> bool {
        *self.foreground.lock().expect("writer foreground lock") != 0
    }

    pub(in crate::runtime::path) fn foreground_release_notify(&self) -> Arc<Notify> {
        self.foreground_released.clone()
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

    /// The ordinary writer retains revocation authority while the separate
    /// QUIC repair writer holds this payload-free handoff. Either occupation
    /// ends the epoch; foreground publication defeats background consume.
    pub(in crate::runtime::path) fn background_handoff(&self) -> Self {
        Self {
            receipt: self.receipt.clone(),
            background: true,
            loan: true,
        }
    }

    pub(in crate::runtime::path) fn set_background(&mut self, background: bool) {
        self.background = background;
    }

    /// Only the actual writer consumes its epoch, immediately before the
    /// synchronous claim transaction. This grants no Product/Native authority.
    pub(in crate::runtime) fn try_consume(&self) -> bool {
        let foreground = self.background.then(|| {
            self.receipt
                .boundary
                .foreground
                .lock()
                .expect("writer foreground lock")
        });
        if foreground.as_ref().is_some_and(|pending| **pending != 0) {
            return false;
        }
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
        if self.loan {
            // Metadata refusal did not occupy either writer. The ordinary
            // owner still holds, and may revoke, this idle epoch.
            return;
        }
        // Revocation is unconditional even when foreground prevents a claim.
        self.background = false;
        self.try_consume();
    }
}

impl Drop for ReliableWriterForegroundGuard {
    fn drop(&mut self) {
        let mut pending = self
            .boundary
            .foreground
            .lock()
            .expect("writer foreground lock");
        *pending = pending.checked_sub(1).expect("writer foreground underflow");
        let released = *pending == 0;
        drop(pending);
        if released {
            self.boundary.foreground_released.notify_waiters();
        }
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

    #[test]
    fn background_handoff_yields_to_foreground_and_keeps_original_authority() {
        let boundary = Arc::new(ReliableWriterBoundary::default());
        let ordinary = boundary
            .publish(CarrierPathInstanceId::from_raw(1))
            .unwrap();
        let offered = ordinary.background_handoff();
        let foreground = boundary.register_foreground();
        assert!(
            !offered.try_consume(),
            "publication wins before background admission"
        );
        assert!(
            ordinary.receipt().is_current(),
            "refusal does not occupy the writer"
        );
        assert!(
            ordinary.try_consume(),
            "foreground keeps its ordinary authority"
        );
        drop(foreground);
        assert!(
            !offered.try_consume(),
            "the old offer cannot follow foreground occupation"
        );
    }

    #[test]
    fn dropping_blocked_background_handoff_preserves_ordinary_idle_epoch() {
        let boundary = Arc::new(ReliableWriterBoundary::default());
        let ordinary = boundary
            .publish(CarrierPathInstanceId::from_raw(1))
            .unwrap();
        let offered = ordinary.background_handoff();
        let foreground = boundary.register_foreground();
        drop(offered);
        assert!(ordinary.receipt().is_current());
        drop(foreground);
        assert!(ordinary.try_consume());
        let successor = boundary
            .publish(CarrierPathInstanceId::from_raw(1))
            .unwrap();
        drop(ordinary);
        assert!(successor.receipt().is_current());
        assert!(successor.background_handoff().try_consume());
    }

    #[tokio::test]
    async fn foreground_release_and_ready_lifetime_are_distinct_events() {
        let boundary = Arc::new(ReliableWriterBoundary::default());
        let ordinary = boundary
            .publish(CarrierPathInstanceId::from_raw(1))
            .unwrap();
        let mut ready_changed = Box::pin(boundary.change_notify().notified_owned());
        ready_changed.as_mut().enable();
        let mut foreground_released =
            Box::pin(boundary.foreground_release_notify().notified_owned());
        foreground_released.as_mut().enable();
        let first = boundary.register_foreground();
        let second = boundary.register_foreground();
        drop(first);
        assert!(futures::poll!(&mut foreground_released).is_pending());
        drop(second);
        assert!(futures::poll!(&mut foreground_released).is_ready());
        assert!(
            futures::poll!(&mut ready_changed).is_pending(),
            "weak foreground release cannot invent a sibling Original writer transition"
        );
        assert!(ordinary.receipt().is_current());

        let mut foreground_released =
            Box::pin(boundary.foreground_release_notify().notified_owned());
        foreground_released.as_mut().enable();
        drop(ordinary.background_handoff());
        assert!(
            ordinary.receipt().is_current(),
            "QUIC loan refusal preserves Ready"
        );
        assert!(futures::poll!(&mut foreground_released).is_pending());
        drop(ordinary);
        assert!(futures::poll!(&mut ready_changed).is_ready());
        assert!(
            futures::poll!(&mut foreground_released).is_pending(),
            "TCP Ready revocation cannot regenerate a refused repair notice"
        );
    }
}
