//! Carrier-owned send availability shared by every product stream on one path.
//!
//! MPP reserves only a service quantum; TCP or QUIC remains authoritative for
//! congestion state. Product Data ACKs never replenish this credit.

use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct CarrierSendCreditSnapshot {
    pub(in crate::runtime) limit_bytes: u64,
    pub(in crate::runtime) committed_bytes: u64,
    pub(in crate::runtime) closed: bool,
}

pub(in crate::runtime) trait CarrierSendCreditSource:
    Send + Sync + fmt::Debug
{
    fn snapshot(&self) -> CarrierSendCreditSnapshot;
    fn notify(&self) -> Arc<Notify>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum CarrierSendCreditError {
    Blocked,
    Closed,
}

#[derive(Clone)]
pub(in crate::runtime) struct CarrierSendCredit {
    state: Arc<CarrierSendCreditState>,
}

struct CarrierSendCreditState {
    source: Arc<dyn CarrierSendCreditSource>,
    reserved_bytes: AtomicU64,
}

impl fmt::Debug for CarrierSendCreditState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CarrierSendCreditState")
            .field("source", &self.source)
            .field(
                "reserved_bytes",
                &self.reserved_bytes.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl fmt::Debug for CarrierSendCredit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CarrierSendCredit")
            .field(&self.state)
            .finish()
    }
}

impl CarrierSendCredit {
    pub(in crate::runtime) fn new(source: Arc<dyn CarrierSendCreditSource>) -> Self {
        Self {
            state: Arc::new(CarrierSendCreditState {
                source,
                reserved_bytes: AtomicU64::new(0),
            }),
        }
    }

    pub(in crate::runtime) fn can_reserve_quantum(&self) -> bool {
        let snapshot = self.state.source.snapshot();
        !snapshot.closed
            && snapshot.limit_bytes > 0
            && snapshot
                .committed_bytes
                .saturating_add(self.state.reserved_bytes.load(Ordering::Acquire))
                < snapshot.limit_bytes
    }

    /// Reserves one complete product quantum when the native window is open.
    /// A quantum may exceed the currently free bytes, just as a packet scheduler
    /// may hand a segment to a transport with a partially open congestion window.
    pub(in crate::runtime) fn try_reserve(
        &self,
        bytes: usize,
    ) -> Result<(), CarrierSendCreditError> {
        if bytes == 0 {
            return Ok(());
        }
        let bytes = bytes as u64;
        loop {
            let snapshot = self.state.source.snapshot();
            if snapshot.closed {
                return Err(CarrierSendCreditError::Closed);
            }
            if snapshot.limit_bytes == 0 {
                return Err(CarrierSendCreditError::Blocked);
            }
            let reserved = self.state.reserved_bytes.load(Ordering::Acquire);
            if snapshot.committed_bytes.saturating_add(reserved) >= snapshot.limit_bytes {
                return Err(CarrierSendCreditError::Blocked);
            }
            let next = reserved.saturating_add(bytes);
            if self
                .state
                .reserved_bytes
                .compare_exchange_weak(reserved, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    pub(in crate::runtime) fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self.state.reserved_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |reserved| Some(reserved.saturating_sub(bytes)),
        );
        self.state.source.notify().notify_waiters();
    }

    pub(in crate::runtime) fn notify(&self) -> Arc<Notify> {
        self.state.source.notify()
    }

    #[cfg(test)]
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.state.reserved_bytes.load(Ordering::Acquire)
    }
}

#[cfg(test)]
#[path = "send_credit_test.rs"]
mod tests;
