//! Session-wide reliable send-buffer ownership.
//!
//! Product bytes are charged once when read from a source, remain charged while
//! queued or retained for Data Sequence reinjection, and are released by Data
//! ACK. Carrier TCP/QUIC congestion state never creates product-layer credit.

use crate::mux::MuxLimits;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::watch;

#[derive(Clone)]
pub(in crate::runtime) struct SessionSendBuffer {
    inner: Arc<SessionSendBufferInner>,
}

struct SessionSendBufferInner {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
    updates: watch::Sender<u64>,
}

impl std::fmt::Debug for SessionSendBuffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionSendBuffer")
            .field("limit_bytes", &self.limit_bytes())
            .field("used_bytes", &self.used_bytes())
            .finish()
    }
}

impl SessionSendBuffer {
    pub(in crate::runtime) fn from_limits(limits: MuxLimits) -> Self {
        // This is a fixed memory/resource boundary. Per-path live measurements
        // decide carrier emission, not how much unique session data may exist.
        let stream_window = usize::try_from(limits.max_stream_window_bytes).unwrap_or(usize::MAX);
        Self::new(limits.max_repair_bytes.min(stream_window).max(1))
    }

    pub(in crate::runtime) fn new(limit_bytes: usize) -> Self {
        let (updates, _) = watch::channel(0);
        Self {
            inner: Arc::new(SessionSendBufferInner {
                limit_bytes: limit_bytes.max(1),
                used_bytes: AtomicUsize::new(0),
                updates,
            }),
        }
    }

    pub(in crate::runtime) fn stream_reservation(&self) -> StreamSendBufferReservation {
        StreamSendBufferReservation {
            buffer: self.clone(),
            held_bytes: 0,
        }
    }

    pub(in crate::runtime) fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.updates.subscribe()
    }

    pub(in crate::runtime) fn limit_bytes(&self) -> usize {
        self.inner.limit_bytes
    }

    pub(in crate::runtime) fn used_bytes(&self) -> usize {
        self.inner.used_bytes.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn available_bytes(&self) -> usize {
        self.limit_bytes().saturating_sub(self.used_bytes())
    }

    pub(in crate::runtime) async fn reserve(
        &self,
        updates: &mut watch::Receiver<u64>,
        max_bytes: usize,
    ) -> SessionSendBufferPermit {
        assert!(
            max_bytes > 0,
            "session send-buffer reservation must be positive"
        );
        loop {
            if let Some(permit) = self.try_reserve(max_bytes) {
                return permit;
            }
            updates
                .changed()
                .await
                .expect("session send-buffer update source");
        }
    }

    fn try_reserve(&self, max_bytes: usize) -> Option<SessionSendBufferPermit> {
        if max_bytes == 0 {
            return None;
        }
        let limit = self.limit_bytes();
        let previous = self
            .inner
            .used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                let reserved = limit.saturating_sub(used).min(max_bytes);
                (reserved > 0).then_some(used + reserved)
            })
            .ok()?;
        Some(SessionSendBufferPermit {
            buffer: self.clone(),
            reserved_bytes: limit.saturating_sub(previous).min(max_bytes),
        })
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.inner
            .used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(bytes)
            })
            .expect("session send-buffer accounting underflow");
        self.inner
            .updates
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

pub(in crate::runtime) struct SessionSendBufferPermit {
    buffer: SessionSendBuffer,
    reserved_bytes: usize,
}

impl SessionSendBufferPermit {
    pub(in crate::runtime) fn bytes(&self) -> usize {
        self.reserved_bytes
    }

    pub(in crate::runtime) fn retain(
        mut self,
        stream: &mut StreamSendBufferReservation,
        bytes: usize,
    ) {
        assert!(
            Arc::ptr_eq(&self.buffer.inner, &stream.buffer.inner),
            "session send-buffer reservation owner mismatch"
        );
        assert!(
            bytes <= self.reserved_bytes,
            "source read exceeded its session send-buffer reservation"
        );
        stream.held_bytes = stream
            .held_bytes
            .checked_add(bytes)
            .expect("stream send-buffer accounting overflow");
        let unused = self.reserved_bytes - bytes;
        self.reserved_bytes = 0;
        self.buffer.release(unused);
    }
}

impl Drop for SessionSendBufferPermit {
    fn drop(&mut self) {
        self.buffer.release(self.reserved_bytes);
    }
}

/// Unique source bytes owned by one reliable product stream.
///
/// Queue-to-flight transfer and reinjection do not change this count. Data ACK
/// releases it, and task cancellation releases any remainder through `Drop`.
pub(in crate::runtime) struct StreamSendBufferReservation {
    buffer: SessionSendBuffer,
    held_bytes: usize,
}

impl StreamSendBufferReservation {
    pub(in crate::runtime) fn release(&mut self, bytes: usize) {
        assert!(
            bytes <= self.held_bytes,
            "Data ACK released unowned session send-buffer bytes"
        );
        self.held_bytes -= bytes;
        self.buffer.release(bytes);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn held_bytes(&self) -> usize {
        self.held_bytes
    }
}

impl Drop for StreamSendBufferReservation {
    fn drop(&mut self) {
        self.buffer.release(self.held_bytes);
        self.held_bytes = 0;
    }
}

#[cfg(test)]
#[path = "tests_send_buffer.rs"]
mod tests;
