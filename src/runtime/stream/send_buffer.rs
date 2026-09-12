//! Session-wide reliable send-buffer ownership.
//!
//! Product bytes are charged once when read from a source, remain charged while
//! queued or retained for Data Sequence reinjection, and are released by Data
//! ACK. Carrier TCP/QUIC congestion state never creates product-layer credit.

use crate::mux::MuxLimits;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Clone)]
pub(in crate::runtime) struct SessionSendBuffer {
    inner: Arc<SessionSendBufferInner>,
}

struct SessionSendBufferInner {
    limit_bytes: usize,
    state: Mutex<SessionSendBufferState>,
}

struct SessionSendBufferState {
    used_bytes: usize,
    next_ticket: u64,
    pending: BTreeMap<u64, PendingSourceReservation>,
}

struct PendingSourceReservation {
    granted_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
    waker: Waker,
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
        Self {
            inner: Arc::new(SessionSendBufferInner {
                limit_bytes: limit_bytes.max(1),
                state: Mutex::new(SessionSendBufferState {
                    used_bytes: 0,
                    next_ticket: 0,
                    pending: BTreeMap::new(),
                }),
            }),
        }
    }

    pub(in crate::runtime) fn stream_reservation(&self) -> StreamSendBufferReservation {
        StreamSendBufferReservation {
            buffer: self.clone(),
            held_bytes: 0,
        }
    }

    pub(in crate::runtime) fn subscribe(&self) -> SessionSendBufferWaiter {
        SessionSendBufferWaiter {
            buffer: self.clone(),
            ticket: None,
            granted_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(in crate::runtime) fn limit_bytes(&self) -> usize {
        self.inner.limit_bytes
    }

    pub(in crate::runtime) fn used_bytes(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("session send-buffer lock")
            .used_bytes
    }

    #[cfg(test)]
    pub(in crate::runtime) fn available_bytes(&self) -> usize {
        self.limit_bytes().saturating_sub(self.used_bytes())
    }

    pub(in crate::runtime) async fn reserve(
        &self,
        waiter: &mut SessionSendBufferWaiter,
        max_bytes: usize,
    ) -> SessionSendBufferPermit {
        assert!(
            max_bytes > 0,
            "session send-buffer reservation must be positive"
        );
        assert!(
            Arc::ptr_eq(&self.inner, &waiter.buffer.inner),
            "session send-buffer waiter owner mismatch"
        );
        SourceReservation {
            buffer: self,
            waiter,
            max_bytes,
            started: false,
            completed: false,
        }
        .await
    }

    #[cfg(test)]
    fn try_reserve(&self, max_bytes: usize) -> Option<SessionSendBufferPermit> {
        if max_bytes == 0 {
            return None;
        }
        let mut state = self.inner.state.lock().expect("session send-buffer lock");
        if !state.pending.is_empty() {
            return None;
        }
        let reserved_bytes = (self.limit_bytes() - state.used_bytes).min(max_bytes);
        if reserved_bytes == 0 {
            return None;
        }
        state.used_bytes += reserved_bytes;
        Some(SessionSendBufferPermit {
            buffer: self.clone(),
            reserved_bytes,
        })
    }

    /// Charge each oldest active request before waking its owner. A dormant
    /// source ticket is absent from this map and cannot hold capacity or a turn.
    fn grant_pending(&self, state: &mut SessionSendBufferState) -> SmallVec<[Waker; 1]> {
        // One grant needs no wake-list allocation; larger releases remain
        // unrestricted and may assign several pending owners in this turn.
        let mut wake = SmallVec::new();
        while state.used_bytes < self.limit_bytes() {
            let Some((_, request)) = state.pending.pop_first() else {
                break;
            };
            let bytes = (self.limit_bytes() - state.used_bytes).min(request.max_bytes);
            state.used_bytes += bytes;
            // This atomic belongs to one borrowed source handle. Every access
            // is under the shared accounting lock; it adds no second lock order.
            let previous = request.granted_bytes.swap(bytes, Ordering::Relaxed);
            assert_eq!(previous, 0, "source reservation already owns a grant");
            wake.push(request.waker);
        }
        wake
    }

    fn cancel_request(&self, ticket: Option<u64>, granted_bytes: &AtomicUsize) {
        let wake = {
            let mut state = self.inner.state.lock().expect("session send-buffer lock");
            if let Some(ticket) = ticket {
                state.pending.remove(&ticket);
            }
            let refund = granted_bytes.swap(0, Ordering::Relaxed);
            state.used_bytes = state
                .used_bytes
                .checked_sub(refund)
                .expect("session send-buffer grant accounting underflow");
            self.grant_pending(&mut state)
        };
        for waker in wake {
            waker.wake();
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let wake = {
            let mut state = self.inner.state.lock().expect("session send-buffer lock");
            state.used_bytes = state
                .used_bytes
                .checked_sub(bytes)
                .expect("session send-buffer accounting underflow");
            self.grant_pending(&mut state)
        };
        for waker in wake {
            waker.wake();
        }
    }
}

/// One source's demand identity. A cancelled pending future keeps its age for
/// re-entry, while ineligibility and completed grants end that demand turn.
pub(in crate::runtime) struct SessionSendBufferWaiter {
    buffer: SessionSendBuffer,
    ticket: Option<u64>,
    granted_bytes: Arc<AtomicUsize>,
}

impl SessionSendBufferWaiter {
    pub(in crate::runtime) fn withdraw(&mut self) {
        let ticket = self.ticket.take();
        if ticket.is_some() {
            self.buffer.cancel_request(ticket, &self.granted_bytes);
        }
    }
}

impl Drop for SessionSendBufferWaiter {
    fn drop(&mut self) {
        self.withdraw();
    }
}

struct SourceReservation<'a> {
    buffer: &'a SessionSendBuffer,
    waiter: &'a mut SessionSendBufferWaiter,
    max_bytes: usize,
    started: bool,
    completed: bool,
}

impl Future for SourceReservation<'_> {
    type Output = SessionSendBufferPermit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this
            .buffer
            .inner
            .state
            .lock()
            .expect("session send-buffer lock");
        let granted = this.waiter.granted_bytes.swap(0, Ordering::Relaxed);
        if granted > 0 {
            this.waiter.ticket = None;
            this.completed = true;
            return Poll::Ready(SessionSendBufferPermit {
                buffer: this.buffer.clone(),
                reserved_bytes: granted,
            });
        }
        if !this.started {
            let available = this.buffer.limit_bytes() - state.used_bytes;
            if state.pending.is_empty() && available > 0 {
                let bytes = available.min(this.max_bytes);
                state.used_bytes += bytes;
                this.waiter.ticket = None;
                this.completed = true;
                return Poll::Ready(SessionSendBufferPermit {
                    buffer: this.buffer.clone(),
                    reserved_bytes: bytes,
                });
            }
            // Release and cancellation eagerly assign all available bytes.
            // Enqueue therefore never leaves positive capacity unassigned.
            debug_assert_eq!(state.used_bytes, this.buffer.limit_bytes());
            let ticket = *this.waiter.ticket.get_or_insert_with(|| {
                let ticket = state.next_ticket;
                state.next_ticket = ticket
                    .checked_add(1)
                    .expect("session send-buffer demand ticket exhausted");
                ticket
            });
            let previous = state.pending.insert(
                ticket,
                PendingSourceReservation {
                    granted_bytes: this.waiter.granted_bytes.clone(),
                    max_bytes: this.max_bytes,
                    waker: cx.waker().clone(),
                },
            );
            assert!(
                previous.is_none(),
                "source already has an active reservation"
            );
            this.started = true;
        } else {
            let ticket = this.waiter.ticket.expect("pending source ticket");
            let request = state
                .pending
                .get_mut(&ticket)
                .expect("active source request");
            if !request.waker.will_wake(cx.waker()) {
                request.waker.clone_from(cx.waker());
            }
        }
        Poll::Pending
    }
}

impl Drop for SourceReservation<'_> {
    fn drop(&mut self) {
        if self.started && !self.completed {
            self.buffer
                .cancel_request(self.waiter.ticket, &self.waiter.granted_bytes);
        }
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
