//! Bounded target delivery retained independently of ordered Product input.

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

/// Owns contiguous, already-validated receive bytes until the target accepts
/// them. The caller retains receive-window and FIN/RESET authority.
pub(super) struct ServerTargetDelivery {
    quantum_bytes: usize,
    window_bytes: usize,
    // Keep the first already-bounded ready batch without copying its payload.
    // Its descriptor bound is the existing collector/receive-item bound.
    active: SmallVec<[Bytes; 8]>,
    active_index: usize,
    active_offset: usize,
    active_bytes: usize,
    // Later batches are packed so repeated tiny frames cannot accumulate one
    // descriptor each while a target write is blocked. This single backing is
    // a ring: consuming and replacing one byte never moves the remaining W.
    deferred: BytesMut,
    deferred_offset: usize,
    deferred_len: usize,
    pending_bytes: usize,
    delivered_offset: u64,
    flush_pending: bool,
}

impl ServerTargetDelivery {
    pub(super) fn new(quantum_bytes: usize, window_bytes: usize) -> Self {
        assert!(
            quantum_bytes > 0,
            "target delivery quantum must be positive"
        );
        assert!(window_bytes > 0, "target delivery window must be positive");
        Self {
            quantum_bytes: quantum_bytes.min(window_bytes),
            window_bytes,
            active: SmallVec::new(),
            active_index: 0,
            active_offset: 0,
            active_bytes: 0,
            deferred: BytesMut::new(),
            deferred_offset: 0,
            deferred_len: 0,
            pending_bytes: 0,
            delivered_offset: 0,
            flush_pending: false,
        }
    }

    /// Append only contiguous bytes returned by the receive model after its
    /// deduplication and advertised-credit checks. This adds no receive credit.
    pub(super) fn append_batch(&mut self, mut batch: SmallVec<[Bytes; 8]>) -> io::Result<()> {
        let bytes = batch.iter().try_fold(0usize, |total, chunk| {
            total.checked_add(chunk.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "target delivery size overflow")
            })
        })?;
        let pending = self.pending_bytes.checked_add(bytes).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "target delivery size overflow")
        })?;
        if pending > self.window_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "target delivery exceeds the existing receive window",
            ));
        }
        let pending_offset = u64::try_from(pending).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "target delivery offset overflow",
            )
        })?;
        self.delivered_offset
            .checked_add(pending_offset)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "target delivery offset overflow",
                )
            })?;
        if bytes == 0 {
            return Ok(());
        }
        if self.is_empty() {
            batch.retain(|chunk| !chunk.is_empty());
            self.active = batch;
            self.active_index = 0;
            self.active_offset = 0;
            self.active_bytes = bytes;
        } else {
            self.reserve_deferred(bytes);
            for chunk in batch {
                let tail = (self.deferred_offset + self.deferred_len) % self.deferred.len();
                let first = chunk.len().min(self.deferred.len() - tail);
                self.deferred[tail..tail + first].copy_from_slice(&chunk[..first]);
                self.deferred[..chunk.len() - first].copy_from_slice(&chunk[first..]);
                self.deferred_len += chunk.len();
            }
        }
        self.pending_bytes = pending;
        Ok(())
    }

    fn reserve_deferred(&mut self, additional: usize) {
        let required = self.deferred_len + additional;
        if required <= self.deferred.len() {
            return;
        }
        let geometric = if self.deferred.is_empty() {
            required
        } else {
            self.deferred.len().saturating_mul(2)
        };
        let capacity = geometric.max(required).min(self.window_bytes);
        // Allocate the selected capacity explicitly: reserve() may double past
        // the existing window. No deferred slices escape and pin old buffers.
        let mut replacement = BytesMut::with_capacity(capacity);
        replacement.resize(capacity, 0);
        let first = self
            .deferred_len
            .min(self.deferred.len() - self.deferred_offset);
        replacement[..first]
            .copy_from_slice(&self.deferred[self.deferred_offset..self.deferred_offset + first]);
        replacement[first..self.deferred_len]
            .copy_from_slice(&self.deferred[..self.deferred_len - first]);
        self.deferred = replacement;
        self.deferred_offset = 0;
    }

    pub(super) fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub(super) fn delivered_offset(&self) -> u64 {
        self.delivered_offset
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending_bytes == 0
    }

    pub(super) fn flush_pending(&self) -> bool {
        self.flush_pending
    }

    #[cfg(test)]
    fn poll_write<W: AsyncWrite + Unpin>(
        &mut self,
        cx: &mut Context<'_>,
        writer: &mut W,
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with_limit(cx, writer, self.quantum_bytes)
    }

    /// Perform at most one native write, offering the remaining write quantum
    /// and the existing writer's eight-slice vectored span. A positive result
    /// advances the cursor before returning; losing another select turn cannot
    /// replay an accepted prefix. Callers should guard empty polling.
    fn poll_write_with_limit<W: AsyncWrite + Unpin>(
        &mut self,
        cx: &mut Context<'_>,
        writer: &mut W,
        byte_limit: usize,
    ) -> Poll<io::Result<usize>> {
        debug_assert!(byte_limit > 0 && byte_limit <= self.quantum_bytes);
        if self.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let (result, offered) = if self.active_bytes > 0 {
            let mut slices = SmallVec::<[IoSlice<'_>; 8]>::new();
            let mut remaining = byte_limit;
            for (index, chunk) in self.active.iter().enumerate().skip(self.active_index) {
                let offset = if index == self.active_index {
                    self.active_offset
                } else {
                    0
                };
                let take = (chunk.len() - offset).min(remaining);
                slices.push(IoSlice::new(&chunk[offset..offset + take]));
                remaining -= take;
                if remaining == 0 || slices.len() == 8 {
                    break;
                }
            }
            if writer.is_write_vectored() {
                let offered = byte_limit - remaining;
                (Pin::new(writer).poll_write_vectored(cx, &slices), offered)
            } else {
                let first = slices.first().expect("active delivery contains bytes");
                (Pin::new(writer).poll_write(cx, first), first.len())
            }
        } else {
            let take = self.deferred_len.min(byte_limit);
            let first = take.min(self.deferred.len() - self.deferred_offset);
            let bytes = &self.deferred[self.deferred_offset..self.deferred_offset + first];
            if first < take && writer.is_write_vectored() {
                let slices = [
                    IoSlice::new(bytes),
                    IoSlice::new(&self.deferred[..take - first]),
                ];
                (Pin::new(writer).poll_write_vectored(cx, &slices), take)
            } else {
                (Pin::new(writer).poll_write(cx, bytes), first)
            }
        };
        let written = match result {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
            Poll::Ready(Ok(written)) if written <= offered => written,
            Poll::Ready(Ok(_)) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "target writer accepted more than the offered bytes",
                )));
            }
        };
        if self.active_bytes > 0 {
            let mut remaining = written;
            // Only descriptors covered by this one bounded I/O span advance.
            while remaining > 0 {
                let available = self.active[self.active_index].len() - self.active_offset;
                let take = available.min(remaining);
                self.active_offset += take;
                remaining -= take;
                if take == available {
                    self.active[self.active_index] = Bytes::new();
                    self.active_index += 1;
                    self.active_offset = 0;
                }
            }
            self.active_bytes -= written;
            if self.active_bytes == 0 {
                self.active = SmallVec::new();
                self.active_index = 0;
                self.active_offset = 0;
            }
        } else {
            self.deferred_offset = (self.deferred_offset + written) % self.deferred.len();
            self.deferred_len -= written;
            if self.deferred_len == 0 {
                self.deferred = BytesMut::new();
                self.deferred_offset = 0;
            }
        }
        self.pending_bytes -= written;
        self.delivered_offset += written as u64;
        self.flush_pending = true;
        Poll::Ready(Ok(written))
    }

    /// Flush the accepted prefix. Completion does not imply that pending DATA
    /// is empty; target shutdown requires both empty delivery and no flush.
    pub(super) fn poll_flush<W: AsyncWrite + Unpin>(
        &mut self,
        cx: &mut Context<'_>,
        writer: &mut W,
    ) -> Poll<io::Result<()>> {
        if !self.flush_pending {
            return Poll::Ready(Ok(()));
        }
        match Pin::new(writer).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.flush_pending = false;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

/// Target I/O phases owned by the Product actor across select cancellation.
/// Its published frontier advances only after flush, preserving the existing
/// delivery-before-credit boundary independently of the write cursor.
pub(super) struct ServerTargetIo {
    delivery: ServerTargetDelivery,
    flushed_offset: u64,
    flushing: bool,
    shutdown_requested: bool,
    shutting_down: bool,
    shutdown: bool,
}

impl ServerTargetIo {
    pub(super) fn new(quantum_bytes: usize, window_bytes: usize) -> Self {
        Self {
            delivery: ServerTargetDelivery::new(quantum_bytes, window_bytes),
            flushed_offset: 0,
            flushing: false,
            shutdown_requested: false,
            shutting_down: false,
            shutdown: false,
        }
    }

    pub(super) fn append_batch(&mut self, batch: SmallVec<[Bytes; 8]>) -> io::Result<()> {
        if (self.shutting_down || self.shutdown) && batch.iter().any(|bytes| !bytes.is_empty()) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "target DATA arrived after shutdown began",
            ));
        }
        self.delivery.append_batch(batch)
    }

    pub(super) fn pending_bytes(&self) -> usize {
        self.delivery.pending_bytes()
    }

    /// Successfully flushed target frontier, not just accepted write bytes.
    pub(super) fn delivered_offset(&self) -> u64 {
        self.flushed_offset
    }

    pub(super) fn request_shutdown(&mut self) {
        self.shutdown_requested = true;
    }

    pub(super) fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    pub(super) fn has_work(&self) -> bool {
        !self.shutdown
            && (!self.delivery.is_empty()
                || self.delivery.flush_pending()
                || self.flushing
                || self.shutdown_requested)
    }

    /// One native write at most, bounded by the remaining relay quantum.
    /// Flush each completed quantum or empty queue so a retained backlog cannot
    /// delay delivery credit until the entire receive window drains. Flush and
    /// shutdown phases persist after writes have advanced their exact cursor.
    pub(super) fn poll_io<W: AsyncWrite + Unpin>(
        &mut self,
        cx: &mut Context<'_>,
        writer: &mut W,
    ) -> Poll<io::Result<()>> {
        if self.shutdown {
            return Poll::Ready(Ok(()));
        }
        if !self.shutting_down {
            // A pending flush retains its phase even if new DATA has arrived.
            if !self.flushing && !self.delivery.is_empty() {
                let unflushed =
                    usize::try_from(self.delivery.delivered_offset() - self.flushed_offset)
                        .expect("unflushed target bytes fit one relay quantum");
                let write_budget = self.delivery.quantum_bytes - unflushed;
                match self
                    .delivery
                    .poll_write_with_limit(cx, writer, write_budget)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(_)) => {}
                }
                let quantum_complete = self.delivery.delivered_offset() - self.flushed_offset
                    == self.delivery.quantum_bytes as u64;
                if !self.delivery.is_empty() && !quantum_complete {
                    return Poll::Ready(Ok(()));
                }
                self.flushing = true;
            }
            if self.flushing || self.delivery.flush_pending() {
                self.flushing = true;
                match self.delivery.poll_flush(cx, writer) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(())) => {}
                }
                self.flushed_offset = self.delivery.delivered_offset();
                self.flushing = false;
            }
            if !self.shutdown_requested || !self.delivery.is_empty() {
                return Poll::Ready(Ok(()));
            }
            self.shutting_down = true;
        }
        match Pin::new(writer).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => {
                self.shutdown = true;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker_ref;
    use smallvec::smallvec;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    enum WriteStep {
        Accept(usize),
        Pending,
        Fail(io::ErrorKind),
    }

    #[derive(Default)]
    struct Writer {
        steps: VecDeque<WriteStep>,
        bytes: Vec<u8>,
        offered: Vec<usize>,
        flush_pending_once: bool,
        flush_polls: usize,
        scalar_only: bool,
        shutdown_pending_once: bool,
        shutdown_polls: usize,
    }

    impl Writer {
        fn write(
            &mut self,
            cx: &mut Context<'_>,
            slices: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let offered = slices.iter().map(|slice| slice.len()).sum();
            self.offered.push(offered);
            let take = match self.steps.pop_front() {
                Some(WriteStep::Pending) => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Some(WriteStep::Fail(kind)) => return Poll::Ready(Err(kind.into())),
                Some(WriteStep::Accept(limit)) => offered.min(limit),
                None => offered,
            };
            let mut remaining = take;
            for slice in slices {
                let count = slice.len().min(remaining);
                self.bytes.extend_from_slice(&slice[..count]);
                remaining -= count;
            }
            Poll::Ready(Ok(take))
        }
    }

    impl AsyncWrite for Writer {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.write(cx, &[IoSlice::new(bytes)])
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            slices: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.write(cx, slices)
        }

        fn is_write_vectored(&self) -> bool {
            !self.scalar_only
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.flush_polls += 1;
            if std::mem::take(&mut self.flush_pending_once) {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.shutdown_polls += 1;
            if std::mem::take(&mut self.shutdown_pending_once) {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    struct Owner {
        bytes: Box<[u8]>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for Owner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn owned(bytes: &[u8], drops: &Arc<AtomicUsize>) -> Bytes {
        Bytes::from_owner(Owner {
            bytes: bytes.into(),
            drops: drops.clone(),
        })
    }

    fn drain(delivery: &mut ServerTargetDelivery, writer: &mut Writer) {
        let mut cx = Context::from_waker(noop_waker_ref());
        let calls = delivery.pending_bytes();
        for _ in 0..calls {
            if delivery.is_empty() {
                return;
            }
            assert!(matches!(delivery.poll_write(&mut cx, writer), Poll::Ready(Ok(n)) if n > 0));
        }
        assert!(delivery.is_empty());
    }

    #[test]
    fn partial_write_pending_and_flush_keep_one_exact_cursor() {
        let first = Bytes::from_static(b"abc");
        let pointer = first.as_ptr();
        let mut delivery = ServerTargetDelivery::new(4, 64);
        delivery
            .append_batch(smallvec![first, Bytes::from_static(b"def")])
            .unwrap();
        assert_eq!(
            delivery.active[0].as_ptr(),
            pointer,
            "first batch remains zero-copy"
        );
        let mut writer = Writer {
            steps: [
                WriteStep::Accept(2),
                WriteStep::Pending,
                WriteStep::Accept(3),
                WriteStep::Accept(1),
            ]
            .into(),
            flush_pending_once: true,
            ..Writer::default()
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            delivery.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(2))
        ));
        assert_eq!(delivery.delivered_offset(), 2);
        assert_eq!(delivery.pending_bytes(), 4);
        assert!(delivery.poll_write(&mut cx, &mut writer).is_pending());
        assert_eq!(delivery.delivered_offset(), 2);
        assert_eq!(writer.bytes, b"ab");
        assert!(matches!(
            delivery.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(3))
        ));
        assert!(matches!(
            delivery.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(1))
        ));
        assert_eq!(writer.bytes, b"abcdef");
        assert!(writer.offered.iter().all(|bytes| *bytes <= 4));
        assert_eq!(delivery.delivered_offset(), 6);
        assert!(delivery.is_empty());
        assert!(delivery.flush_pending());
        assert!(delivery.poll_flush(&mut cx, &mut writer).is_pending());
        assert!(delivery.flush_pending());
        delivery
            .append_batch(smallvec![Bytes::from_static(b"gh")])
            .unwrap();
        assert!(matches!(
            delivery.poll_flush(&mut cx, &mut writer),
            Poll::Ready(Ok(()))
        ));
        assert!(!delivery.flush_pending());
        assert!(
            !delivery.is_empty(),
            "flush completion does not consume newly appended DATA"
        );
        drain(&mut delivery, &mut writer);
        assert_eq!(writer.bytes, b"abcdefgh");
    }

    #[test]
    fn error_after_partial_write_preserves_progress_and_drop_releases_owners() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut delivery = ServerTargetDelivery::new(4, 64);
        delivery
            .append_batch(smallvec![owned(b"abcdef", &drops)])
            .unwrap();
        delivery
            .append_batch(smallvec![owned(b"gh", &drops)])
            .unwrap();
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "packed input owner is released immediately"
        );
        let mut writer = Writer {
            steps: [
                WriteStep::Accept(2),
                WriteStep::Fail(io::ErrorKind::BrokenPipe),
            ]
            .into(),
            ..Writer::default()
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            delivery.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(2))
        ));
        assert!(
            matches!(delivery.poll_write(&mut cx, &mut writer), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        assert_eq!(delivery.delivered_offset(), 2);
        assert_eq!(delivery.pending_bytes(), 6);
        assert_eq!(writer.bytes, b"ab");
        drop(delivery);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn tiny_deferred_batches_pack_without_accumulating_descriptors() {
        const FIRST: usize = 32;
        const DEFERRED: usize = 4096;
        let drops = Arc::new(AtomicUsize::new(0));
        let mut delivery = ServerTargetDelivery::new(16, FIRST + DEFERRED);
        let first = (0..FIRST).map(|_| Bytes::from_static(b"a")).collect();
        delivery.append_batch(first).unwrap();
        assert_eq!(
            delivery.active.len(),
            FIRST,
            "the existing first batch has no new descriptor cap"
        );
        for _ in 0..DEFERRED {
            delivery
                .append_batch(smallvec![owned(b"b", &drops)])
                .unwrap();
            assert_eq!(delivery.active.len(), FIRST);
            assert!(delivery.deferred.capacity() <= FIRST + DEFERRED);
        }
        assert_eq!(drops.load(Ordering::SeqCst), DEFERRED);
        assert_eq!(delivery.pending_bytes(), FIRST + DEFERRED);
        assert_eq!(delivery.deferred_len, DEFERRED);
        let mut writer = Writer::default();
        drain(&mut delivery, &mut writer);
        assert_eq!(&writer.bytes[..FIRST], vec![b'a'; FIRST]);
        assert_eq!(&writer.bytes[FIRST..], vec![b'b'; DEFERRED]);
        assert!(writer.offered.iter().all(|bytes| *bytes <= 16));
        assert_eq!(delivery.delivered_offset(), (FIRST + DEFERRED) as u64);
        assert_eq!(
            delivery.deferred.capacity(),
            0,
            "drain releases deferred backing"
        );
        assert!(
            !delivery.active.spilled(),
            "drain releases active descriptor backing"
        );
        delivery
            .append_batch(smallvec![owned(b"next", &drops)])
            .unwrap();
        drop(delivery);
        assert_eq!(drops.load(Ordering::SeqCst), DEFERRED + 1);
    }

    #[test]
    fn deferred_wrap_and_growth_preserve_partial_prefix_and_append_order() {
        let mut delivery = ServerTargetDelivery::new(8, 64);
        delivery
            .append_batch(smallvec![Bytes::from_static(b"a")])
            .unwrap();
        delivery
            .append_batch(smallvec![Bytes::from(vec![b'b'; 30])])
            .unwrap();
        let mut writer = Writer {
            scalar_only: true,
            ..Writer::default()
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            delivery.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(1))
        ));
        assert!(matches!(
            delivery.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(8))
        ));
        let capacity = delivery.deferred.capacity();
        let pointer = delivery.deferred.as_ptr();
        delivery
            .append_batch(smallvec![Bytes::from_static(b"cccccccc")])
            .unwrap();
        assert_eq!(
            delivery.deferred.capacity(),
            capacity,
            "consumed prefix supplies append capacity"
        );
        assert_eq!(delivery.deferred.as_ptr(), pointer);
        assert_eq!(
            delivery.deferred_offset, 8,
            "append does not compact the retained suffix"
        );
        delivery
            .append_batch(smallvec![Bytes::from_static(b"zz")])
            .unwrap();
        assert!(
            delivery.deferred.capacity() > capacity,
            "growth copies both wrapped segments"
        );
        drain(&mut delivery, &mut writer);
        let mut expected = vec![b'a'];
        expected.extend_from_slice(&[b'b'; 30]);
        expected.extend_from_slice(b"cccccccc");
        expected.extend_from_slice(b"zz");
        assert_eq!(writer.bytes, expected);
    }

    #[test]
    fn full_window_tiny_write_append_reuses_ring_backing() {
        const WINDOW: usize = 64;
        const TURNS: usize = 1024;
        let mut delivery = ServerTargetDelivery::new(8, WINDOW);
        delivery
            .append_batch(smallvec![Bytes::from_static(b"a")])
            .unwrap();
        delivery
            .append_batch(smallvec![Bytes::from(vec![b'b'; WINDOW - 1])])
            .unwrap();
        let mut writer = Writer::default();
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            delivery.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(1))
        ));
        delivery
            .append_batch(smallvec![Bytes::from_static(b"c")])
            .unwrap();
        let pointer = delivery.deferred.as_ptr();
        let capacity = delivery.deferred.capacity();
        let mut expected = vec![b'a'];
        expected.extend_from_slice(&[b'b'; WINDOW - 1]);
        expected.push(b'c');
        for turn in 0..TURNS {
            writer.steps.push_back(WriteStep::Accept(1));
            assert!(matches!(
                delivery.poll_write(&mut cx, &mut writer),
                Poll::Ready(Ok(1))
            ));
            let byte = (turn % 251) as u8;
            delivery
                .append_batch(smallvec![Bytes::copy_from_slice(&[byte])])
                .unwrap();
            expected.push(byte);
            assert_eq!(delivery.pending_bytes(), WINDOW);
            assert_eq!(delivery.deferred_len, WINDOW);
            assert_eq!(delivery.deferred.capacity(), capacity);
            assert_eq!(delivery.deferred.as_ptr(), pointer);
            assert_eq!(delivery.deferred_offset, (turn + 1) % WINDOW);
        }
        drain(&mut delivery, &mut writer);
        assert_eq!(writer.bytes, expected);
        assert_eq!(delivery.deferred.capacity(), 0);
        assert_eq!(delivery.delivered_offset(), expected.len() as u64);
    }

    #[test]
    fn write_zero_and_existing_window_violation_do_not_advance_cursor() {
        let mut delivery = ServerTargetDelivery::new(4, 4);
        delivery
            .append_batch(smallvec![Bytes::from_static(b"abcd")])
            .unwrap();
        assert!(
            delivery
                .append_batch(smallvec![Bytes::from_static(b"e")])
                .is_err()
        );
        let mut writer = Writer {
            steps: [WriteStep::Accept(0)].into(),
            ..Writer::default()
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(
            matches!(delivery.poll_write(&mut cx, &mut writer), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::WriteZero)
        );
        assert_eq!(delivery.delivered_offset(), 0);
        assert_eq!(delivery.pending_bytes(), 4);
        assert!(!delivery.flush_pending());
        drain(&mut delivery, &mut writer);
        assert_eq!(writer.bytes, b"abcd");
    }

    #[test]
    fn target_io_preserves_pending_flush_before_new_data_and_shutdown() {
        let mut target = ServerTargetIo::new(4, 64);
        target
            .append_batch(smallvec![Bytes::from_static(b"abc")])
            .unwrap();
        let mut writer = Writer {
            flush_pending_once: true,
            shutdown_pending_once: true,
            ..Writer::default()
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(target.poll_io(&mut cx, &mut writer).is_pending());
        assert_eq!(writer.bytes, b"abc");
        assert_eq!(target.pending_bytes(), 0);
        assert_eq!(target.delivery.delivered_offset(), 3);
        assert_eq!(
            target.delivered_offset(),
            0,
            "pending flush cannot advance credit frontier"
        );
        target
            .append_batch(smallvec![Bytes::from_static(b"def")])
            .unwrap();
        target.request_shutdown();
        let writes = writer.offered.len();
        assert!(matches!(
            target.poll_io(&mut cx, &mut writer),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(
            writer.offered.len(),
            writes,
            "the retained flush completes before new DATA writes"
        );
        assert_eq!(target.delivered_offset(), 3);
        assert_eq!(target.pending_bytes(), 3);
        assert_eq!(writer.shutdown_polls, 0);
        assert!(target.poll_io(&mut cx, &mut writer).is_pending());
        assert_eq!(writer.bytes, b"abcdef");
        assert_eq!(
            target.delivered_offset(),
            6,
            "successful flush persists across pending shutdown"
        );
        assert_eq!(target.pending_bytes(), 0);
        assert_eq!(writer.shutdown_polls, 1);
        assert!(!target.is_shutdown());
        assert!(target.has_work());
        let writes = writer.offered.len();
        assert!(matches!(
            target.poll_io(&mut cx, &mut writer),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(
            writer.offered.len(),
            writes,
            "pending shutdown never replays DATA"
        );
        assert_eq!(writer.shutdown_polls, 2);
        assert!(target.is_shutdown());
        assert!(!target.has_work());
    }

    #[test]
    fn target_io_write_quantum_and_cancellation_preserve_owned_pending_bytes() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut target = ServerTargetIo::new(4, 64);
        target
            .append_batch(smallvec![owned(b"abcdefgh", &drops)])
            .unwrap();
        target
            .append_batch(smallvec![owned(b"ij", &drops)])
            .unwrap();
        let mut writer = Writer {
            steps: [WriteStep::Accept(2), WriteStep::Pending].into(),
            ..Writer::default()
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        assert!(matches!(
            target.poll_io(&mut cx, &mut writer),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(
            writer.offered.len(),
            1,
            "a ready partial write does not loop over its quantum"
        );
        assert_eq!(target.pending_bytes(), 8);
        assert_eq!(target.delivery.delivered_offset(), 2);
        assert_eq!(target.delivered_offset(), 0);
        assert!(target.poll_io(&mut cx, &mut writer).is_pending());
        assert_eq!(writer.bytes, b"ab");
        assert_eq!(target.pending_bytes(), 8);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(target);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn target_io_flushes_completed_quantum_while_backlog_remains() {
        let mut target = ServerTargetIo::new(4, 24);
        target
            .append_batch(smallvec![Bytes::from_static(b"abcdefghijklmnop")])
            .unwrap();
        let mut writer = Writer {
            steps: [
                WriteStep::Accept(1),
                WriteStep::Accept(1),
                WriteStep::Accept(1),
                WriteStep::Accept(1),
            ]
            .into(),
            flush_pending_once: true,
            ..Writer::default()
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        for written in 1..4 {
            assert!(matches!(
                target.poll_io(&mut cx, &mut writer),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(target.delivery.delivered_offset(), written);
            assert_eq!(target.delivered_offset(), 0);
            assert_eq!(writer.flush_polls, 0, "partial writes share one quantum");
        }
        assert!(target.poll_io(&mut cx, &mut writer).is_pending());
        assert_eq!(writer.offered, [4, 3, 2, 1]);
        assert_eq!(writer.bytes, b"abcd");
        assert_eq!(target.delivery.delivered_offset(), 4);
        assert_eq!(target.delivered_offset(), 0);
        assert_eq!(target.pending_bytes(), 12);
        assert_eq!(writer.flush_polls, 1);
        // These bytes fit the original 24-byte grant even before any flush.
        target
            .append_batch(smallvec![Bytes::from_static(b"qrst")])
            .unwrap();
        let writes = writer.offered.len();
        assert!(matches!(
            target.poll_io(&mut cx, &mut writer),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(
            writer.offered.len(),
            writes,
            "finish the retained flush first"
        );
        assert_eq!(target.delivered_offset(), 4);
        assert_eq!(target.pending_bytes(), 16);
        for frontier in [8, 12, 16, 20] {
            assert!(matches!(
                target.poll_io(&mut cx, &mut writer),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(target.delivered_offset(), frontier);
            assert_eq!(target.pending_bytes(), 20 - frontier as usize);
        }
        assert_eq!(writer.bytes, b"abcdefghijklmnopqrst");
        assert_eq!(
            writer.flush_polls, 6,
            "five quantum flushes, one initially pending"
        );
        assert!(!target.has_work());
    }
}
