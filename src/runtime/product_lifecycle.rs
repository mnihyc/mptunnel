use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Instant;

/// Payload-only activity for one established Product TCP stream or UDP
/// association. Carrier control, ACK, retry, and keepalive work never records
/// activity here.
#[derive(Debug)]
pub(in crate::runtime) struct ProductFlowActivity {
    state: Mutex<ProductFlowActivityState>,
}

#[derive(Debug)]
struct ProductFlowActivityState {
    last_activity: Instant,
    retired: bool,
}

impl ProductFlowActivity {
    pub(in crate::runtime) fn new() -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            state: Mutex::new(ProductFlowActivityState {
                last_activity: now,
                retired: false,
            }),
        })
    }

    /// Records payload only while this exact Product lifetime remains live.
    ///
    /// The same lock commits retirement, so a payload already accepted and
    /// recorded at the deadline deterministically precedes retirement. Once
    /// retirement commits, a later producer cannot revive the old lifetime.
    pub(in crate::runtime) fn record(&self) -> bool {
        let mut state = self.state.lock().expect("Product activity lock");
        if state.retired {
            return false;
        }
        state.last_activity = Instant::now();
        true
    }

    pub(in crate::runtime) fn is_idle(&self, timeout: Option<Duration>) -> bool {
        let Some(timeout) = timeout else {
            return false;
        };
        let state = self.state.lock().expect("Product activity lock");
        state.retired || state.last_activity.elapsed() >= timeout
    }

    /// Commits retirement only if no accepted payload refreshed the deadline.
    pub(in crate::runtime) fn try_retire(&self, timeout: Option<Duration>) -> bool {
        let Some(timeout) = timeout else {
            return false;
        };
        let mut state = self.state.lock().expect("Product activity lock");
        if state.retired {
            return true;
        }
        if state.last_activity.elapsed() < timeout {
            return false;
        }
        state.retired = true;
        true
    }

    /// Waits until the currently observed payload deadline is due without
    /// committing retirement. Queue-owning actors use this to fence requests
    /// accepted before the deadline before calling `try_retire`.
    pub(in crate::runtime) async fn wait_until_idle_candidate(&self, timeout: Option<Duration>) {
        let Some(timeout) = timeout else {
            std::future::pending::<()>().await;
            unreachable!("pending Product idle timer completed");
        };
        loop {
            let deadline = {
                let state = self.state.lock().expect("Product activity lock");
                if state.retired {
                    return;
                }
                state.last_activity + timeout
            };
            tokio::time::sleep_until(deadline).await;
            if self.is_idle(Some(timeout)) {
                return;
            }
        }
    }

    /// Waits for and atomically commits this Product lifetime's idle expiry.
    pub(in crate::runtime) async fn wait_until_idle(&self, timeout: Option<Duration>) {
        loop {
            self.wait_until_idle_candidate(timeout).await;
            if self.try_retire(timeout) {
                return;
            }
        }
    }
}

pub(in crate::runtime) struct ProductFlowActivityIo<S> {
    inner: S,
    activity: Arc<ProductFlowActivity>,
}

impl<S> ProductFlowActivityIo<S> {
    pub(in crate::runtime) fn new(inner: S, activity: Arc<ProductFlowActivity>) -> Self {
        Self { inner, activity }
    }
}

impl<S> AsyncRead for ProductFlowActivityIo<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let filled_before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(&result, Poll::Ready(Ok(()))) && buf.filled().len() > filled_before {
            let _ = this.activity.record();
        }
        result
    }
}

impl<S> AsyncWrite for ProductFlowActivityIo<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        if matches!(&result, Poll::Ready(Ok(written)) if *written > 0) {
            let _ = this.activity.record();
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        if matches!(&result, Poll::Ready(Ok(written)) if *written > 0) {
            let _ = this.activity.record();
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test(start_paused = true)]
    async fn positive_payload_rearms_but_zero_length_io_does_not() {
        let activity = ProductFlowActivity::new();
        let (left, mut peer) = tokio::io::duplex(64);
        let mut observed = ProductFlowActivityIo::new(left, activity.clone());
        let idle = activity.wait_until_idle(Some(Duration::from_secs(5)));
        tokio::pin!(idle);

        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(futures::poll!(&mut idle).is_pending());
        peer.write_all(b"x").await.expect("write payload");
        let mut byte = [0_u8; 1];
        observed.read_exact(&mut byte).await.expect("read payload");
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(futures::poll!(&mut idle).is_pending());

        let mut zero_writer = ProductFlowActivityIo::new(tokio::io::sink(), activity.clone());
        let written = std::future::poll_fn(|cx| Pin::new(&mut zero_writer).poll_write(cx, &[]))
            .await
            .expect("zero-length write");
        assert_eq!(written, 0);
        let mut zero_reader = ProductFlowActivityIo::new(tokio::io::empty(), activity.clone());
        let mut eof_buffer = [0_u8; 1];
        assert_eq!(
            zero_reader
                .read(&mut eof_buffer)
                .await
                .expect("zero-length read"),
            0
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(
            activity.is_idle(Some(Duration::from_secs(5))),
            "zero-length stream I/O must not rearm Product activity"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn accepted_payload_at_idle_boundary_wins_before_retirement() {
        let activity = ProductFlowActivity::new();
        let timeout = Some(Duration::from_secs(5));
        let candidate = activity.wait_until_idle_candidate(timeout);
        tokio::pin!(candidate);
        assert!(futures::poll!(&mut candidate).is_pending());

        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(futures::poll!(&mut candidate).is_ready());
        assert!(activity.record(), "accepted boundary payload stays live");
        assert!(
            !activity.try_retire(timeout),
            "the stale deadline cannot retire the refreshed Product lifetime"
        );

        let next_candidate = activity.wait_until_idle_candidate(timeout);
        tokio::pin!(next_candidate);
        assert!(futures::poll!(&mut next_candidate).is_pending());
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(futures::poll!(&mut next_candidate).is_ready());
        assert!(activity.try_retire(timeout));
        assert!(
            !activity.record(),
            "payload cannot revive an already-retired Product lifetime"
        );
    }
}
