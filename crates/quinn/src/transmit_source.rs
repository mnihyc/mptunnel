//! Opt-in producer futures owned by the connection driver, never by native state.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};

use tokio::sync::mpsc;

use crate::mutex::Mutex;

pub(super) type TransmitSource = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct SourceWake {
    ready: AtomicBool,
    driver: Mutex<Option<Waker>>,
}

impl Wake for SourceWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Release);
        let driver = self.driver.lock("transmit_source_wake").clone();
        if let Some(driver) = driver {
            driver.wake();
        }
    }
}

struct Source {
    future: TransmitSource,
    wake: Arc<SourceWake>,
}

pub(super) struct TransmitSources {
    receiver: mpsc::UnboundedReceiver<TransmitSource>,
    sources: Vec<Source>,
}

impl fmt::Debug for TransmitSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransmitSources")
            .field("active", &self.sources.len())
            .field("queued", &self.receiver.len())
            .finish()
    }
}

impl TransmitSources {
    pub(super) fn new(receiver: mpsc::UnboundedReceiver<TransmitSource>) -> Self {
        Self {
            receiver,
            sources: Vec::new(),
        }
    }

    fn insert(&mut self, future: TransmitSource) {
        self.sources.push(Source {
            future,
            wake: Arc::new(SourceWake {
                ready: AtomicBool::new(true),
                driver: Mutex::new(None),
            }),
        });
    }

    pub(super) fn receive(&mut self, cx: &mut Context<'_>) {
        // Bound this pass by already queued registrations. Concurrent producers
        // cannot keep the driver indefinitely inside a channel-draining loop.
        let queued = self.receiver.len();
        for _ in 0..queued {
            match self.receiver.try_recv() {
                Ok(source) => self.insert(source),
                Err(_) => break,
            }
        }
        // Register the driver for subsequent registrations, including arrivals
        // racing the bounded drain. A ready extra entry schedules another pass.
        if let Poll::Ready(Some(source)) = self.receiver.poll_recv(cx) {
            self.insert(source);
            cx.waker().wake_by_ref();
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Poll each signaled source at most once, with no native mutex held.
    pub(super) fn poll_ready(&mut self, cx: &mut Context<'_>) {
        let mut index = 0;
        while index < self.sources.len() {
            let source = &mut self.sources[index];
            {
                let mut driver = source.wake.driver.lock("transmit_source_driver");
                if driver.as_ref().is_none_or(|old| !old.will_wake(cx.waker())) {
                    *driver = Some(cx.waker().clone());
                }
            }
            let complete = if source.wake.ready.swap(false, Ordering::AcqRel) {
                let waker = Waker::from(source.wake.clone());
                source
                    .future
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_ready()
            } else {
                false
            };
            if complete {
                self.sources.swap_remove(index);
            } else {
                index += 1;
            }
        }
    }

    /// Closing the receiver also breaks queued future -> Connection references.
    /// This method and implicit driver destruction must occur outside native state.
    pub(super) fn close(&mut self) {
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        self.sources.clear();
    }
}
