//! Payload-free wake ownership for either direction's shared prepared source.

use super::commands::ReliablePathCommandSender;
use super::writer_boundary::{ReliableWriterForegroundGuard, ReliableWriterReadyGuard};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance};
use crate::protocol::{Frame, StreamId};
use crate::runtime::path::ClientPathContext;
use crate::runtime::sender::{
    WeakSharedRequestProduct, WeakSharedResponseProduct, claim_prepared_request_data,
    claim_prepared_request_repair, claim_prepared_response_data, claim_prepared_response_repair,
};
use crate::runtime::stream::response::ResponseAcquisitionOutputId;
use crate::scheduler::TrafficClass;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU8, Ordering},
};
use tokio::sync::Notify;

const OUTSTANDING: u8 = 1;
const NOTIFIED_AGAIN: u8 = 2;

pub(in crate::runtime) type PreparedOriginalWait =
    Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub(in crate::runtime) enum PreparedOriginalClaim {
    Claimed(Frame),
    Busy(Pin<Box<tokio::sync::futures::OwnedNotified>>),
    Blocked(PreparedOriginalWait),
    Empty,
}

enum PreparedOriginalSource {
    Request {
        product: WeakSharedRequestProduct,
        context: ClientPathContext,
        instance: RelayPathInstance,
    },
    Response {
        product: WeakSharedResponseProduct,
        instance: ResponseAcquisitionOutputId,
    },
}

#[derive(Debug, Clone, Copy)]
enum PreparedOriginalInstance {
    Request(RelayPathInstance),
    Response(ResponseAcquisitionOutputId),
}

impl PreparedOriginalInstance {
    fn path_instance_id(self) -> CarrierPathInstanceId {
        match self {
            Self::Request(instance) => instance.path_instance_id,
            Self::Response(instance) => instance.path_instance_id,
        }
    }
}

/// Product alone retains this registration. A queued or waiting notice holds
/// only Weak, so carrier work cannot keep cancelled logical source alive.
pub(in crate::runtime) struct PreparedOriginalRegistration {
    source: PreparedOriginalSource,
    stream_id: StreamId,
    instance: PreparedOriginalInstance,
    commands: ReliablePathCommandSender,
    lane: TrafficClass,
    state: AtomicU8,
    notified: Arc<Notify>,
    repair_state: AtomicU8,
    repair_notified: Arc<Notify>,
    // A parked Original's new notification is foreground before the writer
    // polls its retry future and republishes the weak queue notice.
    original_notification: Mutex<Option<ReliableWriterForegroundGuard>>,
    dropped: Arc<Notify>,
}

impl std::fmt::Debug for PreparedOriginalRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedOriginalRegistration")
            .field("stream_id", &self.stream_id)
            .field("instance", &self.instance)
            .field("lane", &self.lane)
            .finish_non_exhaustive()
    }
}

impl PreparedOriginalRegistration {
    pub(in crate::runtime) fn new(
        product: WeakSharedRequestProduct,
        context: ClientPathContext,
        stream_id: StreamId,
        instance: RelayPathInstance,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
    ) -> Arc<Self> {
        Arc::new(Self {
            source: PreparedOriginalSource::Request {
                product,
                context,
                instance,
            },
            stream_id,
            instance: PreparedOriginalInstance::Request(instance),
            commands,
            lane,
            state: AtomicU8::new(0),
            notified: Arc::new(Notify::new()),
            repair_state: AtomicU8::new(0),
            repair_notified: Arc::new(Notify::new()),
            original_notification: Mutex::new(None),
            dropped: Arc::new(Notify::new()),
        })
    }

    pub(in crate::runtime) fn new_response(
        product: WeakSharedResponseProduct,
        stream_id: StreamId,
        instance: ResponseAcquisitionOutputId,
        commands: ReliablePathCommandSender,
        lane: TrafficClass,
    ) -> Arc<Self> {
        Arc::new(Self {
            source: PreparedOriginalSource::Response { product, instance },
            stream_id,
            instance: PreparedOriginalInstance::Response(instance),
            commands,
            lane,
            state: AtomicU8::new(0),
            notified: Arc::new(Notify::new()),
            repair_state: AtomicU8::new(0),
            repair_notified: Arc::new(Notify::new()),
            original_notification: Mutex::new(None),
            dropped: Arc::new(Notify::new()),
        })
    }

    pub(in crate::runtime) fn request_instance(&self) -> Option<RelayPathInstance> {
        match self.instance {
            PreparedOriginalInstance::Request(instance) => Some(instance),
            PreparedOriginalInstance::Response(_) => None,
        }
    }

    pub(in crate::runtime) fn response_instance(&self) -> Option<ResponseAcquisitionOutputId> {
        match self.instance {
            PreparedOriginalInstance::Response(instance) => Some(instance),
            PreparedOriginalInstance::Request(_) => None,
        }
    }
    pub(in crate::runtime) fn lane(&self) -> TrafficClass {
        self.lane
    }

    pub(in crate::runtime) fn notify(self: &Arc<Self>) {
        self.notify_kind(false);
    }

    /// Publishes only weak willingness to re-evaluate retained repair work.
    /// It owns no payload, queue byte charge, slot, or recovery copy record.
    pub(in crate::runtime) fn notify_repair(self: &Arc<Self>) {
        self.notify_kind(true);
    }

    fn notice_state(&self, repair: bool) -> &AtomicU8 {
        if repair {
            &self.repair_state
        } else {
            &self.state
        }
    }

    fn notice_notify(&self, repair: bool) -> &Arc<Notify> {
        if repair {
            &self.repair_notified
        } else {
            &self.notified
        }
    }

    fn notify_kind(self: &Arc<Self>, repair: bool) {
        let mut foreground = (!repair).then(|| {
            self.original_notification
                .lock()
                .expect("prepared notification lock")
        });
        let previous = self
            .notice_state(repair)
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                Some(if state & OUTSTANDING == 0 {
                    OUTSTANDING
                } else {
                    state | NOTIFIED_AGAIN
                })
            })
            .expect("prepared notice state update always succeeds");
        if previous & OUTSTANDING != 0 {
            if previous & NOTIFIED_AGAIN == 0
                && let Some(foreground) = foreground.as_mut()
            {
                **foreground = Some(self.commands.writer_boundary().register_foreground());
            }
            self.notice_notify(repair).notify_waiters();
            return;
        }
        // Rejected enqueue drops its work synchronously, so no registration
        // notification lock may cross the queue ownership transfer.
        drop(foreground);
        self.commands.enqueue_prepared_work(PreparedOriginalWork {
            registration: Arc::downgrade(self),
            stream_id: self.stream_id,
            instance: self.instance,
            lane: self.lane,
            repair,
        });
    }

    fn clear_notification(&self, repair: bool) {
        let mut foreground = (!repair).then(|| {
            self.original_notification
                .lock()
                .expect("prepared notification lock")
        });
        self.notice_state(repair)
            .fetch_and(!NOTIFIED_AGAIN, Ordering::AcqRel);
        if let Some(foreground) = foreground.as_mut() {
            drop(foreground.take());
        }
    }
}

impl Drop for PreparedOriginalRegistration {
    fn drop(&mut self) {
        self.dropped.notify_waiters();
    }
}

/// Exact identifiers are metadata, not a payload, DSN, target reservation or
/// Product owner. One token spans queueing, deferred waiting and writer use.
pub(in crate::runtime) struct PreparedOriginalWork {
    registration: Weak<PreparedOriginalRegistration>,
    stream_id: StreamId,
    instance: PreparedOriginalInstance,
    lane: TrafficClass,
    repair: bool,
}

impl PreparedOriginalWork {
    pub(in crate::runtime) fn is_repair(&self) -> bool {
        self.repair
    }
    pub(in crate::runtime) fn stream_id(&self) -> StreamId {
        self.stream_id
    }
    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.instance.path_instance_id()
    }
    pub(in crate::runtime) fn request_instance(&self) -> Option<RelayPathInstance> {
        match self.instance {
            PreparedOriginalInstance::Request(instance) => Some(instance),
            PreparedOriginalInstance::Response(_) => None,
        }
    }
    pub(in crate::runtime) fn response_instance(&self) -> Option<ResponseAcquisitionOutputId> {
        match self.instance {
            PreparedOriginalInstance::Response(instance) => Some(instance),
            PreparedOriginalInstance::Request(_) => None,
        }
    }
    pub(in crate::runtime) fn lane(&self) -> TrafficClass {
        self.lane
    }

    pub(in crate::runtime) fn try_claim(
        &self,
        ready: &ReliableWriterReadyGuard,
    ) -> PreparedOriginalClaim {
        let Some(registration) = self.registration.upgrade() else {
            return PreparedOriginalClaim::Empty;
        };
        // Arm before detached Native observation: foreground may be published
        // and drained while the Product lock is released. Ready-guard cleanup
        // is deliberately not this event, so a refused repair cannot wake itself.
        let foreground_released = self.repair.then(|| {
            let mut wait = Box::pin(
                registration
                    .commands
                    .writer_boundary()
                    .foreground_release_notify()
                    .notified_owned(),
            );
            wait.as_mut().enable();
            wait
        });
        // A fresh claim observes all work notifications preceding this point.
        // A later notification survives in NOTIFIED_AGAIN until token release.
        registration.clear_notification(self.repair);
        let claim = match &registration.source {
            PreparedOriginalSource::Request {
                product,
                context,
                instance,
            } => {
                let Some(product) = product.upgrade() else {
                    return PreparedOriginalClaim::Empty;
                };
                if self.repair {
                    claim_prepared_request_repair(
                        &product,
                        context,
                        *instance,
                        ready,
                        &registration,
                    )
                } else {
                    claim_prepared_request_data(&product, context, *instance, ready, &registration)
                }
            }
            PreparedOriginalSource::Response { product, instance } => {
                let Some(product) = product.upgrade() else {
                    return PreparedOriginalClaim::Empty;
                };
                if self.repair {
                    claim_prepared_response_repair(&product, *instance, ready, &registration)
                } else {
                    claim_prepared_response_data(&product, *instance, ready, &registration)
                }
            }
        };
        match (claim, foreground_released) {
            (PreparedOriginalClaim::Blocked(wait), Some(mut foreground_released)) => {
                PreparedOriginalClaim::Blocked(Box::pin(async move {
                    tokio::select! {
                        () = wait => {}
                        () = &mut foreground_released => {}
                    }
                }))
            }
            (claim, _) => claim,
        }
    }

    pub(in crate::runtime) fn requeue(self) {
        if let Some(registration) = self.registration.upgrade() {
            registration.commands.enqueue_prepared_work(self);
        }
    }

    pub(in crate::runtime::path) fn defer(self, wait: PreparedOriginalWait) {
        if let Some(registration) = self.registration.upgrade() {
            registration
                .commands
                .enqueue_prepared_wait(self.after_wait(wait));
        }
    }

    /// An occupied writer has observed this notice without claiming source.
    /// Wait for its next physical boundary; retain no source or registration.
    pub(in crate::runtime::path) fn writer_change_wait(&self) -> Option<PreparedOriginalWait> {
        let registration = self.registration.upgrade()?;
        registration.clear_notification(self.repair);
        let mut wait = Box::pin(
            registration
                .commands
                .writer_boundary()
                .change_notify()
                .notified_owned(),
        );
        wait.as_mut().enable();
        Some(wait)
    }

    /// Arm while holding a temporary upgrade, then retain no strong owner in
    /// the wait. Registration removal cannot race past cancellation arming.
    pub(in crate::runtime::path) fn cancellation_wait(&self) -> Option<PreparedOriginalWait> {
        let registration = self.registration.upgrade()?;
        let mut dropped = Box::pin(registration.dropped.clone().notified_owned());
        dropped.as_mut().enable();
        Some(dropped)
    }

    pub(in crate::runtime::path) fn after_wait(
        self,
        wait: PreparedOriginalWait,
    ) -> PreparedOriginalWait {
        let cancellation = self.cancellation_wait();
        let notification = self.registration.upgrade().map(|registration| {
            let mut notified = Box::pin(
                registration
                    .notice_notify(self.repair)
                    .clone()
                    .notified_owned(),
            );
            notified.as_mut().enable();
            let already_notified = registration
                .notice_state(self.repair)
                .load(Ordering::Acquire)
                & NOTIFIED_AGAIN
                != 0;
            // The registration is released before returning this owned wait.
            async move {
                if !already_notified {
                    notified.await;
                }
            }
        });
        Box::pin(async move {
            let (Some(cancelled), Some(notified)) = (cancellation, notification) else {
                return;
            };
            tokio::select! {
                biased;
                () = cancelled => {}
                () = notified => self.requeue(),
                () = wait => self.requeue(),
            }
        })
    }
}

impl Drop for PreparedOriginalWork {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.upgrade() {
            let (previous, foreground) = {
                let mut notification = (!self.repair).then(|| {
                    registration
                        .original_notification
                        .lock()
                        .expect("prepared notification lock")
                });
                let previous = registration
                    .notice_state(self.repair)
                    .swap(0, Ordering::AcqRel);
                let foreground = notification
                    .as_mut()
                    .and_then(|notification| notification.take());
                (previous, foreground)
            };
            if previous & NOTIFIED_AGAIN != 0 {
                registration.notify_kind(self.repair);
            }
            drop(foreground);
        }
    }
}
