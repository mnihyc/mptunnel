//! Payload-free wake ownership for either direction's shared prepared source.

use super::commands::ReliablePathCommandSender;
use super::native_commitment::{
    NativeCommitmentError, NativeCommitmentView, NativeOperationCommitment,
};
use super::writer_boundary::ReliableWriterReadyGuard;
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance};
use crate::protocol::{Frame, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::sender::{
    WeakSharedRequestProduct, WeakSharedResponseProduct, claim_prepared_request_data,
    claim_prepared_response_data,
};
use crate::runtime::stream::response::ResponseAcquisitionOutputId;
use crate::scheduler::TrafficClass;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU8, Ordering},
};
use tokio::sync::Notify;

const OUTSTANDING: u8 = 1;
const NOTIFIED_AGAIN: u8 = 2;

pub(in crate::runtime) type PreparedOriginalWait =
    Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Native reads happen before final Product ownership. The exact FIFO's captured
/// acceptance/packetization boundary then qualifies new Original opportunity.
pub(in crate::runtime) struct PreparedNativeCommitmentInputs<K> {
    entries: Vec<(K, Result<NativeCommitmentView, NativeCommitmentError>)>,
}

impl<K: Copy + Eq> PreparedNativeCommitmentInputs<K> {
    pub(in crate::runtime) fn capture(
        handles: impl IntoIterator<Item = (K, Option<NativeOperationCommitment>)>,
    ) -> Self {
        Self {
            entries: handles
                .into_iter()
                .filter_map(|(key, handle)| handle.map(|handle| (key, handle.capture())))
                .collect(),
        }
    }

    /// Surface only the invoking FIFO's failure through its physical writer.
    /// This uses only the captured Native result; it never queries Native while
    /// the final Product owner is held.
    pub(in crate::runtime) fn check_selected(
        &self,
        identity: K,
    ) -> Result<(), NativeCommitmentError> {
        let Some((_, captured)) = self
            .entries
            .iter()
            .find(|(current, _)| *current == identity)
        else {
            return Ok(());
        };
        captured.as_ref().map(|_| ()).map_err(Clone::clone)
    }

    /// Subscribe to the invoking send half even when no operation is blocked.
    /// Future construction performs no Native read; the writer polls it after
    /// releasing Product ownership. Unsupported FIFOs supply no fabricated wake.
    pub(in crate::runtime) fn selected_terminal_wait(
        &self,
        identity: K,
    ) -> Option<PreparedOriginalWait> {
        let (_, captured) = self
            .entries
            .iter()
            .find(|(current, _)| *current == identity)?;
        Some(match captured {
            Ok(view) => {
                let terminal = view.wait_until_terminated();
                Box::pin(async move {
                    let _ = terminal.await;
                })
            }
            Err(_) => Box::pin(std::future::ready(())),
        })
    }

    pub(in crate::runtime) fn original_ready(
        &self,
        ready: Vec<K>,
        waits: &mut Vec<PreparedOriginalWait>,
    ) -> Vec<K> {
        ready
            .into_iter()
            .filter(|key| {
                let Some((_, captured)) = self.entries.iter().find(|(current, _)| current == key)
                else {
                    // No exact capability was supplied. This is not an empty-FIFO
                    // receipt; all the existing Product/native authorities remain.
                    return true;
                };
                let Ok(view) = captured else {
                    // The invoking writer surfaces its own failure through
                    // check_selected. Other FIFOs remain excluded here until
                    // their own notice or lifecycle retires them.
                    return false;
                };
                match view.barrier() {
                    None => true,
                    Some(barrier) => {
                        waits.push(Box::pin(async move {
                            let _ = barrier.wait_until_packetized().await;
                        }));
                        false
                    }
                }
            })
            .collect()
    }
}

pub(in crate::runtime) fn prepared_wait_with_native_commitment(
    ordinary: PreparedOriginalWait,
    mut native: Vec<PreparedOriginalWait>,
) -> PreparedOriginalWait {
    if native.is_empty() {
        return ordinary;
    }
    native.push(ordinary);
    Box::pin(async move {
        let _ = futures::future::select_all(native).await;
    })
}

pub(in crate::runtime) fn prepared_wait_with_recovery_deadline(
    ordinary: PreparedOriginalWait,
    deadline: Option<std::time::Instant>,
) -> PreparedOriginalWait {
    let Some(deadline) = deadline else {
        return ordinary;
    };
    Box::pin(async move {
        tokio::select! {
            () = ordinary => {}
            () = tokio::time::sleep_until(deadline.into()) => {}
        }
    })
}

pub(in crate::runtime) enum PreparedOriginalClaim {
    Claimed(Frame),
    /// Failure belongs to the invoking FIFO, not the shared Product source.
    CarrierFailed(RuntimeError),
    /// One exact repair queue/copy Apply committed; no Original was claimed.
    RecoveryQueued,
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
        let previous = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                Some(if state & OUTSTANDING == 0 {
                    OUTSTANDING
                } else {
                    state | NOTIFIED_AGAIN
                })
            })
            .expect("prepared notice state update always succeeds");
        if previous & OUTSTANDING != 0 {
            self.notified.notify_waiters();
            return;
        }
        self.commands.enqueue_prepared_work(PreparedOriginalWork {
            registration: Arc::downgrade(self),
            stream_id: self.stream_id,
            instance: self.instance,
            lane: self.lane,
        });
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
}

impl PreparedOriginalWork {
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
        // A fresh claim observes all work notifications preceding this point.
        // A later notification survives in NOTIFIED_AGAIN until token release.
        registration
            .state
            .fetch_and(!NOTIFIED_AGAIN, Ordering::AcqRel);
        match &registration.source {
            PreparedOriginalSource::Request {
                product,
                context,
                instance,
            } => {
                let Some(product) = product.upgrade() else {
                    return PreparedOriginalClaim::Empty;
                };
                claim_prepared_request_data(&product, context, *instance, ready, &registration)
            }
            PreparedOriginalSource::Response { product, instance } => {
                let Some(product) = product.upgrade() else {
                    return PreparedOriginalClaim::Empty;
                };
                claim_prepared_response_data(&product, *instance, ready, &registration)
            }
        }
    }

    pub(in crate::runtime) fn requeue(self) {
        if let Some(registration) = self.registration.upgrade() {
            registration.commands.enqueue_prepared_work(self);
        }
    }

    /// An occupied writer has observed this notice without claiming source.
    /// Wait for its next physical boundary; retain no source or registration.
    pub(in crate::runtime::path) fn writer_change_wait(&self) -> Option<PreparedOriginalWait> {
        let registration = self.registration.upgrade()?;
        registration
            .state
            .fetch_and(!NOTIFIED_AGAIN, Ordering::AcqRel);
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
            let mut notified = Box::pin(registration.notified.clone().notified_owned());
            notified.as_mut().enable();
            let already_notified = registration.state.load(Ordering::Acquire) & NOTIFIED_AGAIN != 0;
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
            let previous = registration.state.swap(0, Ordering::AcqRel);
            if previous & NOTIFIED_AGAIN != 0 {
                registration.notify();
            }
        }
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    #[tokio::test]
    async fn recovery_deadline_crossed_during_handoff_still_wakes() {
        // The query returned a future assignment deadline; task descheduling
        // consumed that interval before its wait was constructed.
        let deadline = std::time::Instant::now();
        let wait =
            prepared_wait_with_recovery_deadline(Box::pin(std::future::pending()), Some(deadline));
        tokio::time::timeout(std::time::Duration::from_secs(1), wait)
            .await
            .expect("crossed immutable deadline must retry without unrelated input");
    }
}
