//! Exact first-packetization opportunity on one established native FIFO.
//!
//! Native progress is captured outside Product ownership. Its immutable view
//! can then gate a new Original transaction without reading Native or retaining
//! Product ranges. Already claimed work keeps its ownership through the existing
//! bounded writer operation; this component never cancels an accepted write.

use quinn::{SendStreamObservationError, SendStreamObserver, SendStreamProgress};
use std::future::Future;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) enum NativeCommitmentError {
    Native(SendStreamObservationError),
    InvalidNativeProgress,
}

impl std::fmt::Display for NativeCommitmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Native(error) => write!(formatter, "native stream progress: {error}"),
            Self::InvalidNativeProgress => {
                formatter.write_str("native stream progress or operation boundary is inconsistent")
            }
        }
    }
}

impl std::error::Error for NativeCommitmentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            Self::InvalidNativeProgress => None,
        }
    }
}

impl From<SendStreamObservationError> for NativeCommitmentError {
    fn from(error: SendStreamObservationError) -> Self {
        Self::Native(error)
    }
}

/// One established observer for an exclusive native writer.
///
/// Clones share observation, not permission for concurrent writes. A captured
/// opportunity is usable only while the caller preserves that writer's current
/// Ready ownership. Opposite directions and independent FIFOs stay separate.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct NativeOperationCommitment {
    observer: Arc<NativeObserver>,
    // One small, coherently published value shared by the writer and observers.
    // Never hold this mutex across a Native read, Product ownership, or an await.
    latest_operation: Arc<Mutex<Option<NativeOperationSpan>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NativeOperationSpan {
    begin: u64,
    completed: SendStreamProgress,
}

/// Captured before one real writer flush; creating or dropping it publishes no
/// opportunity. The exclusive writer completes it only after successful I/O.
#[derive(Debug)]
pub(in crate::runtime) struct NativeOperationStart {
    owner: Arc<Mutex<Option<NativeOperationSpan>>>,
    previous_operation: Option<NativeOperationSpan>,
    progress: SendStreamProgress,
}

#[derive(Debug)]
enum NativeObserver {
    Quic(SendStreamObserver),
    #[cfg(test)]
    Controlled(Arc<tests::ControlledObserver>),
}

impl NativeObserver {
    fn snapshot(&self) -> Result<SendStreamProgress, SendStreamObservationError> {
        match self {
            Self::Quic(observer) => observer.snapshot(),
            #[cfg(test)]
            Self::Controlled(observer) => observer.snapshot(),
        }
    }

    async fn wait_until_terminated(&self) -> SendStreamObservationError {
        match self {
            Self::Quic(observer) => observer.wait_until_terminated().await,
            #[cfg(test)]
            Self::Controlled(observer) => observer.wait_until_terminated().await,
        }
    }

    async fn wait_until_packetized(
        &self,
        end: u64,
    ) -> Result<SendStreamProgress, SendStreamObservationError> {
        match self {
            Self::Quic(observer) => observer.wait_until_packetized(end).await,
            #[cfg(test)]
            Self::Controlled(observer) => observer.wait_until_packetized(end).await,
        }
    }
}

/// One coherent native snapshot; evaluating it performs no further Native read.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct NativeCommitmentView {
    observer: Arc<NativeObserver>,
    progress: SendStreamProgress,
    target: u64,
}

/// Wait for a known operation's start, or the accepted end when its boundary is
/// unknown. Both targets describe first packetization, not an ACK.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct NativeCommitmentBarrier {
    observer: Arc<NativeObserver>,
    native_end: u64,
}

fn checked_progress(
    result: Result<SendStreamProgress, SendStreamObservationError>,
) -> Result<SendStreamProgress, NativeCommitmentError> {
    match result {
        Ok(progress) if progress.first_unpacketized <= progress.accepted_end => Ok(progress),
        Ok(_) => Err(NativeCommitmentError::InvalidNativeProgress),
        Err(error) => Err(error.into()),
    }
}

impl NativeOperationCommitment {
    pub(in crate::runtime) fn new(
        observer: SendStreamObserver,
    ) -> Result<Self, NativeCommitmentError> {
        Self::from_observer(NativeObserver::Quic(observer))
    }

    fn from_observer(observer: NativeObserver) -> Result<Self, NativeCommitmentError> {
        checked_progress(observer.snapshot())?;
        Ok(Self {
            observer: Arc::new(observer),
            latest_operation: Arc::new(Mutex::new(None)),
        })
    }

    /// Copy metadata before the Native read, releasing its mutex first. A later
    /// operation may make this copy stale, but then a larger accepted end falls
    /// back to draining that end. A published boundary can never be newer than
    /// this Native snapshot, so a backward cursor is invalid rather than a race.
    fn observe_operation(
        &self,
    ) -> Result<(Option<NativeOperationSpan>, SendStreamProgress), NativeCommitmentError> {
        let operation = *self
            .latest_operation
            .lock()
            .expect("native operation boundary");
        let progress = checked_progress(self.observer.snapshot())?;
        if operation.is_some_and(|operation| {
            progress.accepted_end < operation.completed.accepted_end
                || progress.first_unpacketized < operation.completed.first_unpacketized
        }) {
            return Err(NativeCommitmentError::InvalidNativeProgress);
        }
        Ok((operation, progress))
    }

    pub(in crate::runtime) fn begin_operation(
        &self,
    ) -> Result<NativeOperationStart, NativeCommitmentError> {
        let (previous_operation, progress) = self.observe_operation()?;
        Ok(NativeOperationStart {
            owner: self.latest_operation.clone(),
            previous_operation,
            progress,
        })
    }

    /// Publish exactly one complete writer transaction, never an advisory claim
    /// or a partial write. Failure leaves the previous boundary unchanged; the
    /// enclosing writer remains responsible for retiring failed or cancelled I/O.
    pub(in crate::runtime) fn complete_operation(
        &self,
        start: NativeOperationStart,
    ) -> Result<(), NativeCommitmentError> {
        if !Arc::ptr_eq(&self.latest_operation, &start.owner) {
            return Err(NativeCommitmentError::InvalidNativeProgress);
        }
        let progress = checked_progress(self.observer.snapshot())?;
        if progress.accepted_end < start.progress.accepted_end
            || progress.first_unpacketized < start.progress.first_unpacketized
        {
            return Err(NativeCommitmentError::InvalidNativeProgress);
        }
        let mut operation = self
            .latest_operation
            .lock()
            .expect("native operation boundary");
        if *operation != start.previous_operation {
            return Err(NativeCommitmentError::InvalidNativeProgress);
        }
        if progress.accepted_end > start.progress.accepted_end {
            *operation = Some(NativeOperationSpan {
                begin: start.progress.accepted_end,
                completed: progress,
            });
        }
        Ok(())
    }

    /// Capture outside Product ownership and before the final writer Ready check.
    pub(in crate::runtime) fn capture(
        &self,
    ) -> Result<NativeCommitmentView, NativeCommitmentError> {
        let (operation, progress) = self.observe_operation()?;
        let target = match operation {
            Some(operation) if operation.completed.accepted_end == progress.accepted_end => {
                // Only positive completed extents are published, so begin + 1
                // is representable and already accepted on this exact FIFO.
                operation.begin + 1
            }
            _ => progress.accepted_end,
        };
        Ok(NativeCommitmentView {
            observer: self.observer.clone(),
            progress,
            target,
        })
    }
}

impl NativeCommitmentView {
    /// Retain a terminal wake even when no packetization target is outstanding.
    /// Creating it performs no Native read; polling belongs outside Product.
    pub(in crate::runtime) fn wait_until_terminated(
        &self,
    ) -> impl Future<Output = NativeCommitmentError> + Send + 'static {
        let observer = self.observer.clone();
        async move { observer.wait_until_terminated().await.into() }
    }

    /// A busy snapshot stays conservative if packetization advances meanwhile.
    /// Its wait then completes immediately. An eligible snapshot grants no
    /// reusable permission across another operation; Ready must still be current.
    pub(in crate::runtime) fn barrier(&self) -> Option<NativeCommitmentBarrier> {
        (self.progress.first_unpacketized < self.target).then(|| NativeCommitmentBarrier {
            observer: self.observer.clone(),
            native_end: self.target,
        })
    }
}

impl NativeCommitmentBarrier {
    #[cfg(test)]
    pub(in crate::runtime) fn native_end(&self) -> u64 {
        self.native_end
    }

    pub(in crate::runtime) async fn wait_until_packetized(
        self,
    ) -> Result<SendStreamProgress, NativeCommitmentError> {
        checked_progress(self.observer.wait_until_packetized(self.native_end).await)
    }
}

#[cfg(test)]
#[path = "tests_native_commitment.rs"]
mod tests;
