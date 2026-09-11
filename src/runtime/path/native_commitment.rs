//! Exact first-packetization opportunity on one established native FIFO.
//!
//! Native progress is captured outside Product ownership. Its immutable view
//! can then gate a new Original transaction without reading Native or retaining
//! Product ranges. Already claimed work keeps its ownership through the existing
//! bounded writer operation; this component never cancels an accepted write.

use quinn::{SendStreamObservationError, SendStreamObserver, SendStreamProgress};
use std::future::Future;
use std::sync::Arc;

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
                formatter.write_str("native progress exceeds its accepted end")
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
/// empty FIFO is usable only while the caller preserves that writer's current
/// Ready ownership. Opposite directions and independent FIFOs stay separate.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct NativeOperationCommitment {
    observer: Arc<NativeObserver>,
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
}

/// Wait for the accepted end from one captured FIFO snapshot, not for an ACK.
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
        })
    }

    /// Capture outside Product ownership and before the final writer Ready check.
    pub(in crate::runtime) fn capture(
        &self,
    ) -> Result<NativeCommitmentView, NativeCommitmentError> {
        let progress = checked_progress(self.observer.snapshot())?;
        Ok(NativeCommitmentView {
            observer: self.observer.clone(),
            progress,
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
    /// Its wait then completes immediately. An empty snapshot grants no reusable
    /// permission across another writer operation; Ready must still be current.
    pub(in crate::runtime) fn barrier(&self) -> Option<NativeCommitmentBarrier> {
        (self.progress.first_unpacketized < self.progress.accepted_end).then(|| {
            NativeCommitmentBarrier {
                observer: self.observer.clone(),
                native_end: self.progress.accepted_end,
            }
        })
    }
}

impl NativeCommitmentBarrier {
    #[cfg(test)]
    pub(in crate::runtime) fn native_end(&self) -> u64 {
        self.native_end
    }

    pub(in crate::runtime) fn wait_until_packetized(
        self,
    ) -> impl Future<Output = Result<SendStreamProgress, NativeCommitmentError>> + Send + 'static
    {
        async move { checked_progress(self.observer.wait_until_packetized(self.native_end).await) }
    }
}

#[cfg(test)]
#[path = "tests_native_commitment.rs"]
mod tests;
