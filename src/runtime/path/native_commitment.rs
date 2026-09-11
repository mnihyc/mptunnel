//! Product settlement associated with accepted operations on one native FIFO.
//!
//! Native progress is captured before taking the ledger lock and outside Product
//! ownership. A captured view can then compare the current Product ACK frontier
//! under its owner without reading Native. This component installs no admission
//! policy and never cancels an accepted native write.

use crate::protocol::{Frame, StreamId};
use quinn::{SendStreamObservationError, SendStreamObserver, SendStreamProgress};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) enum NativeCommitmentError {
    Native(SendStreamObservationError),
    ProductStreamMismatch {
        expected: StreamId,
        actual: StreamId,
    },
    ProductExtentOverflow,
    ForeignOperation,
    NativeOperationDidNotAdvance {
        previous_end: u64,
        accepted_end: u64,
    },
    InvalidNativeProgress,
    ProductFrontierRegressed {
        previous: u64,
        current: u64,
    },
    LedgerPoisoned,
}

impl std::fmt::Display for NativeCommitmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Native(error) => write!(formatter, "native stream progress: {error}"),
            Self::ProductStreamMismatch { expected, actual } => write!(
                formatter,
                "Product stream mismatch: expected {expected:?}, received {actual:?}"
            ),
            Self::ProductExtentOverflow => {
                formatter.write_str("Product data extent is not representable")
            }
            Self::ForeignOperation => {
                formatter.write_str("accepted operation belongs to another native commitment owner")
            }
            Self::NativeOperationDidNotAdvance {
                previous_end,
                accepted_end,
            } => write!(
                formatter,
                "native operation end did not advance from {previous_end} to {accepted_end}"
            ),
            Self::InvalidNativeProgress => {
                formatter.write_str("native progress exceeds its accepted end")
            }
            Self::ProductFrontierRegressed { previous, current } => write!(
                formatter,
                "Product ACK frontier regressed from {previous} to {current}"
            ),
            Self::LedgerPoisoned => formatter.write_str("native commitment ledger lock poisoned"),
        }
    }
}

impl std::error::Error for NativeCommitmentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SendStreamObservationError> for NativeCommitmentError {
    fn from(error: SendStreamObservationError) -> Self {
        Self::Native(error)
    }
}

/// One established observer and one Product send direction's exclusive writer.
///
/// The caller must record each successful operation before publishing the next
/// Ready opportunity. Clones share evidence, not permission for concurrent native
/// writes. Opposite directions and independent native streams use separate owners.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct NativeOperationCommitment {
    shared: Arc<SharedCommitment>,
}

#[derive(Debug)]
struct SharedCommitment {
    observer: NativeObserver,
    stream_id: StreamId,
    ledger: Mutex<CommitmentLedger>,
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

/// Payload-free summary prepared before a potentially cancelling H3 write.
///
/// Dropping this token records nothing. Only successful complete operations may
/// consume it; the exclusive writer supplies the actual native acceptance end.
#[derive(Debug)]
pub(in crate::runtime) struct NativeOperationData {
    shared: Arc<SharedCommitment>,
    product_end: Option<u64>,
    #[cfg(feature = "lab-diagnostics")]
    diagnostic_spans: Option<Vec<(u64, u64)>>,
}

/// A monotone native lower bound plus the shared, uncloned operation ledger.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct NativeCommitmentView {
    shared: Arc<SharedCommitment>,
    progress: SendStreamProgress,
}

/// Owned wait for an enclosing operation, not an exact redundant payload offset.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct NativeCommitmentBarrier {
    shared: Arc<SharedCommitment>,
    native_end: u64,
}

#[derive(Debug, Clone, Copy)]
struct UnsettledOperation {
    native_end: u64,
    product_end: u64,
}

#[derive(Debug)]
struct CommitmentLedger {
    // Native ends are strictly increasing. Once pruned, each entry accounts for
    // at least one remaining native byte; those bytes consume native send memory.
    unsettled: VecDeque<UnsettledOperation>,
    minimum_product_end: Option<u64>,
    settled_native_end: Option<u64>,
    first_unpacketized: u64,
    last_operation_end: u64,
    product_frontier: u64,
    terminal: Option<SendStreamObservationError>,
}

impl CommitmentLedger {
    fn new(progress: SendStreamProgress) -> Self {
        Self {
            unsettled: VecDeque::new(),
            minimum_product_end: None,
            settled_native_end: None,
            first_unpacketized: progress.first_unpacketized,
            last_operation_end: progress.accepted_end,
            product_frontier: 0,
            terminal: None,
        }
    }

    fn prune(&mut self, first_unpacketized: u64) {
        // A previously captured view can arrive after a newer capture. Native
        // progress cannot rewind within the established observer's lifetime.
        self.first_unpacketized = self.first_unpacketized.max(first_unpacketized);
        let mut removed_minimum = false;
        while self
            .unsettled
            .front()
            .is_some_and(|operation| operation.native_end <= self.first_unpacketized)
        {
            let operation = self.unsettled.pop_front().expect("observed front");
            removed_minimum |= self.minimum_product_end == Some(operation.product_end);
        }
        if removed_minimum {
            self.minimum_product_end = self.unsettled.iter().map(|x| x.product_end).min();
        }
        if self
            .settled_native_end
            .is_some_and(|end| end <= self.first_unpacketized)
        {
            self.settled_native_end = None;
        }
        if self.unsettled.is_empty() {
            // Idle/fully pruned owners retain no allocation from an earlier burst.
            self.unsettled = VecDeque::new();
            self.minimum_product_end = None;
        }
    }

    fn clear_native_debt(&mut self) {
        self.unsettled = VecDeque::new();
        self.minimum_product_end = None;
        self.settled_native_end = None;
    }

    fn record(
        &mut self,
        progress: SendStreamProgress,
        product_end: Option<u64>,
    ) -> Result<(), NativeCommitmentError> {
        if let Some(error) = &self.terminal {
            return Err(NativeCommitmentError::Native(error.clone()));
        }
        if progress.accepted_end < self.last_operation_end
            || (product_end.is_some() && progress.accepted_end == self.last_operation_end)
        {
            return Err(NativeCommitmentError::NativeOperationDidNotAdvance {
                previous_end: self.last_operation_end,
                accepted_end: progress.accepted_end,
            });
        }
        self.prune(progress.first_unpacketized);
        self.last_operation_end = progress.accepted_end;
        let Some(product_end) = product_end else {
            return Ok(());
        };
        if progress.accepted_end <= self.first_unpacketized {
            return Ok(());
        }
        if product_end <= self.product_frontier {
            self.settled_native_end = Some(progress.accepted_end);
        } else {
            self.unsettled.push_back(UnsettledOperation {
                native_end: progress.accepted_end,
                product_end,
            });
            self.minimum_product_end = Some(
                self.minimum_product_end
                    .map_or(product_end, |end| end.min(product_end)),
            );
        }
        Ok(())
    }

    fn barrier(&mut self, frontier: u64) -> Result<Option<u64>, NativeCommitmentError> {
        if let Some(error) = &self.terminal {
            return Err(NativeCommitmentError::Native(error.clone()));
        }
        if frontier < self.product_frontier {
            return Err(NativeCommitmentError::ProductFrontierRegressed {
                previous: self.product_frontier,
                current: frontier,
            });
        }
        self.product_frontier = frontier;
        // Positive bookkeeping proves that no unsettled operation can qualify.
        // The normal unacknowledged path therefore needs no full-ledger scan.
        if self.minimum_product_end.is_some_and(|end| end <= frontier) {
            let mut settled_end = self.settled_native_end;
            let mut minimum = None::<u64>;
            self.unsettled.retain(|operation| {
                if operation.product_end <= frontier {
                    settled_end = Some(
                        settled_end
                            .map_or(operation.native_end, |end| end.max(operation.native_end)),
                    );
                    false
                } else {
                    minimum = Some(
                        minimum.map_or(operation.product_end, |end| end.min(operation.product_end)),
                    );
                    true
                }
            });
            self.minimum_product_end = minimum;
            self.settled_native_end = settled_end;
            if self.unsettled.is_empty() {
                self.unsettled = VecDeque::new();
            }
        }
        Ok(self.settled_native_end)
    }
}

impl SharedCommitment {
    fn ledger(&self) -> Result<MutexGuard<'_, CommitmentLedger>, NativeCommitmentError> {
        self.ledger
            .lock()
            .map_err(|_| NativeCommitmentError::LedgerPoisoned)
    }

    fn checked_progress(
        &self,
        result: Result<SendStreamProgress, SendStreamObservationError>,
    ) -> Result<SendStreamProgress, NativeCommitmentError> {
        match result {
            Ok(progress) if progress.first_unpacketized <= progress.accepted_end => Ok(progress),
            Ok(_) => Err(NativeCommitmentError::InvalidNativeProgress),
            Err(error) => Err(self.observation_error(error)),
        }
    }

    fn observation_error(&self, error: SendStreamObservationError) -> NativeCommitmentError {
        if matches!(
            error,
            SendStreamObservationError::ClosedStream
                | SendStreamObservationError::Stopped(_)
                | SendStreamObservationError::ConnectionLost(_)
        ) {
            let mut ledger = match self.ledger() {
                Ok(ledger) => ledger,
                Err(error) => return error,
            };
            ledger.clear_native_debt();
            ledger.terminal = Some(error.clone());
        }
        NativeCommitmentError::Native(error)
    }

    fn wait_until_terminated(
        self: &Arc<Self>,
    ) -> impl Future<Output = NativeCommitmentError> + Send + 'static {
        let shared = self.clone();
        async move {
            let error = shared.observer.wait_until_terminated().await;
            shared.observation_error(error)
        }
    }
}

impl NativeOperationCommitment {
    pub(in crate::runtime) fn new(
        observer: SendStreamObserver,
        stream_id: StreamId,
    ) -> Result<Self, NativeCommitmentError> {
        Self::from_observer(NativeObserver::Quic(observer), stream_id)
    }

    fn from_observer(
        observer: NativeObserver,
        stream_id: StreamId,
    ) -> Result<Self, NativeCommitmentError> {
        let progress = observer.snapshot()?;
        if progress.first_unpacketized > progress.accepted_end {
            return Err(NativeCommitmentError::InvalidNativeProgress);
        }
        Ok(Self {
            shared: Arc::new(SharedCommitment {
                observer,
                stream_id,
                ledger: Mutex::new(CommitmentLedger::new(progress)),
            }),
        })
    }

    /// Validate and summarize before writing, without querying Native or keeping payload.
    pub(in crate::runtime) fn prepare_operation(
        &self,
        frames: &[Frame],
    ) -> Result<NativeOperationData, NativeCommitmentError> {
        let mut product_end = None::<u64>;
        #[cfg(feature = "lab-diagnostics")]
        let mut diagnostic_spans =
            crate::lab_diagnostics::lab_diagnostic_event_enabled("native_product_operation")
                .then(Vec::new);
        for frame in frames {
            let Frame::StreamData {
                stream_id,
                offset,
                payload,
            } = frame
            else {
                continue;
            };
            if *stream_id != self.shared.stream_id {
                return Err(NativeCommitmentError::ProductStreamMismatch {
                    expected: self.shared.stream_id,
                    actual: *stream_id,
                });
            }
            let bytes = u64::try_from(payload.len())
                .map_err(|_| NativeCommitmentError::ProductExtentOverflow)?;
            let end = offset
                .checked_add(bytes)
                .ok_or(NativeCommitmentError::ProductExtentOverflow)?;
            if bytes != 0 {
                product_end = Some(product_end.map_or(end, |current| current.max(end)));
                #[cfg(feature = "lab-diagnostics")]
                if let Some(spans) = &mut diagnostic_spans {
                    spans.push((*offset, end));
                }
            }
        }
        Ok(NativeOperationData {
            shared: self.shared.clone(),
            product_end,
            #[cfg(feature = "lab-diagnostics")]
            diagnostic_spans,
        })
    }

    /// Call only after the complete exclusive write succeeds, before its next Ready claim.
    /// Native is read before the ledger lock, including for control-only operations.
    pub(in crate::runtime) fn record_accepted_operation(
        &self,
        operation: NativeOperationData,
    ) -> Result<(), NativeCommitmentError> {
        if !Arc::ptr_eq(&self.shared, &operation.shared) {
            return Err(NativeCommitmentError::ForeignOperation);
        }
        let progress = self
            .shared
            .checked_progress(self.shared.observer.snapshot())?;
        let mut ledger = self.shared.ledger()?;
        #[cfg(feature = "lab-diagnostics")]
        let previous_native_end = ledger.last_operation_end;
        ledger.record(progress, operation.product_end)?;
        drop(ledger);
        #[cfg(feature = "lab-diagnostics")]
        if let Some(spans) = operation.diagnostic_spans {
            let spans = if spans.is_empty() {
                "none".to_string()
            } else {
                spans
                    .iter()
                    .map(|(start, end)| format!("{start}:{end}"))
                    .collect::<Vec<_>>()
                    .join(",")
            };
            crate::lab_diagnostics::lab_diagnostic(
                "native_product_operation",
                format_args!(
                    "stream_id={} fifo_identity={:p} native_previous_end={} accepted_end={} first_unpacketized={} product_spans={}",
                    self.shared.stream_id.0,
                    Arc::as_ptr(&self.shared),
                    previous_native_end,
                    progress.accepted_end,
                    progress.first_unpacketized,
                    spans,
                ),
            );
        }
        Ok(())
    }

    /// Capture outside Product ownership; the resulting view performs no Native reads.
    pub(in crate::runtime) fn capture(
        &self,
    ) -> Result<NativeCommitmentView, NativeCommitmentError> {
        let progress = self
            .shared
            .checked_progress(self.shared.observer.snapshot())?;
        self.shared.ledger()?.prune(progress.first_unpacketized);
        Ok(NativeCommitmentView {
            shared: self.shared.clone(),
            progress,
        })
    }
}

impl NativeCommitmentView {
    /// Retain a terminal wake while an otherwise idle prepared writer is blocked.
    /// Creating it performs no Native read; polling it must occur outside Product.
    pub(in crate::runtime) fn wait_until_terminated(
        &self,
    ) -> impl Future<Output = NativeCommitmentError> + Send + 'static {
        self.shared.wait_until_terminated()
    }

    /// Compare the final current contiguous ACK frontier under Product ownership.
    pub(in crate::runtime) fn barrier(
        &self,
        current_frontier: u64,
    ) -> Result<Option<NativeCommitmentBarrier>, NativeCommitmentError> {
        let mut ledger = self.shared.ledger()?;
        ledger.prune(self.progress.first_unpacketized);
        let native_end = ledger.barrier(current_frontier)?;
        Ok(native_end.map(|native_end| NativeCommitmentBarrier {
            shared: self.shared.clone(),
            native_end,
        }))
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
        async move {
            let result = self
                .shared
                .observer
                .wait_until_packetized(self.native_end)
                .await;
            let progress = self.shared.checked_progress(result)?;
            self.shared.ledger()?.prune(progress.first_unpacketized);
            Ok(progress)
        }
    }
}

#[cfg(test)]
#[path = "tests_native_commitment.rs"]
mod tests;
