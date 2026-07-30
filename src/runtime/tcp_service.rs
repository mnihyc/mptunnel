//! Validation-only writer ordering and exact flight provenance.
//!
//! These adapters exist only while one RFC TCP service validation is active.
//! Permanent request and response flight records stay unchanged.

use crate::model::tcp_service::{
    TcpServiceAckRelease, TcpServiceBoundary, TcpServiceCarrierFence, TcpServiceCarrierGroupId,
    TcpServiceDataAckEvent, TcpServiceStreamFence, TcpServiceWithdrawalReason,
    TcpServiceWriterLifecycle, TcpServiceWriterPoint,
};
use crate::protocol::OffsetRange;
use crate::runtime::stream::request::RequestTcpServiceFrozenStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum TcpServiceWriterClockError {
    Exhausted,
}

#[derive(Debug)]
pub(in crate::runtime) struct TcpServicePreparedAck {
    pub(in crate::runtime) lifecycle: TcpServiceWriterLifecycle,
    pub(in crate::runtime) stream: TcpServiceStreamFence,
    pub(in crate::runtime) assigned_end: u64,
    pub(in crate::runtime) releases: Vec<TcpServiceAckRelease>,
}

/// Controller request whose carrier identities are still untrusted input.
///
/// The request grants no placement or observer authority. The serialized
/// stream actor resolves it against its current authenticated carrier group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct RequestTcpServiceSnapshotRequest {
    pub(in crate::runtime) carrier_group_id: TcpServiceCarrierGroupId,
    pub(in crate::runtime) candidate: TcpServiceCarrierFence,
    pub(in crate::runtime) max_accepted_paths: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RequestTcpServiceControlOutcome<T> {
    Complete(T),
    Withdrawn(TcpServiceWithdrawalReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RequestTcpServiceObserverInstallation {
    Installed,
    AlreadyInstalled,
}

#[derive(Debug)]
pub(in crate::runtime) struct RequestTcpServiceObserverInstall {
    pub(in crate::runtime) frozen: RequestTcpServiceFrozenStream,
    pub(in crate::runtime) coordinator: Arc<TcpServiceWriterCoordinator>,
    pub(in crate::runtime) max_flight_records: usize,
    pub(in crate::runtime) max_ack_release_records: usize,
}

#[derive(Debug)]
pub(in crate::runtime) enum RequestTcpServiceControl {
    Snapshot {
        request: RequestTcpServiceSnapshotRequest,
        receipt: oneshot::Sender<RequestTcpServiceControlOutcome<RequestTcpServiceFrozenStream>>,
    },
    Install {
        install: RequestTcpServiceObserverInstall,
        receipt:
            oneshot::Sender<RequestTcpServiceControlOutcome<RequestTcpServiceObserverInstallation>>,
    },
    Remove {
        lifecycle: TcpServiceWriterLifecycle,
        receipt: oneshot::Sender<RequestTcpServiceControlOutcome<TcpServiceObserverRemoval>>,
    },
}

/// Model owner called under the active lifecycle's writer transaction lock.
pub(in crate::runtime) trait TcpServiceDataAckSink:
    std::fmt::Debug + Send + Sync
{
    fn apply_data_ack(
        &self,
        event: TcpServiceDataAckEvent,
        now: Instant,
    ) -> Result<TcpServiceAckDisposition, TcpServiceFlightSidecarError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum TcpServiceAckDisposition {
    Continue,
    Stop,
}

/// One strict total-order clock shared by every frozen writer in a validation.
///
/// The atomic coordinate advances past equal or coarse platform-clock readings.
/// It is allocated only for an active validation and is absent from the normal
/// sender path.
#[derive(Debug)]
pub(in crate::runtime) struct TcpServiceWriterClock {
    lifecycle: TcpServiceWriterLifecycle,
    origin: Instant,
    last_coordinate_ns: AtomicU64,
}

impl TcpServiceWriterClock {
    pub(in crate::runtime) fn new(lifecycle: TcpServiceWriterLifecycle) -> Self {
        Self {
            lifecycle,
            origin: Instant::now(),
            last_coordinate_ns: AtomicU64::new(0),
        }
    }

    pub(in crate::runtime) fn mark(
        &self,
    ) -> Result<TcpServiceWriterPoint, TcpServiceWriterClockError> {
        self.mark_observed_at(Instant::now())
    }

    pub(in crate::runtime) fn lifecycle(&self) -> TcpServiceWriterLifecycle {
        self.lifecycle
    }

    fn mark_observed_at(
        &self,
        observed_at: Instant,
    ) -> Result<TcpServiceWriterPoint, TcpServiceWriterClockError> {
        let elapsed_ns = observed_at
            .saturating_duration_since(self.origin)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        let mut current = self.last_coordinate_ns.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(1)
                .map(|strict| strict.max(elapsed_ns))
                .ok_or(TcpServiceWriterClockError::Exhausted)?;
            match self.last_coordinate_ns.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let at = self
                        .origin
                        .checked_add(Duration::from_nanos(next))
                        .ok_or(TcpServiceWriterClockError::Exhausted)?;
                    return Ok(self.lifecycle.point(at));
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Model event time cannot precede a logical point advanced past a coarse
    /// platform-clock tie.
    pub(in crate::runtime) fn now_not_before(point: TcpServiceWriterPoint) -> Instant {
        Instant::now().max(point.at())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum TcpServiceFlightSidecarError {
    ResourceLimit,
    InvalidRelease,
    ObserverStopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum TcpServiceObserverRemoval {
    Removed,
    AlreadyAbsent,
    DifferentLifecycle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpServiceWriterCoordinatorPhase {
    Installing,
    Active,
    Stopped,
}

#[derive(Debug)]
struct TcpServiceWriterCoordinatorState {
    phase: TcpServiceWriterCoordinatorPhase,
    next_ack_sequence: u64,
    failure: Option<TcpServiceFlightSidecarError>,
}

/// Active-only aggregate writer transaction owner.
///
/// Every frozen writer commit and every complete Product ACK transaction takes
/// this same lock. This is stronger than merely assigning total-order labels:
/// no later stream commit can enter between ACK application and its boundary.
#[derive(Debug)]
pub(in crate::runtime) struct TcpServiceWriterCoordinator {
    clock: TcpServiceWriterClock,
    ack_sink: Arc<dyn TcpServiceDataAckSink>,
    state: Mutex<TcpServiceWriterCoordinatorState>,
}

impl TcpServiceWriterCoordinator {
    pub(in crate::runtime) fn new(
        lifecycle: TcpServiceWriterLifecycle,
        ack_sink: Arc<dyn TcpServiceDataAckSink>,
    ) -> Self {
        Self {
            clock: TcpServiceWriterClock::new(lifecycle),
            ack_sink,
            state: Mutex::new(TcpServiceWriterCoordinatorState {
                phase: TcpServiceWriterCoordinatorPhase::Installing,
                next_ack_sequence: 1,
                failure: None,
            }),
        }
    }

    pub(in crate::runtime) fn lifecycle(&self) -> TcpServiceWriterLifecycle {
        self.clock.lifecycle()
    }

    pub(in crate::runtime) fn lock(&self) -> TcpServiceWriterTransaction<'_> {
        TcpServiceWriterTransaction {
            coordinator: self,
            state: self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        }
    }

    /// Cold controller-side failure observation. Product writers never call
    /// this method on their ordinary path.
    pub(in crate::runtime) fn failure(&self) -> Option<TcpServiceFlightSidecarError> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
    }
}

pub(in crate::runtime) struct TcpServiceWriterTransaction<'a> {
    coordinator: &'a TcpServiceWriterCoordinator,
    state: MutexGuard<'a, TcpServiceWriterCoordinatorState>,
}

impl std::fmt::Debug for TcpServiceWriterTransaction<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TcpServiceWriterTransaction")
            .field("lifecycle", &self.coordinator.lifecycle())
            .field("phase", &self.state.phase)
            .finish_non_exhaustive()
    }
}

impl TcpServiceWriterTransaction<'_> {
    pub(in crate::runtime) fn lifecycle(&self) -> TcpServiceWriterLifecycle {
        self.coordinator.lifecycle()
    }

    pub(in crate::runtime) fn mark_commit(
        &mut self,
    ) -> Result<TcpServiceWriterPoint, TcpServiceFlightSidecarError> {
        if self.state.phase == TcpServiceWriterCoordinatorPhase::Stopped
            || self.state.failure.is_some()
        {
            return Err(TcpServiceFlightSidecarError::ObserverStopped);
        }
        self.coordinator
            .clock
            .mark()
            .map_err(|_| TcpServiceFlightSidecarError::ResourceLimit)
    }

    /// Captures the initial ACK/writer boundary while installation remains
    /// serialized against every frozen writer.
    pub(in crate::runtime) fn initial_boundary(
        &mut self,
    ) -> Result<TcpServiceBoundary, TcpServiceFlightSidecarError> {
        if self.state.phase != TcpServiceWriterCoordinatorPhase::Installing
            || self.state.failure.is_some()
        {
            return Err(TcpServiceFlightSidecarError::ObserverStopped);
        }
        let sequence = self.next_ack_sequence()?;
        let writer = self.mark_commit()?;
        Ok(TcpServiceBoundary {
            ack_sequence: sequence,
            acked_at: writer.at(),
            writer,
        })
    }

    /// Enables model ACK delivery only after the model accepted the initial
    /// boundary under this same transaction guard.
    pub(in crate::runtime) fn activate(&mut self) -> bool {
        if self.state.phase != TcpServiceWriterCoordinatorPhase::Installing
            || self.state.failure.is_some()
        {
            return false;
        }
        self.state.phase = TcpServiceWriterCoordinatorPhase::Active;
        true
    }

    pub(in crate::runtime) fn commit_ack(
        &mut self,
        ack: TcpServicePreparedAck,
    ) -> Result<(), TcpServiceFlightSidecarError> {
        if ack.lifecycle != self.lifecycle() {
            return Err(TcpServiceFlightSidecarError::InvalidRelease);
        }
        match self.state.phase {
            TcpServiceWriterCoordinatorPhase::Installing => {
                // Accepted work released during observer installation is
                // pre-boundary by definition and carries no model evidence.
                return Ok(());
            }
            TcpServiceWriterCoordinatorPhase::Stopped => {
                return Err(TcpServiceFlightSidecarError::ObserverStopped);
            }
            TcpServiceWriterCoordinatorPhase::Active => {}
        }
        if let Some(error) = self.state.failure {
            return Err(error);
        }
        let sequence = self.next_ack_sequence()?;
        let next_writer_boundary = self.mark_commit()?;
        let now = TcpServiceWriterClock::now_not_before(next_writer_boundary);
        let event = TcpServiceDataAckEvent {
            sequence,
            stream: ack.stream,
            assigned_end: ack.assigned_end,
            acked_at: next_writer_boundary.at(),
            next_writer_boundary,
            releases: ack.releases,
        };
        match self.coordinator.ack_sink.apply_data_ack(event, now) {
            Ok(TcpServiceAckDisposition::Continue) => {}
            Ok(TcpServiceAckDisposition::Stop) => {
                // Settlement stops later observed commits at this exact ACK
                // boundary. Actor cleanup remains an ordered cold operation.
                self.state.phase = TcpServiceWriterCoordinatorPhase::Stopped;
            }
            Err(error) => {
                self.state.failure.get_or_insert(error);
                self.state.phase = TcpServiceWriterCoordinatorPhase::Stopped;
                return Err(error);
            }
        }
        Ok(())
    }

    pub(in crate::runtime) fn stop(&mut self) {
        self.state.phase = TcpServiceWriterCoordinatorPhase::Stopped;
    }

    pub(in crate::runtime) fn fail(&mut self, error: TcpServiceFlightSidecarError) {
        self.state.failure.get_or_insert(error);
        self.state.phase = TcpServiceWriterCoordinatorPhase::Stopped;
    }

    fn next_ack_sequence(&mut self) -> Result<u64, TcpServiceFlightSidecarError> {
        let sequence = self.state.next_ack_sequence;
        let Some(next) = sequence.checked_add(1) else {
            self.state.failure = Some(TcpServiceFlightSidecarError::ResourceLimit);
            self.state.phase = TcpServiceWriterCoordinatorPhase::Stopped;
            return Err(TcpServiceFlightSidecarError::ResourceLimit);
        };
        self.state.next_ack_sequence = next;
        Ok(sequence)
    }
}

#[derive(Debug, Clone, Copy)]
struct TcpServiceObservedFlight<I> {
    identity: I,
    range: OffsetRange,
    committed_at: TcpServiceWriterPoint,
}

/// One frozen stream's passive observer. Stopping prevents later commits while
/// retaining exact sidecar provenance until ACK processing or final cleanup.
#[derive(Debug)]
pub(in crate::runtime) struct TcpServiceWriterObserver<I> {
    lifecycle: TcpServiceWriterLifecycle,
    flights: TcpServiceFlightSidecar<I>,
    observing: bool,
}

impl<I: Copy + Eq> TcpServiceWriterObserver<I> {
    pub(in crate::runtime) fn new(
        lifecycle: TcpServiceWriterLifecycle,
        max_flight_records: usize,
    ) -> Result<Self, TcpServiceFlightSidecarError> {
        Ok(Self {
            lifecycle,
            flights: TcpServiceFlightSidecar::new(max_flight_records)?,
            observing: true,
        })
    }

    pub(in crate::runtime) fn lifecycle(&self) -> TcpServiceWriterLifecycle {
        self.lifecycle
    }

    pub(in crate::runtime) fn record_at(
        &mut self,
        identity: I,
        range: OffsetRange,
        committed_at: TcpServiceWriterPoint,
    ) -> Result<(), TcpServiceFlightSidecarError> {
        if !self.observing || committed_at.lifecycle() != self.lifecycle {
            return Err(TcpServiceFlightSidecarError::ObserverStopped);
        }
        self.flights.record(identity, range, committed_at)
    }

    /// Mints and records one exact writer point as a single lifecycle
    /// operation. Candidate accounting must consume the returned point rather
    /// than advance the strict clock a second time.
    pub(in crate::runtime) fn record_commit(
        &mut self,
        identity: I,
        range: OffsetRange,
        transaction: &mut TcpServiceWriterTransaction<'_>,
    ) -> Result<TcpServiceWriterPoint, TcpServiceFlightSidecarError> {
        if transaction.lifecycle() != self.lifecycle {
            return Err(TcpServiceFlightSidecarError::InvalidRelease);
        }
        let committed_at = transaction.mark_commit()?;
        self.record_at(identity, range, committed_at)?;
        Ok(committed_at)
    }

    pub(in crate::runtime) fn release(
        &mut self,
        identity: I,
        range: OffsetRange,
    ) -> Result<Option<TcpServiceWriterPoint>, TcpServiceFlightSidecarError> {
        self.flights.release(identity, range)
    }

    pub(in crate::runtime) fn stop(&mut self, lifecycle: TcpServiceWriterLifecycle) -> bool {
        if self.lifecycle() != lifecycle {
            return false;
        }
        self.observing = false;
        true
    }

    pub(in crate::runtime) fn is_observing(&self) -> bool {
        self.observing
    }

    pub(in crate::runtime) fn is_drained(&self) -> bool {
        self.flights.is_empty()
    }
}

/// Bounded validation-only provenance for exact runtime flight copies.
///
/// `I` includes the physical carrier/attachment and transmission kind. Missing
/// provenance is valid for accepted work committed before observer install.
#[derive(Debug)]
pub(in crate::runtime) struct TcpServiceFlightSidecar<I> {
    max_records: usize,
    records: Vec<TcpServiceObservedFlight<I>>,
}

impl<I: Copy + Eq> TcpServiceFlightSidecar<I> {
    pub(in crate::runtime) fn new(
        max_records: usize,
    ) -> Result<Self, TcpServiceFlightSidecarError> {
        if max_records == 0 {
            return Err(TcpServiceFlightSidecarError::ResourceLimit);
        }
        Ok(Self {
            max_records,
            records: Vec::new(),
        })
    }

    pub(in crate::runtime) fn record(
        &mut self,
        identity: I,
        range: OffsetRange,
        committed_at: TcpServiceWriterPoint,
    ) -> Result<(), TcpServiceFlightSidecarError> {
        if range.is_empty() {
            return Err(TcpServiceFlightSidecarError::InvalidRelease);
        }
        if self.records.len() >= self.max_records {
            return Err(TcpServiceFlightSidecarError::ResourceLimit);
        }
        self.records
            .try_reserve(1)
            .map_err(|_| TcpServiceFlightSidecarError::ResourceLimit)?;
        self.records.push(TcpServiceObservedFlight {
            identity,
            range,
            committed_at,
        });
        Ok(())
    }

    /// Releases one exact flight fragment. A missing record is pre-install
    /// accepted work; overlapping-but-noncovering state is invalid provenance.
    pub(in crate::runtime) fn release(
        &mut self,
        identity: I,
        range: OffsetRange,
    ) -> Result<Option<TcpServiceWriterPoint>, TcpServiceFlightSidecarError> {
        if range.is_empty() {
            return Err(TcpServiceFlightSidecarError::InvalidRelease);
        }
        let mut overlapping = false;
        let Some(index) = self.records.iter().position(|record| {
            if record.identity != identity {
                return false;
            }
            if ranges_overlap(record.range, range) {
                overlapping = true;
            }
            record.range.start <= range.start && record.range.end >= range.end
        }) else {
            return if overlapping {
                Err(TcpServiceFlightSidecarError::InvalidRelease)
            } else {
                Ok(None)
            };
        };
        let record = self.records[index];
        let prefix = (record.range.start < range.start).then_some(OffsetRange {
            start: record.range.start,
            end: range.start,
        });
        let suffix = (range.end < record.range.end).then_some(OffsetRange {
            start: range.end,
            end: record.range.end,
        });
        let retained = usize::from(prefix.is_some()) + usize::from(suffix.is_some());
        let resulting_records = self
            .records
            .len()
            .checked_sub(1)
            .and_then(|records| records.checked_add(retained))
            .ok_or(TcpServiceFlightSidecarError::ResourceLimit)?;
        let additional_records = retained.saturating_sub(1);
        if resulting_records > self.max_records
            || self.records.try_reserve(additional_records).is_err()
        {
            return Err(TcpServiceFlightSidecarError::ResourceLimit);
        }
        self.records.swap_remove(index);
        for retained_range in prefix.into_iter().chain(suffix) {
            self.records.push(TcpServiceObservedFlight {
                range: retained_range,
                ..record
            });
        }
        Ok(Some(record.committed_at))
    }

    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

fn ranges_overlap(left: OffsetRange, right: OffsetRange) -> bool {
    left.start < right.end && right.start < left.end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::path::CarrierPathInstanceId;
    use crate::model::tcp_service::{
        TcpServiceCarrierFence, TcpServiceReleaseKind, TcpServiceStreamFence,
    };
    use crate::protocol::{
        AuthNonce, PathId, PathMetricDirection, SessionId, StreamId, TcpCarrierAcceptedPath,
    };

    #[derive(Debug, Default)]
    struct RecordingDataAckSink {
        event: Mutex<Option<(TcpServiceDataAckEvent, Instant)>>,
        stop_after_ack: bool,
    }

    impl TcpServiceDataAckSink for RecordingDataAckSink {
        fn apply_data_ack(
            &self,
            event: TcpServiceDataAckEvent,
            now: Instant,
        ) -> Result<TcpServiceAckDisposition, TcpServiceFlightSidecarError> {
            let mut current = self.event.lock().expect("recording sink");
            if current.is_some() {
                return Err(TcpServiceFlightSidecarError::ResourceLimit);
            }
            *current = Some((event, now));
            Ok(if self.stop_after_ack {
                TcpServiceAckDisposition::Stop
            } else {
                TcpServiceAckDisposition::Continue
            })
        }
    }

    fn lifecycle() -> TcpServiceWriterLifecycle {
        TcpServiceWriterLifecycle::for_runtime_test(
            SessionId(1),
            1,
            PathMetricDirection::ClientToServer,
        )
    }

    #[test]
    fn sidecar_splits_exact_flights_and_rejects_partial_aliases() {
        let now = Instant::now();
        let point = lifecycle().point(now);
        let mut sidecar = TcpServiceFlightSidecar::new(3).expect("bounded sidecar");
        sidecar
            .record(7_u64, OffsetRange { start: 0, end: 30 }, point)
            .expect("record");
        assert_eq!(
            sidecar.release(7, OffsetRange { start: 10, end: 20 }),
            Ok(Some(point))
        );
        assert_eq!(
            sidecar.release(7, OffsetRange { start: 0, end: 10 }),
            Ok(Some(point))
        );
        assert_eq!(
            sidecar.release(7, OffsetRange { start: 20, end: 30 }),
            Ok(Some(point))
        );
        assert!(sidecar.is_empty());
        assert_eq!(
            sidecar.release(8, OffsetRange { start: 0, end: 1 }),
            Ok(None)
        );

        let mut bounded = TcpServiceFlightSidecar::new(1).expect("bounded sidecar");
        bounded
            .record(9_u64, OffsetRange { start: 0, end: 30 }, point)
            .expect("record");
        assert_eq!(
            bounded.release(9, OffsetRange { start: 10, end: 20 }),
            Err(TcpServiceFlightSidecarError::ResourceLimit)
        );
        assert_eq!(
            bounded.release(9, OffsetRange { start: 0, end: 30 }),
            Ok(Some(point)),
            "a rejected split preserves the exact original record"
        );
    }

    #[test]
    fn writer_clock_advances_equal_platform_readings() {
        let lifecycle = lifecycle();
        let clock = TcpServiceWriterClock::new(lifecycle);
        let observed_at = clock.origin;
        let first = clock
            .mark_observed_at(observed_at)
            .expect("first writer point");
        let second = clock
            .mark_observed_at(observed_at)
            .expect("second writer point");
        assert!(second.at() > first.at());
        assert!(TcpServiceWriterClock::now_not_before(second) >= second.at());
    }

    #[test]
    fn coordinator_applies_complete_ack_boundaries_and_stops_at_settlement() {
        let lifecycle = lifecycle();
        let sink = Arc::new(RecordingDataAckSink::default());
        let coordinator = TcpServiceWriterCoordinator::new(lifecycle, sink.clone());
        let initial = {
            let mut transaction = coordinator.lock();
            let boundary = transaction.initial_boundary().expect("initial boundary");
            assert!(transaction.activate());
            boundary
        };
        let committed_at = {
            let mut transaction = coordinator.lock();
            transaction.mark_commit().expect("observed writer commit")
        };
        let stream = TcpServiceStreamFence {
            stream_id: StreamId(9),
            demand_generation: 3,
            attachment_incarnation: 4,
            data_ack_horizon_bytes: 128,
        };
        let carrier = TcpServiceCarrierFence {
            accepted: TcpCarrierAcceptedPath {
                path_id: PathId(2),
                path_join_nonce: AuthNonce([7; 16]),
            },
            local_instance_id: CarrierPathInstanceId::from_raw(12),
            eligibility_generation: 5,
        };
        {
            let mut transaction = coordinator.lock();
            transaction
                .commit_ack(TcpServicePreparedAck {
                    lifecycle,
                    stream,
                    assigned_end: 128,
                    releases: vec![TcpServiceAckRelease {
                        carrier,
                        range: OffsetRange { start: 0, end: 64 },
                        committed_at: Some(committed_at),
                        kind: TcpServiceReleaseKind::Original,
                        unambiguous: true,
                    }],
                })
                .expect("complete ACK transaction");
        }
        let (event, now) = sink
            .event
            .lock()
            .expect("recorded ACK")
            .take()
            .expect("one model event");
        assert_eq!(event.sequence, initial.ack_sequence + 1);
        assert_eq!(event.stream, stream);
        assert_eq!(event.assigned_end, 128);
        assert_eq!(event.releases.len(), 1);
        assert_eq!(event.releases[0].committed_at, Some(committed_at));
        assert!(committed_at.at() > initial.writer.at());
        assert!(event.next_writer_boundary.at() > committed_at.at());
        assert_eq!(event.acked_at, event.next_writer_boundary.at());
        assert!(now >= event.next_writer_boundary.at());

        let settling_sink = Arc::new(RecordingDataAckSink {
            stop_after_ack: true,
            ..RecordingDataAckSink::default()
        });
        let settling =
            TcpServiceWriterCoordinator::new(lifecycle, Arc::clone(&settling_sink) as Arc<_>);
        {
            let mut transaction = settling.lock();
            transaction
                .initial_boundary()
                .expect("settling initial boundary");
            assert!(transaction.activate());
            transaction
                .commit_ack(TcpServicePreparedAck {
                    lifecycle,
                    stream,
                    assigned_end: 128,
                    releases: Vec::new(),
                })
                .expect("settling ACK completes without recursive locking");
            assert_eq!(
                transaction.mark_commit(),
                Err(TcpServiceFlightSidecarError::ObserverStopped),
                "settlement stops later writer commits at the ACK boundary"
            );
            assert_eq!(
                transaction.commit_ack(TcpServicePreparedAck {
                    lifecycle,
                    stream,
                    assigned_end: 128,
                    releases: Vec::new(),
                }),
                Err(TcpServiceFlightSidecarError::ObserverStopped),
                "a stopped lifecycle never invokes the model sink twice"
            );
        }
        assert!(
            settling_sink
                .event
                .lock()
                .expect("settling sink event")
                .is_some()
        );
    }

    #[test]
    fn stopped_observer_retains_release_provenance_without_new_commits() {
        let lifecycle = lifecycle();
        let clock = TcpServiceWriterClock::new(lifecycle);
        let mut observer = TcpServiceWriterObserver::new(lifecycle, 1).expect("observer");
        let range = OffsetRange { start: 4, end: 8 };
        let point = clock.mark().expect("writer point");
        observer
            .record_at(3_u8, range, point)
            .expect("observed commit");
        assert!(observer.stop(lifecycle));
        assert!(!observer.is_observing());
        assert_eq!(
            observer.record_at(3, OffsetRange { start: 8, end: 9 }, point),
            Err(TcpServiceFlightSidecarError::ObserverStopped)
        );
        assert_eq!(observer.release(3, range), Ok(Some(point)));
        assert!(observer.is_drained());
    }
}
