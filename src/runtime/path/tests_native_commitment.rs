use super::*;
use bytes::Bytes;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use tokio::sync::Notify;

/// Controls only the component's input/wait adapter. Actual native packet
/// construction and H3 framing have their separate producer integration tests.
#[derive(Debug)]
pub(super) struct ControlledObserver {
    state: Mutex<Result<SendStreamProgress, SendStreamObservationError>>,
    snapshots: AtomicUsize,
    changed: Notify,
}

impl ControlledObserver {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Ok(progress(0, 0))),
            snapshots: AtomicUsize::new(0),
            changed: Notify::new(),
        })
    }

    pub(super) fn snapshot(&self) -> Result<SendStreamProgress, SendStreamObservationError> {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        self.state.lock().unwrap().clone()
    }

    fn publish(&self, value: Result<SendStreamProgress, SendStreamObservationError>) {
        *self.state.lock().unwrap() = value;
        self.changed.notify_waiters();
    }

    pub(super) async fn wait_until_packetized(
        &self,
        end: u64,
    ) -> Result<SendStreamProgress, SendStreamObservationError> {
        loop {
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            let snapshot = self.snapshot()?;
            if end > snapshot.accepted_end {
                return Err(SendStreamObservationError::UnacceptedEnd {
                    accepted_end: snapshot.accepted_end,
                });
            }
            if snapshot.first_unpacketized >= end {
                return Ok(snapshot);
            }
            changed.await;
        }
    }

    pub(super) async fn wait_until_terminated(&self) -> SendStreamObservationError {
        loop {
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            if let Err(error) = self.snapshot() {
                return error;
            }
            changed.await;
        }
    }
}

fn progress(accepted_end: u64, first_unpacketized: u64) -> SendStreamProgress {
    SendStreamProgress {
        accepted_end,
        first_unpacketized,
    }
}

fn fixture() -> (NativeOperationCommitment, Arc<ControlledObserver>) {
    let native = ControlledObserver::new();
    let owner = NativeOperationCommitment::from_observer(
        NativeObserver::Controlled(native.clone()),
        StreamId(7),
    )
    .unwrap();
    (owner, native)
}

fn data(offset: u64, payload: &'static [u8]) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        payload: Bytes::from_static(payload),
    }
}

fn record(
    owner: &NativeOperationCommitment,
    native: &ControlledObserver,
    frames: &[Frame],
    accepted_end: u64,
    first_unpacketized: u64,
) {
    let operation = owner.prepare_operation(frames).unwrap();
    native.publish(Ok(progress(accepted_end, first_unpacketized)));
    owner.record_accepted_operation(operation).unwrap();
}

fn barrier(owner: &NativeOperationCommitment, frontier: u64) -> Option<NativeCommitmentBarrier> {
    owner.capture().unwrap().barrier(frontier).unwrap()
}

#[test]
fn control_tail_keeps_the_conservative_enclosing_operation_boundary() {
    let (owner, native) = fixture();
    // These are opaque native coordinates, not reconstructed H3 header lengths.
    record(
        &owner,
        &native,
        &[data(0, b"bytes"), Frame::Ping { nonce: 1 }],
        300,
        250,
    );
    assert!(barrier(&owner, 4).is_none());
    assert_eq!(barrier(&owner, 5).unwrap().native_end(), 300);
    native.publish(Ok(progress(300, 299)));
    assert_eq!(barrier(&owner, 5).unwrap().native_end(), 300);
    native.publish(Ok(progress(300, 300)));
    assert!(barrier(&owner, 5).is_none());
}

#[test]
fn ack_before_write_completion_is_reconciled_without_retaining_ack_ranges() {
    let (owner, native) = fixture();
    let operation = owner.prepare_operation(&[data(100, b"abc")]).unwrap();
    native.publish(Ok(progress(10, 0))); // The complete operation is not yet recorded.
    let captured = owner.capture().unwrap();
    assert!(captured.barrier(103).unwrap().is_none());
    native.publish(Ok(progress(73, 0)));
    owner.record_accepted_operation(operation).unwrap();
    let snapshots = native.snapshots.load(Ordering::Relaxed);
    // This deliberately reuses the older native lower bound. Final Product
    // settlement sees the newly recorded operation without any Native read.
    assert_eq!(captured.barrier(103).unwrap().unwrap().native_end(), 73);
    assert_eq!(native.snapshots.load(Ordering::Relaxed), snapshots);
}

#[test]
fn mixed_original_and_repeated_data_require_the_greatest_product_end() {
    let (owner, native) = fixture();
    record(
        &owner,
        &native,
        &[
            data(20, b"later"),
            Frame::Ping { nonce: 2 },
            data(0, b"old"),
        ],
        500,
        0,
    );
    assert!(barrier(&owner, 3).is_none());
    assert!(barrier(&owner, 24).is_none());
    assert_eq!(barrier(&owner, 25).unwrap().native_end(), 500);
    assert!(matches!(
        owner.capture().unwrap().barrier(24),
        Err(NativeCommitmentError::ProductFrontierRegressed {
            previous: 25,
            current: 24
        })
    ));
}

#[test]
fn settled_operations_coalesce_without_losing_an_unsettled_middle_operation() {
    let (owner, native) = fixture();
    record(&owner, &native, &[data(0, b"a")], 100, 0);
    record(&owner, &native, &[data(20, b"b")], 200, 0);
    record(&owner, &native, &[data(0, b"a")], 300, 0);
    assert_eq!(barrier(&owner, 1).unwrap().native_end(), 300);
    {
        let ledger = owner.shared.ledger().unwrap();
        assert_eq!(ledger.unsettled.len(), 1);
        assert_eq!(ledger.minimum_product_end, Some(21));
        assert_eq!(ledger.settled_native_end, Some(300));
    }
    assert_eq!(barrier(&owner, 21).unwrap().native_end(), 300);
    assert!(owner.shared.ledger().unwrap().unsettled.is_empty());
}

#[test]
fn positive_data_is_required_and_cancelling_an_operation_records_nothing() {
    let (owner, native) = fixture();
    let before = native.snapshots.load(Ordering::Relaxed);
    let cancelled = owner.prepare_operation(&[data(0, b"cancelled")]).unwrap();
    assert_eq!(native.snapshots.load(Ordering::Relaxed), before);
    drop(cancelled);
    record(&owner, &native, &[Frame::Ping { nonce: 1 }], 100, 0);
    record(&owner, &native, &[data(u64::MAX, b"")], 200, 0);
    assert!(barrier(&owner, 0).is_none());
    let ledger = owner.shared.ledger().unwrap();
    assert!(ledger.unsettled.is_empty());
    assert_eq!(ledger.settled_native_end, None);
}

#[test]
fn summaries_validate_stream_identity_checked_extents_and_native_binding() {
    let (owner, native) = fixture();
    let before = native.snapshots.load(Ordering::Relaxed);
    assert!(matches!(
        owner.prepare_operation(&[Frame::StreamData {
            stream_id: StreamId(8),
            offset: 0,
            payload: Bytes::new(),
        }]),
        Err(NativeCommitmentError::ProductStreamMismatch { .. })
    ));
    assert!(matches!(
        owner.prepare_operation(&[data(u64::MAX, b"x")]),
        Err(NativeCommitmentError::ProductExtentOverflow)
    ));
    assert_eq!(native.snapshots.load(Ordering::Relaxed), before);
    let (other, _) = fixture();
    let foreign = other.prepare_operation(&[data(0, b"x")]).unwrap();
    assert_eq!(
        owner.record_accepted_operation(foreign),
        Err(NativeCommitmentError::ForeignOperation)
    );
    assert_eq!(native.snapshots.load(Ordering::Relaxed), before);
}

#[test]
fn every_append_prunes_even_when_no_original_claim_or_product_ack_occurs() {
    let (owner, native) = fixture();
    for operation in 1..=256_u64 {
        let end = operation * 100;
        record(&owner, &native, &[data(operation, b"x")], end, end - 100);
        let ledger = owner.shared.ledger().unwrap();
        assert_eq!(ledger.unsettled.len(), 1);
        assert_eq!(ledger.unsettled.front().unwrap().native_end, end);
    }
    // A control-only completion also prunes; repair-only writers cannot retain
    // a lifetime journal just because they never offer another Original claim.
    record(&owner, &native, &[Frame::Ping { nonce: 2 }], 25_700, 25_600);
    let ledger = owner.shared.ledger().unwrap();
    assert!(ledger.unsettled.is_empty());
    assert_eq!(ledger.unsettled.capacity(), 0);
}

#[test]
fn independent_native_fifos_do_not_share_settlement_or_progress() {
    let (first, first_native) = fixture();
    let (second, second_native) = fixture(); // Same Product ID can name opposite directions.
    record(&first, &first_native, &[data(0, b"a")], 100, 0);
    record(&second, &second_native, &[data(0, b"b")], 200, 0);
    assert_eq!(barrier(&first, 1).unwrap().native_end(), 100);
    assert!(barrier(&second, 0).is_none());
    first_native.publish(Ok(progress(100, 100)));
    assert!(barrier(&first, 1).is_none());
    assert_eq!(barrier(&second, 1).unwrap().native_end(), 200);
}

#[test]
fn old_captured_progress_cannot_revive_retired_native_debt() {
    let (owner, native) = fixture();
    record(&owner, &native, &[data(0, b"a")], 100, 0);
    let old = owner.capture().unwrap();
    native.publish(Ok(progress(100, 100)));
    assert!(barrier(&owner, 1).is_none());
    assert!(old.barrier(1).unwrap().is_none());
}

#[test]
fn a_positive_operation_must_advance_actual_native_acceptance() {
    let (owner, native) = fixture();
    record(&owner, &native, &[data(0, b"a")], 100, 0);
    let operation = owner.prepare_operation(&[data(1, b"b")]).unwrap();
    assert_eq!(
        owner.record_accepted_operation(operation),
        Err(NativeCommitmentError::NativeOperationDidNotAdvance {
            previous_end: 100,
            accepted_end: 100,
        })
    );
    assert_eq!(owner.shared.ledger().unwrap().unsettled.len(), 1);
}

#[tokio::test]
async fn cancelling_one_wait_preserves_debt_and_a_later_crossing_prunes_it() {
    let (owner, native) = fixture();
    record(&owner, &native, &[data(0, b"a")], 100, 0);
    let target = barrier(&owner, 1).unwrap();
    {
        let mut cancelled = pin!(target.clone().wait_until_packetized());
        assert!(
            poll_fn(|cx| Poll::Ready(cancelled.as_mut().poll(cx)))
                .await
                .is_pending()
        );
    }
    assert_eq!(barrier(&owner, 1).unwrap().native_end(), 100);
    let mut wait = pin!(target.wait_until_packetized());
    assert!(
        poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    native.publish(Ok(progress(100, 100)));
    assert_eq!(wait.await.unwrap(), progress(100, 100));
    assert!(owner.shared.ledger().unwrap().settled_native_end.is_none());
}

#[tokio::test]
async fn terminal_errors_are_preserved_in_waits_and_already_captured_views() {
    for error in [
        SendStreamObservationError::ClosedStream,
        SendStreamObservationError::Stopped(quinn::VarInt::from_u32(7)),
        SendStreamObservationError::ConnectionLost(quinn::ConnectionError::LocallyClosed),
    ] {
        let (owner, native) = fixture();
        record(&owner, &native, &[data(0, b"a")], 100, 0);
        let view = owner.capture().unwrap();
        let target = view.barrier(1).unwrap().unwrap();
        let mut wait = pin!(target.wait_until_packetized());
        assert!(
            poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        native.publish(Err(error.clone()));
        assert_eq!(
            wait.await,
            Err(NativeCommitmentError::Native(error.clone()))
        );
        assert!(
            matches!(owner.capture(), Err(NativeCommitmentError::Native(actual)) if actual == error)
        );
        assert!(
            matches!(view.barrier(1), Err(NativeCommitmentError::Native(actual)) if actual == error)
        );
        assert!(owner.shared.ledger().unwrap().unsettled.is_empty());
    }
}

#[test]
fn unavailable_native_progress_is_an_error_not_an_empty_supported_fifo() {
    let native = ControlledObserver::new();
    native.publish(Err(SendStreamObservationError::NotEstablished));
    assert!(matches!(
        NativeOperationCommitment::from_observer(NativeObserver::Controlled(native), StreamId(7),),
        Err(NativeCommitmentError::Native(
            SendStreamObservationError::NotEstablished
        ))
    ));
}

#[tokio::test]
async fn idle_terminal_wait_is_owned_cancellation_safe_and_visible_to_old_views() {
    let (owner, native) = fixture();
    let view = owner.capture().unwrap();
    assert!(view.barrier(0).unwrap().is_none());
    let snapshots = native.snapshots.load(Ordering::Relaxed);
    let mut wait = pin!(view.wait_until_terminated());
    assert_eq!(native.snapshots.load(Ordering::Relaxed), snapshots);
    {
        let mut cancelled = pin!(view.wait_until_terminated());
        assert!(
            poll_fn(|cx| Poll::Ready(cancelled.as_mut().poll(cx)))
                .await
                .is_pending()
        );
    }
    assert!(
        poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(owner);
    let error = SendStreamObservationError::Stopped(quinn::VarInt::from_u32(9));
    native.publish(Err(error.clone()));
    assert_eq!(wait.await, NativeCommitmentError::Native(error.clone()));
    assert!(matches!(
        view.barrier(0),
        Err(NativeCommitmentError::Native(actual)) if actual == error
    ));
}
