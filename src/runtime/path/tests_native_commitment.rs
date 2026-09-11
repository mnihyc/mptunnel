use super::*;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::Mutex;
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
    let owner =
        NativeOperationCommitment::from_observer(NativeObserver::Controlled(native.clone()))
            .unwrap();
    (owner, native)
}

fn barrier(owner: &NativeOperationCommitment) -> Option<NativeCommitmentBarrier> {
    owner.capture().unwrap().barrier()
}

#[test]
fn actual_native_acceptance_blocks_without_product_settlement_or_write_metadata() {
    let (owner, native) = fixture();
    assert!(barrier(&owner).is_none());
    // A partial native write is already outstanding. No completed-operation
    // registration or Product ACK is needed to observe its accepted prefix.
    native.publish(Ok(progress(73, 0)));
    assert_eq!(barrier(&owner).unwrap().native_end(), 73);
    native.publish(Ok(progress(300, 72)));
    assert_eq!(barrier(&owner).unwrap().native_end(), 300);
    native.publish(Ok(progress(300, 299)));
    assert_eq!(barrier(&owner).unwrap().native_end(), 300);
    native.publish(Ok(progress(300, 300)));
    assert!(barrier(&owner).is_none());
}

#[test]
fn successive_operation_starts_allow_only_one_active_transaction_and_one_successor() {
    let (owner, native) = fixture();
    let first = owner.begin_operation().unwrap();
    native.publish(Ok(progress(100, 0)));
    owner.complete_operation(first).unwrap();
    assert_eq!(barrier(&owner).unwrap().native_end(), 1);

    native.publish(Ok(progress(100, 1)));
    assert!(barrier(&owner).is_none());
    // Clones share the real operation boundary. Beginning a successor does not
    // publish it or move the boundary for the several claims in its one batch.
    let writer = owner.clone();
    let second = writer.begin_operation().unwrap();
    assert!(barrier(&owner).is_none());
    native.publish(Ok(progress(200, 1)));
    writer.complete_operation(second).unwrap();
    assert_eq!(barrier(&owner).unwrap().native_end(), 101);

    // Finishing the first transaction is insufficient to admit a third. The
    // second must itself start, which proves the first has no unsent tail.
    native.publish(Ok(progress(200, 100)));
    assert_eq!(barrier(&owner).unwrap().native_end(), 101);
    native.publish(Ok(progress(200, 101)));
    assert!(barrier(&owner).is_none());
    let third = owner.begin_operation().unwrap();
    native.publish(Ok(progress(300, 101)));
    owner.complete_operation(third).unwrap();
    assert_eq!(barrier(&writer).unwrap().native_end(), 201);
    native.publish(Ok(progress(300, 200)));
    assert!(barrier(&owner).is_some());
    native.publish(Ok(progress(300, 201)));
    assert!(barrier(&owner).is_none());
}

#[tokio::test]
async fn operation_start_crossing_inside_a_packet_wakes_before_operation_end() {
    let (owner, native) = fixture();
    native.publish(Ok(progress(37, 37)));
    let start = owner.begin_operation().unwrap();
    native.publish(Ok(progress(137, 37)));
    owner.complete_operation(start).unwrap();
    let view = owner.capture().unwrap();
    let target = view.barrier().unwrap();
    assert_eq!(target.native_end(), 38);
    let snapshots = native.snapshots.load(Ordering::Relaxed);
    let mut wait = pin!(target.wait_until_packetized());
    assert_eq!(native.snapshots.load(Ordering::Relaxed), snapshots);
    assert!(
        poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    // STREAM construction can jump past the exact boundary in one packet. It
    // need not land on B+1, finish this operation, or receive any ACK.
    native.publish(Ok(progress(137, 60)));
    assert_eq!(wait.await.unwrap(), progress(137, 60));
    assert!(barrier(&owner).is_none());
    assert_eq!(view.barrier().unwrap().native_end(), 38);
    assert_eq!(
        view.barrier()
            .unwrap()
            .wait_until_packetized()
            .await
            .unwrap(),
        progress(137, 60)
    );
}

#[test]
fn zero_operations_preserve_the_anchor_and_partial_writes_require_the_accepted_end() {
    let (owner, native) = fixture();
    let first = owner.begin_operation().unwrap();
    native.publish(Ok(progress(100, 0)));
    owner.complete_operation(first).unwrap();
    let empty = owner.begin_operation().unwrap();
    owner.complete_operation(empty).unwrap();
    assert_eq!(barrier(&owner).unwrap().native_end(), 1);

    native.publish(Ok(progress(100, 1)));
    let second = owner.begin_operation().unwrap();
    native.publish(Ok(progress(150, 100)));
    // The latest completed span ends at 100, so this unrecorded accepted
    // prefix is not permission to overlap another transaction.
    assert_eq!(barrier(&owner).unwrap().native_end(), 150);
    owner.complete_operation(second).unwrap();
    assert_eq!(barrier(&owner).unwrap().native_end(), 101);
    native.publish(Ok(progress(150, 101)));
    assert!(barrier(&owner).is_none());

    // A later operation may start packetization before its write returns.
    let third = owner.begin_operation().unwrap();
    native.publish(Ok(progress(200, 160)));
    assert_eq!(barrier(&owner).unwrap().native_end(), 200);
    owner.complete_operation(third).unwrap();
    assert!(barrier(&owner).is_none());
}

#[test]
fn abandoned_operation_and_untracked_control_tail_do_not_inherit_start_permission() {
    let (owner, native) = fixture();
    let first = owner.begin_operation().unwrap();
    native.publish(Ok(progress(100, 1)));
    owner.complete_operation(first).unwrap();
    assert!(barrier(&owner).is_none());
    let abandoned = owner.begin_operation().unwrap();
    native.publish(Ok(progress(150, 101)));
    drop(abandoned);
    assert_eq!(barrier(&owner).unwrap().native_end(), 150);

    // A zero-byte flush cannot adopt that untracked accepted tail.
    let empty = owner.begin_operation().unwrap();
    owner.complete_operation(empty).unwrap();
    assert_eq!(barrier(&owner).unwrap().native_end(), 150);
    native.publish(Ok(progress(150, 150)));
    assert!(barrier(&owner).is_none());
}

#[test]
fn operation_tokens_reject_foreign_and_overlapping_completion_without_new_permission() {
    let (first, first_native) = fixture();
    let (second, _) = fixture();
    let foreign = first.begin_operation().unwrap();
    assert_eq!(
        second.complete_operation(foreign),
        Err(NativeCommitmentError::InvalidNativeProgress)
    );
    let accepted = first.begin_operation().unwrap();
    let stale = first.begin_operation().unwrap();
    first_native.publish(Ok(progress(100, 0)));
    first.complete_operation(accepted).unwrap();
    first_native.publish(Ok(progress(200, 100)));
    assert_eq!(
        first.complete_operation(stale),
        Err(NativeCommitmentError::InvalidNativeProgress)
    );
    assert_eq!(barrier(&first).unwrap().native_end(), 200);
    assert!(barrier(&second).is_none());
}

#[test]
fn recorded_native_boundaries_reject_backward_acceptance_or_packetization() {
    let (owner, native) = fixture();
    let start = owner.begin_operation().unwrap();
    native.publish(Ok(progress(100, 50)));
    owner.complete_operation(start).unwrap();
    for invalid in [progress(99, 50), progress(100, 49)] {
        native.publish(Ok(invalid));
        assert!(matches!(
            owner.capture(),
            Err(NativeCommitmentError::InvalidNativeProgress)
        ));
        assert!(matches!(
            owner.begin_operation(),
            Err(NativeCommitmentError::InvalidNativeProgress)
        ));
    }
    for invalid in [progress(99, 50), progress(101, 49)] {
        native.publish(Ok(progress(100, 50)));
        let start = owner.begin_operation().unwrap();
        native.publish(Ok(invalid));
        assert_eq!(
            owner.complete_operation(start),
            Err(NativeCommitmentError::InvalidNativeProgress)
        );
    }
}

#[tokio::test]
async fn terminal_during_operation_fails_completion_and_started_operation_waits() {
    for error in [
        SendStreamObservationError::ClosedStream,
        SendStreamObservationError::Stopped(quinn::VarInt::from_u32(7)),
        SendStreamObservationError::ConnectionLost(quinn::ConnectionError::LocallyClosed),
    ] {
        let (owner, native) = fixture();
        let first = owner.begin_operation().unwrap();
        native.publish(Ok(progress(100, 0)));
        owner.complete_operation(first).unwrap();
        let target = barrier(&owner).unwrap();
        assert_eq!(target.native_end(), 1);
        let in_progress = owner.begin_operation().unwrap();
        let mut wait = pin!(target.wait_until_packetized());
        assert!(
            poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        native.publish(Err(error.clone()));
        assert_eq!(
            owner.complete_operation(in_progress),
            Err(NativeCommitmentError::Native(error.clone()))
        );
        assert_eq!(
            wait.await,
            Err(NativeCommitmentError::Native(error.clone()))
        );
        assert!(matches!(
            owner.begin_operation(),
            Err(NativeCommitmentError::Native(actual)) if actual == error
        ));
    }
}

#[tokio::test]
async fn captured_busy_view_is_conservative_and_does_not_read_native_under_product() {
    let (owner, native) = fixture();
    native.publish(Ok(progress(100, 0)));
    let captured = owner.capture().unwrap();
    native.publish(Ok(progress(100, 100)));
    assert!(barrier(&owner).is_none());
    let snapshots = native.snapshots.load(Ordering::Relaxed);
    let target = captured.barrier().unwrap();
    assert_eq!(target.native_end(), 100);
    let wait = target.wait_until_packetized();
    assert_eq!(native.snapshots.load(Ordering::Relaxed), snapshots);
    // Its earlier busy snapshot does not need a new packet event to wake.
    assert_eq!(wait.await.unwrap(), progress(100, 100));
}

#[tokio::test]
async fn exact_target_crossing_does_not_wait_for_newer_native_work_or_ack() {
    let (owner, native) = fixture();
    native.publish(Ok(progress(100, 0)));
    let mut wait = pin!(barrier(&owner).unwrap().wait_until_packetized());
    assert!(
        poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    // Existing control/repair can append native work while a new Original is
    // refused. The old target must finish, then a new capture sees that work.
    native.publish(Ok(progress(200, 100)));
    assert_eq!(wait.await.unwrap(), progress(200, 100));
    assert_eq!(barrier(&owner).unwrap().native_end(), 200);
}

#[tokio::test]
async fn independent_fifos_do_not_share_packetization_or_wait_completion() {
    let (first, first_native) = fixture();
    let (second, second_native) = fixture();
    first_native.publish(Ok(progress(100, 0)));
    second_native.publish(Ok(progress(200, 0)));
    let mut first_wait = pin!(barrier(&first).unwrap().wait_until_packetized());
    let mut second_wait = pin!(barrier(&second).unwrap().wait_until_packetized());
    assert!(
        poll_fn(|cx| Poll::Ready(first_wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(
        poll_fn(|cx| Poll::Ready(second_wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    first_native.publish(Ok(progress(100, 100)));
    assert_eq!(first_wait.await.unwrap(), progress(100, 100));
    assert!(barrier(&first).is_none());
    assert!(
        poll_fn(|cx| Poll::Ready(second_wait.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    second_native.publish(Ok(progress(200, 200)));
    assert_eq!(second_wait.await.unwrap(), progress(200, 200));
}

#[tokio::test]
async fn cancelling_one_wait_preserves_another_and_native_inventory() {
    let (owner, native) = fixture();
    native.publish(Ok(progress(100, 0)));
    let target = barrier(&owner).unwrap();
    let mut retained = pin!(target.clone().wait_until_packetized());
    assert!(
        poll_fn(|cx| Poll::Ready(retained.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    {
        let mut cancelled = pin!(target.wait_until_packetized());
        assert!(
            poll_fn(|cx| Poll::Ready(cancelled.as_mut().poll(cx)))
                .await
                .is_pending()
        );
    }
    assert_eq!(barrier(&owner).unwrap().native_end(), 100);
    native.publish(Ok(progress(100, 99)));
    assert!(
        poll_fn(|cx| Poll::Ready(retained.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    native.publish(Ok(progress(100, 100)));
    assert_eq!(retained.await.unwrap(), progress(100, 100));
    assert!(barrier(&owner).is_none());
}

#[tokio::test]
async fn native_terminals_fail_pending_packetization_and_future_captures() {
    for error in [
        SendStreamObservationError::ClosedStream,
        SendStreamObservationError::Stopped(quinn::VarInt::from_u32(7)),
        SendStreamObservationError::ConnectionLost(quinn::ConnectionError::LocallyClosed),
    ] {
        let (owner, native) = fixture();
        native.publish(Ok(progress(100, 0)));
        let mut wait = pin!(barrier(&owner).unwrap().wait_until_packetized());
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
    }
}

#[test]
fn unavailable_or_incoherent_native_input_never_becomes_empty_permission() {
    let native = ControlledObserver::new();
    native.publish(Err(SendStreamObservationError::NotEstablished));
    assert!(matches!(
        NativeOperationCommitment::from_observer(NativeObserver::Controlled(native)),
        Err(NativeCommitmentError::Native(
            SendStreamObservationError::NotEstablished
        ))
    ));
    let (owner, native) = fixture();
    native.publish(Ok(progress(100, 101)));
    assert!(matches!(
        owner.capture(),
        Err(NativeCommitmentError::InvalidNativeProgress)
    ));
    assert!(matches!(
        NativeOperationCommitment::from_observer(NativeObserver::Controlled(native.clone())),
        Err(NativeCommitmentError::InvalidNativeProgress)
    ));
    native.publish(Err(SendStreamObservationError::NotEstablished));
    assert!(matches!(
        owner.capture(),
        Err(NativeCommitmentError::Native(
            SendStreamObservationError::NotEstablished
        ))
    ));
}

#[tokio::test]
async fn idle_terminal_wait_owns_its_lifetime_and_survives_other_wait_cancellation() {
    let (owner, native) = fixture();
    let view = owner.capture().unwrap();
    assert!(view.barrier().is_none());
    let snapshots = native.snapshots.load(Ordering::Relaxed);
    let wait = view.wait_until_terminated();
    assert_eq!(native.snapshots.load(Ordering::Relaxed), snapshots);
    let mut wait = pin!(wait);
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
    drop(view);
    let error = SendStreamObservationError::Stopped(quinn::VarInt::from_u32(9));
    native.publish(Err(error.clone()));
    assert_eq!(wait.await, NativeCommitmentError::Native(error));
}

#[tokio::test]
async fn terminal_between_capture_and_first_poll_is_not_a_lost_wake() {
    let (owner, native) = fixture();
    native.publish(Ok(progress(100, 0)));
    let view = owner.capture().unwrap();
    let packetized = view.barrier().unwrap().wait_until_packetized();
    let terminal = view.wait_until_terminated();
    native.publish(Err(SendStreamObservationError::ClosedStream));
    assert_eq!(
        packetized.await,
        Err(NativeCommitmentError::Native(
            SendStreamObservationError::ClosedStream
        ))
    );
    assert_eq!(
        terminal.await,
        NativeCommitmentError::Native(SendStreamObservationError::ClosedStream)
    );
}
