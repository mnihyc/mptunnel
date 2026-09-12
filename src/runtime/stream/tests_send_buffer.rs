use super::*;

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
}

fn ready_permit<F: Future<Output = SessionSendBufferPermit>>(
    future: Pin<&mut F>,
) -> SessionSendBufferPermit {
    match poll_once(future) {
        Poll::Ready(permit) => permit,
        Poll::Pending => panic!("expected an owned source-byte grant"),
    }
}

#[test]
fn unique_bytes_are_shared_and_released_by_data_ack() {
    let buffer = SessionSendBuffer::new(10);
    let mut first = buffer.stream_reservation();
    let mut second = buffer.stream_reservation();

    buffer
        .try_reserve(6)
        .expect("first reservation")
        .retain(&mut first, 6);
    buffer
        .try_reserve(8)
        .expect("remaining reservation")
        .retain(&mut second, 4);
    assert_eq!(buffer.used_bytes(), 10);
    assert!(buffer.try_reserve(1).is_none());

    first.release(3);
    assert_eq!(buffer.available_bytes(), 3);
    buffer
        .try_reserve(8)
        .expect("released capacity")
        .retain(&mut second, 3);
    assert_eq!(second.held_bytes(), 7);
}

#[test]
fn unused_permit_and_stream_drop_return_capacity() {
    let buffer = SessionSendBuffer::new(16);
    let mut stream = buffer.stream_reservation();
    let permit = buffer.try_reserve(10).expect("reservation");
    permit.retain(&mut stream, 4);
    assert_eq!(buffer.used_bytes(), 4);

    drop(buffer.try_reserve(8).expect("temporary reservation"));
    assert_eq!(buffer.used_bytes(), 4);
    drop(stream);
    assert_eq!(buffer.used_bytes(), 0);
}

#[test]
fn release_wakes_only_the_granted_owner_outside_accounting_lock() {
    struct GrantWake {
        buffer: SessionSendBuffer,
        count: AtomicUsize,
    }
    impl futures::task::ArcWake for GrantWake {
        fn wake_by_ref(owner: &Arc<Self>) {
            assert!(
                owner.buffer.inner.state.try_lock().is_ok(),
                "grant wake must not run under accounting ownership"
            );
            owner.count.fetch_add(1, Ordering::SeqCst);
        }
    }
    let buffer = SessionSendBuffer::new(1);
    let occupied = buffer.try_reserve(1).unwrap();
    let mut first = buffer.subscribe();
    let mut second = buffer.subscribe();
    let first_wake = Arc::new(GrantWake {
        buffer: buffer.clone(),
        count: AtomicUsize::new(0),
    });
    let second_wake = Arc::new(GrantWake {
        buffer: buffer.clone(),
        count: AtomicUsize::new(0),
    });
    let first_waker = futures::task::waker(first_wake.clone());
    let second_waker = futures::task::waker(second_wake.clone());
    let mut first_request = Box::pin(buffer.reserve(&mut first, 1));
    let mut second_request = Box::pin(buffer.reserve(&mut second, 1));
    assert!(
        first_request
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        second_request
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );

    drop(occupied);
    assert_eq!(first_wake.count.load(Ordering::SeqCst), 1);
    assert_eq!(second_wake.count.load(Ordering::SeqCst), 0);
    assert_eq!(
        buffer.used_bytes(),
        1,
        "grant is charged before owner polls"
    );
    let permit = ready_permit(first_request.as_mut());
    assert_eq!(permit.bytes(), 1);
    drop(permit);
    assert_eq!(second_wake.count.load(Ordering::SeqCst), 1);
    drop(ready_permit(second_request.as_mut()));
    assert_eq!(buffer.used_bytes(), 0);
}

#[test]
fn released_bytes_belong_to_waiter_before_prior_producer_can_reclaim() {
    let buffer = SessionSendBuffer::new(4);
    let mut producer = buffer.subscribe();
    let mut newcomer = buffer.subscribe();
    let mut producer_read = Box::pin(buffer.reserve(&mut producer, 4));
    let mut occupied = ready_permit(producer_read.as_mut());
    drop(producer_read);

    // Exercise the actual reserve/release producer schedule. Under the old
    // watch policy, the releasing producer could reclaim before the waiting
    // future was polled again; this test specifies the revised ownership rule.
    for _ in 0..3 {
        let mut waiting = Box::pin(buffer.reserve(&mut newcomer, 4));
        assert!(poll_once(waiting.as_mut()).is_pending());
        drop(occupied);
        let mut reclaim = Box::pin(buffer.reserve(&mut producer, 4));
        assert!(poll_once(reclaim.as_mut()).is_pending());
        let granted = ready_permit(waiting.as_mut());
        assert_eq!(granted.bytes(), 4);
        drop(waiting);
        drop(granted);
        occupied = ready_permit(reclaim.as_mut());
    }
    drop(occupied);
    assert_eq!(buffer.used_bytes(), 0);
}

#[test]
fn cancelled_pending_ticket_is_dormant_then_reenters_at_its_original_age() {
    let buffer = SessionSendBuffer::new(1);
    let occupied = buffer.try_reserve(1).unwrap();
    let mut oldest = buffer.subscribe();
    let mut next = buffer.subscribe();
    let mut newest = buffer.subscribe();
    let mut old_request = Box::pin(buffer.reserve(&mut oldest, 1));
    assert!(poll_once(old_request.as_mut()).is_pending());
    drop(old_request);
    let old_ticket = oldest.ticket.expect("cancelled demand retains its ticket");

    let mut next_request = Box::pin(buffer.reserve(&mut next, 1));
    assert!(poll_once(next_request.as_mut()).is_pending());
    drop(occupied);
    let next_permit = ready_permit(next_request.as_mut());
    drop(next_request);
    assert_eq!(next_permit.bytes(), 1, "dormant oldest blocks nobody");

    let mut newest_request = Box::pin(buffer.reserve(&mut newest, 1));
    assert!(poll_once(newest_request.as_mut()).is_pending());
    let mut old_request = Box::pin(buffer.reserve(&mut oldest, 1));
    assert!(poll_once(old_request.as_mut()).is_pending());
    assert!(
        buffer
            .inner
            .state
            .lock()
            .unwrap()
            .pending
            .contains_key(&old_ticket)
    );
    drop(next_permit);
    assert!(poll_once(newest_request.as_mut()).is_pending());
    let old_permit = ready_permit(old_request.as_mut());
    drop(old_request);
    assert!(
        oldest.ticket.is_none(),
        "consuming a grant completes that turn"
    );
    drop(old_permit);
    drop(ready_permit(newest_request.as_mut()));
    assert_eq!(buffer.used_bytes(), 0);
}

#[test]
fn assigned_but_unconsumed_grant_cancellation_refunds_once_to_next_owner() {
    let buffer = SessionSendBuffer::new(5);
    let mut retained = buffer.stream_reservation();
    buffer.try_reserve(5).unwrap().retain(&mut retained, 5);
    let mut first = buffer.subscribe();
    let mut second = buffer.subscribe();
    let mut first_request = Box::pin(buffer.reserve(&mut first, 5));
    let mut second_request = Box::pin(buffer.reserve(&mut second, 5));
    assert!(poll_once(first_request.as_mut()).is_pending());
    assert!(poll_once(second_request.as_mut()).is_pending());

    retained.release(2);
    assert_eq!(buffer.used_bytes(), 5);
    drop(first_request);
    assert!(first.ticket.is_some());
    assert_eq!(
        buffer.used_bytes(),
        5,
        "refund is reassigned to active next owner"
    );
    let permit = ready_permit(second_request.as_mut());
    assert_eq!(
        permit.bytes(),
        2,
        "positive partial grant does not wait for five"
    );
    drop(permit);
    assert_eq!(buffer.used_bytes(), 3);
    first.withdraw();
    assert_eq!(
        buffer.used_bytes(),
        3,
        "withdraw cannot refund the grant twice"
    );
    drop(retained);
    assert_eq!(buffer.used_bytes(), 0);
}

#[test]
fn reentry_refreshes_request_maximum_and_one_release_grants_in_ticket_order() {
    let buffer = SessionSendBuffer::new(8);
    let occupied = buffer.try_reserve(8).unwrap();
    let mut first = buffer.subscribe();
    let mut second = buffer.subscribe();
    let mut first_request = Box::pin(buffer.reserve(&mut first, 8));
    assert!(poll_once(first_request.as_mut()).is_pending());
    drop(first_request);
    let mut second_request = Box::pin(buffer.reserve(&mut second, 8));
    assert!(poll_once(second_request.as_mut()).is_pending());
    let mut first_request = Box::pin(buffer.reserve(&mut first, 2));
    assert!(poll_once(first_request.as_mut()).is_pending());

    drop(occupied);
    let second_permit = ready_permit(second_request.as_mut());
    let first_permit = ready_permit(first_request.as_mut());
    assert_eq!(first_permit.bytes(), 2);
    assert_eq!(second_permit.bytes(), 6);
    assert_eq!(buffer.used_bytes(), 8);
    drop(first_permit);
    drop(second_permit);
    assert_eq!(buffer.used_bytes(), 0);
}

#[test]
fn ineligible_source_withdraws_old_ticket_before_new_demand() {
    let buffer = SessionSendBuffer::new(1);
    let occupied = buffer.try_reserve(1).unwrap();
    let mut first = buffer.subscribe();
    let mut second = buffer.subscribe();
    let mut first_request = Box::pin(buffer.reserve(&mut first, 1));
    assert!(poll_once(first_request.as_mut()).is_pending());
    drop(first_request);
    let mut second_request = Box::pin(buffer.reserve(&mut second, 1));
    assert!(poll_once(second_request.as_mut()).is_pending());

    first.withdraw();
    assert!(first.ticket.is_none());
    let mut first_request = Box::pin(buffer.reserve(&mut first, 1));
    assert!(poll_once(first_request.as_mut()).is_pending());
    drop(occupied);
    assert!(poll_once(first_request.as_mut()).is_pending());
    drop(ready_permit(second_request.as_mut()));
    drop(ready_permit(first_request.as_mut()));
    assert_eq!(buffer.used_bytes(), 0);
}

#[test]
fn dropping_pending_future_and_source_leaves_no_waiter_or_owned_bytes() {
    let buffer = SessionSendBuffer::new(1);
    let occupied = buffer.try_reserve(1).unwrap();
    let mut source = buffer.subscribe();
    let mut pending = Box::pin(buffer.reserve(&mut source, 1));
    assert!(poll_once(pending.as_mut()).is_pending());
    drop(pending);
    drop(source);
    assert!(buffer.inner.state.lock().unwrap().pending.is_empty());
    drop(occupied);
    assert_eq!(buffer.used_bytes(), 0);
}

#[test]
fn fixed_session_limit_does_not_follow_path_flight_capacity() {
    let limits = MuxLimits {
        max_stream_window_bytes: 8 * 1024 * 1024,
        max_repair_bytes: 6 * 1024 * 1024,
        max_path_flight_bytes: 512 * 1024,
        ..MuxLimits::default()
    };
    assert_eq!(
        SessionSendBuffer::from_limits(limits).limit_bytes(),
        6 * 1024 * 1024
    );
}
