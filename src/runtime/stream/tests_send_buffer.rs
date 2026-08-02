use super::*;

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

#[tokio::test]
async fn release_wakes_a_waiting_stream() {
    let buffer = SessionSendBuffer::new(1);
    let mut first = buffer.stream_reservation();
    buffer
        .try_reserve(1)
        .expect("first reservation")
        .retain(&mut first, 1);

    let waiting_buffer = buffer.clone();
    let waiter = tokio::spawn(async move {
        let mut updates = waiting_buffer.subscribe();
        waiting_buffer.reserve(&mut updates, 1).await
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    first.release(1);
    let permit = waiter.await.expect("waiter task");
    assert_eq!(permit.bytes(), 1);
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
