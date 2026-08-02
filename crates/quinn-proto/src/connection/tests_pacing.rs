use super::*;

#[test]
fn does_not_panic_on_bad_instant() {
    let old_instant = Instant::now();
    let new_instant = old_instant + Duration::from_micros(15);
    let rtt = Duration::from_micros(400);

    assert!(Pacer::new(rtt, 30000, 1500, new_instant)
        .delay(Duration::from_micros(0), 0, 1500, 1, old_instant, None,)
        .is_none());
    assert!(Pacer::new(rtt, 30000, 1500, new_instant)
        .delay(Duration::from_micros(0), 1600, 1500, 1, old_instant, None,)
        .is_none());
    assert!(Pacer::new(rtt, 30000, 1500, new_instant)
        .delay(
            Duration::from_micros(0),
            1500,
            1500,
            3000,
            old_instant,
            None,
        )
        .is_none());
}

#[test]
fn derives_initial_capacity() {
    let window = 2_000_000;
    let mtu = 1500;
    let rtt = Duration::from_millis(50);
    let now = Instant::now();

    let pacer = Pacer::new(rtt, window, mtu, now);
    assert_eq!(
        pacer.capacity,
        (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
    );
    assert_eq!(pacer.tokens, pacer.capacity);

    let pacer = Pacer::new(Duration::from_millis(0), window, mtu, now);
    assert_eq!(pacer.capacity, MAX_BURST_SIZE * mtu as u64);
    assert_eq!(pacer.tokens, pacer.capacity);

    let pacer = Pacer::new(rtt, 1, mtu, now);
    assert_eq!(pacer.capacity, MIN_BURST_SIZE * mtu as u64);
    assert_eq!(pacer.tokens, pacer.capacity);
}

#[test]
fn adjusts_capacity() {
    let window = 2_000_000;
    let mtu = 1500;
    let rtt = Duration::from_millis(50);
    let now = Instant::now();

    let mut pacer = Pacer::new(rtt, window, mtu, now);
    assert_eq!(
        pacer.capacity,
        (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
    );
    assert_eq!(pacer.tokens, pacer.capacity);
    let initial_tokens = pacer.tokens;

    pacer.delay(rtt, mtu as u64, mtu, window * 2, now, None);
    assert_eq!(
        pacer.capacity,
        (2 * window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
    );
    assert_eq!(pacer.tokens, initial_tokens);

    pacer.delay(rtt, mtu as u64, mtu, window / 2, now, None);
    assert_eq!(
        pacer.capacity,
        (window as u128 / 2 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
    );
    assert_eq!(pacer.tokens, initial_tokens / 2);

    pacer.delay(rtt, mtu as u64, mtu * 2, window, now, None);
    assert_eq!(
        pacer.capacity,
        (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
    );

    pacer.delay(rtt, mtu as u64, 20_000, window, now, None);
    assert_eq!(pacer.capacity, 20_000_u64 * MIN_BURST_SIZE);
}

#[test]
fn computes_pause_correctly() {
    let window = 2_000_000u64;
    let mtu = 1000;
    let rtt = Duration::from_millis(50);
    let old_instant = Instant::now();

    let mut pacer = Pacer::new(rtt, window, mtu, old_instant);
    let packet_capacity = pacer.capacity / mtu as u64;

    for _ in 0..packet_capacity {
        assert_eq!(
            pacer.delay(rtt, mtu as u64, mtu, window, old_instant, None,),
            None,
            "When capacity is available packets should be sent immediately"
        );

        pacer.on_transmit(mtu);
    }

    let pace_duration = Duration::from_nanos((BURST_INTERVAL_NANOS * 4 / 5) as u64);

    assert_eq!(
        pacer
            .delay(rtt, mtu as u64, mtu, window, old_instant, None,)
            .expect("Send must be delayed")
            .duration_since(old_instant),
        pace_duration
    );

    // Refill half of the tokens
    assert_eq!(
        pacer.delay(
            rtt,
            mtu as u64,
            mtu,
            window,
            old_instant + pace_duration / 2,
            None,
        ),
        None
    );
    assert_eq!(pacer.tokens, pacer.capacity / 2);

    for _ in 0..packet_capacity / 2 {
        assert_eq!(
            pacer.delay(rtt, mtu as u64, mtu, window, old_instant, None,),
            None,
            "When capacity is available packets should be sent immediately"
        );

        pacer.on_transmit(mtu);
    }

    // Refill all capacity by waiting more than the expected duration
    assert_eq!(
        pacer.delay(
            rtt,
            mtu as u64,
            mtu,
            window,
            old_instant + pace_duration * 3 / 2,
            None,
        ),
        None
    );
    assert_eq!(pacer.tokens, pacer.capacity);
}

#[test]
fn controller_pacing_rate_delays_normal_datagrams_after_burst() {
    let rtt = Duration::from_millis(50);
    let now = Instant::now();
    let mtu = 1500;
    let pacing_rate = 200_000_000;
    let mut pacer = Pacer::new(rtt, 2_000_000, mtu, now);
    assert_eq!(
        pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(pacing_rate)),
        None,
    );
    let packet_capacity = pacer.tokens / u64::from(mtu);

    for _ in 0..packet_capacity {
        assert_eq!(
            pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(pacing_rate)),
            None,
        );
        pacer.on_transmit(mtu);
    }

    let refill = pacer.capacity - pacer.tokens;
    assert_eq!(
        pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(pacing_rate)),
        Some(now + duration_for_bytes(refill, pacing_rate)),
    );
}

#[test]
fn lower_controller_rate_immediately_clamps_burst_and_tokens() {
    let rtt = Duration::from_millis(50);
    let now = Instant::now();
    let mtu = 1500;
    let mut pacer = Pacer::new(rtt, 2_000_000, mtu, now);

    pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(100_000_000));
    let startup_capacity = pacer.capacity;
    pacer.tokens = startup_capacity;
    pacer.delay(rtt, u64::from(mtu), mtu, 2_000_000, now, Some(10_000_000));

    assert!(pacer.capacity < startup_capacity);
    assert_eq!(pacer.capacity, 16_000);
    assert_eq!(pacer.tokens, pacer.capacity);
}

#[test]
fn rapid_polls_preserve_sub_byte_refill_time() {
    let rtt = Duration::from_secs(1);
    let now = Instant::now();
    let mut pacer = Pacer::new(rtt, 1, 1, now);
    pacer.capacity = 1;
    pacer.tokens = 0;

    for tenth in 1..10 {
        assert!(pacer
            .delay(
                rtt,
                1,
                1,
                1,
                now + Duration::from_millis(tenth * 100),
                Some(1)
            )
            .is_some());
        assert_eq!(pacer.prev, now);
    }

    assert_eq!(
        pacer.delay(rtt, 1, 1, 1, now + Duration::from_secs(1), Some(1)),
        None,
    );
}
