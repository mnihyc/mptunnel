use super::*;

#[derive(Clone, Copy)]
struct Sent {
    packet_number: u64,
    app_limited: bool,
    delivery_state: PacketDeliveryState,
}

fn send(
    estimator: &mut BandwidthEstimation,
    sent: Instant,
    bytes: u16,
    prior_in_flight: u64,
    packet_number: u64,
    app_limited: bool,
) -> Sent {
    Sent {
        packet_number,
        app_limited,
        delivery_state: estimator.on_packet_sent(
            sent,
            bytes,
            prior_in_flight,
            packet_number,
            app_limited,
        ),
    }
}

fn ack(
    estimator: &mut BandwidthEstimation,
    now: Instant,
    sent: Instant,
    bytes: u64,
    sent_packet: Sent,
) {
    estimator.on_ack(
        now,
        sent,
        bytes,
        sent_packet.packet_number,
        sent_packet.app_limited,
        sent_packet.delivery_state,
    );
}

#[test]
fn first_sample_includes_the_propagation_interval() {
    let base = Instant::now();
    let mut estimator = BandwidthEstimation::default();
    let first = send(&mut estimator, base, 1_000, 0, 0, false);

    ack(
        &mut estimator,
        base + Duration::from_millis(100),
        base,
        1_000,
        first,
    );

    assert_eq!(
        estimator.end_acks(0, Duration::from_millis(100)),
        Some(false)
    );
    assert_eq!(estimator.get_estimate(), 10_000);
}

#[test]
fn stretched_ack_uses_the_newest_packets_send_clock() {
    let base = Instant::now();
    let mut estimator = BandwidthEstimation::default();
    let first = send(&mut estimator, base, 1_000, 0, 0, false);
    let second = send(
        &mut estimator,
        base + Duration::from_millis(100),
        1_000,
        1_000,
        1,
        false,
    );

    let now = base + Duration::from_millis(100);
    ack(&mut estimator, now, base, 1_000, first);
    ack(
        &mut estimator,
        now,
        base + Duration::from_millis(100),
        1_000,
        second,
    );

    assert_eq!(
        estimator.end_acks(0, Duration::from_millis(100)),
        Some(false)
    );
    assert_eq!(estimator.get_estimate(), 20_000);
}

#[test]
fn ack_clock_limits_a_compressed_send_interval() {
    let base = Instant::now();
    let mut estimator = BandwidthEstimation::default();
    let state = send(&mut estimator, base, 1_000, 0, 0, false);

    ack(
        &mut estimator,
        base + Duration::from_millis(100),
        base,
        1_000,
        state,
    );

    assert_eq!(
        estimator.end_acks(0, Duration::from_millis(100)),
        Some(false)
    );
    assert_eq!(estimator.get_estimate(), 10_000);
}

#[test]
fn lower_app_limited_sample_does_not_reduce_estimate() {
    let base = Instant::now();
    let mut estimator = BandwidthEstimation::default();
    let fast = send(&mut estimator, base, 10_000, 0, 0, false);
    ack(
        &mut estimator,
        base + Duration::from_millis(10),
        base,
        10_000,
        fast,
    );
    assert_eq!(
        estimator.end_acks(0, Duration::from_millis(10)),
        Some(false)
    );
    let established = estimator.get_estimate();

    let slow_sent = base + Duration::from_millis(20);
    let slow = send(&mut estimator, slow_sent, 1_000, 0, 1, true);
    ack(
        &mut estimator,
        slow_sent + Duration::from_millis(100),
        slow_sent,
        1_000,
        slow,
    );
    assert_eq!(estimator.end_acks(1, Duration::from_millis(10)), Some(true));
    assert_eq!(estimator.get_estimate(), established);
}

#[test]
fn lower_non_app_limited_samples_age_an_old_maximum() {
    let base = Instant::now();
    let mut estimator = BandwidthEstimation::default();
    let fast = send(&mut estimator, base, 10_000, 0, 0, false);
    ack(
        &mut estimator,
        base + Duration::from_millis(10),
        base,
        10_000,
        fast,
    );
    let _ = estimator.end_acks(0, Duration::from_millis(10));
    let compressed_maximum = estimator.get_estimate();

    for round in 1..=11 {
        let sent = base + Duration::from_millis(100 * round);
        let state = send(&mut estimator, sent, 1_000, 0, round, false);
        ack(
            &mut estimator,
            sent + Duration::from_millis(100),
            sent,
            1_000,
            state,
        );
        let _ = estimator.end_acks(round, Duration::from_millis(10));
    }

    assert!(estimator.get_estimate() < compressed_maximum);
}

#[test]
fn idle_restart_begins_a_new_delivery_interval() {
    let base = Instant::now();
    let mut estimator = BandwidthEstimation::default();
    let first = send(&mut estimator, base, 1_000, 0, 0, false);
    ack(
        &mut estimator,
        base + Duration::from_millis(100),
        base,
        1_000,
        first,
    );
    let _ = estimator.end_acks(0, Duration::from_millis(100));

    let after_idle = base + Duration::from_secs(10);
    let state = send(&mut estimator, after_idle, 10_000, 0, 1, false);
    ack(
        &mut estimator,
        after_idle + Duration::from_millis(100),
        after_idle,
        10_000,
        state,
    );
    let _ = estimator.end_acks(1, Duration::from_millis(100));

    assert_eq!(estimator.get_estimate(), 100_000);
}

#[test]
fn higher_packet_number_wins_when_send_times_match() {
    let base = Instant::now();
    let mut estimator = BandwidthEstimation::default();
    let first = send(&mut estimator, base, 1_000, 0, 1, false);
    let second = send(&mut estimator, base, 1_000, 1_000, 2, true);
    let now = base + Duration::from_millis(100);

    ack(&mut estimator, now, base, 1_000, first);
    ack(&mut estimator, now, base, 1_000, second);

    assert_eq!(
        estimator.end_acks(0, Duration::from_millis(100)),
        Some(true)
    );
}

#[test]
fn sub_min_rtt_interval_is_not_a_rate_sample() {
    let base = Instant::now();
    let mut estimator = BandwidthEstimation::default();
    let state = send(&mut estimator, base, 1_000, 0, 0, false);
    ack(
        &mut estimator,
        base + Duration::from_millis(10),
        base,
        1_000,
        state,
    );

    assert_eq!(estimator.end_acks(0, Duration::from_millis(100)), None);
    assert_eq!(estimator.get_estimate(), 0);
}

#[test]
fn ack_without_packet_state_still_advances_delivery_accounting() {
    let now = Instant::now();
    let mut estimator = BandwidthEstimation::default();

    estimator.on_ack_without_packet_state(now, 1200);

    assert_eq!(estimator.bytes_acked_this_window(), 1200);
    assert_eq!(estimator.end_acks(0, Duration::from_millis(1)), None);
    assert_eq!(estimator.get_estimate(), 0);
}
