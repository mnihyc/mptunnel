//! Rate-evidence regressions through production Controller callbacks.
//! Logical flight times isolate the estimator; this does not model native pacing or the wire.
use super::*;

fn controller(loss_floor: f64) -> Bbr3 {
    let mut config = Bbr3Config::default();
    config.probe_rng_seed = Some([0x5a; 16]);
    config.loss_compensation_floor(loss_floor);
    Bbr3::new(Arc::new(config), BASE_DATAGRAM_SIZE as u16)
}

/// Send a congestion-window-sized, backlogged flight and ACK the whole flight.
/// Packet numbers are monotone; prior_in_flight is the actual fixture ledger.
fn complete_flight(
    bbr: &mut Bbr3,
    next_packet: &mut u64,
    sent: Instant,
    delay: Duration,
) -> Instant {
    let packet_count = bbr.window() / BASE_DATAGRAM_SIZE;
    assert!(packet_count > 0);
    let first = *next_packet;
    let delivered_before = bbr.delivered;
    for index in 0..packet_count {
        <Bbr3 as Controller>::on_packet_sent(
            bbr,
            sent,
            BASE_DATAGRAM_SIZE as u16,
            index * BASE_DATAGRAM_SIZE,
            first + index,
            SpaceId::Data,
            false,
        );
    }
    bbr.on_cwnd_limited();
    let now = sent + delay;
    let rtt = RttEstimator::new(Duration::from_millis(100));
    for index in 0..packet_count {
        bbr.on_ack(
            now,
            sent,
            BASE_DATAGRAM_SIZE,
            first + index,
            SpaceId::Data,
            false,
            &rtt,
        );
    }
    *next_packet += packet_count;
    bbr.on_end_acks(now, 0, false, Some(*next_packet - 1), SpaceId::Data);
    // Unusable rate evidence must not discard real ACK delivery or RTT learning.
    let sample = bbr.rs.expect("this flight produces an ACK sample");
    let delivered_bytes = packet_count * BASE_DATAGRAM_SIZE;
    assert_eq!(bbr.delivered, delivered_before + delivered_bytes);
    assert_eq!(bbr.delivered_time, Some(now));
    assert_eq!(sample.prior_delivered, delivered_before);
    assert_eq!(sample.delivered, delivered_bytes);
    assert_eq!(sample.rtt, delay);
    assert_eq!(bbr.inflight, 0);
    now
}

#[test]
fn invalid_rate_rounds_preserve_plateau_evidence_and_valid_rounds_resume_acquisition() {
    for loss_floor in [0.0, 0.1, 0.2] {
        let mut bbr = controller(loss_floor);
        let mut next_packet = 0;
        let mut sent = Instant::now();
        for (index, delay_ms) in [100, 90, 80, 70].into_iter().enumerate() {
            let baseline_before = bbr.full_bw;
            let count_before = bbr.full_bw_count;
            let now = complete_flight(
                &mut bbr,
                &mut next_packet,
                sent,
                Duration::from_millis(delay_ms),
            );
            let sample = bbr.rs.expect("this ACK generates a sample");
            let published = bbr
                .latest_completed_bandwidth_sample
                .expect("sample metadata");
            assert!(!sample.is_app_limited);
            assert!(bbr.round_start);
            assert_eq!(published.valid, index == 0);
            assert_eq!(bbr.min_rtt, Duration::from_millis(delay_ms));
            if index == 0 {
                assert!(sample.delivery_rate > 0.0);
                assert_eq!(bbr.full_bw, sample.delivery_rate);
                assert_eq!(bbr.full_bw_count, 0);
            } else {
                assert_eq!(sample.delivery_rate, 0.0);
                assert_eq!(bbr.full_bw, baseline_before);
                assert_eq!(bbr.full_bw_count, count_before);
            }
            sent = now + Duration::from_millis(1);
        }
        assert!(
            !bbr.full_bw_reached,
            "rejected rate samples must not vote for a plateau"
        );
        assert_eq!(bbr.full_bw_count, 0);
        assert_eq!(bbr.state, BbrState::Startup);

        // A subsequent interval at the learned minimum RTT is usable again.
        // Keep this continuation short; the separate plateau test covers exit.
        let baseline_before = bbr.full_bw;
        let delay = bbr.min_rtt;
        complete_flight(&mut bbr, &mut next_packet, sent, delay);
        let sample = bbr.rs.unwrap();
        assert!(bbr.latest_completed_bandwidth_sample.unwrap().valid);
        assert!(!sample.is_app_limited);
        assert!(sample.delivery_rate >= baseline_before * FULL_BW_GROWTH);
        assert_eq!(bbr.full_bw, sample.delivery_rate);
        assert_eq!(bbr.full_bw_count, 0);
        assert!(!bbr.full_bw_reached);
        assert_eq!(bbr.state, BbrState::Startup);
    }
}

#[test]
fn valid_rate_plateau_exits_startup() {
    for loss_floor in [0.0, 0.1, 0.2] {
        let mut bbr = controller(loss_floor);
        let mut next_packet = 0;
        let mut sent = Instant::now();
        for _ in 0..4 {
            let packet_count = bbr.window() / BASE_DATAGRAM_SIZE;
            // Exactly BASE_DATAGRAM_SIZE / 10ms of delivery service, even as
            // the controller's offered flight grows. No rate is injected.
            let delay = Duration::from_millis(packet_count * 10);
            let now = complete_flight(&mut bbr, &mut next_packet, sent, delay);
            assert!(bbr.latest_completed_bandwidth_sample.unwrap().valid);
            assert!(!bbr.rs.unwrap().is_app_limited);
            assert_eq!(
                bbr.rs.unwrap().delivery_rate.round() as u64,
                BASE_DATAGRAM_SIZE * 100,
            );
            sent = now + Duration::from_millis(1);
        }
        assert!(bbr.full_bw_reached);
        assert_ne!(bbr.state, BbrState::Startup);
    }
}
