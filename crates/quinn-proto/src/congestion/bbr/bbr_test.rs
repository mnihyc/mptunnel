use super::*;

fn bbr_with_bandwidth(bytes_per_second: u64, min_rtt: Duration) -> Bbr {
    let mut bbr = Bbr::new(Arc::new(BbrConfig::default()), BASE_DATAGRAM_SIZE as u16);
    let sent = Instant::now();
    let mut remaining =
        ((u128::from(bytes_per_second) * min_rtt.as_nanos()) / 1_000_000_000) as u64;
    let mut prior_in_flight = 0;
    let mut packet_number = 0;
    let mut packets = Vec::new();
    while remaining != 0 {
        let bytes = remaining.min(u64::from(u16::MAX)) as u16;
        let state =
            bbr.max_bandwidth
                .on_packet_sent(sent, bytes, prior_in_flight, packet_number, false);
        packets.push((packet_number, bytes, state));
        prior_in_flight += u64::from(bytes);
        remaining -= u64::from(bytes);
        packet_number += 1;
    }
    for (packet_number, bytes, state) in packets {
        bbr.max_bandwidth.on_ack(
            sent + min_rtt,
            sent,
            u64::from(bytes),
            packet_number,
            false,
            state,
        );
    }
    let _ = bbr.max_bandwidth.end_acks(0, min_rtt);
    bbr.min_rtt = min_rtt;
    bbr
}

#[test]
fn startup_cwnd_stops_growing_above_target() {
    let mut bbr = bbr_with_bandwidth(10_000_000, Duration::from_millis(100));
    let target = bbr.get_target_cwnd(bbr.cwnd_gain);
    bbr.cwnd = target + BASE_DATAGRAM_SIZE;
    bbr.acked_bytes = bbr.init_cwnd;

    bbr.calculate_cwnd(BASE_DATAGRAM_SIZE, 0);

    assert_eq!(bbr.cwnd, target + BASE_DATAGRAM_SIZE);
}

#[test]
fn startup_cwnd_grows_toward_target() {
    let mut bbr = bbr_with_bandwidth(10_000_000, Duration::from_millis(100));
    let target = bbr.get_target_cwnd(bbr.cwnd_gain);
    bbr.cwnd = target - BASE_DATAGRAM_SIZE;
    bbr.acked_bytes = bbr.init_cwnd;

    bbr.calculate_cwnd(BASE_DATAGRAM_SIZE, 0);

    assert_eq!(bbr.cwnd, target);
}

#[test]
fn ordinary_recovery_does_not_end_startup_before_three_plateau_rounds() {
    let mut bbr = bbr_with_bandwidth(10_000_000, Duration::from_millis(100));
    bbr.bw_at_last_round = bbr.max_bandwidth.get_estimate();
    bbr.recovery_state = RecoveryState::Growth;

    bbr.check_if_full_bw_reached(false);

    assert_eq!(bbr.round_wo_bw_gain, 1);
    assert!(!bbr.is_at_full_bandwidth);

    bbr.check_if_full_bw_reached(false);
    bbr.check_if_full_bw_reached(false);

    assert!(bbr.is_at_full_bandwidth);
}
