use super::*;
use crate::Duration;
use crate::MAX_UDP_PAYLOAD;
use crate::packet::SpaceId;
use assert_matches::assert_matches;

fn default_mtud() -> MtuDiscovery {
    let config = MtuDiscoveryConfig::default();
    MtuDiscovery::new(1_200, 1_200, None, config)
}

fn completed(mtud: &MtuDiscovery) -> bool {
    matches!(mtud.state.as_ref().unwrap().phase, Phase::Complete(_))
}

/// Drives mtud until it reaches `Phase::Completed`
fn drive_to_completion(
    mtud: &mut MtuDiscovery,
    now: Instant,
    link_payload_size_limit: u16,
) -> Vec<u16> {
    let mut probed_sizes = Vec::new();
    for probe_pn in 1..100 {
        let result = mtud.poll_transmit(now, probe_pn);

        if completed(mtud) {
            break;
        }

        // "Send" next probe
        assert!(result.is_some());
        let probe_size = result.unwrap();
        probed_sizes.push(probe_size);

        if probe_size <= link_payload_size_limit {
            mtud.on_acked(SpaceId::Data, probe_pn, probe_size);
        } else {
            mtud.on_probe_lost();
        }
    }
    probed_sizes
}

#[test]
fn black_hole_detector_ignores_burst_containing_non_suspicious_packet() {
    let mut mtud = default_mtud();
    mtud.on_non_probe_lost(2, 1300);
    mtud.on_non_probe_lost(3, 1300);
    assert_eq!(mtud.black_hole_detector.largest_non_probe_lost(), Some(3));
    assert_eq!(mtud.black_hole_detector.suspicious_loss_burst_count(), 0);

    mtud.on_non_probe_lost(4, 800);
    assert!(!mtud.black_hole_detected(Instant::now()));
    assert_eq!(mtud.black_hole_detector.largest_non_probe_lost(), None);
    assert_eq!(mtud.black_hole_detector.suspicious_loss_burst_count(), 0);
}

#[test]
fn black_hole_detector_counts_burst_containing_only_suspicious_packets() {
    let mut mtud = default_mtud();
    mtud.on_non_probe_lost(2, 1300);
    mtud.on_non_probe_lost(3, 1300);
    assert_eq!(mtud.black_hole_detector.largest_non_probe_lost(), Some(3));
    assert_eq!(mtud.black_hole_detector.suspicious_loss_burst_count(), 0);

    assert!(!mtud.black_hole_detected(Instant::now()));
    assert_eq!(mtud.black_hole_detector.largest_non_probe_lost(), None);
    assert_eq!(mtud.black_hole_detector.suspicious_loss_burst_count(), 1);
}

#[test]
fn black_hole_detector_ignores_empty_burst() {
    let mut mtud = default_mtud();
    assert!(!mtud.black_hole_detected(Instant::now()));
    assert_eq!(mtud.black_hole_detector.suspicious_loss_burst_count(), 0);
}

#[test]
fn mtu_discovery_disabled_does_nothing() {
    let mut mtud = MtuDiscovery::disabled(1_200, 1_200);
    let probe_size = mtud.poll_transmit(Instant::now(), 0);
    assert_eq!(probe_size, None);
}

#[test]
fn mtu_discovery_disabled_lost_four_packet_bursts_triggers_black_hole_detection() {
    let mut mtud = MtuDiscovery::disabled(1_400, 1_250);
    let now = Instant::now();

    for i in 0..4 {
        // The packets are never contiguous, so each one has its own burst
        mtud.on_non_probe_lost(i * 2, 1300);
    }

    assert!(mtud.black_hole_detected(now));
    assert_eq!(mtud.current_mtu, 1250);
    assert_matches!(mtud.state, None);
}

#[test]
fn mtu_discovery_lost_two_packet_bursts_does_not_trigger_black_hole_detection() {
    let mut mtud = default_mtud();
    let now = Instant::now();

    for i in 0..2 {
        mtud.on_non_probe_lost(i, 1300);
        assert!(!mtud.black_hole_detected(now));
    }
}

#[test]
fn mtu_discovery_lost_four_packet_bursts_triggers_black_hole_detection_and_resets_timer() {
    let mut mtud = default_mtud();
    let now = Instant::now();

    for i in 0..4 {
        // The packets are never contiguous, so each one has its own burst
        mtud.on_non_probe_lost(i * 2, 1300);
    }

    assert!(mtud.black_hole_detected(now));
    assert_eq!(mtud.current_mtu, 1200);
    if let Phase::Complete(next_mtud_activation) = mtud.state.unwrap().phase {
        assert_eq!(next_mtud_activation, now + Duration::from_secs(60));
    } else {
        panic!("Unexpected MTUD phase!");
    }
}

#[test]
fn mtu_discovery_after_complete_reactivates_when_interval_elapsed() {
    let mut config = MtuDiscoveryConfig::default();
    config.upper_bound(9_000);
    let mut mtud = MtuDiscovery::new(1_200, 1_200, None, config);
    let now = Instant::now();
    drive_to_completion(&mut mtud, now, 1_500);

    // Polling right after completion does not cause new packets to be sent
    assert_eq!(mtud.poll_transmit(now, 42), None);
    assert!(completed(&mtud));
    assert_eq!(mtud.current_mtu, 1_471);

    // Polling after the interval has passed does (taking the current mtu as lower bound)
    assert_eq!(
        mtud.poll_transmit(now + Duration::from_secs(600), 43),
        Some(5235)
    );

    match mtud.state.unwrap().phase {
        Phase::Searching(state) => {
            assert_eq!(state.lower_bound, 1_471);
            assert_eq!(state.upper_bound, 9_000);
        }
        _ => {
            panic!("Unexpected MTUD phase!")
        }
    }
}

#[test]
fn mtu_discovery_lost_three_probes_lowers_probe_size() {
    let mut mtud = default_mtud();

    let mut probe_sizes = (0..4).map(|i| {
        let probe_size = mtud.poll_transmit(Instant::now(), i);
        assert!(probe_size.is_some(), "no probe returned for packet {i}");

        mtud.on_probe_lost();
        probe_size.unwrap()
    });

    // After the first probe is lost, it gets retransmitted twice
    let first_probe_size = probe_sizes.next().unwrap();
    for _ in 0..2 {
        assert_eq!(probe_sizes.next().unwrap(), first_probe_size)
    }

    // After the third probe is lost, we decrement our probe size
    let fourth_probe_size = probe_sizes.next().unwrap();
    assert!(fourth_probe_size < first_probe_size);
    assert_eq!(
        fourth_probe_size,
        first_probe_size - (first_probe_size - 1_200) / 2 - 1
    );
}

#[test]
fn mtu_discovery_with_peer_max_udp_payload_size_clamps_upper_bound() {
    let mut mtud = default_mtud();

    mtud.on_peer_max_udp_payload_size_received(1300);
    let probed_sizes = drive_to_completion(&mut mtud, Instant::now(), 1500);

    assert_eq!(mtud.state.as_ref().unwrap().peer_max_udp_payload_size, 1300);
    assert_eq!(mtud.current_mtu, 1300);
    let expected_probed_sizes = &[1250, 1275, 1300];
    assert_eq!(probed_sizes, expected_probed_sizes);
    assert!(completed(&mtud));
}

#[test]
fn mtu_discovery_with_previous_peer_max_udp_payload_size_clamps_upper_bound() {
    let mut mtud = MtuDiscovery::new(1500, 1_200, Some(1400), MtuDiscoveryConfig::default());

    assert_eq!(mtud.current_mtu, 1400);
    assert_eq!(mtud.state.as_ref().unwrap().peer_max_udp_payload_size, 1400);

    let probed_sizes = drive_to_completion(&mut mtud, Instant::now(), 1500);

    assert_eq!(mtud.current_mtu, 1400);
    assert!(probed_sizes.is_empty());
    assert!(completed(&mtud));
}

#[cfg(debug_assertions)]
#[test]
#[should_panic]
fn mtu_discovery_with_peer_max_udp_payload_size_after_search_panics() {
    let mut mtud = default_mtud();
    drive_to_completion(&mut mtud, Instant::now(), 1500);
    mtud.on_peer_max_udp_payload_size_received(1300);
}

#[test]
fn mtu_discovery_with_1500_limit() {
    let mut mtud = default_mtud();

    let probed_sizes = drive_to_completion(&mut mtud, Instant::now(), 1500);

    let expected_probed_sizes = &[1326, 1389, 1420, 1452];
    assert_eq!(probed_sizes, expected_probed_sizes);
    assert_eq!(mtud.current_mtu, 1452);
    assert!(completed(&mtud));
}

#[test]
fn mtu_discovery_with_1500_limit_and_10000_upper_bound() {
    let mut config = MtuDiscoveryConfig::default();
    config.upper_bound(10_000);
    let mut mtud = MtuDiscovery::new(1_200, 1_200, None, config);

    let probed_sizes = drive_to_completion(&mut mtud, Instant::now(), 1500);

    let expected_probed_sizes = &[
        5600, 5600, 5600, 3399, 3399, 3399, 2299, 2299, 2299, 1749, 1749, 1749, 1474, 1611,
        1611, 1611, 1542, 1542, 1542, 1507, 1507, 1507,
    ];
    assert_eq!(probed_sizes, expected_probed_sizes);
    assert_eq!(mtud.current_mtu, 1474);
    assert!(completed(&mtud));
}

#[test]
fn mtu_discovery_no_lost_probes_finds_maximum_udp_payload() {
    let mut config = MtuDiscoveryConfig::default();
    config.upper_bound(MAX_UDP_PAYLOAD);
    let mut mtud = MtuDiscovery::new(1200, 1200, None, config);

    drive_to_completion(&mut mtud, Instant::now(), u16::MAX);

    assert_eq!(mtud.current_mtu, 65527);
    assert!(completed(&mtud));
}

#[test]
fn mtu_discovery_lost_half_of_probes_finds_maximum_udp_payload() {
    let mut config = MtuDiscoveryConfig::default();
    config.upper_bound(MAX_UDP_PAYLOAD);
    let mut mtud = MtuDiscovery::new(1200, 1200, None, config);

    let now = Instant::now();
    let mut iterations = 0;
    for i in 1..100 {
        iterations += 1;

        let probe_pn = i * 2 - 1;
        let other_pn = i * 2;

        let result = mtud.poll_transmit(Instant::now(), probe_pn);

        if completed(&mtud) {
            break;
        }

        // "Send" next probe
        assert!(result.is_some());
        assert!(mtud.in_flight_mtu_probe().is_some());

        // Nothing else to send while the probe is in-flight
        assert_matches!(mtud.poll_transmit(now, other_pn), None);

        if i % 2 == 0 {
            // ACK probe and ensure it results in an increase of current_mtu
            let previous_max_size = mtud.current_mtu;
            mtud.on_acked(SpaceId::Data, probe_pn, result.unwrap());
            println!(
                "ACK packet {}. Previous MTU = {previous_max_size}. New MTU = {}",
                result.unwrap(),
                mtud.current_mtu
            );
            // assert!(mtud.current_mtu > previous_max_size);
        } else {
            mtud.on_probe_lost();
        }
    }

    assert_eq!(iterations, 25);
    assert_eq!(mtud.current_mtu, 65527);
    assert!(completed(&mtud));
}

#[test]
fn search_state_lower_bound_higher_than_upper_bound_clamps_upper_bound() {
    let mut config = MtuDiscoveryConfig::default();
    config.upper_bound(1400);

    let state = SearchState::new(1500, u16::MAX, &config);
    assert_eq!(state.lower_bound, 1500);
    assert_eq!(state.upper_bound, 1500);
}

#[test]
fn search_state_lower_bound_higher_than_peer_max_udp_payload_size_clamps_lower_bound() {
    let mut config = MtuDiscoveryConfig::default();
    config.upper_bound(9000);

    let state = SearchState::new(1500, 1300, &config);
    assert_eq!(state.lower_bound, 1300);
    assert_eq!(state.upper_bound, 1300);
}

#[test]
fn search_state_upper_bound_higher_than_peer_max_udp_payload_size_clamps_upper_bound() {
    let mut config = MtuDiscoveryConfig::default();
    config.upper_bound(9000);

    let state = SearchState::new(1200, 1450, &config);
    assert_eq!(state.lower_bound, 1200);
    assert_eq!(state.upper_bound, 1450);
}

// Loss of packets larger than have been acknowledged should indicate a black hole
#[test]
fn simple_black_hole_detection() {
    let mut bhd = BlackHoleDetector::new(1200);
    bhd.on_non_probe_acked((BLACK_HOLE_THRESHOLD + 1) as u64 * 2, 1300);
    for i in 0..BLACK_HOLE_THRESHOLD {
        bhd.on_non_probe_lost(i as u64 * 2, 1400);
    }
    // But not before `BLACK_HOLE_THRESHOLD + 1` bursts
    assert!(!bhd.black_hole_detected());
    bhd.on_non_probe_lost(BLACK_HOLE_THRESHOLD as u64 * 2, 1400);
    assert!(bhd.black_hole_detected());
}

// Loss of packets followed in transmission order by confirmation of a larger packet should not
// indicate a black hole
#[test]
fn non_suspicious_bursts() {
    let mut bhd = BlackHoleDetector::new(1200);
    bhd.on_non_probe_acked((BLACK_HOLE_THRESHOLD + 1) as u64 * 2, 1500);
    for i in 0..(BLACK_HOLE_THRESHOLD + 1) {
        bhd.on_non_probe_lost(i as u64 * 2, 1400);
    }
    assert!(!bhd.black_hole_detected());
}

// Loss of packets smaller than have been acknowledged previously should still indicate a black
// hole
#[test]
fn dynamic_mtu_reduction() {
    let mut bhd = BlackHoleDetector::new(1200);
    bhd.on_non_probe_acked(0, 1500);
    for i in 0..(BLACK_HOLE_THRESHOLD + 1) {
        bhd.on_non_probe_lost(i as u64 * 2, 1400);
    }
    assert!(bhd.black_hole_detected());
}

// Bursts containing heterogeneous packets are judged based on the smallest
#[test]
fn mixed_non_suspicious_bursts() {
    let mut bhd = BlackHoleDetector::new(1200);
    bhd.on_non_probe_acked((BLACK_HOLE_THRESHOLD + 1) as u64 * 3, 1400);
    for i in 0..(BLACK_HOLE_THRESHOLD + 1) {
        bhd.on_non_probe_lost(i as u64 * 3, 1500);
        bhd.on_non_probe_lost(i as u64 * 3 + 1, 1300);
    }
    assert!(!bhd.black_hole_detected());
}

// Multi-packet bursts are only counted once
#[test]
fn bursts_count_once() {
    let mut bhd = BlackHoleDetector::new(1200);
    bhd.on_non_probe_acked((BLACK_HOLE_THRESHOLD + 1) as u64 * 3, 1400);
    for i in 0..(BLACK_HOLE_THRESHOLD) {
        bhd.on_non_probe_lost(i as u64 * 3, 1500);
        bhd.on_non_probe_lost(i as u64 * 3 + 1, 1500);
    }
    assert!(!bhd.black_hole_detected());
    bhd.on_non_probe_lost(BLACK_HOLE_THRESHOLD as u64 * 3, 1500);
    assert!(bhd.black_hole_detected());
}

// Non-suspicious bursts don't interfere with detection of suspicious bursts
#[test]
fn interleaved_bursts() {
    let mut bhd = BlackHoleDetector::new(1200);
    bhd.on_non_probe_acked((BLACK_HOLE_THRESHOLD + 1) as u64 * 4, 1400);
    for i in 0..(BLACK_HOLE_THRESHOLD + 1) {
        bhd.on_non_probe_lost(i as u64 * 4, 1500);
        bhd.on_non_probe_lost(i as u64 * 4 + 2, 1300);
    }
    assert!(bhd.black_hole_detected());
}

// Bursts that are non-suspicious before a delivered packet become suspicious past it
#[test]
fn suspicious_after_acked() {
    let mut bhd = BlackHoleDetector::new(1200);
    bhd.on_non_probe_acked((BLACK_HOLE_THRESHOLD + 1) as u64 * 2, 1400);
    for i in 0..(BLACK_HOLE_THRESHOLD + 1) {
        bhd.on_non_probe_lost(i as u64 * 2, 1300);
    }
    assert!(
        !bhd.black_hole_detected(),
        "1300 byte losses preceding a 1400 byte delivery are not suspicious"
    );
    for i in 0..(BLACK_HOLE_THRESHOLD + 1) {
        bhd.on_non_probe_lost((BLACK_HOLE_THRESHOLD as u64 + 1 + i as u64) * 2, 1300);
    }
    assert!(
        bhd.black_hole_detected(),
        "1300 byte losses following a 1400 byte delivery are suspicious"
    );
}

// Acknowledgment of a packet marks prior loss bursts with the same packet size as
// non-suspicious
#[test]
fn retroactively_non_suspicious() {
    let mut bhd = BlackHoleDetector::new(1200);
    for i in 0..BLACK_HOLE_THRESHOLD {
        bhd.on_non_probe_lost(i as u64 * 2, 1400);
    }
    bhd.on_non_probe_acked(BLACK_HOLE_THRESHOLD as u64 * 2, 1400);
    bhd.on_non_probe_lost(BLACK_HOLE_THRESHOLD as u64 * 2 + 1, 1400);
    assert!(!bhd.black_hole_detected());
}
