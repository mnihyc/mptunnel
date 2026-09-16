//! Exhaustive finite check of the ACK snapshot cleanup transition.
use super::*;

#[test]
fn ack_snapshot_cleanup_preserves_original_transition_exhaustively() {
    let at = Instant::now();
    let spaces = [SpaceId::Initial, SpaceId::Handshake, SpaceId::Data];
    let mut config = Bbr3Config::default();
    config.probe_rng_seed = Some([0x5a; 16]);
    config.loss_compensation_floor(0.0);
    let mut template = Bbr3::new(Arc::new(config), BASE_DATAGRAM_SIZE as u16);
    for space in spaces {
        for number in 0..5 {
            <Bbr3 as Controller>::on_packet_sent(
                &mut template,
                at,
                BASE_DATAGRAM_SIZE as u16,
                number * BASE_DATAGRAM_SIZE,
                number,
                space,
                false,
            );
        }
    }
    // All 8 combinations of the three flags, not merely reachable combinations.
    // Distinct packet spaces deliberately contain overlapping packet numbers.
    for len in 0..=5usize {
        for pattern in 0..(1usize << (3 * len)) {
            let mut bbr = template.clone();
            for space in spaces {
                let packets = &mut bbr.packets[space as usize];
                packets.truncate(len);
                for (index, packet) in packets.iter_mut().enumerate() {
                    let flags = (pattern >> (3 * index)) & 7;
                    packet.acknowledged = flags & 1 != 0;
                    packet.stale = flags & 2 != 0;
                    packet.retired = flags & 4 != 0;
                }
            }
            let mut expected = bbr.packets.clone();
            for packets in &mut expected {
                packets.retain(|p| !p.stale);
                for packet in packets {
                    if packet.acknowledged {
                        packet.stale = true;
                    }
                }
            }
            // No on_ack was called, so ack_epoch_open is false. This runs the
            // real storage cleanup without running unrelated model updates.
            bbr.on_end_acks(at, 0, false, Some(0), SpaceId::Data);
            assert_eq!(
                format!("{:?}", bbr.packets),
                format!("{expected:?}"),
                "length={len}, flag pattern={pattern}",
            );
        }
    }
}

/// Recreate the same logical history at an explicit ring head without relying on
/// VecDeque::clone to preserve physical layout.
fn history_at_head(packets: &[BbrPacket], head: usize) -> VecDeque<BbrPacket> {
    assert!(!packets.is_empty());
    let mut deque = VecDeque::with_capacity(packets.len() + 1);
    assert!(head < deque.capacity());
    deque.resize(deque.capacity(), packets[0]);
    for _ in 0..head {
        deque.pop_front();
    }
    while deque.pop_back().is_some() {}
    deque.extend(packets.iter().copied());
    deque
}

fn snapshot_template(at: Instant, count: u64) -> Bbr3 {
    let mut config = Bbr3Config::default();
    config.probe_rng_seed = Some([0x5a; 16]);
    config.loss_compensation_floor(0.0);
    let mut bbr = Bbr3::new(Arc::new(config), BASE_DATAGRAM_SIZE as u16);
    for space in [SpaceId::Initial, SpaceId::Handshake, SpaceId::Data] {
        for number in 0..count {
            <Bbr3 as Controller>::on_packet_sent(
                &mut bbr,
                at,
                BASE_DATAGRAM_SIZE as u16,
                number * BASE_DATAGRAM_SIZE,
                number,
                space,
                false,
            );
        }
    }
    bbr
}

#[test]
fn ack_snapshot_cleanup_matches_oracle_at_every_short_ring_head() {
    let at = Instant::now();
    let template = snapshot_template(at, 4);
    let mut cases = 0;
    let mut wrapped_cases = 0;
    for len in 1..=4 {
        for head in 0..=len {
            for pattern in 0..(1usize << (3 * len)) {
                let mut bbr = template.clone();
                for (space_index, packets) in bbr.packets.iter_mut().enumerate() {
                    let mut logical: Vec<_> = packets.iter().copied().take(len).collect();
                    for (index, packet) in logical.iter_mut().enumerate() {
                        // Rotate flag assignments between spaces: overlapping packet numbers
                        // must not imply the same ACK/stale/retirement state.
                        let flags = (pattern >> (3 * ((index + space_index) % len))) & 7;
                        packet.acknowledged = flags & 1 != 0;
                        packet.stale = flags & 2 != 0;
                        packet.retired = flags & 4 != 0;
                    }
                    *packets = history_at_head(&logical, head);
                    if !packets.as_slices().1.is_empty() {
                        wrapped_cases += 1;
                    }
                }
                let mut expected = bbr.packets.clone();
                for packets in &mut expected {
                    packets.retain(|packet| !packet.stale);
                    for packet in packets {
                        if packet.acknowledged {
                            packet.stale = true;
                        }
                    }
                }
                bbr.on_end_acks(at, 0, false, Some(0), SpaceId::Data);
                assert_eq!(
                    format!("{:?}", bbr.packets),
                    format!("{expected:?}"),
                    "length={len}, head={head}, flags={pattern}",
                );
                for space in [SpaceId::Initial, SpaceId::Handshake, SpaceId::Data] {
                    for number in 0..len as u64 {
                        let expected_index = expected[space as usize]
                            .iter()
                            .position(|packet| packet.packet_number == number && !packet.retired);
                        assert_eq!(bbr.packet_index(space, number), expected_index);
                    }
                }
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 22_736);
    assert_eq!(wrapped_cases, 40_128);
}

#[test]
fn ack_snapshot_prefix_retirement_preserves_current_ack_loss_and_ecn_evidence() {
    let sent = Instant::now();
    let rtt = RttEstimator::new(Duration::from_millis(100));
    for active_space in [SpaceId::Initial, SpaceId::Handshake, SpaceId::Data] {
        let mut bbr = snapshot_template(sent, 8);
        for packets in &mut bbr.packets {
            let logical: Vec<_> = packets.iter().copied().collect();
            *packets = history_at_head(&logical, 7);
            assert!(!packets.as_slices().1.is_empty());
        }
        let first_ack = sent + Duration::from_millis(100);
        bbr.on_ack(
            first_ack,
            sent,
            BASE_DATAGRAM_SIZE,
            0,
            active_space,
            false,
            &rtt,
        );
        bbr.on_end_acks(
            first_ack,
            23 * BASE_DATAGRAM_SIZE,
            false,
            Some(0),
            active_space,
        );
        assert!(bbr.packets[active_space as usize][0].stale);

        let second_ack = first_ack + Duration::from_millis(10);
        for number in [1, 3, 5] {
            bbr.on_ack(
                second_ack,
                sent,
                BASE_DATAGRAM_SIZE,
                number,
                active_space,
                false,
                &rtt,
            );
        }
        bbr.on_end_acks(
            second_ack,
            20 * BASE_DATAGRAM_SIZE,
            false,
            Some(5),
            active_space,
        );
        assert_eq!(bbr.packet_index(active_space, 0), None);
        for number in [1, 3, 5] {
            let index = bbr
                .packet_index(active_space, number)
                .expect("current ACK snapshot");
            assert!(bbr.packets[active_space as usize][index].stale);
        }
        // Native callback order is ACK completion, terminal losses, then ECN.
        // An interior lost packet must not remove this ACK's neighboring evidence.
        bbr.on_packet_lost(BASE_DATAGRAM_SIZE as u16, 2, active_space, second_ack);
        assert_eq!(bbr.packet_index(active_space, 2), None);
        assert!(bbr.packet_retirement_pending[active_space as usize]);
        bbr.on_congestion_event(
            second_ack,
            sent,
            false,
            false,
            BASE_DATAGRAM_SIZE,
            2,
            active_space,
        );
        assert!(!bbr.packet_retirement_pending[active_space as usize]);
        assert!(bbr.packet_index(active_space, 5).is_some());
        bbr.on_congestion_event(second_ack, sent, false, true, 0, 5, active_space);
        assert_eq!(bbr.packet_index(active_space, 5), None);
        for number in [1, 3] {
            assert!(bbr.packet_index(active_space, number).is_some());
        }

        let third_ack = second_ack + Duration::from_millis(10);
        bbr.on_ack(
            third_ack,
            sent,
            BASE_DATAGRAM_SIZE,
            7,
            active_space,
            false,
            &rtt,
        );
        bbr.on_end_acks(
            third_ack,
            18 * BASE_DATAGRAM_SIZE,
            false,
            Some(7),
            active_space,
        );
        let retained: Vec<_> = bbr.packets[active_space as usize]
            .iter()
            .map(|packet| packet.packet_number)
            .collect();
        assert_eq!(retained, [4, 6, 7]);
        let current = bbr
            .packet_index(active_space, 7)
            .expect("third ACK evidence");
        assert!(bbr.packets[active_space as usize][current].stale);
        for other in [SpaceId::Initial, SpaceId::Handshake, SpaceId::Data] {
            if other != active_space {
                assert_eq!(bbr.packets[other as usize].len(), 8);
                assert!(bbr.packets[other as usize]
                    .iter()
                    .all(|packet| !packet.stale));
            }
        }
    }
}
