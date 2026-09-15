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
