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
                    packet.stale = u8::from(flags & 2 != 0);
                    packet.retired = flags & 4 != 0;
                }
            }
            let mut expected = bbr.packets.clone();
            for packets in &mut expected {
                packets.retain(|p| p.stale == 0);
                for packet in packets {
                    if packet.acknowledged {
                        packet.stale = 1;
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
                        packet.stale = u8::from(flags & 2 != 0);
                        packet.retired = flags & 4 != 0;
                    }
                    *packets = history_at_head(&logical, head);
                    if !packets.as_slices().1.is_empty() {
                        wrapped_cases += 1;
                    }
                }
                let mut expected = bbr.packets.clone();
                for packets in &mut expected {
                    packets.retain(|packet| packet.stale == 0);
                    for packet in packets {
                        if packet.acknowledged {
                            packet.stale = 1;
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
        assert_ne!(bbr.packets[active_space as usize][0].stale, 0);

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
            assert_ne!(bbr.packets[active_space as usize][index].stale, 0);
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
        assert_ne!(bbr.packets[active_space as usize][current].stale, 0);
        for other in [SpaceId::Initial, SpaceId::Handshake, SpaceId::Data] {
            if other != active_space {
                assert_eq!(bbr.packets[other as usize].len(), 8);
                assert!(bbr.packets[other as usize]
                    .iter()
                    .all(|packet| packet.stale == 0));
            }
        }
    }
}

/// Target-relative compatibility check: replacing `bool` with `u8` must not increase the
/// packet-record layout on this Rust target. `repr(Rust)` may differ between targets, so compare
/// the same ordered fields instead of asserting the C++ prototype's 96-byte size.
#[allow(dead_code)]
#[derive(Clone, Copy)]
struct OriginalBbrPacketLayout {
    delivered: u64,
    delivered_time: Instant,
    first_send_time: Instant,
    send_time: Instant,
    is_app_limited: bool,
    is_operational_rtt_evidence: bool,
    tx_in_flight: u64,
    packet_number: u64,
    space: SpaceId,
    size: u16,
    lost: u64,
    acknowledged: bool,
    stale: bool,
    retired: bool,
    round_count: u64,
}

fn normalized_controller(mut controller: Bbr3) -> Bbr3 {
    let packed = controller.packet_epochs_enabled;
    let epoch = controller.packet_epoch;
    let next = epoch.wrapping_add(1);
    for space in 0..controller.packets.len() {
        let records = std::mem::take(&mut controller.packets[space]);
        let mut visible = VecDeque::new();
        for mut packet in records {
            let absent = if packed {
                Bbr3::packet_epoch_absent(&packet, epoch, next)
            } else {
                packet.retired
            };
            if absent {
                continue;
            }
            let stale = if packed {
                controller.packet_is_stale_snapshot(&packet)
            } else {
                packet.stale != 0
            };
            packet.stale = u8::from(stale);
            visible.push_back(packet);
        }
        controller.packets[space] = visible;
    }
    if let Some(mut sample) = controller.rs {
        // This snapshot carries delivery fields only; the source algorithm never reads the
        // packet-store lifetime marker from it.
        sample.last_packet.stale = 0;
        controller.rs = Some(sample);
    }
    controller.packet_epochs_enabled = false;
    controller.packet_epoch = 0;
    controller.packet_epoch_since_sweep = 0;
    controller.packet_epoch_current = [0; 3];
    controller.packet_epoch_previous = [0; 3];
    controller.packet_epoch_dead = [0; 3];
    controller.packet_epochs_disabled_for_test = false;
    controller.packet_cleanup_reference_visits = 0;
    controller.packet_epoch_cleanup_visits = 0;
    controller.packet_epoch_migration_visits = 0;
    controller
}

fn assert_controller_equivalent(original: &Bbr3, adaptive: &Bbr3, step: &str) {
    assert_eq!(original.window(), adaptive.window(), "window at {step}");
    assert_eq!(
        original.pacing_rate(),
        adaptive.pacing_rate(),
        "pacing at {step}"
    );
    assert_eq!(
        original.latest_bandwidth_sample(),
        adaptive.latest_bandwidth_sample(),
        "bandwidth sample at {step}"
    );
    let original = format!("{:?}", normalized_controller(original.clone()));
    let adaptive = format!("{:?}", normalized_controller(adaptive.clone()));
    if original != adaptive {
        let mismatch = original
            .bytes()
            .zip(adaptive.bytes())
            .position(|(left, right)| left != right)
            .unwrap_or_else(|| original.len().min(adaptive.len()));
        let field_start = original[..mismatch.min(original.len())]
            .rfind(", ")
            .map_or(0, |index| index + 2);
        let field_end = original[field_start..]
            .find(", ")
            .map_or(original.len(), |index| field_start + index);
        let other_end = adaptive[field_start.min(adaptive.len())..]
            .find(", ")
            .map_or(adaptive.len(), |index| {
                field_start.min(adaptive.len()) + index
            });
        panic!(
            "complete normalized controller state differs at {step}, near byte {mismatch}: left={} right={}",
            &original[field_start..field_end],
            &adaptive[field_start.min(adaptive.len())..other_end],
        );
    }
}

fn bbr_pair() -> (Bbr3, Bbr3) {
    let mut config = Bbr3Config::default();
    config.probe_rng_seed = Some([0x93; 16]);
    config.loss_compensation_floor(0.10);
    let mut original = Bbr3::new(Arc::new(config.clone()), BASE_DATAGRAM_SIZE as u16);
    original.packet_epochs_disabled_for_test = true;
    let controller = Bbr3::new(Arc::new(config), BASE_DATAGRAM_SIZE as u16);
    (original, controller)
}

#[test]
fn packed_epoch_tag_preserves_rust_packet_record_size() {
    let current = size_of::<BbrPacket>();
    let original = size_of::<OriginalBbrPacketLayout>();
    eprintln!("BbrPacket size on this Rust target: {current} bytes (original {original})");
    assert_eq!(current, original, "bool-to-u8 tag must not widen BbrPacket");
}

fn send_pair(original: &mut Bbr3, adaptive: &mut Bbr3, at: Instant, pn: u64, space: SpaceId) {
    let sent_time = at + Duration::from_micros(pn + (space as u64) * 1000);
    for controller in [original, adaptive] {
        <Bbr3 as Controller>::on_packet_sent(
            controller,
            sent_time,
            BASE_DATAGRAM_SIZE as u16,
            pn * BASE_DATAGRAM_SIZE,
            pn,
            space,
            false,
        );
    }
}

fn ack_pair(
    original: &mut Bbr3,
    adaptive: &mut Bbr3,
    rtt: &RttEstimator,
    at: Instant,
    pn: u64,
    space: SpaceId,
) {
    let sent_time = at + Duration::from_micros(pn + (space as u64) * 1000);
    for controller in [original, adaptive] {
        controller.on_ack(
            at + Duration::from_millis(100),
            sent_time,
            BASE_DATAGRAM_SIZE,
            pn,
            space,
            false,
            rtt,
        );
    }
}

fn end_pair(
    original: &mut Bbr3,
    adaptive: &mut Bbr3,
    at: Instant,
    largest: Option<u64>,
    space: SpaceId,
) {
    for controller in [original, adaptive] {
        controller.on_end_acks(
            at + Duration::from_millis(100),
            200 * BASE_DATAGRAM_SIZE,
            false,
            largest,
            space,
        );
    }
}

#[test]
fn adaptive_packet_epochs_match_controller_through_migration_idle_and_small_reuse() {
    let base = Instant::now();
    let rtt = RttEstimator::new(Duration::from_millis(100));
    let (mut original, mut adaptive) = bbr_pair();

    // Three disjoint packet-number spaces cross the upgrade boundary together. The initial ACK
    // becomes previous-epoch evidence; a no-largest-ACK callback leaves the Handshake ACK current.
    for space in [SpaceId::Initial, SpaceId::Handshake, SpaceId::Data] {
        for pn in 0..84 {
            send_pair(&mut original, &mut adaptive, base, pn, space);
        }
    }
    ack_pair(
        &mut original,
        &mut adaptive,
        &rtt,
        base,
        5,
        SpaceId::Initial,
    );
    end_pair(
        &mut original,
        &mut adaptive,
        base,
        Some(5),
        SpaceId::Initial,
    );
    ack_pair(
        &mut original,
        &mut adaptive,
        &rtt,
        base,
        6,
        SpaceId::Handshake,
    );
    end_pair(&mut original, &mut adaptive, base, None, SpaceId::Handshake);

    // Stage the callback's interior terminal marker while the no-largest batch is open, so the
    // conversion sees old-stale, newly acknowledged, and retired records in one in-place pass.
    for controller in [&mut original, &mut adaptive] {
        let packet = &mut controller.packets[SpaceId::Data as usize][20];
        packet.retired = true;
        controller.packet_retirement_pending[SpaceId::Data as usize] = true;
    }
    for pn in 84..89 {
        send_pair(&mut original, &mut adaptive, base, pn, SpaceId::Data);
    }
    assert!(adaptive.packet_epochs_enabled);
    assert_eq!(adaptive.packet_epoch_previous[SpaceId::Initial as usize], 1);
    assert_eq!(
        adaptive.packet_epoch_current[SpaceId::Handshake as usize],
        1
    );
    assert_eq!(adaptive.packet_epoch_dead[SpaceId::Data as usize], 1);
    assert_controller_equivalent(&original, &adaptive, "upgrade migration");
    assert!(adaptive.packet_index(SpaceId::Initial, 5).is_some());
    assert!(adaptive.packet_index(SpaceId::Handshake, 6).is_some());
    assert_eq!(adaptive.packet_index(SpaceId::Data, 20), None);

    end_pair(
        &mut original,
        &mut adaptive,
        base,
        Some(6),
        SpaceId::Handshake,
    );
    assert_controller_equivalent(&original, &adaptive, "epoch rotation");
    assert_eq!(adaptive.packet_index(SpaceId::Initial, 5), None);
    assert!(adaptive.packet_index(SpaceId::Handshake, 6).is_some());

    // A parked clone must carry the epoch/counters and handle a storage-only terminal identically.
    let mut parked_original = original.clone();
    let mut parked_adaptive = adaptive.clone();
    parked_original.on_packet_discarded(6, SpaceId::Handshake);
    parked_adaptive.on_packet_discarded(6, SpaceId::Handshake);
    assert_controller_equivalent(
        &parked_original,
        &parked_adaptive,
        "cloned storage-only discard",
    );

    // Same-ACK ECN consumes the current snapshot; a real loss and its terminal callback retire an
    // interior record without changing any model output.
    for controller in [&mut original, &mut adaptive] {
        controller.on_congestion_event(
            base + Duration::from_millis(200),
            base,
            false,
            true,
            0,
            6,
            SpaceId::Handshake,
        );
    }
    assert_controller_equivalent(&original, &adaptive, "same-ACK ECN");
    for controller in [&mut original, &mut adaptive] {
        controller.on_packet_lost(
            BASE_DATAGRAM_SIZE as u16,
            21,
            SpaceId::Data,
            base + Duration::from_millis(200),
        );
        controller.on_packet_discarded(21, SpaceId::Data);
    }
    assert_controller_equivalent(&original, &adaptive, "interior loss and discard");
    for controller in [&mut original, &mut adaptive] {
        controller.on_congestion_event(
            base + Duration::from_millis(200),
            base,
            false,
            false,
            BASE_DATAGRAM_SIZE,
            21,
            SpaceId::Data,
        );
    }
    assert_controller_equivalent(&original, &adaptive, "loss batch compaction");

    // Retire the large flight down to one current ACK snapshot. Packed representation stays active
    // through long idle; no-largest callbacks preserve that snapshot and do not advance its epoch.
    let browsing_at = base + Duration::from_millis(200);
    ack_pair(
        &mut original,
        &mut adaptive,
        &rtt,
        browsing_at,
        88,
        SpaceId::Data,
    );
    end_pair(
        &mut original,
        &mut adaptive,
        browsing_at,
        Some(88),
        SpaceId::Data,
    );
    let live: Vec<_> = original
        .packets
        .iter()
        .enumerate()
        .flat_map(|(space, packets)| {
            packets
                .iter()
                .filter(move |packet| {
                    !(space == SpaceId::Data as usize && packet.packet_number == 88)
                })
                .map(move |packet| (space, packet.packet_number))
        })
        .collect();
    for (index, (space, packet_number)) in live.into_iter().enumerate() {
        let space = [SpaceId::Initial, SpaceId::Handshake, SpaceId::Data][space];
        for controller in [&mut original, &mut adaptive] {
            controller.on_packet_discarded(packet_number, space);
        }
        if index % 32 == 0 {
            assert_controller_equivalent(&original, &adaptive, "large-to-small discard");
        }
    }
    assert_controller_equivalent(&original, &adaptive, "one-record store");
    assert!(adaptive.packet_epochs_enabled);
    assert_eq!(adaptive.packets.iter().map(VecDeque::len).sum::<usize>(), 1);
    let epoch_before_idle = adaptive.packet_epoch;
    for _ in 0..260 {
        end_pair(&mut original, &mut adaptive, base, None, SpaceId::Data);
    }
    assert_eq!(adaptive.packet_epoch, epoch_before_idle);
    assert!(adaptive.packet_index(SpaceId::Data, 88).is_some());
    assert_controller_equivalent(&original, &adaptive, "retained snapshot after idle");

    // Long-lived small browsing after bulk drain stays packed while a snapshot remains live;
    // only a completely empty store returns to the original representation.
    for pn in 89..349 {
        let at = browsing_at + Duration::from_millis(pn - 88);
        send_pair(&mut original, &mut adaptive, at, pn, SpaceId::Data);
        ack_pair(&mut original, &mut adaptive, &rtt, at, pn, SpaceId::Data);
        end_pair(&mut original, &mut adaptive, at, Some(pn), SpaceId::Data);
        assert_controller_equivalent(&original, &adaptive, "small active browsing");
    }
    assert!(adaptive.packet_epochs_enabled);
    assert!(adaptive.packet_epoch_since_sweep < PACKET_EPOCH_SWEEP_INTERVAL);
    assert!(adaptive.packets.iter().map(VecDeque::len).sum::<usize>() <= 2);
    adaptive.on_packet_discarded(348, SpaceId::Data);
    original.on_packet_discarded(348, SpaceId::Data);
    assert!(
        !adaptive.packet_epochs_enabled,
        "only a fully empty store demotes"
    );
    assert_controller_equivalent(&original, &adaptive, "empty lifetime boundary");
}

#[test]
fn packed_epoch_wrap_sweep_removes_interior_tombstone_before_tag_reuse() {
    let base = Instant::now();
    let rtt = RttEstimator::new(Duration::from_millis(100));
    let (mut original, mut adaptive) = bbr_pair();
    for pn in 0..257 {
        send_pair(&mut original, &mut adaptive, base, pn, SpaceId::Data);
    }
    assert!(adaptive.packet_epochs_enabled);
    ack_pair(&mut original, &mut adaptive, &rtt, base, 128, SpaceId::Data);
    end_pair(&mut original, &mut adaptive, base, Some(128), SpaceId::Data);
    end_pair(&mut original, &mut adaptive, base, Some(128), SpaceId::Data);
    assert!(adaptive.packet_epoch_dead[SpaceId::Data as usize] > 0);
    assert_controller_equivalent(&original, &adaptive, "interior tombstone before wrap sweep");

    // No new ACK records are added, so the one old interior tombstone remains below the 1/8
    // density threshold until the mandated 128th qualifying cleanup epoch.
    let mut wrap_checkpoints = 0;
    while adaptive.packet_epoch_since_sweep != 127 {
        end_pair(&mut original, &mut adaptive, base, Some(128), SpaceId::Data);
        assert_controller_equivalent(&original, &adaptive, "pre-sweep cleanup");
        wrap_checkpoints += 1;
        assert!(wrap_checkpoints < 128);
    }
    end_pair(&mut original, &mut adaptive, base, Some(128), SpaceId::Data);
    assert_eq!(adaptive.packet_epoch_since_sweep, 0);
    assert_eq!(adaptive.packet_epoch_dead[SpaceId::Data as usize], 0);
    assert_controller_equivalent(&original, &adaptive, "forced wrap sweep");
    for _ in 0..128 {
        end_pair(&mut original, &mut adaptive, base, Some(128), SpaceId::Data);
        assert_controller_equivalent(&original, &adaptive, "post-sweep tag reuse");
    }
    assert_eq!(
        adaptive.packet_epoch, 0,
        "global byte epoch wraps without stale resurrection"
    );
}

#[test]
fn packed_epochs_compact_before_deque_growth_and_settle_empty_pending_loss() {
    let base = Instant::now();
    let mut config = Bbr3Config::default();
    config.probe_rng_seed = Some([0x6d; 16]);
    config.loss_compensation_floor(0.0);
    let mut adaptive = Bbr3::new(Arc::new(config), BASE_DATAGRAM_SIZE as u16);
    let space = SpaceId::Data;
    let mut next_pn = 0;
    while adaptive.packets[space as usize].len() <= PACKET_EPOCH_UPGRADE_RECORDS {
        <Bbr3 as Controller>::on_packet_sent(
            &mut adaptive,
            base + Duration::from_micros(next_pn),
            BASE_DATAGRAM_SIZE as u16,
            next_pn * BASE_DATAGRAM_SIZE,
            next_pn,
            space,
            false,
        );
        next_pn += 1;
    }
    assert!(adaptive.packet_epochs_enabled);
    while adaptive.packets[space as usize].len() < adaptive.packets[space as usize].capacity() {
        <Bbr3 as Controller>::on_packet_sent(
            &mut adaptive,
            base + Duration::from_micros(next_pn),
            BASE_DATAGRAM_SIZE as u16,
            next_pn * BASE_DATAGRAM_SIZE,
            next_pn,
            space,
            false,
        );
        next_pn += 1;
    }
    let capacity = adaptive.packets[space as usize].capacity();
    assert_eq!(adaptive.packets[space as usize].len(), capacity);
    let interior = (capacity / 2) as u64;
    adaptive.on_packet_discarded(interior, space);
    assert_eq!(adaptive.packet_epoch_dead[space as usize], 1);
    assert!(adaptive.packet_epoch_dead[space as usize] < capacity.div_ceil(8));
    <Bbr3 as Controller>::on_packet_sent(
        &mut adaptive,
        base + Duration::from_micros(next_pn),
        BASE_DATAGRAM_SIZE as u16,
        next_pn * BASE_DATAGRAM_SIZE,
        next_pn,
        space,
        false,
    );
    assert_eq!(adaptive.packets[space as usize].capacity(), capacity);
    assert_eq!(adaptive.packets[space as usize].len(), capacity);
    assert_eq!(adaptive.packet_epoch_dead[space as usize], 0);

    // A real interior loss leaves deferred cleanup pending. Terminal callbacks can empty the
    // remaining live store before loss-batch end; the pending bit then becomes redundant.
    adaptive.on_packet_lost(
        BASE_DATAGRAM_SIZE as u16,
        (capacity / 3) as u64,
        space,
        base + Duration::from_millis(10),
    );
    assert!(adaptive.packet_retirement_pending[space as usize]);
    loop {
        let next_live = adaptive.packets[space as usize]
            .iter()
            .find(|packet| adaptive.packet_index(space, packet.packet_number).is_some())
            .map(|packet| packet.packet_number);
        let Some(packet_number) = next_live else {
            break;
        };
        adaptive.on_packet_discarded(packet_number, space);
    }
    assert!(adaptive.packets.iter().all(VecDeque::is_empty));
    assert!(!adaptive.packet_epochs_enabled);
    assert_eq!(adaptive.packet_retirement_pending, [false; 3]);
}

#[derive(Clone, Copy, Debug)]
enum LifecyclePolicy {
    Original,
    Adaptive,
}

#[derive(Clone, Copy, Debug)]
struct LifecycleMeasurement {
    elapsed_ms: f64,
    original_scan_visits: usize,
    epoch_scan_visits: usize,
    migration_visits: usize,
    max_physical_records: usize,
    checksum: u64,
}

fn lifecycle_controller(policy: LifecyclePolicy) -> Bbr3 {
    let mut config = Bbr3Config::default();
    config.probe_rng_seed = Some([0x37; 16]);
    config.loss_compensation_floor(0.0);
    let mut controller = Bbr3::new(Arc::new(config), BASE_DATAGRAM_SIZE as u16);
    match policy {
        LifecyclePolicy::Original => controller.packet_epochs_disabled_for_test = true,
        LifecyclePolicy::Adaptive => {}
    }
    controller
}

fn update_peak(controller: &Bbr3, peak: &mut usize) {
    *peak = (*peak).max(controller.packets.iter().map(VecDeque::len).sum());
}

fn run_ack_lifecycle(
    policy: LifecyclePolicy,
    flight: usize,
    reordered: bool,
    rounds: usize,
) -> LifecycleMeasurement {
    let base = Instant::now();
    let rtt = RttEstimator::new(Duration::from_millis(100));
    let mut controller = lifecycle_controller(policy);
    let started = std::time::Instant::now();
    let mut sent_times = Vec::with_capacity(flight + rounds * 8);
    let mut live: Vec<u64> = (0..flight as u64).collect();
    let mut next_packet = flight as u64;
    let mut peak = 0;
    for packet_number in 0..flight as u64 {
        let sent = base + Duration::from_nanos(packet_number);
        sent_times.push(sent);
        <Bbr3 as Controller>::on_packet_sent(
            &mut controller,
            sent,
            BASE_DATAGRAM_SIZE as u16,
            packet_number * BASE_DATAGRAM_SIZE,
            packet_number,
            SpaceId::Data,
            false,
        );
        update_peak(&controller, &mut peak);
    }

    let acked_per_batch = 8.min((flight / 4).max(1));
    let mut rng = 0x74a9_115b_u64;
    let mut checksum = 0_u64;
    for round in 0..rounds {
        let mut positions = Vec::with_capacity(acked_per_batch);
        for offset in 0..acked_per_batch {
            let position = if reordered {
                loop {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let candidate = (rng as usize) % flight;
                    if !positions.contains(&candidate) {
                        break candidate;
                    }
                }
            } else {
                (round * acked_per_batch + offset) % flight
            };
            positions.push(position);
        }

        let now = base + Duration::from_millis(100 + round as u64);
        let mut largest = 0;
        for position in &positions {
            let packet_number = live[*position];
            largest = largest.max(packet_number);
            <Bbr3 as Controller>::on_ack(
                &mut controller,
                now,
                sent_times[packet_number as usize],
                BASE_DATAGRAM_SIZE,
                packet_number,
                SpaceId::Data,
                false,
                &rtt,
            );
        }
        <Bbr3 as Controller>::on_end_acks(
            &mut controller,
            now,
            (flight - acked_per_batch) as u64 * BASE_DATAGRAM_SIZE,
            false,
            Some(largest),
            SpaceId::Data,
        );
        checksum = checksum
            .wrapping_add(controller.window())
            .wrapping_add(controller.pacing_rate().unwrap_or_default());

        for (offset, position) in positions.into_iter().enumerate() {
            let packet_number = next_packet;
            next_packet += 1;
            let sent = now + Duration::from_nanos(offset as u64);
            sent_times.push(sent);
            <Bbr3 as Controller>::on_packet_sent(
                &mut controller,
                sent,
                BASE_DATAGRAM_SIZE as u16,
                (flight - acked_per_batch + offset) as u64 * BASE_DATAGRAM_SIZE,
                packet_number,
                SpaceId::Data,
                false,
            );
            live[position] = packet_number;
            update_peak(&controller, &mut peak);
        }
    }
    LifecycleMeasurement {
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        original_scan_visits: controller.packet_cleanup_reference_visits,
        epoch_scan_visits: controller.packet_epoch_cleanup_visits,
        migration_visits: controller.packet_epoch_migration_visits,
        max_physical_records: peak,
        checksum,
    }
}

fn run_small_after_bulk_lifecycle(
    policy: LifecyclePolicy,
    retained_flight: usize,
    reordered: bool,
    rounds: usize,
) -> LifecycleMeasurement {
    assert!((2..=512).contains(&retained_flight));
    let base = Instant::now();
    let rtt = RttEstimator::new(Duration::from_millis(100));
    let mut controller = lifecycle_controller(policy);
    let mut sent_times = Vec::with_capacity(512 + rounds * 8);
    for packet_number in 0..512_u64 {
        let sent = base + Duration::from_nanos(packet_number);
        sent_times.push(sent);
        <Bbr3 as Controller>::on_packet_sent(
            &mut controller,
            sent,
            BASE_DATAGRAM_SIZE as u16,
            packet_number * BASE_DATAGRAM_SIZE,
            packet_number,
            SpaceId::Data,
            false,
        );
    }
    let first_retained = 512_u64 - (retained_flight - 1) as u64;
    let first_ack = base + Duration::from_millis(10);
    for packet_number in 1..first_retained {
        <Bbr3 as Controller>::on_ack(
            &mut controller,
            first_ack,
            sent_times[packet_number as usize],
            BASE_DATAGRAM_SIZE,
            packet_number,
            SpaceId::Data,
            false,
            &rtt,
        );
    }
    let largest_acked = first_retained - 1;
    <Bbr3 as Controller>::on_end_acks(
        &mut controller,
        first_ack,
        retained_flight as u64 * BASE_DATAGRAM_SIZE,
        false,
        Some(largest_acked),
        SpaceId::Data,
    );
    // The ACKed records are current evidence through this ACK batch. Advance one further valid
    // cleanup epoch before discarding the remaining unacknowledged flight, as a real long-lived
    // connection would do after that evidence expires.
    <Bbr3 as Controller>::on_end_acks(
        &mut controller,
        first_ack + Duration::from_millis(1),
        retained_flight as u64 * BASE_DATAGRAM_SIZE,
        false,
        Some(largest_acked),
        SpaceId::Data,
    );
    assert_eq!(
        controller.packets[SpaceId::Data as usize].len(),
        retained_flight
    );
    assert!(controller.packet_epochs_enabled == !matches!(policy, LifecyclePolicy::Original));
    controller.packet_cleanup_reference_visits = 0;
    controller.packet_epoch_cleanup_visits = 0;
    controller.packet_epoch_migration_visits = 0;

    let started = std::time::Instant::now();
    let mut next_packet = 512_u64;
    let mut checksum = 0_u64;
    let mut peak = retained_flight;
    let mut live: Vec<u64> = (first_retained..512).collect();
    let acked_per_batch = 8.min((live.len() / 4).max(1));
    let mut rng = 0x2c91_04d7_u64;
    for round in 0..rounds {
        let sent = base + Duration::from_millis(20 + round as u64 * 2);
        let now = sent + Duration::from_millis(1);
        let mut positions = Vec::with_capacity(acked_per_batch);
        for offset in 0..acked_per_batch {
            let position = if reordered {
                loop {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let candidate = (rng as usize) % live.len();
                    if !positions.contains(&candidate) {
                        break candidate;
                    }
                }
            } else {
                (round * acked_per_batch + offset) % live.len()
            };
            positions.push(position);
        }
        let mut largest = 0;
        for position in &positions {
            let packet_number = live[*position];
            largest = largest.max(packet_number);
            <Bbr3 as Controller>::on_ack(
                &mut controller,
                now,
                sent_times[packet_number as usize],
                BASE_DATAGRAM_SIZE,
                packet_number,
                SpaceId::Data,
                false,
                &rtt,
            );
        }
        <Bbr3 as Controller>::on_end_acks(
            &mut controller,
            now,
            (retained_flight - acked_per_batch) as u64 * BASE_DATAGRAM_SIZE,
            false,
            Some(largest),
            SpaceId::Data,
        );
        checksum = checksum
            .wrapping_add(controller.window())
            .wrapping_add(controller.pacing_rate().unwrap_or_default());
        for (offset, position) in positions.into_iter().enumerate() {
            let packet_number = next_packet;
            next_packet += 1;
            let new_sent = now + Duration::from_nanos(offset as u64);
            sent_times.push(new_sent);
            <Bbr3 as Controller>::on_packet_sent(
                &mut controller,
                new_sent,
                BASE_DATAGRAM_SIZE as u16,
                (retained_flight - acked_per_batch + offset) as u64 * BASE_DATAGRAM_SIZE,
                packet_number,
                SpaceId::Data,
                false,
            );
            live[position] = packet_number;
            update_peak(&controller, &mut peak);
        }
    }
    LifecycleMeasurement {
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        original_scan_visits: controller.packet_cleanup_reference_visits,
        epoch_scan_visits: controller.packet_epoch_cleanup_visits,
        migration_visits: controller.packet_epoch_migration_visits,
        max_physical_records: peak,
        checksum,
    }
}

#[test]
#[ignore = "pinned fresh-128 ABBA timing discriminator; run explicitly when the build host is idle"]
fn benchmark_fresh_128_ack_cleanup_abba() {
    let mut expected_checksum = None;
    for block in 0..3 {
        let policies = [
            LifecyclePolicy::Original,
            LifecyclePolicy::Adaptive,
            LifecyclePolicy::Adaptive,
            LifecyclePolicy::Original,
        ];
        for (position, policy) in policies.into_iter().enumerate() {
            let result = run_ack_lifecycle(policy, 128, false, 30_000);
            if let Some(expected) = expected_checksum {
                assert_eq!(
                    result.checksum, expected,
                    "output mismatch in ABBA block {block}"
                );
            } else {
                expected_checksum = Some(result.checksum);
            }
            println!(
                "fresh128_abba block={block} position={position} policy={policy:?} elapsed_ms={:.6} checksum={} original_scan_visits={} epoch_scan_visits={} migration_visits={} peak_physical={}",
                result.elapsed_ms,
                result.checksum,
                result.original_scan_visits,
                result.epoch_scan_visits,
                result.migration_visits,
                result.max_physical_records,
            );
        }
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

#[test]
#[ignore = "bounded real-Rust lifecycle timing; run explicitly to compare the original and adaptive strategies"]
fn benchmark_bbr_packet_cleanup_lifecycle() {
    use std::fmt::Write as _;

    let policies = [LifecyclePolicy::Original, LifecyclePolicy::Adaptive];
    let mut report = String::from("{\"kind\":\"compiled Rust BBR3 packet lifecycle\",\"cases\":[");
    let mut first = true;
    for reordered in [false, true] {
        for flight in [64, 128, 256, 512, 4096] {
            let mut checksums = Vec::new();
            let mut results = Vec::new();
            for policy in policies {
                let mut times = Vec::new();
                let mut last = LifecycleMeasurement {
                    elapsed_ms: 0.0,
                    original_scan_visits: 0,
                    epoch_scan_visits: 0,
                    migration_visits: 0,
                    max_physical_records: 0,
                    checksum: 0,
                };
                for _ in 0..3 {
                    last = run_ack_lifecycle(policy, flight, reordered, 6000);
                    times.push(last.elapsed_ms);
                    checksums.push(last.checksum);
                }
                last.elapsed_ms = median(&mut times);
                results.push((policy, last));
            }
            assert!(checksums.iter().all(|checksum| *checksum == checksums[0]));
            for (policy, result) in results {
                if !first {
                    report.push(',');
                }
                first = false;
                let _ = write!(
                    report,
                    "{{\"workload\":\"{}\",\"flight\":{},\"reordered\":{},\"policy\":\"{:?}\",\"median_ms\":{:.3},\"original_scan_visits\":{},\"epoch_scan_visits\":{},\"migration_visits\":{},\"peak_physical\":{},\"checksum\":{}}}",
                    "steady",
                    flight,
                    reordered,
                    policy,
                    result.elapsed_ms,
                    result.original_scan_visits,
                    result.epoch_scan_visits,
                    result.migration_visits,
                    result.max_physical_records,
                    result.checksum,
                );
            }
        }
    }
    for retained_flight in [64, 128] {
        for reordered in [false, true] {
            let mut checksums = Vec::new();
            let mut results = Vec::new();
            for policy in policies {
                let mut times = Vec::new();
                let mut last = LifecycleMeasurement {
                    elapsed_ms: 0.0,
                    original_scan_visits: 0,
                    epoch_scan_visits: 0,
                    migration_visits: 0,
                    max_physical_records: 0,
                    checksum: 0,
                };
                for _ in 0..3 {
                    last =
                        run_small_after_bulk_lifecycle(policy, retained_flight, reordered, 30_000);
                    times.push(last.elapsed_ms);
                    checksums.push(last.checksum);
                }
                last.elapsed_ms = median(&mut times);
                results.push((policy, last));
            }
            assert!(checksums.iter().all(|checksum| *checksum == checksums[0]));
            for (policy, last) in results {
                if !first {
                    report.push(',');
                }
                first = false;
                let _ = write!(
                    report,
                    "{{\"workload\":\"small_after_large\",\"flight\":{},\"reordered\":{},\"policy\":\"{:?}\",\"median_ms\":{:.3},\"original_scan_visits\":{},\"epoch_scan_visits\":{},\"migration_visits\":{},\"peak_physical\":{},\"checksum\":{}}}",
                    retained_flight,
                    reordered,
                    policy,
                    last.elapsed_ms,
                    last.original_scan_visits,
                    last.epoch_scan_visits,
                    last.migration_visits,
                    last.max_physical_records,
                    last.checksum,
                );
            }
        }
    }
    report.push_str("]}");
    println!("{report}");
}
