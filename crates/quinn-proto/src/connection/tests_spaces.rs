use super::*;

#[test]
fn sanity() {
    let mut dedup = Dedup::new();
    assert!(!dedup.insert(0));
    assert_eq!(dedup.next, 1);
    assert_eq!(dedup.window, 0b1);
    assert!(dedup.insert(0));
    assert_eq!(dedup.next, 1);
    assert_eq!(dedup.window, 0b1);
    assert!(!dedup.insert(1));
    assert_eq!(dedup.next, 2);
    assert_eq!(dedup.window, 0b11);
    assert!(!dedup.insert(2));
    assert_eq!(dedup.next, 3);
    assert_eq!(dedup.window, 0b111);
    assert!(!dedup.insert(4));
    assert_eq!(dedup.next, 5);
    assert_eq!(dedup.window, 0b11110);
    assert!(!dedup.insert(7));
    assert_eq!(dedup.next, 8);
    assert_eq!(dedup.window, 0b1111_0100);
    assert!(dedup.insert(4));
    assert!(!dedup.insert(3));
    assert_eq!(dedup.next, 8);
    assert_eq!(dedup.window, 0b1111_1100);
    assert!(!dedup.insert(6));
    assert_eq!(dedup.next, 8);
    assert_eq!(dedup.window, 0b1111_1101);
    assert!(!dedup.insert(5));
    assert_eq!(dedup.next, 8);
    assert_eq!(dedup.window, 0b1111_1111);
}

#[test]
fn happypath() {
    let mut dedup = Dedup::new();
    for i in 0..(2 * WINDOW_SIZE) {
        assert!(!dedup.insert(i));
        for j in 0..=i {
            assert!(dedup.insert(j));
        }
    }
}

#[test]
fn jump() {
    let mut dedup = Dedup::new();
    dedup.insert(2 * WINDOW_SIZE);
    assert!(dedup.insert(WINDOW_SIZE));
    assert_eq!(dedup.next, 2 * WINDOW_SIZE + 1);
    assert_eq!(dedup.window, 0);
    assert!(!dedup.insert(WINDOW_SIZE + 1));
    assert_eq!(dedup.next, 2 * WINDOW_SIZE + 1);
    assert_eq!(dedup.window, 1 << (WINDOW_SIZE - 2));
}

#[test]
fn dedup_has_missing() {
    let mut dedup = Dedup::new();

    dedup.insert(0);
    assert!(!dedup.missing_in_interval(0, 0));

    dedup.insert(1);
    assert!(!dedup.missing_in_interval(0, 1));

    dedup.insert(3);
    assert!(dedup.missing_in_interval(1, 3));

    dedup.insert(4);
    assert!(!dedup.missing_in_interval(3, 4));
    assert!(dedup.missing_in_interval(0, 4));

    dedup.insert(2);
    assert!(!dedup.missing_in_interval(0, 4));
}

#[test]
fn dedup_outside_of_window_has_missing() {
    let mut dedup = Dedup::new();

    for i in 0..140 {
        dedup.insert(i);
    }

    // 0 and 4 are outside of the window
    assert!(!dedup.missing_in_interval(0, 4));
    dedup.insert(160);
    assert!(!dedup.missing_in_interval(0, 4));
    assert!(!dedup.missing_in_interval(0, 140));
    assert!(dedup.missing_in_interval(0, 160));
}

#[test]
fn dedup_smallest_missing() {
    let mut dedup = Dedup::new();

    dedup.insert(0);
    assert_eq!(dedup.smallest_missing_in_interval(0, 0), None);

    dedup.insert(1);
    assert_eq!(dedup.smallest_missing_in_interval(0, 1), None);

    dedup.insert(5);
    dedup.insert(7);
    assert_eq!(dedup.smallest_missing_in_interval(0, 7), Some(2));
    assert_eq!(dedup.smallest_missing_in_interval(5, 7), Some(6));

    dedup.insert(2);
    assert_eq!(dedup.smallest_missing_in_interval(1, 7), Some(3));

    dedup.insert(170);
    dedup.insert(172);
    dedup.insert(300);
    assert_eq!(dedup.smallest_missing_in_interval(170, 172), None);

    dedup.insert(500);
    assert_eq!(dedup.smallest_missing_in_interval(0, 500), Some(372));
    assert_eq!(dedup.smallest_missing_in_interval(0, 373), Some(372));
    assert_eq!(dedup.smallest_missing_in_interval(0, 372), None);
}

#[test]
fn pending_acks_first_packet_is_not_considered_reordered() {
    let mut acks = PendingAcks::new();
    let mut dedup = Dedup::new();
    dedup.insert(0);
    acks.packet_received(Instant::now(), 0, true, &dedup);
    assert!(!acks.immediate_ack_required);
}

#[test]
fn pending_acks_after_immediate_ack_set() {
    let mut acks = PendingAcks::new();
    let mut dedup = Dedup::new();

    // Receive ack-eliciting packet
    dedup.insert(0);
    let now = Instant::now();
    acks.insert_one(0, now);
    acks.packet_received(now, 0, true, &dedup);

    // Sanity check
    assert!(!acks.ranges.is_empty());
    assert!(!acks.can_send());

    // Can send ACK after max_ack_delay exceeded
    acks.set_immediate_ack_required();
    assert!(acks.can_send());
}

#[test]
fn pending_acks_ack_delay() {
    let mut acks = PendingAcks::new();
    let mut dedup = Dedup::new();

    let t1 = Instant::now();
    let t2 = t1 + Duration::from_millis(2);
    let t3 = t2 + Duration::from_millis(5);
    assert_eq!(acks.ack_delay(t1), Duration::from_millis(0));
    assert_eq!(acks.ack_delay(t2), Duration::from_millis(0));
    assert_eq!(acks.ack_delay(t3), Duration::from_millis(0));

    // In-order packet
    dedup.insert(0);
    acks.insert_one(0, t1);
    acks.packet_received(t1, 0, true, &dedup);
    assert_eq!(acks.ack_delay(t1), Duration::from_millis(0));
    assert_eq!(acks.ack_delay(t2), Duration::from_millis(2));
    assert_eq!(acks.ack_delay(t3), Duration::from_millis(7));

    // Out of order (higher than expected)
    dedup.insert(3);
    acks.insert_one(3, t2);
    acks.packet_received(t2, 3, true, &dedup);
    assert_eq!(acks.ack_delay(t2), Duration::from_millis(0));
    assert_eq!(acks.ack_delay(t3), Duration::from_millis(5));

    // Out of order (lower than expected, so previous instant is kept)
    dedup.insert(2);
    acks.insert_one(2, t3);
    acks.packet_received(t3, 2, true, &dedup);
    assert_eq!(acks.ack_delay(t3), Duration::from_millis(5));
}

#[test]
fn sent_packet_size() {
    // The tracking state of sent packets should be minimal, and not grow
    // over time.
    let size = std::mem::size_of::<SentPacket>();
    assert!(size <= 128, "SentPacket grew to {size} bytes");
}
