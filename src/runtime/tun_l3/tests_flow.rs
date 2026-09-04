use super::*;

fn flow(source_port: u16) -> IpPacketFlowKey {
    let mut packet = vec![
        0x45, 0, 0, 24, 0, 1, 0, 0, 64, 17, 0, 0, 10, 88, 0, 2, 10, 88, 0, 1, 0, 0, 0, 53,
    ];
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    crate::model::tun_l3::parse_ip_packet(&packet)
        .expect("IPv4 packet")
        .flow_key
}

#[test]
fn recent_load_expires_without_discarding_affinity_cache() {
    let now = Instant::now();
    let timeout = Duration::from_millis(20);
    let mut table = PacketFlowTable::new(8);
    let first = flow(1);
    table.bind(first.clone(), 7_u8, now, timeout);
    assert_eq!(table.active_load_for(7, &first), 0);
    assert_eq!(table.active_load_for(7, &flow(2)), 1);

    let after_idle = now + timeout + Duration::from_millis(1);
    assert_eq!(table.current(&first, after_idle, |_| true), None);
    assert_eq!(table.active_load_for(7, &flow(2)), 0);

    table.bind(first.clone(), 9, after_idle, timeout);
    assert_eq!(table.current(&first, after_idle, |_| true), Some(9));
}

#[test]
fn advisory_affinity_read_does_not_extend_activity_before_acceptance() {
    let now = Instant::now();
    let timeout = Duration::from_millis(100);
    let mut table = PacketFlowTable::new(8);
    let key = flow(3);
    table.bind(key.clone(), 7_u8, now, timeout);

    assert_eq!(
        table.planned_current(&key, now + Duration::from_millis(90), |_| true),
        Some(7),
    );
    assert_eq!(
        table.planned_current(&key, now + Duration::from_millis(110), |_| true),
        None,
        "an advisory read cannot keep a blocked or stale carrier affinity live",
    );

    table.bind(key.clone(), 9_u8, now, timeout);
    assert!(table.commit_planned_current(&key, 9, now + Duration::from_millis(90),));
    assert_eq!(
        table.planned_current(&key, now + Duration::from_millis(110), |_| true),
        Some(9),
        "only a carrier-accepted packet extends the activity clock",
    );
}

#[test]
fn carrier_removal_clears_only_its_affinities() {
    let now = Instant::now();
    let timeout = Duration::from_secs(1);
    let mut table = PacketFlowTable::new(8);
    let first = flow(1);
    let second = flow(2);
    table.bind(first.clone(), 1_u8, now, timeout);
    table.bind(second.clone(), 2_u8, now, timeout);

    table.remove_carrier(1);
    assert_eq!(table.current(&first, now, |_| true), None);
    assert_eq!(table.current(&second, now, |_| true), Some(2));
    assert_eq!(table.active_load_for(1, &first), 0);
}

#[test]
fn expiry_index_remains_bounded_during_flow_churn() {
    let now = Instant::now();
    let timeout = Duration::from_secs(60);
    let mut table = PacketFlowTable::new(8);

    for source_port in 1..=128 {
        table.bind(flow(source_port), 1_u8, now, timeout);
        assert!(table.bindings.len() <= 8);
        assert!(table.load_expiries.len() <= table.bindings.len().saturating_mul(2));
    }

    let retained = flow(128);
    for _ in 0..128 {
        table.bind(retained.clone(), 1_u8, now, timeout);
        assert!(table.load_expiries.len() <= table.bindings.len().saturating_mul(2));
    }
}
