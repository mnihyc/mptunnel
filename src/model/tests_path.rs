use super::*;

#[test]
fn carrier_order_distinguishes_equal_path_ids_across_underlays() {
    let tcp = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(7),
    };
    let udp = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(7),
    };

    assert_ne!(carrier_path_key_order(tcp, udp), std::cmp::Ordering::Equal);
    assert_eq!(
        carrier_path_key_order(tcp, udp),
        carrier_path_key_order(udp, tcp).reverse()
    );
}

#[test]
fn carrier_order_keeps_path_id_as_the_primary_key() {
    let earlier_udp = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(3),
    };
    let later_tcp = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(4),
    };

    assert_eq!(
        carrier_path_key_order(earlier_udp, later_tcp),
        std::cmp::Ordering::Less
    );
}

#[test]
fn carrier_instance_identity_issues_max_once_then_fails_permanently_closed() {
    let next = AtomicU64::new(u64::MAX);

    assert!(carrier_path_instance_identity_is_available_in(&next));
    assert_eq!(
        try_allocate_carrier_path_instance_id(&next).map(CarrierPathInstanceId::as_u64),
        Some(u64::MAX),
    );
    assert_eq!(next.load(Ordering::Acquire), 0);
    assert!(!carrier_path_instance_identity_is_available_in(&next));
    assert!(try_allocate_carrier_path_instance_id(&next).is_none());
    assert!(try_allocate_carrier_path_instance_id(&next).is_none());
    assert!(!carrier_path_instance_identity_is_available_in(&next));
}
