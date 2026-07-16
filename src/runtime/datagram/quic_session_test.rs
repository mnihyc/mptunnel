use super::*;
use crate::model::path::next_carrier_path_instance_id;
use crate::runtime::path::{ClientPathHealth, ClientPathHealthRecord};

#[test]
fn quic_datagram_status_uses_the_spawning_carrier_instance_fence() {
    let state = ClientPathState::new(ClientPathHealth {
        tcp: Vec::new(),
        udp: vec![ClientPathHealthRecord::default()],
    });
    let stale_instance_id = next_carrier_path_instance_id();
    let current_instance_id = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        current_instance_id,
        0,
        PathUsage::Available,
    );

    assert!(
        !apply_client_udp_datagram_path_status(
            &state,
            0,
            stale_instance_id,
            PathId(0),
            PathId(0),
            99,
            PathUsage::Backup,
        )
        .expect("matching wire path ID")
    );
    assert!(
        apply_client_udp_datagram_path_status(
            &state,
            0,
            current_instance_id,
            PathId(0),
            PathId(0),
            1,
            PathUsage::Backup,
        )
        .expect("matching wire path ID")
    );
    assert!(
        !apply_client_udp_datagram_path_status(
            &state,
            0,
            current_instance_id,
            PathId(0),
            PathId(0),
            1,
            PathUsage::Available,
        )
        .expect("matching wire path ID")
    );
    assert_eq!(
        state.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Backup)
    );
}

#[test]
fn quic_datagram_status_rejects_a_different_wire_path_id() {
    let state = ClientPathState::new(ClientPathHealth {
        tcp: Vec::new(),
        udp: vec![ClientPathHealthRecord::default()],
    });
    let path_instance_id = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        path_instance_id,
        0,
        PathUsage::Available,
    );

    assert!(
        apply_client_udp_datagram_path_status(
            &state,
            0,
            path_instance_id,
            PathId(0),
            PathId(1),
            1,
            PathUsage::Backup,
        )
        .is_err()
    );
}
