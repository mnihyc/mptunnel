use super::*;
use crate::model::path::next_carrier_path_instance_id;
use crate::runtime::path::health::{ClientPathHealth, ClientPathHealthRecord};

#[test]
fn quic_status_update_is_fenced_by_the_spawning_carrier_instance() {
    let state = ClientPathState::new(ClientPathHealth {
        tcp: Vec::new(),
        udp: vec![ClientPathHealthRecord::default()],
    });
    let old_instance = next_carrier_path_instance_id();
    let current_instance = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        current_instance,
        0,
        PathUsage::Available,
    );

    assert!(
        !apply_client_udp_path_status(
            &state,
            0,
            old_instance,
            PathId(0),
            PathId(0),
            9,
            PathUsage::Backup,
        )
        .expect("matching wire path ID")
    );
    assert_eq!(
        state.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Available),
    );
    assert!(
        apply_client_udp_path_status(
            &state,
            0,
            current_instance,
            PathId(0),
            PathId(0),
            1,
            PathUsage::Backup,
        )
        .expect("matching wire path ID")
    );
    assert_eq!(
        state.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Backup),
    );
}

#[test]
fn quic_status_update_rejects_a_different_wire_path_id() {
    let state = ClientPathState::new(ClientPathHealth {
        tcp: Vec::new(),
        udp: vec![ClientPathHealthRecord::default()],
    });
    let instance = next_carrier_path_instance_id();
    state.install_peer_path_usage(UnderlayProtocol::Udp, 0, instance, 0, PathUsage::Available);

    assert!(
        apply_client_udp_path_status(
            &state,
            0,
            instance,
            PathId(0),
            PathId(1),
            1,
            PathUsage::Backup,
        )
        .is_err()
    );
}
