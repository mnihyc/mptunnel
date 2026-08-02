use super::*;
use crate::runtime::path::health::{ClientPathHealth, ClientPathHealthRecord};

#[test]
fn address_retry_uses_rfc_delay_for_normal_budgets() {
    assert_eq!(
        quic_address_attempt_delay(Duration::from_secs(4), 3),
        QUIC_ADDRESS_ATTEMPT_DELAY
    );
}

#[test]
fn address_retry_fits_alternates_inside_short_budget() {
    assert_eq!(
        quic_address_attempt_delay(Duration::from_millis(120), 3),
        Duration::from_millis(30)
    );
    assert_eq!(
        quic_address_attempt_delay(Duration::from_nanos(1), 1),
        Duration::ZERO
    );
}

#[test]
fn stream_open_path_status_uses_carrier_instance_and_sequence_fences() {
    let state = ClientPathState::new(ClientPathHealth::new(
        Vec::new(),
        vec![ClientPathHealthRecord::default()],
    ));
    let old_instance = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        old_instance,
        4,
        PathUsage::Available,
    );

    assert!(
        apply_client_udp_path_status(
            &state,
            0,
            old_instance,
            PathId(0),
            PathId(0),
            5,
            PathUsage::Backup,
        )
        .expect("matching stream-open PATH_STATUS")
    );
    assert!(
        !apply_client_udp_path_status(
            &state,
            0,
            old_instance,
            PathId(0),
            PathId(0),
            4,
            PathUsage::Available,
        )
        .expect("stale stream-open PATH_STATUS")
    );

    let replacement_instance = next_carrier_path_instance_id();
    state.install_peer_path_usage(
        UnderlayProtocol::Udp,
        0,
        replacement_instance,
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
            99,
            PathUsage::Backup,
        )
        .expect("prior carrier stream-open PATH_STATUS")
    );
    assert_eq!(
        state.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Available),
    );
}

#[test]
fn stream_open_path_status_rejects_a_different_wire_path() {
    let state = ClientPathState::new(ClientPathHealth::new(
        Vec::new(),
        vec![ClientPathHealthRecord::default()],
    ));
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
