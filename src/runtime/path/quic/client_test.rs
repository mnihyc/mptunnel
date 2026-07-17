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
fn transient_probe_preserves_network_identity_but_isolates_session_state() {
    let path: PathSpec = "udp://127.0.0.1:443".parse().expect("UDP path");
    let security = SecurityConfig::encrypted(
        crate::config::SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("secret"),
    );
    let state = ClientPathState::new(ClientPathHealth {
        tcp: Vec::new(),
        udp: vec![ClientPathHealthRecord::default()],
    });
    let runtime = ClientUdpPathSessionRuntime {
        paths: Arc::new(vec![path]),
        config_index: 0,
        path_index: 0,
        carrier_identity: CarrierPathIdentity {
            group_ordinal: 7,
            path_ordinal: 11,
        },
        session_id: SessionId(17),
        security: Arc::new(vec![security]),
        codec_limits: CodecLimits::default(),
        mux_limits: MuxLimits::default(),
        stream_frame_queue: 8,
        state: state.clone(),
        carrier_network: Arc::new(crate::transport::SystemCarrierNetworkProvider),
        peer_status: PeerStatusBroker::new(true),
        peer_status_snapshot: PeerStatusSnapshotSource::new(Vec::new),
    };
    let durable = ClientUdpPathSessionHandle::new(runtime);
    let probe = durable.transient_probe().expect("transient probe handle");

    assert_ne!(probe.runtime.session_id, durable.runtime.session_id);
    assert_eq!(
        probe.runtime.carrier_identity,
        durable.runtime.carrier_identity
    );
    assert_eq!(probe.runtime.config_index, durable.runtime.config_index);
    assert_eq!(probe.runtime.path_index, durable.runtime.path_index);
    assert!(Arc::ptr_eq(&probe.runtime.paths, &durable.runtime.paths));
    assert!(Arc::ptr_eq(
        &probe.runtime.security,
        &durable.runtime.security
    ));
    assert!(!Arc::ptr_eq(&probe.runtime.state, &state));
    assert!(!Arc::ptr_eq(&probe.connection, &durable.connection));
}

#[test]
fn stream_open_path_status_uses_carrier_instance_and_sequence_fences() {
    let state = ClientPathState::new(ClientPathHealth {
        tcp: Vec::new(),
        udp: vec![ClientPathHealthRecord::default()],
    });
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
