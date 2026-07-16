use super::*;
use crate::model::path::next_carrier_path_instance_id;
use crate::protocol::UnderlayProtocol;
use crate::runtime::path::{ClientPathHealth, ClientPathHealthRecord, ClientPathState};

#[test]
fn tcp_datagram_status_updates_only_its_session_snapshot() {
    let state = ClientPathState::new(ClientPathHealth {
        tcp: vec![ClientPathHealthRecord::default()],
        udp: Vec::new(),
    });
    state.install_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        next_carrier_path_instance_id(),
        0,
        PathUsage::Backup,
    );
    let mut snapshot = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 10_000_000.0);
    snapshot.peer_usage = Some(PathUsage::Backup);
    let mut peer_usage_sequence = 0;

    assert!(
        apply_client_tcp_datagram_path_status(
            PathId(0),
            PathId(0),
            1,
            PathUsage::Available,
            &mut peer_usage_sequence,
            &mut snapshot,
        )
        .expect("matching wire path ID")
    );
    assert_eq!(snapshot.peer_usage, Some(PathUsage::Available));
    assert_eq!(
        state.peer_path_usage(UnderlayProtocol::Tcp, 0),
        Some(PathUsage::Backup)
    );
}

#[test]
fn tcp_datagram_status_rejects_stale_sequence_and_path_id() {
    let mut snapshot = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 10_000_000.0);
    snapshot.peer_usage = Some(PathUsage::Available);
    let mut peer_usage_sequence = 2;

    assert!(
        !apply_client_tcp_datagram_path_status(
            PathId(0),
            PathId(0),
            2,
            PathUsage::Backup,
            &mut peer_usage_sequence,
            &mut snapshot,
        )
        .expect("matching wire path ID")
    );
    assert!(
        apply_client_tcp_datagram_path_status(
            PathId(0),
            PathId(1),
            3,
            PathUsage::Backup,
            &mut peer_usage_sequence,
            &mut snapshot,
        )
        .is_err()
    );
    assert_eq!(snapshot.peer_usage, Some(PathUsage::Available));
    assert_eq!(peer_usage_sequence, 2);
}
