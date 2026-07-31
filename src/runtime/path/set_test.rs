use super::*;
use crate::model::path::CarrierPathInstanceId;
use crate::runtime::path::state::ClientTcpCarrierPublication;

fn tcp_path(port: u16) -> PathSpec {
    format!("tcp://127.0.0.1:{port}").parse().expect("TCP path")
}

fn tcp_state(path_count: usize) -> Arc<ClientPathState> {
    ClientPathState::new(ClientPathHealth {
        tcp: vec![ClientPathHealthRecord::default(); path_count],
        udp: Vec::new(),
    })
}

fn publish_tcp_carrier(state: &ClientPathState, path_index: usize, path_id: PathId, instance: u64) {
    state.publish_tcp_peer_path_usage_committed(
        ClientTcpCarrierPublication {
            path_index,
            path_id,
            path_instance_id: CarrierPathInstanceId::from_raw(instance),
            peer_usage_sequence: 0,
            peer_usage: PathUsage::Available,
            readiness_rtt: None,
        },
        || {},
    );
}

#[test]
fn peer_status_omits_tcp_configuration_without_an_authenticated_carrier() {
    let paths = [tcp_path(12_700), tcp_path(12_701)];
    let state = tcp_state(paths.len());
    let inventory = AuthenticatedCarrierInventory::default();

    assert_eq!(
        client_peer_status_snapshot(&paths, &[], &state, &inventory),
        Some(Vec::new())
    );
}

#[test]
fn peer_status_never_substitutes_a_local_index_for_tcp_wire_identity() {
    let paths = [tcp_path(12_700)];
    let state = tcp_state(paths.len());
    let inventory = AuthenticatedCarrierInventory::default();
    state.install_peer_path_usage(
        UnderlayProtocol::Tcp,
        0,
        CarrierPathInstanceId::from_raw(1),
        0,
        PathUsage::Available,
    );
    let _registration = inventory.register();

    assert_eq!(
        client_peer_status_snapshot(&paths, &[], &state, &inventory),
        None
    );
}

#[test]
fn peer_status_uses_the_authenticated_tcp_wire_identity() {
    let paths = [tcp_path(12_700)];
    let state = tcp_state(paths.len());
    let inventory = AuthenticatedCarrierInventory::default();
    publish_tcp_carrier(&state, 0, PathId(47), 1);
    let _registration = inventory.register();

    let snapshot =
        client_peer_status_snapshot(&paths, &[], &state, &inventory).expect("exact carrier set");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].metrics.path_id, PathId(47));
}

#[test]
fn peer_status_rejects_replacement_overlap_instead_of_returning_a_partial_set() {
    let paths = [tcp_path(12_700)];
    let state = tcp_state(paths.len());
    let inventory = AuthenticatedCarrierInventory::default();
    publish_tcp_carrier(&state, 0, PathId(48), 2);
    let _predecessor_registration = inventory.register();
    let _successor_registration = inventory.register();

    assert_eq!(
        client_peer_status_snapshot(&paths, &[], &state, &inventory),
        None
    );
}
