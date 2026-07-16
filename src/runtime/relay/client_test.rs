use super::{ClientRelayDeliveryState, ClientRelayState};
use crate::model::path::RelayPathKey;
use crate::protocol::UnderlayProtocol;
use bytes::Bytes;

#[test]
fn delivery_attribution_credits_current_frame_not_released_buffer() {
    let path_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    let delivered = [
        Bytes::from_static(&[0; 1024]),
        Bytes::from_static(&[1; 4096]),
    ];
    let mut delivery = ClientRelayDeliveryState::default();

    let delivered_bytes = delivery.record_response(path_key, &delivered, 1024);

    assert_eq!(delivered_bytes, 5120);
    assert_eq!(delivery.total.payload_bytes, 5120);
    assert_eq!(
        delivery
            .by_path
            .get(&path_key)
            .expect("path stat")
            .payload_bytes,
        1024,
        "hole-closing carrier must not inherit buffered bytes released from other paths"
    );
}

#[test]
fn endpoint_transitions_keep_fin_state_coherent() {
    let mut state = ClientRelayState::new();
    state.record_local_eof();
    assert!(!state.endpoint.local_open);
    assert!(state.endpoint.pending_local_fin);

    state.record_local_fin_sent();
    assert!(!state.endpoint.pending_local_fin);
    assert!(state.endpoint.local_fin_sent);
    assert!(!state.endpoint.terminal_fin_replayed);

    state.record_terminal_fin_replayed();
    assert!(state.endpoint.terminal_fin_replayed);
}
