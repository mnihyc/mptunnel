use super::{ClientRelayDeliveryState, ClientRelayState};
use crate::model::capacity::adaptive_reliable_relay_reinjection_bytes;
use crate::model::path::RelayPathKey;
use crate::model::work::reliable_persistent_ack_gap_reinjection_limit_bytes;
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::protocol::{OffsetRange, PathId, StreamId, UnderlayProtocol};
use crate::runtime::sender::ReliableRelaySenderQueue;
use crate::scheduler::{PathSnapshot, TrafficClass};
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

#[test]
fn completion_requires_terminal_control_ack_and_reorder_drain() {
    let stream_id = StreamId(9);
    let limits = MuxLimits::default();
    let mut state = ClientRelayState::new();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let mut recv_stream = ReliableRecvStream::new(stream_id, limits);
    let mut sender_queue = ReliableRelaySenderQueue::default();

    state.record_local_eof();
    state.record_remote_finished();
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));

    state.record_local_fin_sent();
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));
    state.record_terminal_fin_replayed();
    assert!(state.is_finished(&send_stream, &recv_stream, &sender_queue));

    send_stream
        .send_data(Bytes::from_static(b"sent"))
        .expect("retain unique bytes until Data ACK");
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));
    send_stream.apply_ack(&[OffsetRange { start: 0, end: 4 }]);
    assert!(state.is_finished(&send_stream, &recv_stream, &sender_queue));

    recv_stream
        .receive_data(4, Bytes::from_static(b"tail"))
        .expect("buffer out-of-order response data");
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));
    recv_stream
        .receive_data(0, Bytes::from_static(b"head"))
        .expect("close response hole");
    assert!(state.is_finished(&send_stream, &recv_stream, &sender_queue));

    sender_queue.push_data(Bytes::from_static(b"queued"));
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));
    assert!(sender_queue.pop_front().is_some());
    assert!(state.is_finished(&send_stream, &recv_stream, &sender_queue));
}

#[test]
fn request_ack_gap_exceeds_one_quantum_only_with_measured_persistent_target() {
    let limits = MuxLimits::default();
    let measured_target = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let base_limit = adaptive_reliable_relay_reinjection_bytes(
        Some(measured_target),
        TrafficClass::Throughput,
        limits,
    );
    let ordinary_limit = base_limit.min(limits.max_repair_bytes);
    let persistent_limit = reliable_persistent_ack_gap_reinjection_limit_bytes(
        Some(measured_target),
        Some(UnderlayProtocol::Tcp),
        TrafficClass::Throughput,
        limits.max_repair_bytes,
        limits,
    );

    assert_eq!(ordinary_limit, base_limit);
    assert!(
        persistent_limit > ordinary_limit,
        "request recovery must not refill a modeled TCP flight until persistent ACK evidence selects that measured target"
    );
}
