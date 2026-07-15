use super::*;
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::protocol::{OffsetRange, StreamFlags, StreamId, UnderlayProtocol};
use bytes::Bytes;

fn request_test_path_instance(
    underlay: UnderlayProtocol,
    index: usize,
    id: u64,
) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey { underlay, index },
        id,
    }
}

#[test]
fn tcp_request_contention_requires_present_request_work() {
    let threshold = 256 * 1024;
    assert!(!reliable_tcp_service_request_bulk_flow_is_active(
        true, threshold, threshold, 0, 0,
    ));
    assert!(reliable_tcp_service_request_bulk_flow_is_active(
        true, threshold, threshold, 1, 0,
    ));
    assert!(!reliable_tcp_service_request_bulk_flow_is_active(
        true,
        threshold - 1,
        threshold,
        0,
        1,
    ));
    assert!(!reliable_tcp_service_request_bulk_flow_is_active(
        false, threshold, threshold, 1, 1,
    ));
    assert!(reliable_tcp_service_request_bulk_flow_is_active(
        true, threshold, threshold, 0, 1,
    ));
}

#[test]
fn tcp_request_outstanding_limit_uses_service_reservoir_then_ack_headroom() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let mut window = RequestOutstandingWindow::new();
    let tcp = request_test_path_instance(UnderlayProtocol::Tcp, 0, 1);
    let limit = window.limit_bytes(Some(tcp), FlowLane::Throughput, payload_bytes, mux_limits);
    assert_eq!(limit, 4 * 1024 * 1024);

    let mut send_stream = ReliableSendStream::new(StreamId(90), mux_limits);
    send_stream
        .send_data(Bytes::from(vec![0x11; 512 * 1024]), StreamFlags::NONE)
        .expect("first dispatched request chunk");
    send_stream
        .send_data(Bytes::from(vec![0x22; 512 * 1024]), StreamFlags::NONE)
        .expect("second dispatched request chunk");
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from(vec![0x33; 1024 * 1024]));

    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(&send_stream, &sender_queue, limit),
        2 * 1024 * 1024
    );
    sender_queue.push_data(Bytes::from(vec![0x44; 2 * 1024 * 1024]));
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(&send_stream, &sender_queue, limit),
        0,
        "raw request data and ACK-retained repair bytes share one unique-byte budget"
    );
    let ack = send_stream.apply_ack(&[OffsetRange {
        start: 0,
        end: 1024 * 1024,
    }]);
    assert_eq!(ack.released_bytes, 1024 * 1024);
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(&send_stream, &sender_queue, limit),
        1024 * 1024,
        "unique STREAM_ACK release must resume source reads without double-counting raw queue bytes"
    );
}
