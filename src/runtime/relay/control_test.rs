use super::*;
use crate::mux::MuxLimits;
use crate::protocol::{OffsetRange, StreamId};
use bytes::Bytes;

#[test]
fn request_outstanding_limit_uses_connection_window_then_ack_headroom() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let mut window = RequestOutstandingWindow::new();
    let limit = window.limit_bytes(TrafficClass::Throughput, payload_bytes, mux_limits);
    assert_eq!(limit, 4 * 1024 * 1024);

    let mut send_stream = ReliableSendStream::new(StreamId(90), mux_limits);
    send_stream
        .send_data(Bytes::from(vec![0x11; 512 * 1024]))
        .expect("first dispatched request chunk");
    send_stream
        .send_data(Bytes::from(vec![0x22; 512 * 1024]))
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
        "raw request data and Data-ACK-retained ranges share one unique-byte budget"
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
