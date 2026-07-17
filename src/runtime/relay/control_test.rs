use super::*;
use crate::mux::MuxLimits;
use crate::protocol::{OffsetRange, StreamId};
use bytes::Bytes;

#[test]
fn request_outstanding_limit_uses_stream_resources_then_exact_ack_headroom() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024 * 1024,
        max_repair_bytes: 4 * 1024 * 1024,
        max_reorder_bytes: 4 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 64 * 1024;
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Throughput,
        payload_bytes,
        mux_limits,
    );
    let accounting_limit = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(mux_limits.max_stream_window_bytes as usize);
    assert_eq!(limit, accounting_limit);

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
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        2 * 1024 * 1024
    );
    sender_queue.push_data(Bytes::from(vec![0x44; 2 * 1024 * 1024]));
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        0,
        "raw request data and Data-ACK-retained ranges share one unique-byte budget"
    );
    let ack = send_stream.apply_ack(&[OffsetRange {
        start: 0,
        end: 1024 * 1024,
    }]);
    assert_eq!(ack.released_bytes, 1024 * 1024);
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        1024 * 1024,
        "unique STREAM_ACK release must resume source reads without double-counting raw queue bytes"
    );
}

#[test]
fn latency_request_outstanding_limit_keeps_the_staging_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Latency,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(limit, reliable_relay_buffer_len(mux_limits));
    assert!(limit < mux_limits.max_stream_window_bytes as usize);
}

#[test]
fn bulk_request_outstanding_limit_is_the_configured_stream_resource_ceiling() {
    let limits = MuxLimits::default();
    assert_eq!(
        reliable_relay_request_outstanding_limit_bytes(TrafficClass::Throughput, 64 * 1024, limits,),
        limits
            .max_repair_bytes
            .min(limits.max_reorder_bytes)
            .min(limits.max_stream_window_bytes as usize),
    );
}
