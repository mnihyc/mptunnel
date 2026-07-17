use super::*;

fn limits() -> MuxLimits {
    MuxLimits {
        max_payload_bytes: 1024,
        max_ack_ranges: 8,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_stream_window_bytes: 4096,
        max_repair_bytes: 2048,
        max_reorder_bytes: 2048,
        max_datagram_queue_bytes: 16,
        max_path_flight_bytes: 2048,
        max_reliable_relay_chunk_bytes: 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        quic_path_keep_alive_interval: crate::config::DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
        quic_path_idle_timeout: crate::config::DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
    }
}

#[test]
fn datagram_queue_emits_compact_frame_with_remaining_ttl() {
    let mut flow = DatagramFlow::new(DatagramFlowId(3), limits());
    let datagram_id = flow
        .enqueue(100, 1000, Bytes::from_static(b"dns"))
        .expect("enqueue");

    let frame = flow.pop_frame(250).expect("frame");
    assert_eq!(flow.queued_bytes(), 0);
    assert_eq!(
        frame,
        Frame::DatagramData {
            flow_id: DatagramFlowId(3),
            datagram_id,
            ttl_ms: 850,
            payload: Bytes::from_static(b"dns")
        }
    );
}

#[test]
fn datagram_queue_drops_expired_items_before_send() {
    let mut flow = DatagramFlow::new(DatagramFlowId(3), limits());
    flow.enqueue(0, 10, Bytes::from_static(b"expired"))
        .expect("enqueue");

    assert_eq!(flow.pop_frame(10), None);
    assert_eq!(flow.dropped_expired(), 1);
    assert_eq!(flow.queued_bytes(), 0);
}

#[test]
fn datagram_queue_enforces_size_limits() {
    let mut flow = DatagramFlow::new(DatagramFlowId(3), limits());
    flow.enqueue(0, 100, Bytes::from_static(b"1234567890abcdef"))
        .expect("fills queue");

    assert!(matches!(
        flow.enqueue(0, 100, Bytes::from_static(b"x")),
        Err(DatagramError::QueueFull { .. })
    ));
    assert_eq!(flow.dropped_queue_full(), 1);

    let mut limit = limits();
    limit.max_payload_bytes = 4;
    let mut flow = DatagramFlow::new(DatagramFlowId(3), limit);
    assert!(matches!(
        flow.enqueue(0, 100, Bytes::from_static(b"hello")),
        Err(DatagramError::PayloadTooLarge { .. })
    ));
    assert_eq!(flow.dropped_oversize(), 1);
}
