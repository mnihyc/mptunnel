use super::*;

#[test]
fn quic_product_payload_uses_sender_quantum_not_packet_train_cap() {
    let mux_limits = MuxLimits::default();
    let codec_limits = CodecLimits::default();
    let payload_cap = udp_path_max_stream_payload_bytes(codec_limits, mux_limits);

    assert!(
        payload_cap >= BBR_MAX_SEND_QUANTUM_BYTES,
        "QUIC product dispatch must stay BDP/service-quantum sized; only carrier serialization may split records"
    );
}

#[test]
fn quic_reliable_stream_reader_queue_stays_logical_product_queue() {
    let mux_limits = MuxLimits::default();
    let codec_limits = CodecLimits::default();
    let queue = udp_reliable_stream_frame_queue(codec_limits, mux_limits);

    assert_eq!(
        queue,
        reliable_stream_frame_queue(mux_limits),
        "carrier recordization must not multiply the product reader queue or hide backlog"
    );
}

#[test]
fn quic_udp_command_queue_tracks_sender_quantum_not_record_size() {
    let mux_limits = MuxLimits::default();
    let codec_limits = CodecLimits::default();
    let product_queue = reliable_path_command_queue(mux_limits);
    let quic_udp_queue = udp_path_command_queue(mux_limits, codec_limits);
    let sender_quantum =
        reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits);
    let record_sized_queue = reliable_path_command_queue_for_payload(
        mux_limits,
        sender_quantum.min(UDP_DEFAULT_MTU_PAYLOAD_BYTES).max(1),
    );

    assert_eq!(
        quic_udp_queue, product_queue,
        "command queue capacity must stay tied to the logical sender quantum"
    );
    assert_ne!(
        quic_udp_queue, record_sized_queue,
        "carrier packet/record sizing must not inflate the command queue"
    );
}
