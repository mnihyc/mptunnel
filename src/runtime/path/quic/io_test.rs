use super::*;
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, UDP_BASELINE_PACKET_PAYLOAD_BYTES,
    reliable_relay_scheduler_quantum_cap,
};
use crate::protocol::{DatagramFlowId, DatagramId, PathId, StreamId};
use crate::runtime::path::commands::{
    reliable_path_command_queue_for_payload, reliable_stream_frame_queue,
};
use crate::scheduler::FlowLane;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[test]
fn quic_resolution_keeps_all_unique_source_compatible_addresses() {
    let v4_first = SocketAddr::from(([192, 0, 2, 10], 443));
    let v6 = SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, 10], 443));
    let v4_second = SocketAddr::from(([192, 0, 2, 11], 443));
    let resolved = [v4_first, v6, v4_first, v4_second];

    assert_eq!(
        compatible_udp_path_socket_addrs(resolved, None),
        vec![v4_first, v6, v4_second]
    );
    assert_eq!(
        compatible_udp_path_socket_addrs(resolved, Some(IpAddr::V4(Ipv4Addr::LOCALHOST))),
        vec![v4_first, v4_second]
    );
    assert_eq!(
        compatible_udp_path_socket_addrs(resolved, Some(IpAddr::V6(Ipv6Addr::LOCALHOST))),
        vec![v6]
    );
}

#[test]
fn quic_ordinary_writer_enforces_measurement_ownership() {
    let capacity = Frame::PathCapacityData {
        path_id: PathId(4),
        calibration_id: 17,
        payload: Bytes::from_static(b"capacity"),
    };
    let finish = Frame::PathCapacityFinish {
        path_id: PathId(4),
        calibration_id: 17,
        payload_bytes: 8,
    };
    let receipt = Frame::PathCapacityReceipt {
        path_id: PathId(4),
        calibration_id: 17,
        received_payload_bytes: 8,
    };
    let stream = Frame::StreamData {
        stream_id: StreamId(8),
        offset: 0,
        payload: Bytes::from_static(b"stream"),
    };
    let datagram = Frame::DatagramData {
        flow_id: DatagramFlowId(2),
        datagram_id: DatagramId(3),
        ttl_ms: 1_000,
        payload: Bytes::from_static(b"datagram"),
    };

    for frame in [&capacity, &finish, &receipt] {
        assert!(matches!(
            ensure_quic_ordinary_frames(std::slice::from_ref(frame)),
            Err(RuntimeError::Protocol(
                "QUIC measurement records require the dedicated writer"
            ))
        ));
    }
    assert!(ensure_quic_ordinary_frames(&[stream.clone(), datagram.clone()]).is_ok());
    assert!(matches!(
        ensure_quic_ordinary_frames(&[stream, capacity, datagram]),
        Err(RuntimeError::Protocol(
            "QUIC measurement records require the dedicated writer"
        ))
    ));

    assert_eq!(
        Frame::StreamData {
            stream_id: StreamId(8),
            offset: 0,
            payload: Bytes::from_static(b"stream"),
        }
        .write_class(),
        crate::protocol::FrameWriteClass::Ordinary {
            delivery_evidence_bytes: 6,
        }
    );
    assert_eq!(
        Frame::Ping { nonce: 1 }.write_class(),
        crate::protocol::FrameWriteClass::Ordinary {
            delivery_evidence_bytes: 0,
        }
    );
}

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
        sender_quantum.min(UDP_BASELINE_PACKET_PAYLOAD_BYTES).max(1),
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
