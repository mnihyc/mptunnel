use super::*;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, UDP_BASELINE_PACKET_PAYLOAD_BYTES,
    reliable_relay_scheduler_quantum_cap,
};
use crate::protocol::{DatagramFlowId, DatagramId, PathId, StreamId};
use crate::runtime::path::commands::{
    reliable_path_command_queue_for_payload, reliable_stream_frame_queue,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tokio::sync::{mpsc, oneshot};

#[test]
fn quic_stream_priority_uses_only_default_and_latency_levels() {
    for lane in [
        TrafficClass::Control,
        TrafficClass::Latency,
        TrafficClass::RealtimeDatagram,
    ] {
        assert_eq!(quic_stream_priority(lane), QUIC_LATENCY_STREAM_PRIORITY);
    }
    assert_eq!(
        quic_stream_priority(TrafficClass::Throughput),
        QUIC_DEFAULT_STREAM_PRIORITY
    );
}

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
fn quic_writer_rejects_tcp_capacity_frames() {
    let capacity = Frame::PathCapacityData {
        path_id: PathId(4),
        measurement_id: 17,
        payload: Bytes::from_static(b"capacity"),
    };
    let finish = Frame::PathCapacityFinish {
        path_id: PathId(4),
        measurement_id: 17,
        payload_bytes: 8,
    };
    let receipt = Frame::PathCapacityReceipt {
        path_id: PathId(4),
        measurement_id: 17,
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
            ensure_quic_data_plane_frames(std::slice::from_ref(frame)),
            Err(RuntimeError::Protocol(
                "PATH_CAPACITY frames are not valid on QUIC carriers"
            ))
        ));
    }
    assert!(ensure_quic_data_plane_frames(&[stream.clone(), datagram.clone()]).is_ok());
    assert!(matches!(
        ensure_quic_data_plane_frames(&[stream, capacity, datagram]),
        Err(RuntimeError::Protocol(
            "PATH_CAPACITY frames are not valid on QUIC carriers"
        ))
    ));
}

#[test]
fn quic_product_payload_uses_sender_quantum_not_packet_train_cap() {
    let mux_limits = MuxLimits::default();
    let codec_limits = CodecLimits::default();
    let payload_cap = udp_path_max_stream_payload_bytes(codec_limits, mux_limits);

    assert!(
        payload_cap >= MAX_RELIABLE_SERVICE_QUANTUM_BYTES,
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
        reliable_relay_scheduler_quantum_cap(None, TrafficClass::Throughput, mux_limits);
    let record_sized_queue = reliable_path_command_queue_for_payload(
        mux_limits,
        sender_quantum.clamp(1, UDP_BASELINE_PACKET_PAYLOAD_BYTES),
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

#[test]
fn quic_clean_stream_finish_is_distinct_from_truncated_frame() {
    let clean = RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::StreamFinished);
    let truncated = RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::UnexpectedEnd);

    assert!(udp_path_input_finished(&clean));
    assert!(!udp_path_frame_finished(&clean));
    assert!(!udp_path_input_finished(&truncated));
    assert!(!udp_path_frame_finished(&truncated));
}

#[test]
fn quic_h3_zero_code_peer_abandonment_is_operation_scoped() {
    let abandoned = RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::H3Stream(
        h3::error::StreamError::RemoteTerminate {
            code: h3::error::Code::from(0_u64),
        },
    ));

    assert!(!udp_runtime_error_is_expected_shutdown(&abandoned));
    assert!(udp_operation_error_is_expected_shutdown(&abandoned));
}

#[test]
fn quic_h3_application_cancel_and_other_failures_remain_unexpected() {
    let cancelled = RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::H3Stream(
        h3::error::StreamError::RemoteTerminate {
            code: h3::error::Code::H3_REQUEST_CANCELLED,
        },
    ));
    let malformed = RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::UnexpectedEnd);

    for error in [&cancelled, &malformed] {
        assert!(!udp_runtime_error_is_expected_shutdown(error));
        assert!(!udp_operation_error_is_expected_shutdown(error));
    }
}

#[tokio::test]
async fn quic_write_wait_routes_stream_feedback_before_an_ordering_barrier() {
    let (input_tx, mut input_rx) = mpsc::channel(4);
    let (release_write, write_released) = oneshot::channel::<()>();
    let (routed_signal, routed) = oneshot::channel::<()>();
    let (barrier_signal, barrier) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        let mut routed_signal = Some(routed_signal);
        let mut barrier_signal = Some(barrier_signal);
        let mut deferred_input = None;
        let (write_result, routed_frames) = await_udp_write_while_routing_stream_frames(
            async move {
                write_released.await.expect("release simulated QUIC write");
                17usize
            },
            &mut input_rx,
            &mut deferred_input,
            |frame| match frame {
                Frame::StreamAck { .. } => {
                    if let Some(signal) = routed_signal.take() {
                        let _ = signal.send(());
                    }
                    Ok(None)
                }
                frame => {
                    if let Some(signal) = barrier_signal.take() {
                        let _ = signal.send(());
                    }
                    Ok(Some(frame))
                }
            },
        )
        .await;
        (write_result, routed_frames, deferred_input)
    });

    input_tx
        .send(Ok(Frame::StreamAck {
            stream_id: StreamId(8),
            complete: false,
            ranges: Vec::new(),
        }))
        .await
        .expect("queue stream feedback");
    routed.await.expect("route stream feedback during write");
    input_tx
        .send(Ok(Frame::Ping { nonce: 9 }))
        .await
        .expect("queue ordering barrier");
    barrier.await.expect("defer ordering barrier during write");
    assert!(
        !task.is_finished(),
        "an ordering barrier must not cancel the in-flight QUIC write"
    );
    release_write
        .send(())
        .expect("complete simulated QUIC write");

    let (write_result, routed_frames, deferred_input) = task.await.expect("join write wait");
    assert_eq!(write_result, 17);
    assert_eq!(routed_frames, 1);
    assert!(matches!(deferred_input, Some(Ok(Frame::Ping { nonce: 9 }))));
}
