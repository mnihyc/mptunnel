use super::super::Endpoint;
use super::*;
use crate::mux::MuxLimits;
use crate::protocol::StreamId;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::timeout;

#[test]
fn quic_writer_splits_large_stream_data_below_product_scheduler() {
    let limits = CodecLimits::default();
    let payload = Bytes::from(vec![7u8; QUIC_STREAM_RECORD_PAYLOAD_BYTES * 2 + 17]);
    let mut packet = Vec::new();
    encode_quic_length_prefixed_frame(
        &Frame::StreamData {
            stream_id: StreamId(9),
            offset: 123,
            payload,
        },
        limits,
        &mut packet,
    )
    .expect("encode split stream data");

    let mut cursor = 0usize;
    let mut decoded = Vec::new();
    while cursor < packet.len() {
        let len = u32::from_be_bytes([
            packet[cursor],
            packet[cursor + 1],
            packet[cursor + 2],
            packet[cursor + 3],
        ]) as usize;
        cursor += FRAME_LEN_BYTES;
        let frame = decode_frame_bytes(
            Bytes::copy_from_slice(&packet[cursor..cursor + len]),
            limits,
        )
        .expect("decode split carrier record");
        decoded.push(frame);
        cursor += len;
    }

    assert_eq!(decoded.len(), 3);
    let mut expected_offset = 123u64;
    for frame in &decoded {
        let Frame::StreamData {
            stream_id,
            offset,
            payload,
        } = frame
        else {
            panic!("all split records must remain STREAM_DATA");
        };
        assert_eq!(*stream_id, StreamId(9));
        assert_eq!(*offset, expected_offset);
        expected_offset = expected_offset.saturating_add(payload.len() as u64);
        assert!(payload.len() <= QUIC_STREAM_RECORD_PAYLOAD_BYTES);
    }
}

#[tokio::test]
async fn quic_carrier_round_trips_product_frames() {
    let secret = b"0123456789abcdef0123456789abcdef";
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        match read_frame(&mut recv, limits)
            .await
            .expect("server read ping")
        {
            Frame::Ping { nonce } => {
                write_frame(&mut send, &Frame::Pong { nonce }, limits)
                    .await
                    .expect("server write pong");
                finish_stream(&mut send).expect("server finish stream");
            }
            frame => panic!("unexpected frame: {frame:?}"),
        }
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("client stream");
    send.set_priority(1).expect("set QUIC stream priority");
    assert_eq!(send.priority().expect("read QUIC stream priority"), 1);
    write_frame(&mut send, &Frame::Ping { nonce: 42 }, limits)
        .await
        .expect("client write ping");
    assert_eq!(connection.congestion_metrics().pending_bytes, 0);
    assert!(!connection.is_closed());
    finish_stream(&mut send).expect("client finish stream");
    let response = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("response timeout")
        .expect("client read pong");
    assert_eq!(response, Frame::Pong { nonce: 42 });
    let finished = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("stream finish timeout")
        .expect_err("server finished its QUIC send half");
    assert!(matches!(finished, QuicCarrierError::StreamFinished));
    let _ = client_done_tx.send(());

    server_task.await.expect("server task");
}
#[tokio::test]
async fn stopped_quic_stream_write_keeps_the_shared_connection_available() {
    let secret = b"0123456789abcdef0123456789abcdef";
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let stop_code = VarInt::from_u32(37);
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read opener"),
            Frame::Ping { nonce: 1 }
        );
        recv.stream.stop(stop_code).expect("stop client writer");
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .expect("accept replacement stream");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read replacement"),
            Frame::Ping { nonce: 2 }
        );
        write_frame(&mut send, &Frame::Pong { nonce: 2 }, limits)
            .await
            .expect("write replacement response");
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("open carrier stream");
    assert_eq!(connection.congestion_metrics().pending_bytes, 0);
    assert!(!connection.is_closed());

    assert_eq!(
        timeout(Duration::from_secs(5), send.stream.stopped())
            .await
            .expect("STOP_SENDING timeout")
            .expect("connection remains available"),
        Some(stop_code)
    );
    let payload = Bytes::from_static(b"monotonic delivery evidence");
    let payload_len = payload.len() as u64;
    let err = write_frame(
        &mut send,
        &Frame::StreamData {
            stream_id: StreamId(9),
            offset: 0,
            payload,
        },
        limits,
    )
    .await
    .expect_err("stopped stream write must fail");

    assert!(matches!(
        err,
        QuicCarrierError::Write(quinn::WriteError::Stopped(code)) if code == stop_code
    ));
    let metrics = connection.congestion_metrics();
    assert_eq!(metrics.pending_bytes, 0);
    assert_eq!(metrics.delivery_evidence_written_bytes, payload_len);
    assert!(!connection.is_closed());

    let (mut replacement_send, mut replacement_recv) =
        connection.open_bi().await.expect("open replacement stream");
    write_frame(&mut replacement_send, &Frame::Ping { nonce: 2 }, limits)
        .await
        .expect("write replacement request");
    assert_eq!(
        read_frame(&mut replacement_recv, limits)
            .await
            .expect("read replacement response"),
        Frame::Pong { nonce: 2 }
    );

    let _ = client_done_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn cancelled_quic_write_fail_closes_and_releases_backlog() {
    let secret = b"0123456789abcdef0123456789abcdef";
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024,
        max_repair_bytes: 4 * 1024,
        max_reorder_bytes: 4 * 1024,
        max_datagram_queue_bytes: 4 * 1024,
        max_path_flight_bytes: 4 * 1024,
        max_reliable_relay_chunk_bytes: 4 * 1024,
        ..MuxLimits::default()
    };
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (server_ready_tx, server_ready_rx) = tokio::sync::oneshot::channel();
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read opener"),
            Frame::Ping { nonce: 1 }
        );
        let _ = server_ready_tx.send(());
        let _recv = recv;
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("open carrier stream");
    timeout(Duration::from_secs(5), server_ready_rx)
        .await
        .expect("server ready timeout")
        .expect("server ready sender");

    let payload_len = 256 * 1024;
    let write_task = tokio::spawn(async move {
        write_frame(
            &mut send,
            &Frame::StreamData {
                stream_id: StreamId(9),
                offset: 0,
                payload: Bytes::from(vec![0x5a; payload_len]),
            },
            limits,
        )
        .await
    });
    timeout(Duration::from_secs(5), async {
        loop {
            if connection.congestion_metrics().pending_bytes > 0 {
                break;
            }
            assert!(!write_task.is_finished(), "constrained write must block");
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write did not enter backlog");

    write_task.abort();
    assert!(
        write_task
            .await
            .expect_err("aborted writer must be cancelled")
            .is_cancelled()
    );
    let metrics = connection.congestion_metrics();
    assert_eq!(metrics.pending_bytes, 0);
    assert_eq!(metrics.delivery_evidence_written_bytes, payload_len as u64);
    assert!(connection.is_closed());

    let _ = client_done_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn quic_carrier_batches_multiple_product_frames_per_write() {
    let secret = b"0123456789abcdef0123456789abcdef";
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read first"),
            Frame::Ping { nonce: 1 }
        );
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read second"),
            Frame::Pong { nonce: 2 }
        );
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frames(
        &mut send,
        &[Frame::Ping { nonce: 1 }, Frame::Pong { nonce: 2 }],
        limits,
    )
    .await
    .expect("client write batch");
    finish_stream(&mut send).expect("client finish stream");
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server task timeout")
        .expect("server task");
}
