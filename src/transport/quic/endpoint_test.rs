use super::*;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn quic_carrier_rejects_wrong_shared_secret_before_product_frames() {
    let server_secret = b"0123456789abcdef0123456789abcdef";
    let wrong_client_secret = b"fedcba9876543210fedcba9876543210";
    let good_client_secret = server_secret;
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        server_secret,
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move {
        timeout(Duration::from_secs(5), server.accept())
            .await
            .expect("server accept timeout")
            .expect("server should accept the later valid client");
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        wrong_client_secret,
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let err = timeout(Duration::from_secs(5), client.connect(server_addr))
        .await
        .expect("connect timeout")
        .expect_err("wrong secret must fail QUIC authentication");
    match err {
        QuicCarrierError::Connection(_) => {}
        err => panic!("unexpected QUIC wrong-secret error: {err:?}"),
    }

    let good_client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("good client addr"),
        good_client_secret,
        mux_limits,
    )
    .await
    .expect("good client endpoint");
    timeout(Duration::from_secs(5), good_client.connect(server_addr))
        .await
        .expect("good connect timeout")
        .expect("valid client should connect after failed handshake");

    server_task.await.expect("server task");
}

#[tokio::test]
async fn quic_keep_alive_preserves_a_quiet_authenticated_carrier() {
    let secret = b"0123456789abcdef0123456789abcdef";
    let mux_limits = MuxLimits {
        quic_path_keep_alive_interval: Duration::from_millis(20),
        quic_path_idle_timeout: Duration::from_millis(80),
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
    let accepted = tokio::spawn(async move { server.accept().await.expect("server connection") });
    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let client_connection = client
        .connect(server_addr)
        .await
        .expect("client connection");
    let server_connection = accepted.await.expect("accept task");

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(!client_connection.is_closed());
    assert!(!server_connection.is_closed());
}

#[test]
fn quic_transport_profile_follows_mux_resource_envelope() {
    let mux_limits = MuxLimits::default();
    let transport = quic_transport_config(mux_limits).expect("transport config");
    let rendered = format!("{transport:?}");
    let stream_window = mux_limits.max_stream_window_bytes;
    let receive_window = stream_window
        + mux_limits.max_repair_bytes as u64
        + mux_limits.max_reorder_bytes as u64
        + mux_limits.max_datagram_queue_bytes as u64
        + mux_limits.max_path_flight_bytes as u64;
    let send_window = mux_limits.max_path_flight_bytes as u64;
    let bidi_streams = mux_limits.max_quic_concurrent_bidi_streams;
    assert!(rendered.contains(&format!("stream_receive_window: {stream_window}")));
    assert!(rendered.contains(&format!("receive_window: {receive_window}")));
    assert!(rendered.contains(&format!("send_window: {send_window}")));
    assert!(rendered.contains(&format!("max_concurrent_bidi_streams: {bidi_streams}")));
    assert!(rendered.contains("max_concurrent_uni_streams: 0"));
    assert!(rendered.contains("max_idle_timeout: Some(30000)"));
    assert!(rendered.contains("keep_alive_interval: Some(10s)"));
}

#[test]
fn quic_stream_limit_is_independent_from_receive_window_ratio() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 64 * 1024 * 1024,
        max_repair_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        max_datagram_queue_bytes: 16 * 1024 * 1024,
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_streams: 65_536,
        max_quic_concurrent_bidi_streams: 4096,
        ..MuxLimits::default()
    };
    let transport = quic_transport_config(mux_limits).expect("transport config");
    let rendered = format!("{transport:?}");

    assert!(rendered.contains("max_concurrent_bidi_streams: 4096"));
    assert!(!rendered.contains("max_concurrent_bidi_streams: 4,"));
}
