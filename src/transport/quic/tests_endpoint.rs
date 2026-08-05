use super::*;
use crate::protocol::Frame;
use crate::protocol::codec::CodecLimits;
use crate::transport::{CarrierPathIdentity, CarrierSocket, CarrierSocketRequest, PathSpec};
use std::time::Duration;
use tokio::time::timeout;

async fn spawn_udp_forwarder(
    upstream_addr: SocketAddr,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let public = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("public UDP forwarder");
    let public_addr = public.local_addr().expect("public forwarder address");
    let upstream = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("upstream UDP socket");
    upstream
        .connect(upstream_addr)
        .await
        .expect("connect UDP forwarder upstream");
    let task = tokio::spawn(async move {
        let mut client_addr = None;
        let mut ingress = vec![0_u8; 65_535];
        let mut egress = vec![0_u8; 65_535];
        loop {
            tokio::select! {
                packet = public.recv_from(&mut ingress) => {
                    let Ok((len, source)) = packet else {
                        return;
                    };
                    client_addr = Some(source);
                    if upstream.send(&ingress[..len]).await.is_err() {
                        return;
                    }
                }
                packet = upstream.recv(&mut egress) => {
                    let Ok(len) = packet else {
                        return;
                    };
                    if let Some(destination) = client_addr
                        && public.send_to(&egress[..len], destination).await.is_err()
                    {
                        return;
                    }
                }
            }
        }
    });
    (public_addr, task)
}

async fn spawn_observed_udp_forwarder(
    upstream_addr: SocketAddr,
) -> (
    SocketAddr,
    tokio::sync::mpsc::UnboundedReceiver<usize>,
    tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    tokio::task::JoinHandle<()>,
) {
    let public = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("observed UDP forwarder");
    let public_addr = public.local_addr().expect("observed forwarder address");
    let upstream = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("observed upstream socket");
    upstream
        .connect(upstream_addr)
        .await
        .expect("connect observed forwarder upstream");
    let (requests, observed_requests) = tokio::sync::mpsc::unbounded_channel();
    let (responses, observed_responses) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let mut client_addr = None;
        let mut request = vec![0_u8; 65_535];
        let mut response = vec![0_u8; 65_535];
        loop {
            tokio::select! {
                received = public.recv_from(&mut request) => {
                    let Ok((len, source)) = received else {
                        return;
                    };
                    client_addr = Some(source);
                    let _ = requests.send(len);
                    if upstream.send(&request[..len]).await.is_err() {
                        return;
                    }
                }
                received = upstream.recv(&mut response) => {
                    let Ok(len) = received else {
                        return;
                    };
                    let _ = responses.send(response[..len].to_vec());
                    if let Some(destination) = client_addr
                        && public.send_to(&response[..len], destination).await.is_err()
                    {
                        return;
                    }
                }
            }
        }
    });
    (public_addr, observed_requests, observed_responses, task)
}

async fn assert_quic_ping_round_trip(client: &Connection, server: &Connection, nonce: u64) {
    let limits = CodecLimits::default();
    let server = server.clone();
    let server_stream = tokio::spawn(async move {
        let (mut send, mut recv) = server.accept_bi().await.expect("server test stream");
        assert_eq!(
            crate::transport::quic::read_frame(&mut recv, limits)
                .await
                .expect("server read ping"),
            Frame::Ping { nonce }
        );
        crate::transport::quic::write_frame(&mut send, &Frame::Pong { nonce }, limits)
            .await
            .expect("server write pong");
    });
    let (mut send, mut recv) = client.open_bi().await.expect("client test stream");
    crate::transport::quic::write_frame(&mut send, &Frame::Ping { nonce }, limits)
        .await
        .expect("client write ping");
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            crate::transport::quic::read_frame(&mut recv, limits)
        )
        .await
        .expect("pong timeout")
        .expect("client read pong"),
        Frame::Pong { nonce }
    );
    server_stream.await.expect("server stream task");
}

#[tokio::test]
async fn quic_carrier_rejects_wrong_independent_tls_identity_before_product_frames() {
    let mux_limits = MuxLimits::default();
    let ip_tls = crate::transport::encrypted::test_client_tls_config_for_server_name("127.0.0.1");
    assert!(matches!(
        Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("client addr"),
            &ip_tls,
            super::super::test_candidate_selector(),
            mux_limits,
        )
        .await,
        Err(QuicCarrierError::H3AuthorityRequiresDnsName)
    ));

    let server_tls = crate::transport::encrypted::test_server_tls_config();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &server_tls,
        super::super::test_candidate_verifier(),
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

    let rcgen::CertifiedKey {
        cert: wrong_certificate,
        ..
    } = rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
        .expect("wrong test certificate");
    let wrong_client_tls = crate::transport::encrypted::test_client_tls_config_for_pinned_leaf(
        wrong_certificate.der().clone(),
    );
    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &wrong_client_tls,
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let err = timeout(Duration::from_secs(5), client.connect(server_addr))
        .await
        .expect("connect timeout")
        .expect_err("wrong TLS identity must fail QUIC authentication");
    match err {
        QuicCarrierError::Connection(_) => {}
        err => panic!("unexpected QUIC wrong-identity error: {err:?}"),
    }

    let good_client_tls = crate::transport::encrypted::test_client_tls_config();
    let good_client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("good client addr"),
        &good_client_tls,
        super::super::test_candidate_selector(),
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
async fn private_initial_rejects_wrong_secret_without_a_response_or_acceptance() {
    let mux_limits = MuxLimits::default();
    let transport_secret = [0x5a; 32];
    let server_tls =
        crate::transport::encrypted::test_server_tls_config_with_transport_secret(transport_secret);
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &server_tls,
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move {
        timeout(Duration::from_secs(5), server.accept())
            .await
            .expect("server accept timeout")
            .expect("server accepts later valid client");
    });

    let wrong_tls =
        crate::transport::encrypted::test_client_tls_config_with_transport_secret([0x3c; 32]);
    let wrong_client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("wrong client addr"),
        &wrong_tls,
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("wrong client endpoint");
    assert!(
        timeout(
            Duration::from_millis(200),
            wrong_client.connect(server_addr)
        )
        .await
        .is_err(),
        "wrong Initial secret must elicit no response"
    );

    let good_client_tls =
        crate::transport::encrypted::test_client_tls_config_with_transport_secret(transport_secret);
    let good_client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("good client addr"),
        &good_client_tls,
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("good client endpoint");
    timeout(Duration::from_secs(5), good_client.connect(server_addr))
        .await
        .expect("good connect timeout")
        .expect("valid client connects after wrong-secret probe");
    server_task.await.expect("server task");
}

#[tokio::test]
async fn private_initial_sends_no_certificate_flight_to_a_public_quic_client() {
    let mux_limits = MuxLimits::default();
    let server_tls =
        crate::transport::encrypted::test_server_tls_config_with_transport_secret([0x5a; 32]);
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &server_tls,
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move { server.accept().await });
    let (forwarder_addr, mut client_requests, mut server_responses, forwarder_task) =
        spawn_observed_udp_forwarder(server_addr).await;

    let public_client_tls = crate::transport::encrypted::test_client_tls_config();
    let public_client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &public_client_tls,
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("public client endpoint");
    let client_task = tokio::spawn(async move { public_client.connect(forwarder_addr).await });

    timeout(Duration::from_secs(1), client_requests.recv())
        .await
        .expect("public Initial timeout")
        .expect("public Initial observed");

    assert!(
        timeout(Duration::from_millis(300), server_responses.recv())
            .await
            .is_err(),
        "a public QUIC Initial must elicit no server bytes or certificate flight"
    );

    client_task.abort();
    server_task.abort();
    forwarder_task.abort();
    let _ = client_task.await;
    let _ = server_task.await;
    let _ = forwarder_task.await;
}

#[tokio::test]
async fn private_initial_suppresses_unauthenticated_endpoint_responses() {
    let mux_limits = MuxLimits::default();
    let transport_secret = [0x5a; 32];
    let server_tls =
        crate::transport::encrypted::test_server_tls_config_with_transport_secret(transport_secret);
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &server_tls,
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move {
        timeout(Duration::from_secs(5), server.accept())
            .await
            .expect("server accept timeout")
            .expect("server accepts later valid client");
    });

    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("probe socket");
    let mut unsupported_initial = Vec::new();
    unsupported_initial.push(0xc0);
    unsupported_initial.extend_from_slice(&0x0a1a_2a3au32.to_be_bytes());
    unsupported_initial.push(8);
    unsupported_initial.extend_from_slice(&[0x11; 8]);
    unsupported_initial.push(8);
    unsupported_initial.extend_from_slice(&[0x22; 8]);
    probe
        .send_to(&unsupported_initial, server_addr)
        .await
        .expect("send unsupported-version Initial");
    let mut response = [0u8; 1500];
    assert!(
        timeout(Duration::from_millis(200), probe.recv_from(&mut response))
            .await
            .is_err(),
        "private endpoint must not send Version Negotiation before authentication"
    );

    let mut unknown_short_header = vec![0u8; 64];
    unknown_short_header[0] = 0x40;
    getrandom::fill(&mut unknown_short_header[1..]).expect("random short-header probe");
    probe
        .send_to(&unknown_short_header, server_addr)
        .await
        .expect("send unknown short-header packet");
    assert!(
        timeout(Duration::from_millis(200), probe.recv_from(&mut response))
            .await
            .is_err(),
        "private endpoint must not send a stateless reset before authentication"
    );

    let good_client_tls =
        crate::transport::encrypted::test_client_tls_config_with_transport_secret(transport_secret);
    let good_client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("good client addr"),
        &good_client_tls,
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("good client endpoint");
    timeout(Duration::from_secs(5), good_client.connect(server_addr))
        .await
        .expect("good connect timeout")
        .expect("valid client connects after unauthenticated probes");
    server_task.await.expect("server task");
}

#[tokio::test]
async fn quic_carrier_rejects_non_h3_alpn_during_tls_handshake() {
    let server_tls = crate::transport::encrypted::test_server_tls_config();
    let base_client_tls = crate::transport::encrypted::test_client_tls_config();
    let mut wrong_alpn = (*base_client_tls.rustls_config()).clone();
    wrong_alpn.alpn_protocols = vec![b"h2".to_vec()];
    let wrong_client_tls = base_client_tls.with_rustls_config_for_test(wrong_alpn);
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server address"),
        &server_tls,
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client address"),
        &wrong_client_tls,
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let server_addr = server.local_addr().expect("server address");
    let server_task =
        tokio::spawn(async move { timeout(Duration::from_secs(5), server.accept()).await });

    timeout(Duration::from_secs(5), client.connect(server_addr))
        .await
        .expect("connect timeout")
        .expect_err("QUIC must reject a non-H3 ALPN before product frames");
    server_task.abort();
    let _ = server_task.await;
}

#[tokio::test]
async fn quic_keep_alive_preserves_a_quiet_authenticated_carrier() {
    let mux_limits = MuxLimits {
        quic_path_keep_alive_interval: Duration::from_millis(20),
        quic_path_idle_timeout: Duration::from_millis(80),
        ..MuxLimits::default()
    };
    let server_tls = crate::transport::encrypted::test_server_tls_config();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &server_tls,
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let accepted = tokio::spawn(async move { server.accept().await.expect("server connection") });
    let client_tls = crate::transport::encrypted::test_client_tls_config();
    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &client_tls,
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let client_connection = client
        .connect(server_addr)
        .await
        .expect("client connection");
    let server_connection = accepted.await.expect("accept task");

    assert_eq!(
        client_connection.negotiated_protocol().as_deref(),
        Some(crate::transport::encrypted::HTTP_3_ALPN)
    );
    assert_eq!(
        server_connection.negotiated_protocol().as_deref(),
        Some(crate::transport::encrypted::HTTP_3_ALPN)
    );

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(!client_connection.is_closed());
    assert!(!server_connection.is_closed());
}

#[tokio::test]
async fn quic_destination_port_migration_preserves_connection_and_streams() {
    let mux_limits = MuxLimits {
        quic_path_keep_alive_interval: Duration::from_millis(20),
        quic_path_idle_timeout: Duration::from_secs(2),
        ..MuxLimits::default()
    };
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server address"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server address");
    let (first_port, first_forwarder) = spawn_udp_forwarder(server_addr).await;
    let (second_port, second_forwarder) = spawn_udp_forwarder(server_addr).await;
    let accepted = tokio::spawn(async move { server.accept().await.expect("server connection") });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client address"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let client_connection = timeout(Duration::from_secs(5), client.connect(first_port))
        .await
        .expect("initial connection timeout")
        .expect("initial client connection");
    let server_connection = timeout(Duration::from_secs(5), accepted)
        .await
        .expect("server accept timeout")
        .expect("server accept task");
    assert_quic_ping_round_trip(&client_connection, &server_connection, 90).await;

    let first = first_port.port().min(second_port.port());
    let last = first_port.port().max(second_port.port());
    let path: PathSpec = format!("udp://127.0.0.1:{first}-{last}")
        .parse()
        .expect("ranged UDP path");
    let carrier = CarrierSocket::system(CarrierSocketRequest {
        path: &path,
        identity: CarrierPathIdentity {
            group_ordinal: 0,
            path_ordinal: 0,
        },
        remote_addr: second_port,
    })
    .expect("replacement carrier socket");
    let receipt = client
        .rebind_client_socket(carrier, first_port, second_port)
        .expect("start destination-port migration");
    timeout(Duration::from_secs(5), receipt.wait())
        .await
        .expect("destination-port migration confirmation");
    assert_quic_ping_round_trip(&client_connection, &server_connection, 91).await;

    let carrier = CarrierSocket::system(CarrierSocketRequest {
        path: &path,
        identity: CarrierPathIdentity {
            group_ordinal: 0,
            path_ordinal: 0,
        },
        remote_addr: first_port,
    })
    .expect("second replacement carrier socket");
    let receipt = client
        .rebind_client_socket(carrier, first_port, first_port)
        .expect("start second destination-port migration");
    timeout(Duration::from_secs(5), receipt.wait())
        .await
        .expect("second destination-port migration confirmation");
    assert_quic_ping_round_trip(&client_connection, &server_connection, 92).await;

    assert!(!client_connection.is_closed());
    first_forwarder.abort();
    second_forwarder.abort();
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
    assert!(rendered.contains("max_concurrent_uni_streams: 4"));
    assert!(rendered.contains("max_idle_timeout: Some(30000)"));
    assert!(rendered.contains("keep_alive_interval: Some(10s)"));
}

#[tokio::test]
async fn transport_secret_holder_without_mpp_credential_receives_uniform_not_found() {
    use bytes::Buf;
    use h3::ConnectionState;
    use std::future::poll_fn;

    let mux_limits = MuxLimits::default();
    let transport_secret = [0x5a; 32];
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config_with_transport_secret(
            transport_secret,
        ),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (probes_done, probes_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted H3 connection");
        tokio::select! {
            biased;
            accepted = connection.accept_bi() => {
                panic!("unauthorized request reached the accepted stream queue: {accepted:?}");
            }
            result = probes_done_rx => {
                result.expect("probe completion signal");
            }
        }
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config_with_transport_secret(
            transport_secret,
        ),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let raw = client
        .endpoint
        .connect(server_addr, "mptunnel.test")
        .expect("start QUIC connect")
        .await
        .expect("complete QUIC connect");
    let (mut driver, mut requests): (
        h3::client::Connection<h3_quinn::Connection, bytes::Bytes>,
        h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    ) = h3::client::builder()
        .max_field_section_size(4 * 1024)
        .enable_datagram(true)
        .build(h3_quinn::Connection::new(raw))
        .await
        .expect("build ordinary H3 client");
    let driver_task = tokio::spawn(async move {
        let _ = poll_fn(|cx| driver.poll_close(cx)).await;
    });
    let mut stream = requests
        .send_request(
            http::Request::get("https://mptunnel.test/")
                .body(())
                .expect("ordinary request"),
        )
        .await
        .expect("send ordinary H3 request");
    stream.finish().await.expect("finish ordinary request");
    let response = stream.recv_response().await.expect("receive H3 response");
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
    let public_headers = response.headers().clone();
    let mut body = stream
        .recv_data()
        .await
        .expect("receive H3 body")
        .expect("404 has a body");
    assert_eq!(body.copy_to_bytes(body.remaining()), b"Not Found\n"[..]);

    // Even after possessing the endpoint transport secret and pinned identity,
    // knowing the request shape and sending binary body bytes is insufficient:
    // without an MPP credential the request takes the uniform decoy response
    // path and never enters the MPP stream queue.
    let mut probe = requests
        .send_request(
            http::Request::post("https://mptunnel.test/")
                .header(http::header::CONTENT_TYPE, "application/octet-stream")
                .header("mpp-datagram", "?1")
                .header(
                    http::header::AUTHORIZATION,
                    format!("Bearer {}", "00".repeat(32)),
                )
                .body(())
                .expect("source-informed probe"),
        )
        .await
        .expect("send source-informed probe");
    probe
        .send_data(bytes::Bytes::from_static(b"\0\0\0\x08MPP probe"))
        .await
        .expect("send binary probe body");
    probe.finish().await.expect("finish binary probe");
    let response = probe.recv_response().await.expect("receive probe response");
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);
    assert_eq!(response.headers(), &public_headers);
    let mut body = probe
        .recv_data()
        .await
        .expect("receive probe body")
        .expect("probe 404 has a body");
    assert_eq!(body.copy_to_bytes(body.remaining()), b"Not Found\n"[..]);
    assert!(
        requests.settings().enable_datagram(),
        "peer SETTINGS must explicitly negotiate H3 DATAGRAM"
    );
    assert!(
        !requests.settings().enable_extended_connect(),
        "the POST-based MPP extension must not claim Extended CONNECT"
    );

    probes_done.send(()).expect("server probe waiter");
    server_task.await.expect("server task");
    driver_task.abort();
    let _ = driver_task.await;
}

async fn malformed_http_datagram_close(packet: bytes::Bytes) -> quinn::ConnectionError {
    use h3::ConnectionState;
    use std::future::poll_fn;

    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task =
        tokio::spawn(async move { server.accept().await.expect("server connection") });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let raw = client
        .endpoint
        .connect(server_addr, "mptunnel.test")
        .expect("start QUIC connect")
        .await
        .expect("complete QUIC connect");
    let (mut driver, requests): (
        h3::client::Connection<h3_quinn::Connection, bytes::Bytes>,
        h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    ) = h3::client::builder()
        .enable_datagram(true)
        .build(h3_quinn::Connection::new(raw.clone()))
        .await
        .expect("build H3 client");
    let driver_task = tokio::spawn(async move {
        let _ = poll_fn(|cx| driver.poll_close(cx)).await;
    });
    let _server_connection = timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server H3 setup timeout")
        .expect("server H3 task");

    for _ in 0..64 {
        if requests.settings().enable_datagram() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        requests.settings().enable_datagram(),
        "peer SETTINGS must negotiate H3 DATAGRAM before the malformed packet"
    );
    raw.send_datagram(packet)
        .expect("send malformed HTTP Datagram");
    let error = timeout(Duration::from_secs(1), raw.closed())
        .await
        .expect("malformed Quarter Stream ID must close the H3 connection");
    driver_task.abort();
    let _ = driver_task.await;
    error
}

#[tokio::test]
async fn malformed_quarter_stream_id_closes_h3_with_datagram_error() {
    let oversized =
        bytes::Bytes::copy_from_slice(&((0b11_u64 << 62) | (1_u64 << 60)).to_be_bytes());
    for malformed in [bytes::Bytes::from_static(&[0x40]), oversized] {
        match malformed_http_datagram_close(malformed).await {
            quinn::ConnectionError::ApplicationClosed(close) => {
                assert_eq!(
                    close.error_code.into_inner(),
                    h3::error::Code::H3_DATAGRAM_ERROR.value()
                );
                assert!(close.reason.is_empty());
            }
            error => panic!("unexpected malformed HTTP Datagram close: {error:?}"),
        }
    }
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

    let session_limited = quic_transport_config(MuxLimits {
        max_streams: 32,
        max_quic_concurrent_bidi_streams: 4096,
        ..MuxLimits::default()
    })
    .expect("session-limited QUIC transport");
    assert!(
        format!("{session_limited:?}").contains("max_concurrent_bidi_streams: 32"),
        "QUIC/H3 admission must not exceed the session stream envelope"
    );
}
