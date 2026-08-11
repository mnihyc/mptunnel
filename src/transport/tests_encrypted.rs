use super::*;
use crate::protocol::{Frame, SessionId, StreamId};
use bytes::Bytes;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf, duplex};

async fn connected_pair(
    capacity: usize,
) -> (
    EncryptedFramedStream<tokio::io::DuplexStream>,
    EncryptedFramedStream<tokio::io::DuplexStream>,
) {
    let (client_io, server_io) = duplex(capacity);
    let limits = CodecLimits::default();
    let (client, server) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &test_tls_configs().0, limits),
        EncryptedFramedStream::accept(server_io, &test_tls_configs().1, limits),
    );
    (
        client.expect("client TLS handshake"),
        server.expect("server TLS handshake"),
    )
}

async fn transport_secret_pair(
    capacity: usize,
) -> (
    EncryptedFramedStream<tokio::io::DuplexStream>,
    EncryptedFramedStream<tokio::io::DuplexStream>,
) {
    let secret = [0x5a; 32];
    let client_config = test_client_tls_config_with_transport_secret(secret);
    let server_config = test_server_tls_config_with_transport_secret(secret);
    let (client_io, server_io) = duplex(capacity);
    let limits = CodecLimits::default();
    let (client, server) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &client_config, limits),
        EncryptedFramedStream::accept(server_io, &server_config, limits),
    );
    (
        client.expect("client Noise handshake"),
        server.expect("server Noise handshake"),
    )
}

fn independent_tls_configs(server_name: &str) -> (TcpClientTlsConfig, TcpServerTlsConfig) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![server_name.to_string()])
            .expect("generate TLS identity");
    let certificate = CertificateDer::from(cert);
    let private_key =
        rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into();
    (
        TcpClientTlsConfig::new(server_name, certificate.clone()).expect("client config"),
        TcpServerTlsConfig::new(vec![certificate], private_key).expect("server config"),
    )
}

struct TamperNextWrite<S> {
    inner: S,
    armed: Arc<AtomicBool>,
}

struct CountWrites<S> {
    inner: S,
    bytes: Arc<AtomicU64>,
}

struct CaptureWrites<S> {
    inner: S,
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for CaptureWrites<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CaptureWrites<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.bytes
                    .lock()
                    .expect("capture state")
                    .extend_from_slice(&buf[..written]);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CountWrites<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountWrites<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.bytes.fetch_add(written as u64, Ordering::Relaxed);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TamperNextWrite<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TamperNextWrite<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.armed.swap(false, Ordering::AcqRel) && !buf.is_empty() {
            let mut tampered = buf.to_vec();
            let index = tampered.len() - 1;
            tampered[index] ^= 1;
            Pin::new(&mut this.inner).poll_write(cx, &tampered)
        } else {
            Pin::new(&mut this.inner).poll_write(cx, buf)
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn tls13_carrier_round_trips_duplex_frames_and_batches() {
    let (mut client, mut server) = connected_pair(64 * 1024).await;
    let hello = Frame::SessionHello {
        session_id: SessionId(42),
    };
    let replies = [Frame::Ping { nonce: 7 }, Frame::Pong { nonce: 7 }];

    client.write_frame(&hello).await.expect("write hello");
    client.flush().await.expect("flush hello");
    assert_eq!(server.read_frame().await.expect("read hello"), hello);

    server.write_frames(&replies).await.expect("write replies");
    server.flush().await.expect("flush replies");
    assert_eq!(client.read_frame().await.expect("read ping"), replies[0]);
    assert_eq!(client.read_frame().await.expect("read pong"), replies[1]);
}

#[tokio::test]
async fn exact_leaf_pin_rejects_a_different_server_identity() {
    let (client_io, server_io) = duplex(64 * 1024);
    let (client, _) = independent_tls_configs("mptunnel.test");
    let (_, wrong_server) = independent_tls_configs("mptunnel.test");
    let (client_result, server_result) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &client, CodecLimits::default()),
        EncryptedFramedStream::accept(server_io, &wrong_server, CodecLimits::default()),
    );

    assert!(client_result.is_err());
    assert!(server_result.is_err());
}

#[tokio::test]
async fn webpki_rejects_a_pinned_certificate_for_the_wrong_server_name() {
    let (client_io, server_io) = duplex(64 * 1024);
    let (_, server) = independent_tls_configs("right.mptunnel.test");
    let certificate = server.certificate_chain[0].clone();
    let client =
        TcpClientTlsConfig::new("wrong.mptunnel.test", certificate).expect("client config");
    let (client_result, server_result) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &client, CodecLimits::default()),
        EncryptedFramedStream::accept(server_io, &server, CodecLimits::default()),
    );

    assert!(client_result.is_err());
    assert!(server_result.is_err());
}

#[tokio::test]
async fn wrong_shared_transport_secret_is_rejected_before_transport_mode() {
    let (client_io, server_io) = duplex(64 * 1024);
    let server_written = Arc::new(AtomicU64::new(0));
    let server_io = CountWrites {
        inner: server_io,
        bytes: server_written.clone(),
    };
    let client = test_client_tls_config_with_transport_secret([0x5a; 32]);
    let server = test_server_tls_config_with_transport_secret([0x33; 32]);
    let (client_result, server_result) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &client, CodecLimits::default()),
        EncryptedFramedStream::accept(server_io, &server, CodecLimits::default()),
    );
    assert!(client_result.is_err());
    assert!(server_result.is_err());
    assert_eq!(
        server_written.load(Ordering::Relaxed),
        0,
        "wrong shared transport secret must elicit no server bytes"
    );
}

#[tokio::test]
async fn transport_secret_configuration_is_symmetric_and_never_downgrades() {
    let (tls_client, tls_server) = test_tls_configs();
    let noise_client = test_client_tls_config_with_transport_secret([0x5a; 32]);
    let noise_server = test_server_tls_config_with_transport_secret([0x5a; 32]);

    for (client, server) in [(&noise_client, tls_server), (tls_client, &noise_server)] {
        let (client_io, server_io) = duplex(64 * 1024);
        let (client_result, server_result) =
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                tokio::join!(
                    EncryptedFramedStream::connect(client_io, client, CodecLimits::default()),
                    EncryptedFramedStream::accept(server_io, server, CodecLimits::default()),
                )
            })
            .await
            .expect("mismatched transport modes terminate promptly");

        assert!(
            client_result.is_err(),
            "client must reject the mode mismatch"
        );
        assert!(
            server_result.is_err(),
            "server must reject the mode mismatch"
        );
    }
}

#[tokio::test]
async fn replayed_noise_client_hello_is_rejected_without_server_bytes() {
    let secret = [0x5a; 32];
    let client_config = test_client_tls_config_with_transport_secret(secret);
    let server_config = test_server_tls_config_with_transport_secret(secret);
    let captured = Arc::new(Mutex::new(Vec::new()));
    let (client_io, server_io) = duplex(64 * 1024);
    let client_io = CaptureWrites {
        inner: client_io,
        bytes: captured.clone(),
    };
    let (client, server) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &client_config, CodecLimits::default()),
        EncryptedFramedStream::accept(server_io, &server_config, CodecLimits::default()),
    );
    drop(client.expect("original client handshake"));
    drop(server.expect("original server handshake"));
    let first_flight = captured.lock().expect("captured first flight").clone();
    assert!(!first_flight.is_empty());

    let (mut replay_io, server_io) = duplex(64 * 1024);
    let server_written = Arc::new(AtomicU64::new(0));
    let server_io = CountWrites {
        inner: server_io,
        bytes: server_written.clone(),
    };
    let (replay_result, server_result) = tokio::join!(
        async {
            replay_io.write_all(&first_flight).await?;
            replay_io.shutdown().await
        },
        EncryptedFramedStream::accept(server_io, &server_config, CodecLimits::default()),
    );
    replay_result.expect("replay reached server");
    assert!(matches!(
        server_result,
        Err(EncryptedFramedTransportError::NoiseClientHelloRejected)
    ));
    assert_eq!(
        server_written.load(Ordering::Relaxed),
        0,
        "replayed protected first flight must not expose a response or certificate"
    );
}

#[tokio::test]
async fn tls_has_no_tcp_alpn_and_bindings_are_per_connection() {
    let (client, server) = connected_pair(64 * 1024).await;
    let client_alpn = match &client.inner {
        EncryptedFramedStreamInner::Tls(stream) => match &stream.stream {
            TlsStream::Client(stream) => stream.get_ref().1.alpn_protocol(),
            TlsStream::Server(_) => unreachable!("client stream role"),
        },
        EncryptedFramedStreamInner::Noise(_) => unreachable!("legacy TLS profile"),
    };
    let server_alpn = match &server.inner {
        EncryptedFramedStreamInner::Tls(stream) => match &stream.stream {
            TlsStream::Server(stream) => stream.get_ref().1.alpn_protocol(),
            TlsStream::Client(_) => unreachable!("server stream role"),
        },
        EncryptedFramedStreamInner::Noise(_) => unreachable!("legacy TLS profile"),
    };
    assert_eq!(client_alpn, None);
    assert_eq!(server_alpn, None);
    let binding = client.tcp_admission_binding().expect("client binding");
    assert_eq!(
        binding,
        server.tcp_admission_binding().expect("server binding")
    );
    let (other_client, other_server) = connected_pair(64 * 1024).await;
    let other_binding = other_client
        .tcp_admission_binding()
        .expect("other client binding");
    assert_eq!(
        other_binding,
        other_server
            .tcp_admission_binding()
            .expect("other server binding")
    );
    assert_ne!(binding, other_binding);
}

#[tokio::test]
async fn noise_bindings_match_only_their_connection_and_quic_retains_h3_identity() {
    let (client, server) = transport_secret_pair(64 * 1024).await;
    let (tls_client, tls_server) = test_tls_configs();
    assert!(
        !tls_client.config.enable_early_data,
        "QUIC/H3 credentials must never be admitted as 0-RTT work"
    );
    assert_eq!(tls_server.config.max_early_data_size, 0);
    assert_eq!(tls_client.config.alpn_protocols, vec![HTTP_3_ALPN.to_vec()]);
    assert_eq!(tls_server.config.alpn_protocols, vec![HTTP_3_ALPN.to_vec()]);
    let binding = client
        .tcp_admission_binding()
        .expect("client Noise binding");
    assert_eq!(
        binding,
        server
            .tcp_admission_binding()
            .expect("server Noise binding")
    );

    let (other_client, other_server) = transport_secret_pair(64 * 1024).await;
    let other_binding = other_client
        .tcp_admission_binding()
        .expect("other client Noise binding");
    assert_eq!(
        other_binding,
        other_server
            .tcp_admission_binding()
            .expect("other server Noise binding")
    );
    assert_ne!(
        binding, other_binding,
        "independent Noise handshakes must not share admission binding"
    );
}

async fn read_raw_tcp_admission(
    request: &[u8; TCP_ADMISSION_PRELUDE_LEN],
) -> [u8; TCP_ADMISSION_PRELUDE_LEN] {
    let (mut client, mut server) = connected_pair(64 * 1024).await;
    let (_, server_result) = tokio::join!(
        async {
            client
                .write_tcp_admission(request, &[])
                .await
                .expect("write admission bytes");
        },
        server.read_tcp_admission(),
    );
    server_result.expect("read fixed admission")
}

#[tokio::test]
async fn tcp_admission_has_one_fixed_binary_input_shape() {
    let mut input = [0u8; TCP_ADMISSION_PRELUDE_LEN];
    input[..16].copy_from_slice(b"GET / HTTP/1.1\r\n");
    assert_eq!(read_raw_tcp_admission(&input).await, input);
}

#[tokio::test]
async fn noise_rejects_tampered_application_records() {
    let (client_io, server_io) = duplex(64 * 1024);
    let armed = Arc::new(AtomicBool::new(false));
    let client_io = TamperNextWrite {
        inner: client_io,
        armed: armed.clone(),
    };
    let limits = CodecLimits::default();
    let client_config = test_client_tls_config_with_transport_secret([0x5a; 32]);
    let server_config = test_server_tls_config_with_transport_secret([0x5a; 32]);
    let (client, server) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &client_config, limits),
        EncryptedFramedStream::accept(server_io, &server_config, limits),
    );
    let mut client = client.expect("client Noise handshake");
    let mut server = server.expect("server Noise handshake");

    armed.store(true, Ordering::Release);
    client
        .write_frame(&Frame::Ping { nonce: 17 })
        .await
        .expect("tampered bytes reached the wire");
    let error = server
        .read_frame()
        .await
        .expect_err("tampered Noise record must fail authentication");
    assert!(matches!(
        error,
        EncryptedFramedTransportError::NoiseRecord(_)
    ));
}

#[tokio::test]
async fn protected_wire_counter_excludes_handshake_and_advances_on_frame_write() {
    for (profile, (mut client, mut server)) in [
        ("TLS", connected_pair(64 * 1024).await),
        ("Noise", transport_secret_pair(64 * 1024).await),
    ] {
        client
            .write_frame(&Frame::SessionHello {
                session_id: SessionId(1),
            })
            .await
            .expect("prime both directions");
        server.read_frame().await.expect("read hello");
        server
            .write_frame(&Frame::Pong { nonce: 1 })
            .await
            .expect("prime response");
        client.read_frame().await.expect("read response");

        let (_reader, mut writer) = client.split().expect("split protected stream");
        assert_eq!(
            writer.wire_bytes_written(),
            0,
            "{profile} handshake bytes must precede the accounting baseline"
        );
        writer
            .write_frame(&Frame::Ping { nonce: 9 })
            .await
            .expect("write frame");
        assert!(
            writer.wire_bytes_written() > FRAME_HEADER_LEN as u64,
            "{profile} frame bytes must advance protected-wire accounting"
        );
    }
}

#[tokio::test]
async fn abrupt_protected_truncation_is_not_reported_as_a_protocol_frame() {
    for (profile, (mut client, server)) in [
        ("TLS", connected_pair(64 * 1024).await),
        ("Noise", transport_secret_pair(64 * 1024).await),
    ] {
        drop(server);
        let error = client
            .read_frame()
            .await
            .expect_err("truncated protected stream");
        assert!(
            matches!(error, EncryptedFramedTransportError::Io(_)),
            "{profile} truncation must remain a carrier I/O failure: {error:?}"
        );
    }
}

#[tokio::test]
async fn noise_record_boundaries_are_invisible_to_large_mpp_frames() {
    let (mut client, mut server) = transport_secret_pair(64 * 1024).await;
    let frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 0,
        payload: Bytes::from(vec![0x7b; TCP_NOISE_MAX_PLAINTEXT + 4096]),
    };
    let (write_result, read_result) = tokio::join!(client.write_frame(&frame), server.read_frame());
    write_result.expect("write frame spanning Noise records");
    assert_eq!(
        read_result.expect("read frame spanning Noise records"),
        frame
    );
}

#[tokio::test]
async fn noise_rekeys_both_directions_during_full_duplex_traffic() {
    let (client, server) = transport_secret_pair(64 * 1024).await;
    let (mut client_reader, mut client_writer) = client.split().expect("split client");
    let (mut server_reader, mut server_writer) = server.split().expect("split server");
    let records = TCP_NOISE_REKEY_RECORD_INTERVAL + 3;

    tokio::join!(
        async {
            for nonce in 0..records {
                client_writer
                    .write_frame(&Frame::Ping { nonce })
                    .await
                    .expect("client write");
                client_writer.flush().await.expect("client flush");
            }
        },
        async {
            for nonce in 0..records {
                assert_eq!(
                    server_reader.read_frame().await.expect("server read"),
                    Frame::Ping { nonce }
                );
            }
        },
        async {
            for nonce in 0..records {
                server_writer
                    .write_frame(&Frame::Pong { nonce })
                    .await
                    .expect("server write");
                server_writer.flush().await.expect("server flush");
            }
        },
        async {
            for nonce in 0..records {
                assert_eq!(
                    client_reader.read_frame().await.expect("client read"),
                    Frame::Pong { nonce }
                );
            }
        }
    );
}

#[tokio::test]
async fn transport_profiles_never_negotiate_or_fall_back() {
    let (client_io, server_io) = duplex(64 * 1024);
    let client = test_client_tls_config_with_transport_secret([0x5a; 32]);
    let server = test_server_tls_config();
    let (client_result, server_result) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &client, CodecLimits::default()),
        EncryptedFramedStream::accept(server_io, &server, CodecLimits::default()),
    );
    assert!(client_result.is_err());
    assert!(server_result.is_err());
}
