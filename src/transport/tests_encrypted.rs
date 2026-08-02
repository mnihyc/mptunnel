use super::*;
use crate::protocol::{Frame, SessionId};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, duplex};

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
async fn tcp_tls_has_no_alpn_or_early_data_and_exporters_match_only_this_connection() {
    let (client, server) = connected_pair(64 * 1024).await;
    let client_alpn = match &client.stream {
        TlsStream::Client(stream) => stream.get_ref().1.alpn_protocol(),
        TlsStream::Server(_) => unreachable!("client stream role"),
    };
    let server_alpn = match &server.stream {
        TlsStream::Server(stream) => stream.get_ref().1.alpn_protocol(),
        TlsStream::Client(_) => unreachable!("server stream role"),
    };
    assert_eq!(client_alpn, None);
    assert_eq!(server_alpn, None);
    let (tls_client, tls_server) = test_tls_configs();
    assert!(!tls_client.tcp_config.enable_early_data);
    assert_eq!(tls_server.tcp_config.max_early_data_size, 0);
    assert!(
        !tls_client.config.enable_early_data,
        "QUIC/H3 credentials must never be admitted as 0-RTT work"
    );
    assert_eq!(tls_server.config.max_early_data_size, 0);
    assert_eq!(tls_client.config.alpn_protocols, vec![HTTP_3_ALPN.to_vec()]);
    assert_eq!(tls_server.config.alpn_protocols, vec![HTTP_3_ALPN.to_vec()]);
    let exporter = client
        .tcp_admission_exporter()
        .expect("client TLS exporter");
    assert_eq!(
        exporter,
        server
            .tcp_admission_exporter()
            .expect("server TLS exporter")
    );

    let (other_client, other_server) = connected_pair(64 * 1024).await;
    let other_exporter = other_client
        .tcp_admission_exporter()
        .expect("other client TLS exporter");
    assert_eq!(
        other_exporter,
        other_server
            .tcp_admission_exporter()
            .expect("other server TLS exporter")
    );
    assert_ne!(
        exporter, other_exporter,
        "independent TLS handshakes must not share admission binding"
    );
}

async fn read_raw_tcp_admission(
    request: &[u8; TCP_ADMISSION_PRELUDE_LEN],
) -> [u8; TCP_ADMISSION_PRELUDE_LEN] {
    let (mut client, mut server) = connected_pair(64 * 1024).await;
    let (_, server_result) = tokio::join!(
        async {
            client
                .stream
                .write_all(request)
                .await
                .expect("write admission bytes");
            client.stream.flush().await.expect("flush admission bytes");
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
async fn rustls_rejects_tampered_application_records() {
    let (client_io, server_io) = duplex(64 * 1024);
    let armed = Arc::new(AtomicBool::new(false));
    let client_io = TamperNextWrite {
        inner: client_io,
        armed: armed.clone(),
    };
    let limits = CodecLimits::default();
    let (client, server) = tokio::join!(
        EncryptedFramedStream::connect(client_io, &test_tls_configs().0, limits),
        EncryptedFramedStream::accept(server_io, &test_tls_configs().1, limits),
    );
    let mut client = client.expect("client TLS handshake");
    let mut server = server.expect("server TLS handshake");

    armed.store(true, Ordering::Release);
    client
        .write_frame(&Frame::Ping { nonce: 17 })
        .await
        .expect("tampered bytes reached the wire");
    let error = server
        .read_frame()
        .await
        .expect_err("tampered TLS record must fail authentication");
    assert!(matches!(error, EncryptedFramedTransportError::Io(_)));
}

#[tokio::test]
async fn raw_tls_wire_counter_excludes_handshake_and_advances_on_frame_write() {
    let (mut client, mut server) = connected_pair(64 * 1024).await;
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

    let (_reader, mut writer) = client.split().expect("split TLS stream");
    assert_eq!(writer.wire_bytes_written(), 0);
    writer
        .write_frame(&Frame::Ping { nonce: 9 })
        .await
        .expect("write frame");
    assert!(writer.wire_bytes_written() > FRAME_HEADER_LEN as u64);
}

#[tokio::test]
async fn abrupt_tls_truncation_is_not_reported_as_a_protocol_frame() {
    let (mut client, server) = connected_pair(64 * 1024).await;
    drop(server);

    let error = client.read_frame().await.expect_err("truncated TLS stream");
    assert!(matches!(
        error,
        EncryptedFramedTransportError::Io(_) | EncryptedFramedTransportError::TlsHandshake(_)
    ));
}
