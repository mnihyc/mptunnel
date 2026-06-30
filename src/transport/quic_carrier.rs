use crate::mux::MuxLimits;
use crate::protocol::Frame;
use crate::protocol::codec::{CodecLimits, decode_frame, encode_frame};
use quinn::{
    ClientConfig, ConnectionError, Endpoint as QuinnEndpoint, ServerConfig, TransportConfig, VarInt,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

const FRAME_LEN_BYTES: usize = 4;
const QUIC_CERT_DNS_NAME: &str = "mptunnel.invalid";
const ED25519_PKCS8_PREFIX: &[u8] = &[
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];
#[derive(Debug)]
pub struct Endpoint {
    endpoint: QuinnEndpoint,
}

#[derive(Debug, Clone)]
pub struct Connection {
    connection: quinn::Connection,
}

#[derive(Debug)]
pub struct SendStream {
    stream: quinn::SendStream,
}

#[derive(Debug)]
pub struct RecvStream {
    stream: quinn::RecvStream,
}

impl Endpoint {
    pub async fn bind_server(
        addr: SocketAddr,
        secret: &[u8],
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        let endpoint = QuinnEndpoint::server(server_config(secret, mux_limits)?, addr)?;
        Ok(Self { endpoint })
    }

    pub async fn bind_client(
        addr: SocketAddr,
        secret: &[u8],
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        let mut endpoint = QuinnEndpoint::client(addr)?;
        endpoint.set_default_client_config(client_config(secret, mux_limits)?);
        Ok(Self { endpoint })
    }

    pub async fn connect(&self, remote: SocketAddr) -> Result<Connection, QuicCarrierError> {
        let connecting = self
            .endpoint
            .connect(remote, QUIC_CERT_DNS_NAME)
            .map_err(QuicCarrierError::Connect)?;
        Ok(Connection {
            connection: connecting.await?,
        })
    }

    pub async fn accept(&self) -> Option<Connection> {
        let incoming = self.endpoint.accept().await?;
        match incoming.await {
            Ok(connection) => Some(Connection { connection }),
            Err(err) => {
                eprintln!("warning: QUIC carrier accept failed: {err}");
                None
            }
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }
}

impl Connection {
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok((SendStream { stream: send }, RecvStream { stream: recv }))
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let (send, recv) = self.connection.accept_bi().await?;
        Ok((SendStream { stream: send }, RecvStream { stream: recv }))
    }

    pub fn close(&self) {
        self.connection.close(VarInt::from_u32(0), b"closed");
    }

    pub fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.connection.stats()
    }
}

pub async fn write_frame(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    let encoded = encode_frame(frame, limits)?;
    let len = u32::try_from(encoded.len()).map_err(|_| QuicCarrierError::FrameTooLarge)?;
    let mut packet = Vec::with_capacity(FRAME_LEN_BYTES + encoded.len());
    packet.extend_from_slice(&len.to_be_bytes());
    packet.extend_from_slice(&encoded);
    send.stream.write_all(&packet).await?;
    Ok(())
}

pub async fn read_frame(
    recv: &mut RecvStream,
    limits: CodecLimits,
) -> Result<Frame, QuicCarrierError> {
    let mut len = [0u8; FRAME_LEN_BYTES];
    recv.stream
        .read_exact(&mut len)
        .await
        .map_err(QuicCarrierError::ReadExact)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > limits.max_frame_bytes {
        return Err(QuicCarrierError::FrameTooLarge);
    }
    let mut encoded = vec![0u8; len];
    recv.stream
        .read_exact(&mut encoded)
        .await
        .map_err(QuicCarrierError::ReadExact)?;
    Ok(decode_frame(&encoded, limits)?)
}

pub fn finish_stream(send: &mut SendStream) -> Result<(), QuicCarrierError> {
    Ok(send.stream.finish()?)
}

pub fn max_stream_payload_bytes(limits: CodecLimits) -> usize {
    limits.max_payload_bytes.max(1)
}

fn server_config(secret: &[u8], mux_limits: MuxLimits) -> Result<ServerConfig, QuicCarrierError> {
    let (cert_der, key_der) = secret_bound_certificate(secret)?;
    let mut config = ServerConfig::with_single_cert(vec![cert_der], key_der.into())?;
    config.transport = Arc::new(quic_transport_config(mux_limits));
    Ok(config)
}

fn client_config(secret: &[u8], mux_limits: MuxLimits) -> Result<ClientConfig, QuicCarrierError> {
    let (cert_der, _) = secret_bound_certificate(secret)?;
    let mut config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(PinnedServerCertificate::new(cert_der))
        .with_no_client_auth();
    config.enable_sni = false;
    let mut config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(config)?,
    ));
    config.transport_config(Arc::new(quic_transport_config(mux_limits)));
    Ok(config)
}

fn quic_transport_config(mux_limits: MuxLimits) -> TransportConfig {
    let stream_receive_window = mux_limits.max_stream_window_bytes.max(1);
    let connection_receive_window = stream_receive_window
        .saturating_add(mux_limits.max_repair_bytes as u64)
        .saturating_add(mux_limits.max_reorder_bytes as u64)
        .saturating_add(mux_limits.max_datagram_queue_bytes as u64)
        .saturating_add(mux_limits.max_tcp_path_inflight_bytes as u64);
    let send_window = connection_receive_window.max(stream_receive_window);
    let concurrent_streams = (connection_receive_window / stream_receive_window)
        .max(1)
        .min(mux_limits.max_streams as u64);

    let mut transport = TransportConfig::default();
    transport
        .stream_receive_window(varint_saturating(stream_receive_window))
        .receive_window(varint_saturating(connection_receive_window))
        .send_window(send_window)
        .max_concurrent_bidi_streams(varint_saturating(concurrent_streams))
        .max_concurrent_uni_streams(0_u8.into())
        .datagram_receive_buffer_size(Some(mux_limits.max_datagram_queue_bytes))
        .datagram_send_buffer_size(mux_limits.max_datagram_queue_bytes)
        .congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
    transport
}

fn varint_saturating(value: u64) -> VarInt {
    VarInt::from_u64(value.min(VarInt::MAX.into_inner()))
        .expect("bounded to QUIC variable integer range")
}

fn secret_bound_certificate(
    secret: &[u8],
) -> Result<(CertificateDer<'static>, PrivatePkcs8KeyDer<'static>), QuicCarrierError> {
    if secret.is_empty() {
        return Err(QuicCarrierError::EmptySecret);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"mptunnel quic cert ed25519 seed v1");
    hasher.update(secret);
    let seed = hasher.finalize();
    let mut pkcs8 = Vec::with_capacity(ED25519_PKCS8_PREFIX.len() + seed.len());
    pkcs8.extend_from_slice(ED25519_PKCS8_PREFIX);
    pkcs8.extend_from_slice(&seed);

    let key_der = PrivatePkcs8KeyDer::from(pkcs8);
    let key_pair = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(&key_der, &rcgen::PKCS_ED25519)?;
    let params = rcgen::CertificateParams::new(vec![QUIC_CERT_DNS_NAME.into()])?;
    let cert = params.self_signed(&key_pair)?;
    Ok((CertificateDer::from(cert), key_der))
}

#[derive(Debug)]
struct PinnedServerCertificate {
    expected_der: CertificateDer<'static>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl PinnedServerCertificate {
    fn new(expected_der: CertificateDer<'static>) -> Arc<Self> {
        Arc::new(Self {
            expected_der,
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        })
    }
}

impl rustls::client::danger::ServerCertVerifier for PinnedServerCertificate {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.expected_der.as_ref() {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(
                "QUIC server certificate does not match shared secret".into(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
pub enum QuicCarrierError {
    Io(std::io::Error),
    Connect(quinn::ConnectError),
    Connection(ConnectionError),
    Write(quinn::WriteError),
    ReadExact(quinn::ReadExactError),
    ClosedStream(quinn::ClosedStream),
    FrameTooLarge,
    Codec(crate::protocol::codec::CodecError),
    Rustls(rustls::Error),
    QuinnCrypto(quinn::crypto::rustls::NoInitialCipherSuite),
    Rcgen(rcgen::Error),
    EmptySecret,
}

impl fmt::Display for QuicCarrierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "QUIC carrier I/O failed: {err}"),
            Self::Connect(err) => write!(f, "QUIC carrier connect failed: {err}"),
            Self::Connection(err) => write!(f, "QUIC carrier connection failed: {err}"),
            Self::Write(err) => write!(f, "QUIC carrier write failed: {err}"),
            Self::ReadExact(err) => write!(f, "QUIC carrier read failed: {err}"),
            Self::ClosedStream(err) => write!(f, "QUIC carrier stream already closed: {err}"),
            Self::FrameTooLarge => write!(f, "QUIC carrier frame exceeds configured limits"),
            Self::Codec(err) => write!(f, "QUIC carrier frame codec failed: {err}"),
            Self::Rustls(err) => write!(f, "QUIC carrier TLS config failed: {err}"),
            Self::QuinnCrypto(err) => write!(f, "QUIC carrier crypto config failed: {err}"),
            Self::Rcgen(err) => write!(f, "QUIC carrier certificate generation failed: {err}"),
            Self::EmptySecret => write!(f, "QUIC carrier shared secret must not be empty"),
        }
    }
}

impl std::error::Error for QuicCarrierError {}

impl From<std::io::Error> for QuicCarrierError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ConnectionError> for QuicCarrierError {
    fn from(value: ConnectionError) -> Self {
        Self::Connection(value)
    }
}

impl From<quinn::WriteError> for QuicCarrierError {
    fn from(value: quinn::WriteError) -> Self {
        Self::Write(value)
    }
}

impl From<quinn::ClosedStream> for QuicCarrierError {
    fn from(value: quinn::ClosedStream) -> Self {
        Self::ClosedStream(value)
    }
}

impl From<crate::protocol::codec::CodecError> for QuicCarrierError {
    fn from(value: crate::protocol::codec::CodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<rustls::Error> for QuicCarrierError {
    fn from(value: rustls::Error) -> Self {
        Self::Rustls(value)
    }
}

impl From<quinn::crypto::rustls::NoInitialCipherSuite> for QuicCarrierError {
    fn from(value: quinn::crypto::rustls::NoInitialCipherSuite) -> Self {
        Self::QuinnCrypto(value)
    }
}

impl From<rcgen::Error> for QuicCarrierError {
    fn from(value: rcgen::Error) -> Self {
        Self::Rcgen(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Frame;
    use tokio::time::{Duration, timeout};

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
        write_frame(&mut send, &Frame::Ping { nonce: 42 }, limits)
            .await
            .expect("client write ping");
        finish_stream(&mut send).expect("client finish stream");
        let response = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
            .await
            .expect("response timeout")
            .expect("client read pong");
        assert_eq!(response, Frame::Pong { nonce: 42 });
        let _ = client_done_tx.send(());

        server_task.await.expect("server task");
    }

    #[tokio::test]
    async fn quic_carrier_rejects_wrong_shared_secret_before_product_frames() {
        let server_secret = b"0123456789abcdef0123456789abcdef";
        let wrong_client_secret = b"fedcba9876543210fedcba9876543210";
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
            let _ = timeout(Duration::from_secs(5), server.accept()).await;
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

        server_task.await.expect("server task");
    }

    #[test]
    fn quic_transport_profile_follows_mux_resource_envelope() {
        let mux_limits = MuxLimits::default();
        let transport = quic_transport_config(mux_limits);
        let rendered = format!("{transport:?}");
        assert!(rendered.contains("stream_receive_window: 67108864"));
        assert!(rendered.contains("receive_window: 251658240"));
        assert!(rendered.contains("send_window: 251658240"));
        assert!(rendered.contains("max_concurrent_bidi_streams: 3"));
        assert!(rendered.contains("max_concurrent_uni_streams: 0"));
    }
}
