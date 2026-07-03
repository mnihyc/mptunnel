#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::mux::MuxLimits;
use crate::protocol::Frame;
use crate::protocol::codec::{
    CodecLimits, decode_frame_bytes, encode_frame_into, encoded_frame_capacity_hint,
};
use bytes::BytesMut;
use quinn::{
    ClientConfig, ConnectionError, Endpoint as QuinnEndpoint, ServerConfig, TransportConfig, VarInt,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

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
    write_backlog: Arc<AtomicU64>,
    product_data_written: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Copy)]
pub struct CongestionMetrics {
    pub congestion_window: u64,
    pub bytes_in_flight: Option<u64>,
    pub pending_bytes: u64,
    pub pacing_rate_bps: Option<u64>,
    pub loss_ppm: Option<u32>,
    pub ecn_ppm: Option<u32>,
    pub newly_acked_bytes: Option<u64>,
    pub product_data_written_bytes: u64,
    pub delivery_sample_count: u64,
    pub app_limited: bool,
}

#[derive(Debug)]
pub struct SendStream {
    stream: quinn::SendStream,
    write_backlog: Arc<AtomicU64>,
    product_data_written: Arc<AtomicU64>,
    encode_buffer: Vec<u8>,
}

#[derive(Debug)]
pub struct RecvStream {
    stream: quinn::RecvStream,
}

#[derive(Debug, Default)]
struct InstrumentedBbrConfig;

#[derive(Debug, Default)]
struct QuicCarrierTelemetry {
    bytes_in_flight: AtomicU64,
    newly_acked_bytes: AtomicU64,
    delivery_sample_count: AtomicU64,
    sent_bytes: AtomicU64,
    lost_bytes: AtomicU64,
    app_limited: AtomicBool,
}

#[derive(Debug, Clone, Copy)]
struct QuicCarrierTelemetrySnapshot {
    bytes_in_flight: u64,
    newly_acked_bytes: Option<u64>,
    delivery_sample_count: u64,
    loss_ppm: Option<u32>,
    app_limited: bool,
}

struct InstrumentedController {
    inner: Box<dyn quinn::congestion::Controller>,
    telemetry: Arc<QuicCarrierTelemetry>,
}

impl std::fmt::Debug for InstrumentedController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedController")
            .field("telemetry", &self.telemetry)
            .finish_non_exhaustive()
    }
}

impl QuicCarrierTelemetry {
    fn snapshot(&self) -> QuicCarrierTelemetrySnapshot {
        let newly_acked_bytes = self.newly_acked_bytes.swap(0, Ordering::Relaxed);
        let delivery_sample_count = self.delivery_sample_count.swap(0, Ordering::Relaxed);
        let sent_bytes = self.sent_bytes.load(Ordering::Relaxed);
        let lost_bytes = self.lost_bytes.load(Ordering::Relaxed);
        let loss_ppm = (sent_bytes > 0).then(|| {
            let ratio = (lost_bytes as f64 / sent_bytes as f64).clamp(0.0, 1.0);
            (ratio * 1_000_000.0).round() as u32
        });
        QuicCarrierTelemetrySnapshot {
            bytes_in_flight: self.bytes_in_flight.load(Ordering::Relaxed),
            newly_acked_bytes: (newly_acked_bytes > 0).then_some(newly_acked_bytes),
            delivery_sample_count,
            loss_ppm,
            app_limited: self.app_limited.load(Ordering::Relaxed),
        }
    }

    fn add_sent(&self, bytes: u64) {
        self.sent_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.bytes_in_flight.fetch_add(bytes, Ordering::Relaxed);
    }

    fn add_acked(&self, bytes: u64, app_limited: bool) {
        if bytes > 0 {
            self.newly_acked_bytes.fetch_add(bytes, Ordering::Relaxed);
            self.delivery_sample_count.fetch_add(1, Ordering::Relaxed);
        }
        self.app_limited.store(app_limited, Ordering::Relaxed);
        let _ =
            self.bytes_in_flight
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(bytes))
                });
    }

    fn finish_ack_batch(&self, in_flight: u64, app_limited: bool) {
        self.bytes_in_flight.store(in_flight, Ordering::Relaxed);
        self.app_limited.store(app_limited, Ordering::Relaxed);
    }

    fn add_lost(&self, lost_bytes: u64) {
        if lost_bytes > 0 {
            self.lost_bytes.fetch_add(lost_bytes, Ordering::Relaxed);
            let _ = self.bytes_in_flight.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(lost_bytes)),
            );
        }
    }
}

impl quinn::congestion::ControllerFactory for InstrumentedBbrConfig {
    fn build(
        self: Arc<Self>,
        now: Instant,
        current_mtu: u16,
    ) -> Box<dyn quinn::congestion::Controller> {
        let inner = Arc::new(quinn::congestion::BbrConfig::default()).build(now, current_mtu);
        Box::new(InstrumentedController {
            inner,
            telemetry: Arc::new(QuicCarrierTelemetry::default()),
        })
    }
}

impl quinn::congestion::Controller for InstrumentedController {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.telemetry.add_sent(bytes);
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.telemetry.add_acked(bytes, app_limited);
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.telemetry.finish_ack_batch(in_flight, app_limited);
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.telemetry.add_lost(lost_bytes);
        self.inner
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn metrics(&self) -> quinn::congestion::ControllerMetrics {
        self.inner.metrics()
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            telemetry: self.telemetry.clone(),
        })
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
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
            write_backlog: Arc::new(AtomicU64::new(0)),
            product_data_written: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn accept(&self) -> Option<Connection> {
        loop {
            let incoming = self.endpoint.accept().await?;
            match incoming.await {
                Ok(connection) => {
                    return Some(Connection {
                        connection,
                        write_backlog: Arc::new(AtomicU64::new(0)),
                        product_data_written: Arc::new(AtomicU64::new(0)),
                    });
                }
                Err(err) => {
                    eprintln!("warning: QUIC carrier accept failed: {err}");
                    continue;
                }
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
        Ok((
            SendStream {
                stream: send,
                write_backlog: self.write_backlog.clone(),
                product_data_written: self.product_data_written.clone(),
                encode_buffer: Vec::new(),
            },
            RecvStream { stream: recv },
        ))
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let (send, recv) = self.connection.accept_bi().await?;
        Ok((
            SendStream {
                stream: send,
                write_backlog: self.write_backlog.clone(),
                product_data_written: self.product_data_written.clone(),
                encode_buffer: Vec::new(),
            },
            RecvStream { stream: recv },
        ))
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

    pub fn congestion_metrics(&self) -> CongestionMetrics {
        let controller = self.connection.congestion_state();
        let metrics = controller.metrics();
        let telemetry = controller
            .into_any()
            .downcast::<InstrumentedController>()
            .ok()
            .map(|controller| controller.telemetry.snapshot());
        let (bytes_in_flight, loss_ppm, newly_acked_bytes, delivery_sample_count, app_limited) =
            telemetry.map_or((None, None, None, 0, true), |snapshot| {
                (
                    Some(snapshot.bytes_in_flight),
                    snapshot.loss_ppm,
                    snapshot.newly_acked_bytes,
                    snapshot.delivery_sample_count,
                    snapshot.app_limited,
                )
            });
        CongestionMetrics {
            congestion_window: metrics.congestion_window,
            bytes_in_flight,
            pending_bytes: self.write_backlog.load(Ordering::Relaxed),
            pacing_rate_bps: metrics.pacing_rate,
            loss_ppm,
            ecn_ppm: None,
            newly_acked_bytes,
            product_data_written_bytes: self.product_data_written.load(Ordering::Relaxed),
            delivery_sample_count,
            app_limited,
        }
    }
}

pub async fn write_frame(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    write_frames(send, std::slice::from_ref(frame), limits).await
}

pub async fn write_frames(
    send: &mut SendStream,
    frames: &[Frame],
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    if frames.is_empty() {
        return Ok(());
    }
    #[cfg(feature = "lab-diagnostics")]
    let encode_started = std::time::Instant::now();
    let product_data_bytes = frames.iter().fold(0u64, |total, frame| {
        total.saturating_add(frame_product_data_bytes(frame) as u64)
    });
    let packet_len = {
        let packet = &mut send.encode_buffer;
        packet.clear();
        let capacity_hint = frames.iter().fold(0usize, |total, frame| {
            total
                .saturating_add(FRAME_LEN_BYTES)
                .saturating_add(encoded_frame_capacity_hint(frame))
        });
        packet.reserve(capacity_hint);
        for frame in frames {
            encode_length_prefixed_frame(frame, limits, packet)?;
        }
        packet.len() as u64
    };
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.encode_frames",
        encode_started.elapsed(),
        packet_len as usize,
    );
    #[cfg(feature = "lab-diagnostics")]
    let write_started = std::time::Instant::now();
    send.write_backlog.fetch_add(packet_len, Ordering::Relaxed);
    let write_result = send.stream.write_all(&send.encode_buffer).await;
    let _ = send
        .write_backlog
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(packet_len))
        });
    write_result?;
    if product_data_bytes > 0 {
        send.product_data_written
            .fetch_add(product_data_bytes, Ordering::Relaxed);
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.write_frames_wait",
        write_started.elapsed(),
        packet_len as usize,
    );
    Ok(())
}

fn frame_product_data_bytes(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } | Frame::DatagramData { payload, .. } => payload.len(),
        _ => 0,
    }
}

fn encode_length_prefixed_frame(
    frame: &Frame,
    limits: CodecLimits,
    packet: &mut Vec<u8>,
) -> Result<(), QuicCarrierError> {
    let len_offset = packet.len();
    packet.extend_from_slice(&[0u8; FRAME_LEN_BYTES]);
    let frame_start = packet.len();
    encode_frame_into(frame, limits, packet)?;
    let frame_len = packet.len().saturating_sub(frame_start);
    let frame_len = u32::try_from(frame_len).map_err(|_| QuicCarrierError::FrameTooLarge)?;
    packet[len_offset..len_offset + FRAME_LEN_BYTES].copy_from_slice(&frame_len.to_be_bytes());
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
    let mut encoded = BytesMut::with_capacity(len);
    encoded.resize(len, 0);
    recv.stream
        .read_exact(&mut encoded)
        .await
        .map_err(QuicCarrierError::ReadExact)?;
    Ok(decode_frame_bytes(encoded.freeze(), limits)?)
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
        .saturating_add(mux_limits.max_path_flight_bytes as u64);
    let send_window = (mux_limits.max_path_flight_bytes as u64)
        .max(mux_limits.max_reliable_relay_chunk_bytes as u64)
        .max(1);
    let concurrent_streams = (mux_limits.max_quic_concurrent_bidi_streams as u64)
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
        .congestion_controller_factory(Arc::new(InstrumentedBbrConfig));
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

    #[test]
    fn quic_transport_profile_follows_mux_resource_envelope() {
        let mux_limits = MuxLimits::default();
        let transport = quic_transport_config(mux_limits);
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
        let transport = quic_transport_config(mux_limits);
        let rendered = format!("{transport:?}");

        assert!(rendered.contains("max_concurrent_bidi_streams: 4096"));
        assert!(!rendered.contains("max_concurrent_bidi_streams: 4,"));
    }
}
