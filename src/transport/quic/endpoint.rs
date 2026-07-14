//! QUIC endpoint, connection, authentication, and transport configuration.

use super::{
    CongestionMetrics, InstrumentedBbrConfig, InstrumentedController, QuicCarrierError,
    QuicCarrierTelemetry, RecvStream, SendStream,
};
use crate::mux::MuxLimits;
use crate::transport::CarrierSocket;
use quinn::{
    ClientConfig, Endpoint as QuinnEndpoint, EndpointConfig, ServerConfig, TransportConfig, VarInt,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

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
    delivery_evidence_written: Arc<AtomicU64>,
    telemetry: Arc<QuicCarrierTelemetry>,
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

    /// Builds Quinn on a socket already prepared by the host network adapter.
    pub async fn bind_client_socket(
        socket: CarrierSocket,
        secret: &[u8],
        mux_limits: MuxLimits,
    ) -> Result<Self, QuicCarrierError> {
        let runtime = quinn::default_runtime()
            .ok_or_else(|| std::io::Error::other("no async runtime found"))?;
        let mut endpoint = QuinnEndpoint::new(
            EndpointConfig::default(),
            None,
            socket.into_udp_socket()?,
            runtime,
        )?;
        endpoint.set_default_client_config(client_config(secret, mux_limits)?);
        Ok(Self { endpoint })
    }

    pub async fn connect(&self, remote: SocketAddr) -> Result<Connection, QuicCarrierError> {
        let connecting = self
            .endpoint
            .connect(remote, QUIC_CERT_DNS_NAME)
            .map_err(QuicCarrierError::Connect)?;
        Ok(Connection::from_quinn(connecting.await?))
    }

    pub async fn accept(&self) -> Option<Connection> {
        loop {
            let incoming = self.endpoint.accept().await?;
            match incoming.await {
                Ok(connection) => {
                    return Some(Connection::from_quinn(connection));
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
    fn from_quinn(connection: quinn::Connection) -> Self {
        let telemetry = connection
            .congestion_state()
            .into_any()
            .downcast::<InstrumentedController>()
            .expect("QUIC carrier must use the instrumented congestion controller")
            .telemetry
            .clone();
        Self {
            connection,
            write_backlog: Arc::new(AtomicU64::new(0)),
            delivery_evidence_written: Arc::new(AtomicU64::new(0)),
            telemetry,
        }
    }

    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok((
            SendStream {
                stream: send,
                connection: self.connection.clone(),
                write_backlog: self.write_backlog.clone(),
                delivery_evidence_written: self.delivery_evidence_written.clone(),
                telemetry: self.telemetry.clone(),
                encode_buffer: Vec::new(),
            },
            RecvStream::new(recv),
        ))
    }

    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), QuicCarrierError> {
        let (send, recv) = self.connection.accept_bi().await?;
        Ok((
            SendStream {
                stream: send,
                connection: self.connection.clone(),
                write_backlog: self.write_backlog.clone(),
                delivery_evidence_written: self.delivery_evidence_written.clone(),
                telemetry: self.telemetry.clone(),
                encode_buffer: Vec::new(),
            },
            RecvStream::new(recv),
        ))
    }

    pub fn close(&self) {
        self.connection.close(VarInt::from_u32(0), b"closed");
    }

    pub fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }

    pub fn measurement_active(&self) -> bool {
        self.telemetry.measurement_active()
    }

    pub async fn wait_for_measurement_release(&self) {
        self.telemetry.wait_for_measurement_release().await;
    }

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.connection.stats()
    }

    pub fn congestion_metrics(&self) -> CongestionMetrics {
        let controller = self.connection.congestion_state();
        let metrics = controller.metrics();
        let current_telemetry = controller
            .into_any()
            .downcast::<InstrumentedController>()
            .expect("QUIC carrier must use the instrumented congestion controller")
            .telemetry
            .clone();
        if !Arc::ptr_eq(&current_telemetry, &self.telemetry) {
            // Quinn creates a fresh controller for a cross-address migration.
            // Existing streams still hold the old write gate, so fail this
            // carrier instead of publishing split ownership from two epochs.
            self.telemetry.mark_measurement_failed_closed();
            self.connection.close(
                VarInt::from_u32(1),
                b"QUIC congestion controller ownership changed",
            );
        }
        let snapshot = current_telemetry.snapshot();
        CongestionMetrics {
            congestion_window: metrics.congestion_window,
            bytes_in_flight: snapshot.bytes_in_flight,
            pending_bytes: self.write_backlog.load(Ordering::Relaxed),
            pacing_rate_bps: metrics.pacing_rate,
            loss_ppm: snapshot.loss_ppm,
            ecn_ppm: None,
            newly_acked_bytes: snapshot.newly_acked_bytes,
            non_app_limited_acked_bytes: snapshot.non_app_limited_acked_bytes,
            timed_non_app_limited_acked_bytes: snapshot.timed_non_app_limited_acked_bytes,
            non_app_limited_ack_elapsed: snapshot.non_app_limited_ack_elapsed,
            delivery_evidence_written_bytes: self.delivery_evidence_written.load(Ordering::Relaxed),
            delivery_sample_count: snapshot.delivery_sample_count,
            non_app_limited_delivery_sample_count: snapshot.non_app_limited_delivery_sample_count,
            timed_non_app_limited_delivery_sample_count: snapshot
                .timed_non_app_limited_delivery_sample_count,
            app_limited: snapshot.app_limited,
            measurement: snapshot.measurement,
        }
    }

    pub fn cancel_measurement(&self, token: u64) -> bool {
        let should_close = self.telemetry.abort_measurement(token);
        if should_close {
            self.connection
                .close(VarInt::from_u32(1), b"cancelled measurement epoch");
        }
        should_close
    }

    pub fn retire_measurement(&self, token: u64) -> bool {
        self.telemetry.retire_measurement(token)
    }

    pub fn confirm_measurement_receipt(
        &self,
        token: u64,
        received_payload_bytes: u64,
        received_at: Instant,
    ) -> bool {
        let current_telemetry = self
            .connection
            .congestion_state()
            .into_any()
            .downcast::<InstrumentedController>()
            .expect("QUIC carrier must use the instrumented congestion controller")
            .telemetry
            .clone();
        if !Arc::ptr_eq(&current_telemetry, &self.telemetry) {
            self.connection.close(
                VarInt::from_u32(1),
                b"QUIC congestion controller ownership changed",
            );
            return false;
        }
        self.telemetry.confirm_measurement_receipt(
            token,
            received_payload_bytes,
            received_at,
            self.connection.rtt(),
        )
    }
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

#[cfg(test)]
#[path = "endpoint_test.rs"]
mod tests;
