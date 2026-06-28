use crate::mux::MuxLimits;
use crate::protocol::Frame;
use crate::protocol::codec::{
    CodecError, CodecLimits, FRAME_HEADER_LEN, decode_frame, decode_payload_len_from_header,
    encode_frame,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::rustls::{
    self, DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature},
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
};
use quinn::{
    ClientConfig, RecvStream, SendStream, ServerConfig, TransportConfig, VarInt, congestion,
};
use rcgen::{CertificateParams, KeyPair, SerialNumber, date_time_ymd};
use ring::signature::Ed25519KeyPair;
use sha2::{Digest, Sha256};
use std::{error::Error, fmt, sync::Arc, time::Duration};

type HmacSha256 = Hmac<Sha256>;

pub const CONNECT_SERVER_NAME: &str = "localhost";

const CERT_KEY_CONTEXT: &[u8] = b"mptunnel udp carrier ed25519 certificate key v1";
const CERT_SERIAL_CONTEXT: &[u8] = b"mptunnel udp carrier certificate serial v1";
const PRIVATE_ALPN_CONTEXT: &[u8] = b"mptunnel udp carrier private alpn v1";
const PRIVATE_ALPN_SEED_BYTES: usize = 16;
const UDP_CARRIER_INITIAL_MTU_BYTES: u16 = 1_200;
const UDP_CARRIER_MIN_MTU_BYTES: u16 = 1_200;
const UDP_CARRIER_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);
const UDP_CARRIER_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct UdpCarrierCredential {
    pub cert_der: CertificateDer<'static>,
    pub key_der: PrivateKeyDer<'static>,
    pub fingerprint_sha256: [u8; 32],
}

impl UdpCarrierCredential {
    fn clone_key(&self) -> PrivateKeyDer<'static> {
        self.key_der.clone_key()
    }
}

#[derive(Debug)]
pub enum UdpCarrierTransportError {
    EmptySecret,
    Certificate(rcgen::Error),
    PrivateKeyRejected,
    Tls(rustls::Error),
    InitialCipherSuite(quinn::crypto::rustls::NoInitialCipherSuite),
    VarInt(quinn::VarIntBoundsExceeded),
}

impl fmt::Display for UdpCarrierTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySecret => write!(f, "UDP carrier shared secret must not be empty"),
            Self::Certificate(err) => write!(f, "failed to build UDP carrier certificate: {err}"),
            Self::PrivateKeyRejected => {
                write!(f, "failed to build UDP carrier Ed25519 private key")
            }
            Self::Tls(err) => write!(f, "failed to build UDP carrier TLS config: {err}"),
            Self::InitialCipherSuite(err) => {
                write!(f, "failed to build UDP carrier initial cipher suite: {err}")
            }
            Self::VarInt(err) => write!(f, "UDP carrier transport limit is out of range: {err}"),
        }
    }
}

impl Error for UdpCarrierTransportError {}

#[derive(Debug)]
pub enum UdpCarrierFrameError {
    Read(quinn::ReadExactError),
    Write(quinn::WriteError),
    Finish(quinn::ClosedStream),
    Codec(CodecError),
}

impl fmt::Display for UdpCarrierFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(err) => write!(f, "failed to read UDP carrier frame: {err}"),
            Self::Write(err) => write!(f, "failed to write UDP carrier frame: {err}"),
            Self::Finish(err) => write!(f, "failed to finish UDP carrier stream: {err}"),
            Self::Codec(err) => write!(f, "failed to encode/decode UDP carrier frame: {err}"),
        }
    }
}

impl Error for UdpCarrierFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read(err) => Some(err),
            Self::Write(err) => Some(err),
            Self::Finish(err) => Some(err),
            Self::Codec(err) => Some(err),
        }
    }
}

impl From<quinn::ReadExactError> for UdpCarrierFrameError {
    fn from(value: quinn::ReadExactError) -> Self {
        Self::Read(value)
    }
}

impl From<quinn::WriteError> for UdpCarrierFrameError {
    fn from(value: quinn::WriteError) -> Self {
        Self::Write(value)
    }
}

impl From<quinn::ClosedStream> for UdpCarrierFrameError {
    fn from(value: quinn::ClosedStream) -> Self {
        Self::Finish(value)
    }
}

impl From<CodecError> for UdpCarrierFrameError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<rcgen::Error> for UdpCarrierTransportError {
    fn from(value: rcgen::Error) -> Self {
        Self::Certificate(value)
    }
}

impl From<rustls::Error> for UdpCarrierTransportError {
    fn from(value: rustls::Error) -> Self {
        Self::Tls(value)
    }
}

impl From<quinn::crypto::rustls::NoInitialCipherSuite> for UdpCarrierTransportError {
    fn from(value: quinn::crypto::rustls::NoInitialCipherSuite) -> Self {
        Self::InitialCipherSuite(value)
    }
}

impl From<quinn::VarIntBoundsExceeded> for UdpCarrierTransportError {
    fn from(value: quinn::VarIntBoundsExceeded) -> Self {
        Self::VarInt(value)
    }
}

pub fn client_config(
    secret: &[u8],
    mux_limits: MuxLimits,
) -> Result<ClientConfig, UdpCarrierTransportError> {
    let tls = client_tls_config(secret)?;
    let crypto = QuicClientConfig::try_from(tls)?;
    let mut config = ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport_config(mux_limits)?));
    Ok(config)
}

fn client_tls_config(secret: &[u8]) -> Result<rustls::ClientConfig, UdpCarrierTransportError> {
    let credential = derive_credential(secret)?;
    let verifier = Arc::new(PinnedCertificateVerifier::new(
        credential.fingerprint_sha256,
        Arc::new(rustls::crypto::ring::default_provider()),
    ));
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .dangerous()
    .with_custom_certificate_verifier(verifier)
    .with_no_client_auth();
    tls.enable_sni = false;
    tls.alpn_protocols = vec![derive_private_alpn(secret)?];
    tls.enable_early_data = true;
    Ok(tls)
}

pub fn server_config(
    secret: &[u8],
    mux_limits: MuxLimits,
) -> Result<ServerConfig, UdpCarrierTransportError> {
    let tls = server_tls_config(secret)?;
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)?;
    let mut config = ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(Arc::new(transport_config(mux_limits)?));
    Ok(config)
}

fn server_tls_config(secret: &[u8]) -> Result<rustls::ServerConfig, UdpCarrierTransportError> {
    let credential = derive_credential(secret)?;
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_no_client_auth()
    .with_single_cert(vec![credential.cert_der.clone()], credential.clone_key())?;
    tls.alpn_protocols = vec![derive_private_alpn(secret)?];
    tls.max_early_data_size = u32::MAX;
    Ok(tls)
}

pub fn transport_config(
    mux_limits: MuxLimits,
) -> Result<TransportConfig, UdpCarrierTransportError> {
    let mut config = TransportConfig::default();
    let stream_window = varint(mux_limits.max_stream_window_bytes)?;
    let connection_window = varint(mux_limits.max_stream_window_bytes.saturating_mul(2))?;
    let max_bidi_streams = mux_limits.max_streams.min(u32::MAX as usize) as u32;
    let datagram_queue = mux_limits
        .max_datagram_queue_bytes
        .max(mux_limits.max_payload_bytes);

    config
        .max_concurrent_bidi_streams(VarInt::from_u32(max_bidi_streams))
        .max_concurrent_uni_streams(VarInt::from_u32(0))
        .stream_receive_window(stream_window)
        .receive_window(connection_window)
        .send_window(mux_limits.max_stream_window_bytes.saturating_mul(2))
        .send_fairness(true)
        .initial_mtu(UDP_CARRIER_INITIAL_MTU_BYTES)
        .min_mtu(UDP_CARRIER_MIN_MTU_BYTES)
        .keep_alive_interval(Some(UDP_CARRIER_KEEP_ALIVE_INTERVAL))
        .max_idle_timeout(Some(UDP_CARRIER_MAX_IDLE_TIMEOUT.try_into()?))
        .datagram_receive_buffer_size(Some(datagram_queue))
        .datagram_send_buffer_size(datagram_queue)
        .congestion_controller_factory(Arc::new(congestion::BbrConfig::default()))
        .enable_segmentation_offload(false);

    Ok(config)
}

pub fn derive_credential(secret: &[u8]) -> Result<UdpCarrierCredential, UdpCarrierTransportError> {
    let seed = derive_seed(secret, CERT_KEY_CONTEXT)?;
    let pkcs8 = ed25519_pkcs8_from_seed(&seed);
    Ed25519KeyPair::from_pkcs8_maybe_unchecked(&pkcs8)
        .map_err(|_| UdpCarrierTransportError::PrivateKeyRejected)?;

    let key = PrivatePkcs8KeyDer::from(pkcs8.clone());
    let signing_key = KeyPair::from_pkcs8_der_and_sign_algo(&key, &rcgen::PKCS_ED25519)?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.not_before = date_time_ymd(2026, 1, 1);
    params.not_after = date_time_ymd(2126, 1, 1);
    params.serial_number = Some(certificate_serial(secret)?);
    let cert = params.self_signed(&signing_key)?;
    let cert_der = cert.der().clone();
    let fingerprint_sha256 = certificate_fingerprint(&cert_der);

    Ok(UdpCarrierCredential {
        cert_der,
        key_der: PrivateKeyDer::from(PrivatePkcs8KeyDer::from(pkcs8)),
        fingerprint_sha256,
    })
}

pub fn certificate_fingerprint(cert: &CertificateDer<'_>) -> [u8; 32] {
    Sha256::digest(cert.as_ref()).into()
}

pub fn derive_private_alpn(secret: &[u8]) -> Result<Vec<u8>, UdpCarrierTransportError> {
    let seed = derive_seed(secret, PRIVATE_ALPN_CONTEXT)?;
    Ok(URL_SAFE_NO_PAD
        .encode(&seed[..PRIVATE_ALPN_SEED_BYTES])
        .into_bytes())
}

pub async fn read_frame(
    recv: &mut RecvStream,
    limits: CodecLimits,
) -> Result<Frame, UdpCarrierFrameError> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    recv.read_exact(&mut header).await?;
    let payload_len = decode_payload_len_from_header(&header, limits)?;
    let mut encoded = Vec::with_capacity(FRAME_HEADER_LEN + payload_len);
    encoded.extend_from_slice(&header);
    encoded.resize(FRAME_HEADER_LEN + payload_len, 0);
    recv.read_exact(&mut encoded[FRAME_HEADER_LEN..]).await?;
    Ok(decode_frame(&encoded, limits)?)
}

pub async fn write_frame(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<(), UdpCarrierFrameError> {
    let encoded = encode_frame(frame, limits)?;
    send.write_all(&encoded).await?;
    Ok(())
}

pub fn finish_stream(send: &mut SendStream) -> Result<(), UdpCarrierFrameError> {
    Ok(send.finish()?)
}

fn derive_seed(secret: &[u8], context: &[u8]) -> Result<[u8; 32], UdpCarrierTransportError> {
    if secret.is_empty() {
        return Err(UdpCarrierTransportError::EmptySecret);
    }
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts keys of any length");
    mac.update(context);
    Ok(mac.finalize().into_bytes().into())
}

fn certificate_serial(secret: &[u8]) -> Result<SerialNumber, UdpCarrierTransportError> {
    let mut serial = derive_seed(secret, CERT_SERIAL_CONTEXT)?;
    serial[0] &= 0x7f;
    if serial.iter().all(|byte| *byte == 0) {
        serial[31] = 1;
    }
    Ok(SerialNumber::from_slice(&serial[..20]))
}

fn ed25519_pkcs8_from_seed(seed: &[u8; 32]) -> Vec<u8> {
    let mut der = Vec::with_capacity(48);
    der.extend_from_slice(&[
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ]);
    der.extend_from_slice(seed);
    der
}

fn varint(value: u64) -> Result<VarInt, UdpCarrierTransportError> {
    Ok(VarInt::from_u64(value)?)
}

#[derive(Debug)]
struct PinnedCertificateVerifier {
    expected_fingerprint: [u8; 32],
    provider: Arc<CryptoProvider>,
}

impl PinnedCertificateVerifier {
    fn new(expected_fingerprint: [u8; 32], provider: Arc<CryptoProvider>) -> Self {
        Self {
            expected_fingerprint,
            provider,
        }
    }

    fn verify_pin(&self, cert: &CertificateDer<'_>) -> Result<(), rustls::Error> {
        if certificate_fingerprint(cert) == self.expected_fingerprint {
            Ok(())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }
}

impl ServerCertVerifier for PinnedCertificateVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.verify_pin(end_entity)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
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
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"mptunnel integration test secret with enough entropy";
    const VISIBLE_PRODUCT_TOKENS: &[&[u8]] = &[b"mptunnel", b"mptun", b"quic", b"udp-carrier"];

    #[test]
    fn credentials_are_secret_derived_and_stable() {
        let first = derive_credential(SECRET).expect("first credential");
        let second = derive_credential(SECRET).expect("second credential");
        let other = derive_credential(b"other high entropy test secret").expect("other credential");

        assert_eq!(first.cert_der.as_ref(), second.cert_der.as_ref());
        assert_eq!(first.fingerprint_sha256, second.fingerprint_sha256);
        assert_ne!(first.cert_der.as_ref(), other.cert_der.as_ref());
        assert_ne!(first.fingerprint_sha256, other.fingerprint_sha256);
    }

    #[test]
    fn empty_secret_is_rejected() {
        assert!(matches!(
            derive_credential(b""),
            Err(UdpCarrierTransportError::EmptySecret)
        ));
    }

    #[test]
    fn derived_key_is_accepted_by_ring_and_rustls() {
        let credential = derive_credential(SECRET).expect("credential");
        let key_der = match &credential.key_der {
            PrivateKeyDer::Pkcs8(key) => key.secret_pkcs8_der(),
            _ => panic!("expected pkcs8 key"),
        };

        Ed25519KeyPair::from_pkcs8_maybe_unchecked(key_der).expect("ring parses key");
        rustls::crypto::ring::default_provider()
            .key_provider
            .load_private_key(credential.key_der.clone_key())
            .expect("rustls parses key");
    }

    #[test]
    fn certificate_does_not_expose_product_metadata() {
        let credential = derive_credential(SECRET).expect("credential");
        for token in VISIBLE_PRODUCT_TOKENS {
            assert!(
                !credential
                    .cert_der
                    .as_ref()
                    .windows(token.len())
                    .any(|window| window == *token),
                "certificate should not expose {:?}",
                String::from_utf8_lossy(token)
            );
        }
    }

    #[test]
    fn tls_configs_do_not_advertise_product_protocol_metadata() {
        let client = client_tls_config(SECRET).expect("client tls config");
        let server = server_tls_config(SECRET).expect("server tls config");
        let alpn = derive_private_alpn(SECRET).expect("private alpn");

        assert!(!client.enable_sni);
        assert_eq!(client.alpn_protocols, vec![alpn.clone()]);
        assert_eq!(server.alpn_protocols, vec![alpn.clone()]);
        assert_ne!(
            alpn,
            derive_private_alpn(b"other high entropy test secret").expect("other private alpn")
        );
        for token in VISIBLE_PRODUCT_TOKENS {
            assert!(
                !alpn
                    .windows(token.len())
                    .any(|window| window.eq_ignore_ascii_case(token)),
                "private alpn should not expose {:?}",
                String::from_utf8_lossy(token)
            );
        }
    }

    #[test]
    fn pinned_verifier_accepts_only_derived_certificate() {
        let credential = derive_credential(SECRET).expect("credential");
        let verifier = PinnedCertificateVerifier::new(
            credential.fingerprint_sha256,
            Arc::new(rustls::crypto::ring::default_provider()),
        );
        let other = derive_credential(b"other high entropy test secret").expect("other");

        verifier
            .verify_server_cert(
                &credential.cert_der,
                &[],
                &ServerName::try_from(CONNECT_SERVER_NAME).expect("server name"),
                &[],
                UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
            )
            .expect("matching certificate");
        assert!(
            verifier
                .verify_server_cert(
                    &other.cert_der,
                    &[],
                    &ServerName::try_from(CONNECT_SERVER_NAME).expect("server name"),
                    &[],
                    UnixTime::since_unix_epoch(Duration::from_secs(1_800_000_000)),
                )
                .is_err()
        );
    }

    #[test]
    fn configs_build_with_default_limits() {
        let limits = MuxLimits::default();
        client_config(SECRET, limits).expect("client config");
        server_config(SECRET, limits).expect("server config");
        transport_config(limits).expect("transport config");
    }
}
