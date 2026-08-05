//! Carrier cryptographic configuration and protected TCP framing.
//!
//! The default profile retains TLS 1.3 TCP and public QUIC Initial packets.
//! An optional endpoint-wide shared transport secret selects a PSK-gated
//! Noise TCP carrier and private QUIC Initial keys. MPP client credentials
//! remain an independent admission layer in both profiles.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::Frame;
use crate::protocol::codec::{
    CodecError, CodecLimits, FRAME_HEADER_LEN, decode_frame_bytes, decode_payload_len_from_header,
    encode_frame_into,
};
use bytes::BytesMut;
use hmac::{Hmac, Mac};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use sha2::{Digest, Sha256};
use snow::{Builder as NoiseBuilder, HandshakeState, StatelessTransportState, params::NoiseParams};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf, ReadHalf, WriteHalf};
use tokio_rustls::TlsStream;

/// QUIC presents as the standardized HTTP/3 application protocol. MPP
/// admission is carried only in encrypted HTTP/3 request DATA; legacy TCP
/// deliberately negotiates no ALPN.
pub const HTTP_3_ALPN: &[u8] = b"h3";

/// One fixed, encrypted TCP admission request. This is deliberately not a
/// public wire marker: the bytes are written only after carrier protection
/// completes.
pub(crate) const TCP_ADMISSION_PRELUDE_LEN: usize = 131;

const TCP_ADMISSION_BINDING_LEN: usize = 32;
const TCP_ADMISSION_EXPORTER_LABEL: &[u8] = b"EXPORTER-mptunnel-tcp-admission-v1";
const TCP_NOISE_PROTOCOL: &str = "Noise_NNpsk0_25519_AESGCM_SHA256";
const TCP_NOISE_TAG_LEN: usize = 16;
const TCP_NOISE_MAX_CIPHERTEXT: usize = u16::MAX as usize;
const TCP_NOISE_MAX_PLAINTEXT: usize = TCP_NOISE_MAX_CIPHERTEXT - TCP_NOISE_TAG_LEN;
const TCP_NOISE_MIN_PADDING: usize = 8;
const TCP_NOISE_MAX_PADDING: usize = 63;
const TCP_NOISE_EPHEMERAL_LEN: usize = 32;
const TCP_NOISE_MASKED_LENGTH_LEN: usize = 2;
const TCP_NOISE_CLIENT_HELLO_VERSION: u8 = 1;
const TCP_NOISE_CLIENT_HELLO_HEADER_LEN: usize = 1 + 8 + 16;
#[cfg(not(test))]
const TCP_NOISE_REKEY_RECORD_INTERVAL: u64 = 1 << 20;
#[cfg(test)]
const TCP_NOISE_REKEY_RECORD_INTERVAL: u64 = 8;

const TCP_NOISE_CLIENT_HANDSHAKE_LABEL: &[u8] = b"mptunnel noise client handshake length v1";
const TCP_NOISE_SERVER_HANDSHAKE_LABEL: &[u8] = b"mptunnel noise server handshake length v1";
const TCP_NOISE_CLIENT_RECORD_LABEL: &[u8] = b"mptunnel noise client record length v1";
const TCP_NOISE_SERVER_RECORD_LABEL: &[u8] = b"mptunnel noise server record length v1";
const TCP_NOISE_ADMISSION_LABEL: &[u8] = b"mptunnel noise admission binding v1";
const TCP_NOISE_PSK_LABEL: &[u8] = b"mptunnel tcp noise psk v1";
const QUIC_PRIVATE_INITIAL_LABEL: &[u8] = b"mptunnel quic private initial key v1";

type HmacSha256 = Hmac<Sha256>;

/// Endpoint-wide transport protection material. This is deliberately a
/// distinct type from MPP client credentials and is never used for admission.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SharedTransportSecret([u8; 32]);

impl SharedTransportSecret {
    pub(crate) fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    fn derive(&self, label: &[u8]) -> [u8; 32] {
        keyed_digest(&self.0, label, &[])
    }
}

#[derive(Debug)]
struct TransportReplayCache {
    capacity: usize,
    entries: HashMap<[u8; 16], u64>,
    expirations: BTreeMap<u64, Vec<[u8; 16]>>,
    observed_unix_secs: u64,
}

impl TransportReplayCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            expirations: BTreeMap::new(),
            observed_unix_secs: 0,
        }
    }

    fn try_insert(
        &mut self,
        nonce: [u8; 16],
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> bool {
        self.observed_unix_secs = self.observed_unix_secs.max(now_unix_secs);
        while let Some((&expiry, _)) = self.expirations.first_key_value() {
            if expiry >= self.observed_unix_secs {
                break;
            }
            let (_, nonces) = self.expirations.pop_first().expect("first expiry exists");
            for expired in nonces {
                if self.entries.get(&expired) == Some(&expiry) {
                    self.entries.remove(&expired);
                }
            }
        }
        if expires_at_unix_secs < self.observed_unix_secs
            || self.entries.contains_key(&nonce)
            || self.entries.len() >= self.capacity
        {
            return false;
        }
        self.entries.insert(nonce, expires_at_unix_secs);
        self.expirations
            .entry(expires_at_unix_secs)
            .or_default()
            .push(nonce);
        true
    }
}

#[derive(Clone)]
struct ServerSharedTransportSecret {
    secret: SharedTransportSecret,
    freshness_window: Duration,
    replay_capacity: usize,
    replay: Arc<Mutex<TransportReplayCache>>,
}

impl PartialEq for ServerSharedTransportSecret {
    fn eq(&self, other: &Self) -> bool {
        self.secret == other.secret
            && self.freshness_window == other.freshness_window
            && self.replay_capacity == other.replay_capacity
    }
}

impl Eq for ServerSharedTransportSecret {}

#[derive(Clone)]
pub struct TcpClientTlsConfig {
    server_name: ServerName<'static>,
    pinned_leaf: CertificateDer<'static>,
    tcp_config: Arc<ClientConfig>,
    config: Arc<ClientConfig>,
    transport_secret: Option<SharedTransportSecret>,
}

impl TcpClientTlsConfig {
    /// Builds TLS 1.3-only client trust from an independently distributed
    /// end-entity certificate and its explicit WebPKI server name.
    pub fn new(
        server_name: impl Into<String>,
        pinned_leaf: CertificateDer<'static>,
    ) -> Result<Self, EncryptedFramedTransportError> {
        let server_name_text = server_name.into();
        let server_name = ServerName::try_from(server_name_text.clone()).map_err(|_| {
            EncryptedFramedTransportError::TlsConfiguration(format!(
                "invalid TLS server name {server_name_text:?}"
            ))
        })?;
        let mut roots = RootCertStore::empty();
        roots.add(pinned_leaf.clone()).map_err(|error| {
            EncryptedFramedTransportError::TlsConfiguration(format!(
                "pinned TLS certificate is not a valid trust anchor: {error}"
            ))
        })?;
        let webpki = WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|error| {
                EncryptedFramedTransportError::TlsConfiguration(format!(
                    "failed to build WebPKI verifier: {error}"
                ))
            })?;
        let verifier = Arc::new(ExactLeafVerifier {
            webpki,
            pinned_leaf: pinned_leaf.clone(),
        });
        let mut tcp_config =
            ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .dangerous()
                .with_custom_certificate_verifier(verifier)
                .with_no_client_auth();
        tcp_config.enable_early_data = false;
        tcp_config.alpn_protocols.clear();
        let mut config = tcp_config.clone();
        config.alpn_protocols = vec![HTTP_3_ALPN.to_vec()];
        Ok(Self {
            server_name,
            pinned_leaf,
            tcp_config: Arc::new(tcp_config),
            config: Arc::new(config),
            transport_secret: None,
        })
    }

    pub(crate) fn with_shared_transport_secret(
        mut self,
        transport_secret: SharedTransportSecret,
    ) -> Self {
        self.transport_secret = Some(transport_secret);
        self
    }

    fn shared_transport_secret(&self) -> Option<&SharedTransportSecret> {
        self.transport_secret.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn shared_transport_secret_configured(&self) -> bool {
        self.transport_secret.is_some()
    }

    /// QUIC-facing TLS identity. TCP uses the separate Noise configuration.
    pub(in crate::transport) fn rustls_config(&self) -> Arc<ClientConfig> {
        self.config.clone()
    }

    pub(in crate::transport) fn quic_initial_secret(&self) -> Option<[u8; 32]> {
        self.transport_secret
            .as_ref()
            .map(|secret| secret.derive(QUIC_PRIVATE_INITIAL_LABEL))
    }

    /// Returns the DNS identity usable as both QUIC SNI and exact HTTP/3
    /// authority. TLS deliberately omits SNI for IP identities, so they remain
    /// valid only for TCP-only path groups.
    pub(crate) fn quic_server_name_text(&self) -> Option<String> {
        match &self.server_name {
            ServerName::DnsName(name) => Some(name.as_ref().to_string()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(in crate::transport) fn with_rustls_config_for_test(
        mut self,
        config: ClientConfig,
    ) -> Self {
        self.config = Arc::new(config);
        self
    }
}

impl std::fmt::Debug for TcpClientTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpClientTlsConfig")
            .field("server_name", &self.server_name)
            .field("pinned_leaf_len", &self.pinned_leaf.as_ref().len())
            .finish_non_exhaustive()
    }
}

impl PartialEq for TcpClientTlsConfig {
    fn eq(&self, other: &Self) -> bool {
        self.server_name == other.server_name
            && self.pinned_leaf == other.pinned_leaf
            && self.transport_secret == other.transport_secret
    }
}

impl Eq for TcpClientTlsConfig {}

#[derive(Clone)]
pub struct TcpServerTlsConfig {
    certificate_chain: Arc<Vec<CertificateDer<'static>>>,
    private_key_fingerprint: [u8; 32],
    tcp_config: Arc<ServerConfig>,
    config: Arc<ServerConfig>,
    transport_secret: Option<ServerSharedTransportSecret>,
}

impl TcpServerTlsConfig {
    /// Builds a TLS 1.3-only server identity from already loaded secret
    /// material. File handling remains a Product-layer responsibility.
    pub fn new(
        certificate_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> Result<Self, EncryptedFramedTransportError> {
        if certificate_chain.is_empty() {
            return Err(EncryptedFramedTransportError::TlsConfiguration(
                "TLS certificate chain must not be empty".to_string(),
            ));
        }
        let private_key_fingerprint: [u8; 32] = Sha256::digest(private_key.secret_der()).into();
        let mut tcp_config =
            ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(certificate_chain.clone(), private_key)
                .map_err(|error| {
                    EncryptedFramedTransportError::TlsConfiguration(format!(
                        "invalid TLS certificate/private-key identity: {error}"
                    ))
                })?;
        tcp_config.max_early_data_size = 0;
        tcp_config.alpn_protocols.clear();
        let mut config = tcp_config.clone();
        config.alpn_protocols = vec![HTTP_3_ALPN.to_vec()];
        Ok(Self {
            certificate_chain: Arc::new(certificate_chain),
            private_key_fingerprint,
            tcp_config: Arc::new(tcp_config),
            config: Arc::new(config),
            transport_secret: None,
        })
    }

    pub(crate) fn with_shared_transport_secret(
        mut self,
        transport_secret: SharedTransportSecret,
        freshness_window: Duration,
        max_pending_authentications: usize,
    ) -> Self {
        let seconds = usize::try_from(freshness_window.as_secs()).unwrap_or(usize::MAX);
        let replay_capacity = max_pending_authentications
            .max(1)
            .saturating_mul(seconds.saturating_add(1).max(1));
        self.transport_secret = Some(ServerSharedTransportSecret {
            secret: transport_secret,
            freshness_window,
            replay_capacity,
            replay: Arc::new(Mutex::new(TransportReplayCache::new(replay_capacity))),
        });
        self
    }

    fn shared_transport_secret(&self) -> Option<&ServerSharedTransportSecret> {
        self.transport_secret.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn shared_transport_secret_configured(&self) -> bool {
        self.transport_secret.is_some()
    }

    /// QUIC-facing TLS identity. TCP uses the separate Noise configuration.
    pub(in crate::transport) fn rustls_config(&self) -> Arc<ServerConfig> {
        self.config.clone()
    }

    pub(in crate::transport) fn quic_initial_secret(&self) -> Option<[u8; 32]> {
        self.transport_secret
            .as_ref()
            .map(|config| config.secret.derive(QUIC_PRIVATE_INITIAL_LABEL))
    }
}

impl std::fmt::Debug for TcpServerTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpServerTlsConfig")
            .field("certificate_chain_len", &self.certificate_chain.len())
            .field("private_key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl PartialEq for TcpServerTlsConfig {
    fn eq(&self, other: &Self) -> bool {
        self.certificate_chain == other.certificate_chain
            && self.private_key_fingerprint == other.private_key_fingerprint
            && self.transport_secret == other.transport_secret
    }
}

impl Eq for TcpServerTlsConfig {}

#[derive(Debug)]
struct ExactLeafVerifier {
    webpki: Arc<WebPkiServerVerifier>,
    pinned_leaf: CertificateDer<'static>,
}

impl ServerCertVerifier for ExactLeafVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.webpki.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        if end_entity.as_ref() != self.pinned_leaf.as_ref() {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.webpki.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.webpki.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.webpki.supported_verify_schemes()
    }
}

#[derive(Clone, Default)]
struct WireByteCounter(Arc<AtomicU64>);

impl WireByteCounter {
    fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    fn add(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let previous = self.0.fetch_add(bytes, Ordering::Relaxed);
        debug_assert!(previous.checked_add(bytes).is_some());
    }
}

struct CountingIo<S> {
    inner: S,
    written: WireByteCounter,
}

impl<S> CountingIo<S> {
    fn new(inner: S, written: WireByteCounter) -> Self {
        Self { inner, written }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CountingIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountingIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.written.add(written);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(written)) => {
                this.written.add(written);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }
}

struct TlsFramedStream<S> {
    stream: TlsStream<CountingIo<S>>,
    limits: CodecLimits,
    wire_bytes: WireByteCounter,
    encode_buffer: Vec<u8>,
}

struct TlsFramedReader<S> {
    stream: ReadHalf<TlsStream<CountingIo<S>>>,
    limits: CodecLimits,
}

struct TlsFramedWriter<S> {
    stream: WriteHalf<TlsStream<CountingIo<S>>>,
    limits: CodecLimits,
    wire_bytes: WireByteCounter,
    wire_baseline: u64,
    write_poisoned: bool,
    encode_buffer: Vec<u8>,
}

impl<S> TlsFramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    async fn connect(
        stream: S,
        tls: &TcpClientTlsConfig,
        limits: CodecLimits,
    ) -> Result<Self, EncryptedFramedTransportError> {
        let wire_bytes = WireByteCounter::default();
        let stream = CountingIo::new(stream, wire_bytes.clone());
        let stream = tokio_rustls::TlsConnector::from(tls.tcp_config.clone())
            .connect(tls.server_name.clone(), stream)
            .await
            .map_err(EncryptedFramedTransportError::TlsHandshake)?;
        let stream = TlsStream::Client(stream);
        ensure_no_tcp_alpn(&stream)?;
        Ok(Self {
            stream,
            limits,
            wire_bytes,
            encode_buffer: Vec::new(),
        })
    }

    async fn accept(
        stream: S,
        tls: &TcpServerTlsConfig,
        limits: CodecLimits,
    ) -> Result<Self, EncryptedFramedTransportError> {
        let wire_bytes = WireByteCounter::default();
        let stream = CountingIo::new(stream, wire_bytes.clone());
        let stream = tokio_rustls::TlsAcceptor::from(tls.tcp_config.clone())
            .accept(stream)
            .await
            .map_err(EncryptedFramedTransportError::TlsHandshake)?;
        let stream = TlsStream::Server(stream);
        ensure_no_tcp_alpn(&stream)?;
        Ok(Self {
            stream,
            limits,
            wire_bytes,
            encode_buffer: Vec::new(),
        })
    }

    fn admission_binding(
        &self,
    ) -> Result<[u8; TCP_ADMISSION_BINDING_LEN], EncryptedFramedTransportError> {
        let output = [0u8; TCP_ADMISSION_BINDING_LEN];
        match &self.stream {
            TlsStream::Client(stream) => stream
                .get_ref()
                .1
                .export_keying_material(output, TCP_ADMISSION_EXPORTER_LABEL, None)
                .map_err(EncryptedFramedTransportError::TlsExporter),
            TlsStream::Server(stream) => stream
                .get_ref()
                .1
                .export_keying_material(output, TCP_ADMISSION_EXPORTER_LABEL, None)
                .map_err(EncryptedFramedTransportError::TlsExporter),
        }
    }

    async fn write_tcp_admission(
        &mut self,
        prelude: &[u8; TCP_ADMISSION_PRELUDE_LEN],
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        self.encode_buffer.clear();
        self.encode_buffer.extend_from_slice(prelude);
        for frame in frames {
            encode_frame_into(frame, self.limits, &mut self.encode_buffer)?;
        }
        self.stream.write_all(&self.encode_buffer).await?;
        Ok(())
    }

    async fn read_tcp_admission(
        &mut self,
    ) -> Result<[u8; TCP_ADMISSION_PRELUDE_LEN], EncryptedFramedTransportError> {
        let mut prelude = [0u8; TCP_ADMISSION_PRELUDE_LEN];
        self.stream.read_exact(&mut prelude).await?;
        Ok(prelude)
    }

    async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        read_tls_frame_from(&mut self.stream, self.limits).await
    }

    async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        write_tls_frames_to(
            &mut self.stream,
            self.limits,
            frames,
            &mut self.encode_buffer,
            None,
        )
        .await
    }

    async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        flush_stream(&mut self.stream).await
    }

    fn split(self) -> (TlsFramedReader<S>, TlsFramedWriter<S>) {
        let baseline = self.wire_bytes.load();
        let (reader, writer) = tokio::io::split(self.stream);
        (
            TlsFramedReader {
                stream: reader,
                limits: self.limits,
            },
            TlsFramedWriter {
                stream: writer,
                limits: self.limits,
                wire_bytes: self.wire_bytes,
                wire_baseline: baseline,
                write_poisoned: false,
                encode_buffer: Vec::new(),
            },
        )
    }
}

impl<S> TlsFramedReader<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        read_tls_frame_from(&mut self.stream, self.limits).await
    }
}

impl<S> TlsFramedWriter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn wire_bytes_written(&self) -> u64 {
        self.wire_bytes.load().saturating_sub(self.wire_baseline)
    }

    async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        if self.write_poisoned {
            return Err(EncryptedFramedTransportError::WriteStatePoisoned);
        }
        write_tls_frames_to(
            &mut self.stream,
            self.limits,
            frames,
            &mut self.encode_buffer,
            Some(&mut self.write_poisoned),
        )
        .await
    }

    async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        flush_stream(&mut self.stream).await
    }
}

fn ensure_no_tcp_alpn<S>(
    stream: &TlsStream<CountingIo<S>>,
) -> Result<(), EncryptedFramedTransportError> {
    let negotiated = match stream {
        TlsStream::Client(stream) => stream.get_ref().1.alpn_protocol(),
        TlsStream::Server(stream) => stream.get_ref().1.alpn_protocol(),
    };
    if negotiated.is_some() {
        return Err(EncryptedFramedTransportError::UnexpectedTcpAlpn(
            negotiated.map(ToOwned::to_owned).unwrap_or_default(),
        ));
    }
    Ok(())
}

async fn read_tls_frame_from<R>(
    stream: &mut R,
    limits: CodecLimits,
) -> Result<Frame, EncryptedFramedTransportError>
where
    R: AsyncRead + Unpin,
{
    #[cfg(feature = "lab-diagnostics")]
    let total_started = std::time::Instant::now();
    let mut header = [0u8; FRAME_HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let payload_len = decode_payload_len_from_header(&header, limits)?;
    let frame_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(EncryptedFramedTransportError::LengthOverflow)?;
    let mut encoded = BytesMut::with_capacity(frame_len);
    encoded.extend_from_slice(&header);
    encoded.resize(frame_len, 0);
    stream.read_exact(&mut encoded[FRAME_HEADER_LEN..]).await?;
    let frame = decode_frame_bytes(encoded.freeze(), limits)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.tls_read_frame_total",
        total_started.elapsed(),
        frame_len,
    );
    Ok(frame)
}

async fn write_tls_frames_to<W>(
    stream: &mut W,
    limits: CodecLimits,
    frames: &[Frame],
    encode_buffer: &mut Vec<u8>,
    mut write_poisoned: Option<&mut bool>,
) -> Result<(), EncryptedFramedTransportError>
where
    W: AsyncWrite + Unpin,
{
    if frames.is_empty() {
        return Ok(());
    }
    #[cfg(feature = "lab-diagnostics")]
    let total_started = std::time::Instant::now();
    encode_buffer.clear();
    for frame in frames {
        encode_frame_into(frame, limits, encode_buffer)?;
    }
    if let Some(poisoned) = write_poisoned.as_deref_mut() {
        *poisoned = true;
    }
    stream.write_all(encode_buffer).await?;
    if let Some(poisoned) = write_poisoned {
        *poisoned = false;
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.tls_write_frames_total",
        total_started.elapsed(),
        encode_buffer.len(),
    );
    Ok(())
}

struct NoiseHandshakeResult {
    transport: Arc<RwLock<StatelessTransportState>>,
    admission_binding: [u8; TCP_ADMISSION_BINDING_LEN],
    read_length_key: [u8; 32],
    write_length_key: [u8; 32],
}

struct NoiseReadState {
    nonce: u64,
    length_key: [u8; 32],
    ciphertext: Vec<u8>,
    plaintext: Vec<u8>,
    plaintext_offset: usize,
    poisoned: bool,
}

impl NoiseReadState {
    fn new(length_key: [u8; 32]) -> Self {
        Self {
            nonce: 0,
            length_key,
            ciphertext: Vec::new(),
            plaintext: Vec::new(),
            plaintext_offset: 0,
            poisoned: false,
        }
    }
}

struct NoiseWriteState {
    nonce: u64,
    length_key: [u8; 32],
    wire: Vec<u8>,
    poisoned: bool,
}

impl NoiseWriteState {
    fn new(length_key: [u8; 32]) -> Self {
        Self {
            nonce: 0,
            length_key,
            wire: Vec::new(),
            poisoned: false,
        }
    }
}

struct NoiseFramedStream<S> {
    stream: CountingIo<S>,
    transport: Arc<RwLock<StatelessTransportState>>,
    admission_binding: [u8; TCP_ADMISSION_BINDING_LEN],
    read: NoiseReadState,
    write: NoiseWriteState,
    limits: CodecLimits,
    wire_bytes: WireByteCounter,
    encode_buffer: Vec<u8>,
}

struct NoiseFramedReader<S> {
    stream: ReadHalf<CountingIo<S>>,
    transport: Arc<RwLock<StatelessTransportState>>,
    read: NoiseReadState,
    limits: CodecLimits,
}

struct NoiseFramedWriter<S> {
    stream: WriteHalf<CountingIo<S>>,
    transport: Arc<RwLock<StatelessTransportState>>,
    write: NoiseWriteState,
    limits: CodecLimits,
    wire_bytes: WireByteCounter,
    wire_baseline: u64,
    encode_buffer: Vec<u8>,
}

impl<S> std::fmt::Debug for NoiseFramedStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedFramedStream")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl<S> NoiseFramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn connect(
        stream: S,
        transport_secret: &SharedTransportSecret,
        limits: CodecLimits,
    ) -> Result<Self, EncryptedFramedTransportError> {
        let wire_bytes = WireByteCounter::default();
        let mut stream = CountingIo::new(stream, wire_bytes.clone());
        let handshake = noise_client_handshake(&mut stream, transport_secret).await?;
        Ok(Self {
            stream,
            transport: handshake.transport,
            admission_binding: handshake.admission_binding,
            read: NoiseReadState::new(handshake.read_length_key),
            write: NoiseWriteState::new(handshake.write_length_key),
            limits,
            wire_bytes,
            encode_buffer: Vec::new(),
        })
    }

    pub async fn accept(
        stream: S,
        transport_secret: &ServerSharedTransportSecret,
        limits: CodecLimits,
    ) -> Result<Self, EncryptedFramedTransportError> {
        let wire_bytes = WireByteCounter::default();
        let mut stream = CountingIo::new(stream, wire_bytes.clone());
        let handshake = noise_server_handshake(&mut stream, transport_secret).await?;
        Ok(Self {
            stream,
            transport: handshake.transport,
            admission_binding: handshake.admission_binding,
            read: NoiseReadState::new(handshake.read_length_key),
            write: NoiseWriteState::new(handshake.write_length_key),
            limits,
            wire_bytes,
            encode_buffer: Vec::new(),
        })
    }

    /// Channel binding shared only by the endpoints of this Noise session.
    pub(crate) fn tcp_admission_binding(
        &self,
    ) -> Result<[u8; TCP_ADMISSION_BINDING_LEN], EncryptedFramedTransportError> {
        Ok(self.admission_binding)
    }

    /// Writes the one-time prelude and its immediately following setup frames
    /// in one protected write. This avoids adding a handshake syscall or
    /// record boundary while leaving steady-state frame writes unchanged.
    pub(crate) async fn write_tcp_admission(
        &mut self,
        prelude: &[u8; TCP_ADMISSION_PRELUDE_LEN],
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        self.encode_buffer.clear();
        self.encode_buffer.extend_from_slice(prelude);
        for frame in frames {
            encode_frame_into(frame, self.limits, &mut self.encode_buffer)?;
        }
        write_noise_plaintext(
            &mut self.stream,
            &self.transport,
            &mut self.write,
            &self.encode_buffer,
        )
        .await
    }

    /// Reads the one fixed encrypted admission prelude without exposing a
    /// carrier-specific response branch to unauthenticated input.
    pub(crate) async fn read_tcp_admission(
        &mut self,
    ) -> Result<[u8; TCP_ADMISSION_PRELUDE_LEN], EncryptedFramedTransportError> {
        let mut prelude = [0u8; TCP_ADMISSION_PRELUDE_LEN];
        read_noise_exact(
            &mut self.stream,
            &self.transport,
            &mut self.read,
            &mut prelude,
        )
        .await?;
        Ok(prelude)
    }

    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        read_noise_frame_from(
            &mut self.stream,
            &self.transport,
            &mut self.read,
            self.limits,
        )
        .await
    }

    pub async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        write_noise_frames_to(
            &mut self.stream,
            &self.transport,
            &mut self.write,
            self.limits,
            frames,
            &mut self.encode_buffer,
        )
        .await
    }

    pub async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        flush_stream(&mut self.stream).await
    }

    fn split(
        self,
    ) -> Result<(NoiseFramedReader<S>, NoiseFramedWriter<S>), EncryptedFramedTransportError> {
        if self.read.poisoned {
            return Err(EncryptedFramedTransportError::ReadStatePoisoned);
        }
        if self.write.poisoned {
            return Err(EncryptedFramedTransportError::WriteStatePoisoned);
        }
        let baseline = self.wire_bytes.load();
        let (reader, writer) = tokio::io::split(self.stream);
        Ok((
            NoiseFramedReader {
                stream: reader,
                transport: self.transport.clone(),
                read: self.read,
                limits: self.limits,
            },
            NoiseFramedWriter {
                stream: writer,
                transport: self.transport,
                write: self.write,
                limits: self.limits,
                wire_bytes: self.wire_bytes,
                wire_baseline: baseline,
                encode_buffer: Vec::new(),
            },
        ))
    }
}

impl<S> NoiseFramedReader<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        read_noise_frame_from(
            &mut self.stream,
            &self.transport,
            &mut self.read,
            self.limits,
        )
        .await
    }
}

impl<S> NoiseFramedWriter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Raw protected bytes accepted by the underlying socket since this writer was split.
    pub fn wire_bytes_written(&self) -> u64 {
        self.wire_bytes.load().saturating_sub(self.wire_baseline)
    }

    pub async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        write_noise_frames_to(
            &mut self.stream,
            &self.transport,
            &mut self.write,
            self.limits,
            frames,
            &mut self.encode_buffer,
        )
        .await
    }

    pub async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        flush_stream(&mut self.stream).await
    }
}

// The TLS stream already carried this state inline before transport profiles
// were selectable. Boxing only that variant would add a heap allocation to
// every legacy TCP carrier solely to equalize enum variant sizes.
#[allow(clippy::large_enum_variant)]
enum EncryptedFramedStreamInner<S> {
    Tls(TlsFramedStream<S>),
    Noise(NoiseFramedStream<S>),
}

enum EncryptedFramedReaderInner<S> {
    Tls(TlsFramedReader<S>),
    Noise(NoiseFramedReader<S>),
}

enum EncryptedFramedWriterInner<S> {
    Tls(TlsFramedWriter<S>),
    Noise(NoiseFramedWriter<S>),
}

pub struct EncryptedFramedStream<S> {
    inner: EncryptedFramedStreamInner<S>,
}

pub struct EncryptedFramedReader<S> {
    inner: EncryptedFramedReaderInner<S>,
}

pub struct EncryptedFramedWriter<S> {
    inner: EncryptedFramedWriterInner<S>,
}

pub type EncryptedFramedSplit<S> = (EncryptedFramedReader<S>, EncryptedFramedWriter<S>);

impl<S> std::fmt::Debug for EncryptedFramedStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedFramedStream")
            .field(
                "profile",
                &match &self.inner {
                    EncryptedFramedStreamInner::Tls(_) => "tls",
                    EncryptedFramedStreamInner::Noise(_) => "noise",
                },
            )
            .finish_non_exhaustive()
    }
}

impl<S> EncryptedFramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn connect(
        stream: S,
        tls: &TcpClientTlsConfig,
        limits: CodecLimits,
    ) -> Result<Self, EncryptedFramedTransportError> {
        let inner = match tls.shared_transport_secret() {
            Some(secret) => EncryptedFramedStreamInner::Noise(
                NoiseFramedStream::connect(stream, secret, limits).await?,
            ),
            None => EncryptedFramedStreamInner::Tls(
                TlsFramedStream::connect(stream, tls, limits).await?,
            ),
        };
        Ok(Self { inner })
    }

    pub async fn accept(
        stream: S,
        tls: &TcpServerTlsConfig,
        limits: CodecLimits,
    ) -> Result<Self, EncryptedFramedTransportError> {
        let inner = match tls.shared_transport_secret() {
            Some(secret) => EncryptedFramedStreamInner::Noise(
                NoiseFramedStream::accept(stream, secret, limits).await?,
            ),
            None => {
                EncryptedFramedStreamInner::Tls(TlsFramedStream::accept(stream, tls, limits).await?)
            }
        };
        Ok(Self { inner })
    }

    pub fn limits(&self) -> CodecLimits {
        match &self.inner {
            EncryptedFramedStreamInner::Tls(stream) => stream.limits,
            EncryptedFramedStreamInner::Noise(stream) => stream.limits,
        }
    }

    /// Per-carrier channel binding used by MPP admission. The binding comes
    /// from the completed TLS or Noise handshake and never from a client
    /// credential or the endpoint-wide transport secret directly.
    pub(crate) fn tcp_admission_binding(
        &self,
    ) -> Result<[u8; TCP_ADMISSION_BINDING_LEN], EncryptedFramedTransportError> {
        match &self.inner {
            EncryptedFramedStreamInner::Tls(stream) => stream.admission_binding(),
            EncryptedFramedStreamInner::Noise(stream) => stream.tcp_admission_binding(),
        }
    }

    pub(crate) async fn write_tcp_admission(
        &mut self,
        prelude: &[u8; TCP_ADMISSION_PRELUDE_LEN],
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        match &mut self.inner {
            EncryptedFramedStreamInner::Tls(stream) => {
                stream.write_tcp_admission(prelude, frames).await
            }
            EncryptedFramedStreamInner::Noise(stream) => {
                stream.write_tcp_admission(prelude, frames).await
            }
        }
    }

    pub(crate) async fn read_tcp_admission(
        &mut self,
    ) -> Result<[u8; TCP_ADMISSION_PRELUDE_LEN], EncryptedFramedTransportError> {
        match &mut self.inner {
            EncryptedFramedStreamInner::Tls(stream) => stream.read_tcp_admission().await,
            EncryptedFramedStreamInner::Noise(stream) => stream.read_tcp_admission().await,
        }
    }

    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        match &mut self.inner {
            EncryptedFramedStreamInner::Tls(stream) => stream.read_frame().await,
            EncryptedFramedStreamInner::Noise(stream) => stream.read_frame().await,
        }
    }

    pub async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> Result<(), EncryptedFramedTransportError> {
        self.write_frames(std::slice::from_ref(frame)).await
    }

    pub async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        match &mut self.inner {
            EncryptedFramedStreamInner::Tls(stream) => stream.write_frames(frames).await,
            EncryptedFramedStreamInner::Noise(stream) => stream.write_frames(frames).await,
        }
    }

    pub async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        match &mut self.inner {
            EncryptedFramedStreamInner::Tls(stream) => stream.flush().await,
            EncryptedFramedStreamInner::Noise(stream) => stream.flush().await,
        }
    }

    pub fn split(self) -> Result<EncryptedFramedSplit<S>, EncryptedFramedTransportError> {
        let (reader, writer) = match self.inner {
            EncryptedFramedStreamInner::Tls(stream) => {
                let (reader, writer) = stream.split();
                (
                    EncryptedFramedReaderInner::Tls(reader),
                    EncryptedFramedWriterInner::Tls(writer),
                )
            }
            EncryptedFramedStreamInner::Noise(stream) => {
                let (reader, writer) = stream.split()?;
                (
                    EncryptedFramedReaderInner::Noise(reader),
                    EncryptedFramedWriterInner::Noise(writer),
                )
            }
        };
        Ok((
            EncryptedFramedReader { inner: reader },
            EncryptedFramedWriter { inner: writer },
        ))
    }
}

impl<S> EncryptedFramedReader<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        match &mut self.inner {
            EncryptedFramedReaderInner::Tls(reader) => reader.read_frame().await,
            EncryptedFramedReaderInner::Noise(reader) => reader.read_frame().await,
        }
    }

    pub async fn read_frames(&mut self) -> Result<Vec<Frame>, EncryptedFramedTransportError> {
        Ok(vec![self.read_frame().await?])
    }
}

impl<S> EncryptedFramedWriter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Raw protected bytes accepted by the underlying socket since this
    /// writer was split.
    pub fn wire_bytes_written(&self) -> u64 {
        match &self.inner {
            EncryptedFramedWriterInner::Tls(writer) => writer.wire_bytes_written(),
            EncryptedFramedWriterInner::Noise(writer) => writer.wire_bytes_written(),
        }
    }

    pub async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> Result<(), EncryptedFramedTransportError> {
        self.write_frames(std::slice::from_ref(frame)).await
    }

    pub async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        match &mut self.inner {
            EncryptedFramedWriterInner::Tls(writer) => writer.write_frames(frames).await,
            EncryptedFramedWriterInner::Noise(writer) => writer.write_frames(frames).await,
        }
    }

    pub async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        match &mut self.inner {
            EncryptedFramedWriterInner::Tls(writer) => writer.flush().await,
            EncryptedFramedWriterInner::Noise(writer) => writer.flush().await,
        }
    }
}

fn noise_parameters() -> Result<NoiseParams, EncryptedFramedTransportError> {
    TCP_NOISE_PROTOCOL.parse::<NoiseParams>().map_err(|error| {
        EncryptedFramedTransportError::NoiseConfiguration(format!(
            "invalid TCP Noise protocol definition: {error}"
        ))
    })
}

fn keyed_digest(secret: &[u8], label: &[u8], context: &[u8]) -> [u8; 32] {
    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(secret).expect("HMAC accepts arbitrary-length keys");
    mac.update(label);
    mac.update(context);
    mac.finalize().into_bytes().into()
}

fn masked_length(secret: &[u8], label: &[u8], context: &[u8], length: u16) -> u16 {
    let digest = keyed_digest(secret, label, context);
    length ^ u16::from_be_bytes([digest[0], digest[1]])
}

fn random_handshake_padding() -> Result<Vec<u8>, EncryptedFramedTransportError> {
    let span = TCP_NOISE_MAX_PADDING - TCP_NOISE_MIN_PADDING + 1;
    let unbiased_limit = u8::MAX - (usize::from(u8::MAX) + 1).rem_euclid(span) as u8;
    let length = loop {
        let mut selector = [0u8; 1];
        getrandom::getrandom(&mut selector).map_err(EncryptedFramedTransportError::Random)?;
        if selector[0] <= unbiased_limit {
            break TCP_NOISE_MIN_PADDING + usize::from(selector[0]) % span;
        }
    };
    let mut padding = vec![0u8; length];
    getrandom::getrandom(&mut padding).map_err(EncryptedFramedTransportError::Random)?;
    Ok(padding)
}

async fn write_noise_handshake<W>(
    stream: &mut W,
    handshake: &mut HandshakeState,
    handshake_length_key: &[u8; 32],
    direction_label: &[u8],
    payload: &[u8],
) -> Result<(), EncryptedFramedTransportError>
where
    W: AsyncWrite + Unpin,
{
    let mut message = vec![0u8; TCP_NOISE_MAX_CIPHERTEXT];
    let message_len = handshake
        .write_message(payload, &mut message)
        .map_err(EncryptedFramedTransportError::NoiseHandshake)?;
    if message_len < TCP_NOISE_EPHEMERAL_LEN {
        return Err(EncryptedFramedTransportError::InvalidNoiseHandshakeLength(
            message_len,
        ));
    }
    message.truncate(message_len);

    let remaining_len = u16::try_from(message_len - TCP_NOISE_EPHEMERAL_LEN)
        .map_err(|_| EncryptedFramedTransportError::InvalidNoiseHandshakeLength(message_len))?;
    let ephemeral = &message[..TCP_NOISE_EPHEMERAL_LEN];
    let encoded_len = masked_length(
        handshake_length_key,
        direction_label,
        ephemeral,
        remaining_len,
    );
    let mut wire = Vec::with_capacity(message_len + TCP_NOISE_MASKED_LENGTH_LEN);
    wire.extend_from_slice(ephemeral);
    wire.extend_from_slice(&encoded_len.to_be_bytes());
    wire.extend_from_slice(&message[TCP_NOISE_EPHEMERAL_LEN..]);
    stream.write_all(&wire).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_noise_handshake<R>(
    stream: &mut R,
    handshake: &mut HandshakeState,
    handshake_length_key: &[u8; 32],
    direction_label: &[u8],
    minimum_payload_len: usize,
    maximum_payload_len: usize,
) -> Result<Vec<u8>, EncryptedFramedTransportError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; TCP_NOISE_EPHEMERAL_LEN + TCP_NOISE_MASKED_LENGTH_LEN];
    stream.read_exact(&mut header).await?;
    let ephemeral = &header[..TCP_NOISE_EPHEMERAL_LEN];
    let encoded_len = u16::from_be_bytes([
        header[TCP_NOISE_EPHEMERAL_LEN],
        header[TCP_NOISE_EPHEMERAL_LEN + 1],
    ]);
    let remaining_len = usize::from(masked_length(
        handshake_length_key,
        direction_label,
        ephemeral,
        encoded_len,
    ));
    let message_len = TCP_NOISE_EPHEMERAL_LEN.checked_add(remaining_len).ok_or(
        EncryptedFramedTransportError::InvalidNoiseHandshakeLength(remaining_len),
    )?;
    let minimum_len = TCP_NOISE_EPHEMERAL_LEN + TCP_NOISE_TAG_LEN + minimum_payload_len;
    let maximum_len = TCP_NOISE_EPHEMERAL_LEN + TCP_NOISE_TAG_LEN + maximum_payload_len;
    if !(minimum_len..=maximum_len).contains(&message_len) {
        return Err(EncryptedFramedTransportError::InvalidNoiseHandshakeLength(
            message_len,
        ));
    }

    let mut message = vec![0u8; message_len];
    message[..TCP_NOISE_EPHEMERAL_LEN].copy_from_slice(ephemeral);
    stream
        .read_exact(&mut message[TCP_NOISE_EPHEMERAL_LEN..])
        .await?;
    let mut payload = vec![0u8; message_len];
    let payload_len = handshake
        .read_message(&message, &mut payload)
        .map_err(EncryptedFramedTransportError::NoiseHandshake)?;
    if !(minimum_payload_len..=maximum_payload_len).contains(&payload_len) {
        return Err(EncryptedFramedTransportError::InvalidNoiseHandshakePayloadLength(payload_len));
    }
    payload.truncate(payload_len);
    Ok(payload)
}

fn unix_time_seconds() -> Result<u64, EncryptedFramedTransportError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(EncryptedFramedTransportError::SystemClock)
}

fn noise_client_hello() -> Result<Vec<u8>, EncryptedFramedTransportError> {
    let mut nonce = [0u8; 16];
    getrandom::getrandom(&mut nonce).map_err(EncryptedFramedTransportError::Random)?;
    let padding = random_handshake_padding()?;
    let mut payload = Vec::with_capacity(TCP_NOISE_CLIENT_HELLO_HEADER_LEN + padding.len());
    payload.push(TCP_NOISE_CLIENT_HELLO_VERSION);
    payload.extend_from_slice(&unix_time_seconds()?.to_be_bytes());
    payload.extend_from_slice(&nonce);
    payload.extend_from_slice(&padding);
    Ok(payload)
}

fn admit_noise_client_hello(
    payload: &[u8],
    transport_secret: &ServerSharedTransportSecret,
) -> Result<(), EncryptedFramedTransportError> {
    let header = payload
        .get(..TCP_NOISE_CLIENT_HELLO_HEADER_LEN)
        .ok_or(EncryptedFramedTransportError::NoiseClientHelloRejected)?;
    if header[0] != TCP_NOISE_CLIENT_HELLO_VERSION {
        return Err(EncryptedFramedTransportError::NoiseClientHelloRejected);
    }
    let issued_at_unix_secs = u64::from_be_bytes(
        header[1..9]
            .try_into()
            .map_err(|_| EncryptedFramedTransportError::NoiseClientHelloRejected)?,
    );
    let nonce: [u8; 16] = header[9..TCP_NOISE_CLIENT_HELLO_HEADER_LEN]
        .try_into()
        .map_err(|_| EncryptedFramedTransportError::NoiseClientHelloRejected)?;
    let now_unix_secs = unix_time_seconds()?;
    let freshness_seconds = transport_secret.freshness_window.as_secs();
    if issued_at_unix_secs == 0 || issued_at_unix_secs.abs_diff(now_unix_secs) > freshness_seconds {
        return Err(EncryptedFramedTransportError::NoiseClientHelloRejected);
    }
    let expires_at_unix_secs = issued_at_unix_secs.saturating_add(freshness_seconds);
    let admitted = transport_secret
        .replay
        .lock()
        .map_err(|_| EncryptedFramedTransportError::NoiseReplayStatePoisoned)?
        .try_insert(nonce, expires_at_unix_secs, now_unix_secs);
    if !admitted {
        return Err(EncryptedFramedTransportError::NoiseClientHelloRejected);
    }
    Ok(())
}

fn finish_noise_handshake(
    handshake: HandshakeState,
    transport_secret: &SharedTransportSecret,
    initiator: bool,
) -> Result<NoiseHandshakeResult, EncryptedFramedTransportError> {
    let handshake_hash: [u8; 32] = handshake.get_handshake_hash().try_into().map_err(|_| {
        EncryptedFramedTransportError::NoiseConfiguration(
            "TCP Noise handshake hash is not SHA-256 sized".to_string(),
        )
    })?;
    let admission_binding = keyed_digest(
        &transport_secret.0,
        TCP_NOISE_ADMISSION_LABEL,
        &handshake_hash,
    );
    let client_length_key = keyed_digest(
        &transport_secret.0,
        TCP_NOISE_CLIENT_RECORD_LABEL,
        &handshake_hash,
    );
    let server_length_key = keyed_digest(
        &transport_secret.0,
        TCP_NOISE_SERVER_RECORD_LABEL,
        &handshake_hash,
    );
    let transport = Arc::new(RwLock::new(
        handshake
            .into_stateless_transport_mode()
            .map_err(EncryptedFramedTransportError::NoiseHandshake)?,
    ));
    let (read_length_key, write_length_key) = if initiator {
        (server_length_key, client_length_key)
    } else {
        (client_length_key, server_length_key)
    };
    Ok(NoiseHandshakeResult {
        transport,
        admission_binding,
        read_length_key,
        write_length_key,
    })
}

async fn noise_client_handshake<S>(
    stream: &mut S,
    transport_secret: &SharedTransportSecret,
) -> Result<NoiseHandshakeResult, EncryptedFramedTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let noise_psk = transport_secret.derive(TCP_NOISE_PSK_LABEL);
    let client_length_key = transport_secret.derive(TCP_NOISE_CLIENT_HANDSHAKE_LABEL);
    let server_length_key = transport_secret.derive(TCP_NOISE_SERVER_HANDSHAKE_LABEL);
    let mut handshake = NoiseBuilder::new(noise_parameters()?)
        .prologue(b"mptunnel tcp carrier v1")
        .and_then(|builder| builder.psk(0, &noise_psk))
        .and_then(NoiseBuilder::build_initiator)
        .map_err(EncryptedFramedTransportError::NoiseHandshake)?;
    let client_hello = noise_client_hello()?;
    write_noise_handshake(
        stream,
        &mut handshake,
        &client_length_key,
        TCP_NOISE_CLIENT_HANDSHAKE_LABEL,
        &client_hello,
    )
    .await?;
    read_noise_handshake(
        stream,
        &mut handshake,
        &server_length_key,
        TCP_NOISE_SERVER_HANDSHAKE_LABEL,
        TCP_NOISE_MIN_PADDING,
        TCP_NOISE_MAX_PADDING,
    )
    .await?;
    finish_noise_handshake(handshake, transport_secret, true)
}

async fn noise_server_handshake<S>(
    stream: &mut S,
    transport_secret: &ServerSharedTransportSecret,
) -> Result<NoiseHandshakeResult, EncryptedFramedTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let noise_psk = transport_secret.secret.derive(TCP_NOISE_PSK_LABEL);
    let client_length_key = transport_secret
        .secret
        .derive(TCP_NOISE_CLIENT_HANDSHAKE_LABEL);
    let server_length_key = transport_secret
        .secret
        .derive(TCP_NOISE_SERVER_HANDSHAKE_LABEL);
    let mut handshake = NoiseBuilder::new(noise_parameters()?)
        .prologue(b"mptunnel tcp carrier v1")
        .and_then(|builder| builder.psk(0, &noise_psk))
        .and_then(NoiseBuilder::build_responder)
        .map_err(EncryptedFramedTransportError::NoiseHandshake)?;
    let client_hello = read_noise_handshake(
        stream,
        &mut handshake,
        &client_length_key,
        TCP_NOISE_CLIENT_HANDSHAKE_LABEL,
        TCP_NOISE_CLIENT_HELLO_HEADER_LEN + TCP_NOISE_MIN_PADDING,
        TCP_NOISE_CLIENT_HELLO_HEADER_LEN + TCP_NOISE_MAX_PADDING,
    )
    .await?;
    admit_noise_client_hello(&client_hello, transport_secret)?;
    let server_padding = random_handshake_padding()?;
    write_noise_handshake(
        stream,
        &mut handshake,
        &server_length_key,
        TCP_NOISE_SERVER_HANDSHAKE_LABEL,
        &server_padding,
    )
    .await?;
    finish_noise_handshake(handshake, &transport_secret.secret, false)
}

fn noise_rekey_boundary(nonce: u64) -> bool {
    nonce != 0 && nonce.is_multiple_of(TCP_NOISE_REKEY_RECORD_INTERVAL)
}

fn read_noise_message(
    transport: &RwLock<StatelessTransportState>,
    nonce: u64,
    ciphertext: &[u8],
    plaintext: &mut [u8],
) -> Result<usize, EncryptedFramedTransportError> {
    if noise_rekey_boundary(nonce) {
        let mut transport = transport
            .write()
            .map_err(|_| EncryptedFramedTransportError::NoiseStatePoisoned)?;
        transport.rekey_incoming();
        transport
            .read_message(nonce, ciphertext, plaintext)
            .map_err(EncryptedFramedTransportError::NoiseRecord)
    } else {
        transport
            .read()
            .map_err(|_| EncryptedFramedTransportError::NoiseStatePoisoned)?
            .read_message(nonce, ciphertext, plaintext)
            .map_err(EncryptedFramedTransportError::NoiseRecord)
    }
}

fn write_noise_message(
    transport: &RwLock<StatelessTransportState>,
    nonce: u64,
    plaintext: &[u8],
    ciphertext: &mut [u8],
) -> Result<usize, EncryptedFramedTransportError> {
    if noise_rekey_boundary(nonce) {
        let mut transport = transport
            .write()
            .map_err(|_| EncryptedFramedTransportError::NoiseStatePoisoned)?;
        transport.rekey_outgoing();
        transport
            .write_message(nonce, plaintext, ciphertext)
            .map_err(EncryptedFramedTransportError::NoiseRecord)
    } else {
        transport
            .read()
            .map_err(|_| EncryptedFramedTransportError::NoiseStatePoisoned)?
            .write_message(nonce, plaintext, ciphertext)
            .map_err(EncryptedFramedTransportError::NoiseRecord)
    }
}

async fn read_noise_exact<R>(
    stream: &mut R,
    transport: &RwLock<StatelessTransportState>,
    state: &mut NoiseReadState,
    output: &mut [u8],
) -> Result<(), EncryptedFramedTransportError>
where
    R: AsyncRead + Unpin,
{
    if state.poisoned {
        return Err(EncryptedFramedTransportError::ReadStatePoisoned);
    }
    let mut output_offset = 0;
    while output_offset < output.len() {
        if state.plaintext_offset < state.plaintext.len() {
            let available = state.plaintext.len() - state.plaintext_offset;
            let copied = available.min(output.len() - output_offset);
            output[output_offset..output_offset + copied].copy_from_slice(
                &state.plaintext[state.plaintext_offset..state.plaintext_offset + copied],
            );
            state.plaintext_offset += copied;
            output_offset += copied;
            continue;
        }

        state.poisoned = true;
        let nonce = state.nonce;
        let next_nonce = nonce
            .checked_add(1)
            .ok_or(EncryptedFramedTransportError::NoiseNonceExhausted)?;
        let mut encoded_len = [0u8; TCP_NOISE_MASKED_LENGTH_LEN];
        stream.read_exact(&mut encoded_len).await?;
        let encoded_len = u16::from_be_bytes(encoded_len);
        let ciphertext_len = usize::from(masked_length(
            &state.length_key,
            b"mptunnel noise record header v1",
            &nonce.to_be_bytes(),
            encoded_len,
        ));
        if !((TCP_NOISE_TAG_LEN + 1)..=TCP_NOISE_MAX_CIPHERTEXT).contains(&ciphertext_len) {
            return Err(EncryptedFramedTransportError::InvalidNoiseRecordLength(
                ciphertext_len,
            ));
        }
        state.ciphertext.resize(ciphertext_len, 0);
        stream.read_exact(&mut state.ciphertext).await?;
        state.plaintext.resize(ciphertext_len, 0);
        let plaintext_len =
            read_noise_message(transport, nonce, &state.ciphertext, &mut state.plaintext)?;
        state.plaintext.truncate(plaintext_len);
        state.plaintext_offset = 0;
        state.nonce = next_nonce;
        state.poisoned = false;
    }
    Ok(())
}

async fn write_noise_plaintext<W>(
    stream: &mut W,
    transport: &RwLock<StatelessTransportState>,
    state: &mut NoiseWriteState,
    plaintext: &[u8],
) -> Result<(), EncryptedFramedTransportError>
where
    W: AsyncWrite + Unpin,
{
    if state.poisoned {
        return Err(EncryptedFramedTransportError::WriteStatePoisoned);
    }
    if plaintext.is_empty() {
        return Ok(());
    }

    state.poisoned = true;
    state.wire.clear();
    for chunk in plaintext.chunks(TCP_NOISE_MAX_PLAINTEXT) {
        let nonce = state.nonce;
        let next_nonce = nonce
            .checked_add(1)
            .ok_or(EncryptedFramedTransportError::NoiseNonceExhausted)?;
        let record_start = state.wire.len();
        state.wire.resize(
            record_start + TCP_NOISE_MASKED_LENGTH_LEN + chunk.len() + TCP_NOISE_TAG_LEN,
            0,
        );
        let ciphertext_len = write_noise_message(
            transport,
            nonce,
            chunk,
            &mut state.wire[record_start + TCP_NOISE_MASKED_LENGTH_LEN..],
        )?;
        let record_end = record_start + TCP_NOISE_MASKED_LENGTH_LEN + ciphertext_len;
        state.wire.truncate(record_end);
        let ciphertext_len = u16::try_from(ciphertext_len)
            .map_err(|_| EncryptedFramedTransportError::InvalidNoiseRecordLength(ciphertext_len))?;
        let encoded_len = masked_length(
            &state.length_key,
            b"mptunnel noise record header v1",
            &nonce.to_be_bytes(),
            ciphertext_len,
        );
        state.wire[record_start..record_start + TCP_NOISE_MASKED_LENGTH_LEN]
            .copy_from_slice(&encoded_len.to_be_bytes());
        state.nonce = next_nonce;
    }
    stream.write_all(&state.wire).await?;
    state.poisoned = false;
    Ok(())
}

async fn read_noise_frame_from<R>(
    stream: &mut R,
    transport: &RwLock<StatelessTransportState>,
    read: &mut NoiseReadState,
    limits: CodecLimits,
) -> Result<Frame, EncryptedFramedTransportError>
where
    R: AsyncRead + Unpin,
{
    #[cfg(feature = "lab-diagnostics")]
    let total_started = std::time::Instant::now();
    let mut header = [0u8; FRAME_HEADER_LEN];
    read_noise_exact(stream, transport, read, &mut header).await?;
    let payload_len = decode_payload_len_from_header(&header, limits)?;
    let frame_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(EncryptedFramedTransportError::LengthOverflow)?;
    let mut encoded = BytesMut::with_capacity(frame_len);
    encoded.extend_from_slice(&header);
    encoded.resize(frame_len, 0);
    read_noise_exact(stream, transport, read, &mut encoded[FRAME_HEADER_LEN..]).await?;
    let frame = decode_frame_bytes(encoded.freeze(), limits)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.noise_read_frame_total",
        total_started.elapsed(),
        frame_len,
    );
    Ok(frame)
}

async fn write_noise_frames_to<W>(
    stream: &mut W,
    transport: &RwLock<StatelessTransportState>,
    write: &mut NoiseWriteState,
    limits: CodecLimits,
    frames: &[Frame],
    encode_buffer: &mut Vec<u8>,
) -> Result<(), EncryptedFramedTransportError>
where
    W: AsyncWrite + Unpin,
{
    if frames.is_empty() {
        return Ok(());
    }
    #[cfg(feature = "lab-diagnostics")]
    let total_started = std::time::Instant::now();
    encode_buffer.clear();
    for frame in frames {
        encode_frame_into(frame, limits, encode_buffer)?;
    }
    write_noise_plaintext(stream, transport, write, encode_buffer).await?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.noise_write_frames_total",
        total_started.elapsed(),
        encode_buffer.len(),
    );
    Ok(())
}

async fn flush_stream<W>(stream: &mut W) -> Result<(), EncryptedFramedTransportError>
where
    W: AsyncWrite + Unpin,
{
    #[cfg(feature = "lab-diagnostics")]
    let started = std::time::Instant::now();
    stream.flush().await?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record("transport.tcp.flush_wait", started.elapsed(), 0);
    Ok(())
}

#[derive(Debug)]
pub enum EncryptedFramedTransportError {
    Io(io::Error),
    Codec(CodecError),
    TlsConfiguration(String),
    TlsHandshake(io::Error),
    TlsExporter(rustls::Error),
    UnexpectedTcpAlpn(Vec<u8>),
    NoiseConfiguration(String),
    NoiseHandshake(snow::Error),
    NoiseRecord(snow::Error),
    NoiseStatePoisoned,
    NoiseReplayStatePoisoned,
    NoiseClientHelloRejected,
    SystemClock(std::time::SystemTimeError),
    Random(getrandom::Error),
    InvalidNoiseHandshakeLength(usize),
    InvalidNoiseHandshakePayloadLength(usize),
    InvalidNoiseRecordLength(usize),
    NoiseNonceExhausted,
    ReadStatePoisoned,
    WriteStatePoisoned,
    LengthOverflow,
}

impl From<io::Error> for EncryptedFramedTransportError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<CodecError> for EncryptedFramedTransportError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl std::fmt::Display for EncryptedFramedTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "encrypted TCP carrier I/O failed: {error}"),
            Self::Codec(error) => write!(f, "encrypted TCP frame codec failed: {error}"),
            Self::TlsConfiguration(error) => {
                write!(f, "TLS carrier configuration failed: {error}")
            }
            Self::TlsHandshake(error) => write!(f, "TLS 1.3 carrier handshake failed: {error}"),
            Self::TlsExporter(error) => {
                write!(f, "TLS carrier admission exporter failed: {error}")
            }
            Self::UnexpectedTcpAlpn(negotiated) => write!(
                f,
                "TLS TCP carrier unexpectedly negotiated ALPN {:?}",
                String::from_utf8_lossy(negotiated)
            ),
            Self::NoiseConfiguration(error) => {
                write!(f, "TCP Noise configuration failed: {error}")
            }
            Self::NoiseHandshake(error) => write!(f, "TCP Noise handshake failed: {error}"),
            Self::NoiseRecord(error) => write!(f, "TCP Noise record failed: {error}"),
            Self::NoiseStatePoisoned => write!(f, "TCP Noise transport state is poisoned"),
            Self::NoiseReplayStatePoisoned => {
                write!(f, "TCP Noise replay state is poisoned")
            }
            Self::NoiseClientHelloRejected => write!(f, "TCP Noise client hello rejected"),
            Self::SystemClock(error) => write!(f, "system clock is before the Unix epoch: {error}"),
            Self::Random(error) => write!(f, "TCP Noise random padding failed: {error}"),
            Self::InvalidNoiseHandshakeLength(length) => {
                write!(f, "invalid TCP Noise handshake length {length}")
            }
            Self::InvalidNoiseHandshakePayloadLength(length) => {
                write!(f, "invalid TCP Noise handshake payload length {length}")
            }
            Self::InvalidNoiseRecordLength(length) => {
                write!(f, "invalid TCP Noise record length {length}")
            }
            Self::NoiseNonceExhausted => write!(f, "TCP Noise record nonce exhausted"),
            Self::ReadStatePoisoned => {
                write!(
                    f,
                    "TCP Noise read state is unusable after an incomplete record"
                )
            }
            Self::WriteStatePoisoned => {
                write!(
                    f,
                    "TCP Noise write state is unusable after an incomplete write"
                )
            }
            Self::LengthOverflow => write!(f, "encrypted TCP frame length overflow"),
        }
    }
}

impl std::error::Error for EncryptedFramedTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) | Self::TlsHandshake(error) => Some(error),
            Self::TlsExporter(error) => Some(error),
            Self::NoiseHandshake(error) | Self::NoiseRecord(error) => Some(error),
            Self::SystemClock(error) => Some(error),
            Self::Codec(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
fn test_tls_configs() -> &'static (TcpClientTlsConfig, TcpServerTlsConfig) {
    static CONFIGS: std::sync::OnceLock<(TcpClientTlsConfig, TcpServerTlsConfig)> =
        std::sync::OnceLock::new();
    CONFIGS.get_or_init(|| {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["mptunnel.test".to_string()])
                .expect("generate test TLS identity");
        let certificate = CertificateDer::from(cert);
        let private_key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into();
        let client = TcpClientTlsConfig::new("mptunnel.test", certificate.clone())
            .expect("build test TLS client config");
        let server = TcpServerTlsConfig::new(vec![certificate], private_key)
            .expect("build test TLS server config");
        (client, server)
    })
}

#[cfg(test)]
pub(crate) fn test_client_tls_config() -> TcpClientTlsConfig {
    test_tls_configs().0.clone()
}

#[cfg(test)]
pub(crate) fn test_client_tls_config_for_server_name(server_name: &str) -> TcpClientTlsConfig {
    TcpClientTlsConfig::new(server_name, test_tls_configs().0.pinned_leaf.clone())
        .expect("build named test TLS client config")
}

#[cfg(test)]
pub(crate) fn test_client_tls_config_for_pinned_leaf(
    pinned_leaf: CertificateDer<'static>,
) -> TcpClientTlsConfig {
    TcpClientTlsConfig::new("mptunnel.test", pinned_leaf)
        .expect("build pinned test TLS client config")
}

#[cfg(test)]
pub(crate) fn test_client_tls_config_with_transport_secret(
    transport_secret: [u8; 32],
) -> TcpClientTlsConfig {
    test_tls_configs()
        .0
        .clone()
        .with_shared_transport_secret(SharedTransportSecret::new(transport_secret))
}

#[cfg(test)]
pub(crate) fn test_server_tls_config() -> TcpServerTlsConfig {
    test_tls_configs().1.clone()
}

#[cfg(test)]
pub(crate) fn test_server_tls_config_with_transport_secret(
    transport_secret: [u8; 32],
) -> TcpServerTlsConfig {
    test_tls_configs().1.clone().with_shared_transport_secret(
        SharedTransportSecret::new(transport_secret),
        Duration::from_secs(300),
        128,
    )
}

#[cfg(test)]
#[path = "tests_encrypted.rs"]
mod tests;
