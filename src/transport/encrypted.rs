//! TLS 1.3 TCP carrier framing.
//!
//! TLS owns confidentiality, integrity, forward secrecy, record sizing, and
//! traffic-key updates. MPTunnel owns only its bounded admission prelude and
//! protocol-frame codec.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::Frame;
use crate::protocol::codec::{
    CodecError, CodecLimits, FRAME_HEADER_LEN, decode_frame_bytes, decode_payload_len_from_header,
    encode_frame_into,
};
use bytes::BytesMut;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use sha2::{Digest, Sha256};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf, ReadHalf, WriteHalf};
use tokio_rustls::TlsStream;

/// QUIC presents as the standardized HTTP/3 application protocol. MPP
/// admission is carried only in encrypted HTTP/3 request DATA; TCP
/// deliberately negotiates no ALPN.
pub const HTTP_3_ALPN: &[u8] = b"h3";

/// One fixed, encrypted TCP admission request. This is deliberately not a
/// public wire marker: the bytes are written only after TLS 1.3 completes.
pub(crate) const TCP_ADMISSION_PRELUDE_LEN: usize = 131;

const TCP_ADMISSION_EXPORTER_LEN: usize = 32;
const TCP_ADMISSION_EXPORTER_LABEL: &[u8] = b"EXPORTER-mptunnel-tcp-admission-v1";

#[derive(Clone)]
pub struct TcpClientTlsConfig {
    server_name: ServerName<'static>,
    pinned_leaf: CertificateDer<'static>,
    tcp_config: Arc<ClientConfig>,
    config: Arc<ClientConfig>,
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
        })
    }

    /// QUIC-facing TLS identity. TCP consumes the separately cached ALPN-free
    /// configuration built from the same certificate verifier.
    pub(in crate::transport) fn rustls_config(&self) -> Arc<ClientConfig> {
        self.config.clone()
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
    pub(in crate::transport) fn from_config(
        server_name: ServerName<'static>,
        pinned_leaf: CertificateDer<'static>,
        config: ClientConfig,
    ) -> Self {
        let mut tcp_config = config.clone();
        tcp_config.enable_early_data = false;
        tcp_config.alpn_protocols.clear();
        Self {
            server_name,
            pinned_leaf,
            tcp_config: Arc::new(tcp_config),
            config: Arc::new(config),
        }
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
        self.server_name == other.server_name && self.pinned_leaf == other.pinned_leaf
    }
}

impl Eq for TcpClientTlsConfig {}

#[derive(Clone)]
pub struct TcpServerTlsConfig {
    certificate_chain: Arc<Vec<CertificateDer<'static>>>,
    private_key_fingerprint: [u8; 32],
    tcp_config: Arc<ServerConfig>,
    config: Arc<ServerConfig>,
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
        })
    }

    /// QUIC-facing TLS identity. TCP consumes the separately cached ALPN-free
    /// configuration built from the same certificate and key.
    pub(in crate::transport) fn rustls_config(&self) -> Arc<ServerConfig> {
        self.config.clone()
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

pub struct EncryptedFramedStream<S> {
    stream: TlsStream<CountingIo<S>>,
    limits: CodecLimits,
    wire_bytes: WireByteCounter,
    encode_buffer: Vec<u8>,
}

pub struct EncryptedFramedReader<S> {
    stream: ReadHalf<TlsStream<CountingIo<S>>>,
    limits: CodecLimits,
}

pub struct EncryptedFramedWriter<S> {
    stream: WriteHalf<TlsStream<CountingIo<S>>>,
    limits: CodecLimits,
    wire_bytes: WireByteCounter,
    wire_baseline: u64,
    write_poisoned: bool,
    encode_buffer: Vec<u8>,
}

pub type EncryptedFramedSplit<S> = (EncryptedFramedReader<S>, EncryptedFramedWriter<S>);

impl<S> std::fmt::Debug for EncryptedFramedStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedFramedStream")
            .field("limits", &self.limits)
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

    pub async fn accept(
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

    pub fn limits(&self) -> CodecLimits {
        self.limits
    }

    /// TLS 1.3 exporter shared by the endpoints of this exact TCP connection.
    ///
    /// rustls derives this only from a completed full handshake; its early
    /// exporter is never used. The value is consumed once by admission and is
    /// not retained by the steady-state framed transport.
    pub(crate) fn tcp_admission_exporter(
        &self,
    ) -> Result<[u8; TCP_ADMISSION_EXPORTER_LEN], EncryptedFramedTransportError> {
        let output = [0u8; TCP_ADMISSION_EXPORTER_LEN];
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

    /// Writes the one-time prelude and its immediately following setup frames
    /// in one plaintext/TLS write. This avoids adding a handshake syscall or
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
        self.stream.write_all(&self.encode_buffer).await?;
        Ok(())
    }

    /// Reads the one fixed encrypted admission prelude without exposing a
    /// carrier-specific response branch to unauthenticated input.
    pub(crate) async fn read_tcp_admission(
        &mut self,
    ) -> Result<[u8; TCP_ADMISSION_PRELUDE_LEN], EncryptedFramedTransportError> {
        let mut prelude = [0u8; TCP_ADMISSION_PRELUDE_LEN];
        self.stream.read_exact(&mut prelude).await?;
        Ok(prelude)
    }

    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        read_frame_from(&mut self.stream, self.limits).await
    }

    pub async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> Result<(), EncryptedFramedTransportError> {
        write_frames_to(
            &mut self.stream,
            self.limits,
            std::slice::from_ref(frame),
            &mut self.encode_buffer,
            None,
        )
        .await
    }

    pub async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        write_frames_to(
            &mut self.stream,
            self.limits,
            frames,
            &mut self.encode_buffer,
            None,
        )
        .await
    }

    pub async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        flush_stream(&mut self.stream).await
    }

    pub fn split(self) -> Result<EncryptedFramedSplit<S>, EncryptedFramedTransportError> {
        let baseline = self.wire_bytes.load();
        let (reader, writer) = tokio::io::split(self.stream);
        Ok((
            EncryptedFramedReader {
                stream: reader,
                limits: self.limits,
            },
            EncryptedFramedWriter {
                stream: writer,
                limits: self.limits,
                wire_bytes: self.wire_bytes,
                wire_baseline: baseline,
                write_poisoned: false,
                encode_buffer: Vec::new(),
            },
        ))
    }
}

impl<S> EncryptedFramedReader<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        read_frame_from(&mut self.stream, self.limits).await
    }

    pub async fn read_frames(&mut self) -> Result<Vec<Frame>, EncryptedFramedTransportError> {
        Ok(vec![self.read_frame().await?])
    }
}

impl<S> EncryptedFramedWriter<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Raw TLS bytes accepted by the underlying socket since this writer was split.
    pub fn wire_bytes_written(&self) -> u64 {
        self.wire_bytes.load().saturating_sub(self.wire_baseline)
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
        if self.write_poisoned {
            return Err(EncryptedFramedTransportError::WriteStatePoisoned);
        }
        write_frames_to(
            &mut self.stream,
            self.limits,
            frames,
            &mut self.encode_buffer,
            Some(&mut self.write_poisoned),
        )
        .await
    }

    pub async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
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

async fn read_frame_from<R>(
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

async fn write_frames_to<W>(
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
            Self::Io(error) => write!(f, "TLS carrier I/O failed: {error}"),
            Self::Codec(error) => write!(f, "TLS carrier frame codec failed: {error}"),
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
            Self::WriteStatePoisoned => {
                write!(
                    f,
                    "TLS carrier write state is unusable after an incomplete write"
                )
            }
            Self::LengthOverflow => write!(f, "TLS carrier frame length overflow"),
        }
    }
}

impl std::error::Error for EncryptedFramedTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) | Self::TlsHandshake(error) => Some(error),
            Self::TlsExporter(error) => Some(error),
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
pub(crate) fn test_server_tls_config() -> TcpServerTlsConfig {
    test_tls_configs().1.clone()
}

#[cfg(test)]
#[path = "tests_encrypted.rs"]
mod tests;
