use hmac::{Hmac, KeyInit, Mac};
use quinn::ConnectionError;
use sha2::Sha256;
use std::fmt;

// Thin QUIC carrier boundary. Quinn owns packet recovery and congestion state;
// this module adds mptunnel record framing and exports coherent native metrics.
// It deliberately does not decide which product flow or range uses this path.

mod congestion;
mod endpoint;
mod native_datagram;
mod presentation;
mod socket;
mod stream;

pub use congestion::CongestionMetrics;
pub use endpoint::{Connection, Endpoint};
pub use stream::IpPacketSender;
pub use stream::{RecvStream, SendStream, finish_stream, read_frame, write_frame, write_frames};

use congestion::{InstrumentedBbrConfig, InstrumentedController, QuicCarrierTelemetry};

type CandidateSelectorMac = Hmac<Sha256>;

const QUIC_CANDIDATE_SELECTOR_CONTEXT: &[u8] = b"mptunnel quic candidate selector v1";

/// Opaque request selector used only to gate encrypted HTTP/3 requests before
/// they can enter the MPP frame parser.
///
/// This is proof of credential knowledge, not session or path authorization.
/// Full SESSION_AUTH and PATH_JOIN verification remains mandatory.
#[derive(Clone)]
pub struct QuicCandidateSelector(hmac::digest::CtOutput<CandidateSelectorMac>);

impl QuicCandidateSelector {
    /// Derives one fixed selector from a named credential.
    pub fn derive(credential_id: &str, secret: &[u8]) -> Self {
        let mut mac = CandidateSelectorMac::new_from_slice(secret)
            .expect("HMAC-SHA256 accepts keys of every length");
        mac.update(QUIC_CANDIDATE_SELECTOR_CONTEXT);
        mac.update(&(credential_id.len() as u64).to_be_bytes());
        mac.update(credential_id.as_bytes());
        Self(mac.finalize())
    }

    /// Constant-time selector equality for verifier and connection-latch use.
    pub fn matches(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn from_bytes(bytes: [u8; 32]) -> Self {
        let mut output = hmac::digest::Output::<CandidateSelectorMac>::default();
        output.copy_from_slice(&bytes);
        Self(hmac::digest::CtOutput::new(output))
    }

    fn bytes(&self) -> [u8; 32] {
        self.0.clone().into_bytes().into()
    }
}

impl fmt::Debug for QuicCandidateSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QuicCandidateSelector(<redacted>)")
    }
}

/// Product-side lookup boundary consulted once, before the first accepted MPP
/// request on a QUIC connection. Implementations receive no credential ID or
/// secret through this interface.
pub trait QuicCandidateVerifier: Send + Sync + fmt::Debug {
    fn accepts(&self, selector: &QuicCandidateSelector) -> bool;
}

#[cfg(test)]
#[derive(Debug)]
struct ExactTestCandidateVerifier(QuicCandidateSelector);

#[cfg(test)]
impl QuicCandidateVerifier for ExactTestCandidateVerifier {
    fn accepts(&self, selector: &QuicCandidateSelector) -> bool {
        self.0.matches(selector)
    }
}

#[cfg(test)]
pub(super) fn test_candidate_selector() -> QuicCandidateSelector {
    QuicCandidateSelector::derive("test-credential", b"mptunnel test candidate secret 00")
}

#[cfg(test)]
pub(super) fn test_candidate_verifier() -> std::sync::Arc<dyn QuicCandidateVerifier> {
    std::sync::Arc::new(ExactTestCandidateVerifier(test_candidate_selector()))
}

#[derive(Debug)]
pub enum QuicCarrierError {
    Io(std::io::Error),
    Connect(quinn::ConnectError),
    Connection(ConnectionError),
    Write(quinn::WriteError),
    Read(quinn::ReadError),
    H3Connection(h3::error::ConnectionError),
    H3Stream(h3::error::StreamError),
    H3Http(http::Error),
    H3Status(http::StatusCode),
    H3Role(&'static str),
    H3AuthorityRequiresDnsName,
    H3DriverClosed,
    H3StreamFinished,
    H3DatagramNotNegotiated,
    NativeDatagram(quinn::SendDatagramError),
    NativeDatagramUnavailable,
    NativeDatagramRoutesExhausted,
    NativeDatagramFlowsExhausted,
    NativeDatagramTooLarge,
    InvalidNativeDatagram(&'static str),
    StreamFinished,
    UnexpectedEnd,
    ClosedStream(quinn::ClosedStream),
    FrameTooLarge,
    Codec(crate::protocol::codec::CodecError),
    Rustls(rustls::Error),
    QuinnCrypto(quinn::crypto::rustls::NoInitialCipherSuite),
    CapacityFrameOnQuic,
    IdleTimeoutOutOfRange(quinn::VarIntBoundsExceeded),
}

impl QuicCarrierError {
    /// Whether an established QUIC/H3 carrier instance ended without proving
    /// a Product-stream failure. The relay may retire that exact instance and
    /// preserve its logical stream on other authenticated carriers.
    pub(crate) fn is_path_lifetime_failure(&self) -> bool {
        matches!(
            self,
            Self::Io(_)
                | Self::Connection(_)
                | Self::Write(_)
                | Self::Read(_)
                | Self::H3Connection(_)
                | Self::H3Stream(_)
                | Self::H3DriverClosed
                | Self::H3StreamFinished
                | Self::StreamFinished
                | Self::UnexpectedEnd
                | Self::ClosedStream(_)
        )
    }
}

impl fmt::Display for QuicCarrierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "QUIC carrier I/O failed: {err}"),
            Self::Connect(err) => write!(f, "QUIC carrier connect failed: {err}"),
            Self::Connection(err) => write!(f, "QUIC carrier connection failed: {err}"),
            Self::Write(err) => write!(f, "QUIC carrier write failed: {err}"),
            Self::Read(err) => write!(f, "QUIC carrier read failed: {err}"),
            Self::H3Connection(err) => write!(f, "HTTP/3 connection failed: {err}"),
            Self::H3Stream(err) => write!(f, "HTTP/3 request stream failed: {err}"),
            Self::H3Http(err) => write!(f, "HTTP/3 message construction failed: {err}"),
            Self::H3Status(status) => {
                write!(
                    f,
                    "HTTP/3 carrier request was rejected with status {status}"
                )
            }
            Self::H3Role(message) => write!(f, "HTTP/3 carrier role error: {message}"),
            Self::H3AuthorityRequiresDnsName => write!(
                f,
                "QUIC HTTP/3 requires a DNS TLS server name because request authority is bound to SNI"
            ),
            Self::H3DriverClosed => write!(f, "HTTP/3 connection driver closed"),
            Self::H3StreamFinished => write!(f, "HTTP/3 request stream already finished"),
            Self::H3DatagramNotNegotiated => {
                write!(f, "peer did not negotiate HTTP/3 DATAGRAM")
            }
            Self::NativeDatagram(err) => write!(f, "QUIC DATAGRAM send failed: {err}"),
            Self::NativeDatagramUnavailable => {
                write!(f, "QUIC DATAGRAM is unavailable on this HTTP/3 connection")
            }
            Self::NativeDatagramRoutesExhausted => {
                write!(f, "HTTP Datagram request-stream route limit reached")
            }
            Self::NativeDatagramFlowsExhausted => {
                write!(f, "HTTP Datagram live-flow limit reached")
            }
            Self::NativeDatagramTooLarge => {
                write!(
                    f,
                    "MPP datagram exceeds the bounded native fragment envelope"
                )
            }
            Self::InvalidNativeDatagram(message) => {
                write!(f, "invalid native HTTP Datagram: {message}")
            }
            Self::StreamFinished => write!(f, "QUIC carrier stream finished"),
            Self::UnexpectedEnd => write!(f, "QUIC carrier stream ended mid-frame"),
            Self::ClosedStream(err) => write!(f, "QUIC carrier stream already closed: {err}"),
            Self::FrameTooLarge => write!(f, "QUIC carrier frame exceeds configured limits"),
            Self::Codec(err) => write!(f, "QUIC carrier frame codec failed: {err}"),
            Self::Rustls(err) => write!(f, "QUIC carrier TLS config failed: {err}"),
            Self::QuinnCrypto(err) => write!(f, "QUIC carrier crypto config failed: {err}"),
            Self::CapacityFrameOnQuic => {
                write!(f, "PATH_CAPACITY frames are not valid on QUIC carriers")
            }
            Self::IdleTimeoutOutOfRange(err) => {
                write!(f, "QUIC carrier idle timeout is out of range: {err}")
            }
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

impl From<quinn::VarIntBoundsExceeded> for QuicCarrierError {
    fn from(value: quinn::VarIntBoundsExceeded) -> Self {
        Self::IdleTimeoutOutOfRange(value)
    }
}

impl From<h3::error::ConnectionError> for QuicCarrierError {
    fn from(value: h3::error::ConnectionError) -> Self {
        Self::H3Connection(value)
    }
}

impl From<h3::error::StreamError> for QuicCarrierError {
    fn from(value: h3::error::StreamError) -> Self {
        Self::H3Stream(value)
    }
}

impl From<quinn::SendDatagramError> for QuicCarrierError {
    fn from(value: quinn::SendDatagramError) -> Self {
        Self::NativeDatagram(value)
    }
}
