use quinn::ConnectionError;
use std::fmt;

// Thin QUIC carrier boundary. Quinn owns packet recovery and congestion state;
// this module adds mptunnel record framing and exports coherent native metrics.
// It deliberately does not decide which product flow or range uses this path.

mod congestion;
mod endpoint;
mod stream;

pub use congestion::{CarrierSendCreditSnapshot, CongestionMetrics};
pub use endpoint::{Connection, Endpoint};
pub use stream::{RecvStream, SendStream, finish_stream, read_frame, write_frame, write_frames};

use congestion::{InstrumentedBbrConfig, InstrumentedController, QuicCarrierTelemetry};
#[derive(Debug)]
pub enum QuicCarrierError {
    Io(std::io::Error),
    Connect(quinn::ConnectError),
    Connection(ConnectionError),
    Write(quinn::WriteError),
    Read(quinn::ReadError),
    UnexpectedEnd,
    ClosedStream(quinn::ClosedStream),
    FrameTooLarge,
    Codec(crate::protocol::codec::CodecError),
    Rustls(rustls::Error),
    QuinnCrypto(quinn::crypto::rustls::NoInitialCipherSuite),
    Rcgen(rcgen::Error),
    EmptySecret,
    CapacityFrameOnQuic,
}

impl fmt::Display for QuicCarrierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "QUIC carrier I/O failed: {err}"),
            Self::Connect(err) => write!(f, "QUIC carrier connect failed: {err}"),
            Self::Connection(err) => write!(f, "QUIC carrier connection failed: {err}"),
            Self::Write(err) => write!(f, "QUIC carrier write failed: {err}"),
            Self::Read(err) => write!(f, "QUIC carrier read failed: {err}"),
            Self::UnexpectedEnd => write!(f, "QUIC carrier stream ended mid-frame"),
            Self::ClosedStream(err) => write!(f, "QUIC carrier stream already closed: {err}"),
            Self::FrameTooLarge => write!(f, "QUIC carrier frame exceeds configured limits"),
            Self::Codec(err) => write!(f, "QUIC carrier frame codec failed: {err}"),
            Self::Rustls(err) => write!(f, "QUIC carrier TLS config failed: {err}"),
            Self::QuinnCrypto(err) => write!(f, "QUIC carrier crypto config failed: {err}"),
            Self::Rcgen(err) => write!(f, "QUIC carrier certificate generation failed: {err}"),
            Self::EmptySecret => write!(f, "QUIC carrier shared secret must not be empty"),
            Self::CapacityFrameOnQuic => {
                write!(f, "PATH_CAPACITY frames are not valid on QUIC carriers")
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

impl From<rcgen::Error> for QuicCarrierError {
    fn from(value: rcgen::Error) -> Self {
        Self::Rcgen(value)
    }
}
