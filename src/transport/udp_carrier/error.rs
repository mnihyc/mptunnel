use crate::protocol::codec::CodecError;
use std::{error::Error, fmt};

#[derive(Debug)]
pub enum UdpCarrierTransportError {
    EmptySecret,
    Io(std::io::Error),
    Random(getrandom::Error),
}

impl fmt::Display for UdpCarrierTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySecret => write!(f, "UDP carrier shared secret must not be empty"),
            Self::Io(err) => write!(f, "UDP carrier socket failed: {err}"),
            Self::Random(err) => write!(f, "UDP carrier random source failed: {err}"),
        }
    }
}

impl Error for UdpCarrierTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Random(err) => Some(err),
            Self::EmptySecret => None,
        }
    }
}

impl From<std::io::Error> for UdpCarrierTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<getrandom::Error> for UdpCarrierTransportError {
    fn from(value: getrandom::Error) -> Self {
        Self::Random(value)
    }
}

#[derive(Debug)]
pub enum UdpCarrierFrameError {
    Closed,
    Codec(CodecError),
    Crypto,
    FrameTooLarge { actual: usize, limit: usize },
    InvalidPacket(&'static str),
    Io(std::io::Error),
    QueueClosed,
}

impl fmt::Display for UdpCarrierFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "UDP carrier stream closed"),
            Self::Codec(err) => write!(f, "failed to encode/decode UDP carrier frame: {err}"),
            Self::Crypto => write!(f, "UDP carrier packet authentication failed"),
            Self::FrameTooLarge { actual, limit } => {
                write!(f, "UDP carrier frame is {actual} bytes, limit is {limit}")
            }
            Self::InvalidPacket(message) => write!(f, "invalid UDP carrier packet: {message}"),
            Self::Io(err) => write!(f, "UDP carrier I/O failed: {err}"),
            Self::QueueClosed => write!(f, "UDP carrier queue closed"),
        }
    }
}

impl Error for UdpCarrierFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(err) => Some(err),
            Self::Io(err) => Some(err),
            Self::Closed
            | Self::Crypto
            | Self::FrameTooLarge { .. }
            | Self::InvalidPacket(_)
            | Self::QueueClosed => None,
        }
    }
}

impl From<CodecError> for UdpCarrierFrameError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<std::io::Error> for UdpCarrierFrameError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug)]
pub enum UdpCarrierConnectionError {
    Closed,
    Io(std::io::Error),
}

impl fmt::Display for UdpCarrierConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "UDP carrier connection closed"),
            Self::Io(err) => write!(f, "UDP carrier connection I/O failed: {err}"),
        }
    }
}

impl Error for UdpCarrierConnectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Closed => None,
        }
    }
}

impl From<std::io::Error> for UdpCarrierConnectionError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
