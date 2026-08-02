use crate::protocol::Frame;
use crate::protocol::codec::{
    CodecError, CodecLimits, FRAME_HEADER_LEN, decode_frame_bytes, decode_payload_len_from_header,
    encode_frame_into,
};
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug)]
pub struct FramedStream<S> {
    stream: S,
    limits: CodecLimits,
    encode_buffer: Vec<u8>,
}

impl<S> FramedStream<S> {
    pub fn new(stream: S, limits: CodecLimits) -> Self {
        Self {
            stream,
            limits,
            encode_buffer: Vec::new(),
        }
    }

    pub fn limits(&self) -> CodecLimits {
        self.limits
    }

    pub fn into_inner(self) -> S {
        self.stream
    }
}

impl<S> FramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn read_frame(&mut self) -> Result<Frame, FramedTransportError> {
        let mut header = [0u8; FRAME_HEADER_LEN];
        self.stream.read_exact(&mut header).await?;
        let payload_len = decode_payload_len_from_header(&header, self.limits)?;
        let mut encoded = BytesMut::with_capacity(FRAME_HEADER_LEN + payload_len);
        encoded.extend_from_slice(&header);
        encoded.resize(FRAME_HEADER_LEN + payload_len, 0);
        self.stream
            .read_exact(&mut encoded[FRAME_HEADER_LEN..])
            .await?;
        Ok(decode_frame_bytes(encoded.freeze(), self.limits)?)
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), FramedTransportError> {
        self.encode_buffer.clear();
        encode_frame_into(frame, self.limits, &mut self.encode_buffer)?;
        self.stream.write_all(&self.encode_buffer).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), FramedTransportError> {
        self.stream.flush().await?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum FramedTransportError {
    Io(std::io::Error),
    Codec(CodecError),
}

impl From<std::io::Error> for FramedTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<CodecError> for FramedTransportError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl std::fmt::Display for FramedTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Codec(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for FramedTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Codec(err) => Some(err),
        }
    }
}

#[cfg(test)]
#[path = "tests_framed.rs"]
mod tests;
