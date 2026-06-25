use crate::protocol::Frame;
use crate::protocol::codec::{
    CodecError, CodecLimits, FRAME_HEADER_LEN, decode_frame, decode_payload_len_from_header,
    encode_frame,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug)]
pub struct FramedStream<S> {
    stream: S,
    limits: CodecLimits,
}

impl<S> FramedStream<S> {
    pub fn new(stream: S, limits: CodecLimits) -> Self {
        Self { stream, limits }
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
        let mut encoded = Vec::with_capacity(FRAME_HEADER_LEN + payload_len);
        encoded.extend_from_slice(&header);
        encoded.resize(FRAME_HEADER_LEN + payload_len, 0);
        self.stream
            .read_exact(&mut encoded[FRAME_HEADER_LEN..])
            .await?;
        Ok(decode_frame(&encoded, self.limits)?)
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), FramedTransportError> {
        let encoded = encode_frame(frame, self.limits)?;
        self.stream.write_all(&encoded).await?;
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
mod tests {
    use super::*;
    use crate::protocol::{DatagramFlowId, DatagramId, Frame, SessionId};
    use bytes::Bytes;
    use tokio::io::duplex;

    #[tokio::test]
    async fn framed_stream_round_trips_frames_over_async_io() {
        let (client, server) = duplex(1024);
        let limits = CodecLimits::default();
        let mut client = FramedStream::new(client, limits);
        let mut server = FramedStream::new(server, limits);

        let sent = Frame::SessionHello {
            session_id: SessionId(42),
        };
        client.write_frame(&sent).await.expect("write");
        client.flush().await.expect("flush");

        let received = server.read_frame().await.expect("read");
        assert_eq!(received, sent);
    }

    #[tokio::test]
    async fn framed_stream_preserves_compact_datagram_frame() {
        let (client, server) = duplex(1024);
        let limits = CodecLimits::default();
        let mut client = FramedStream::new(client, limits);
        let mut server = FramedStream::new(server, limits);
        let sent = Frame::DatagramData {
            flow_id: DatagramFlowId(3),
            datagram_id: DatagramId(9),
            ttl_ms: 250,
            payload: Bytes::from_static(b"dns"),
        };

        client.write_frame(&sent).await.expect("write");
        let received = server.read_frame().await.expect("read");

        assert_eq!(received, sent);
    }

    #[tokio::test]
    async fn framed_stream_rejects_oversize_inbound_frame_before_allocating_payload() {
        let (mut writer, reader) = duplex(1024);
        let limits = CodecLimits {
            max_frame_bytes: 16,
            ..CodecLimits::default()
        };
        let mut reader = FramedStream::new(reader, limits);
        writer
            .write_all(&[b'M', b'P', b'T', b'F', 1, 16, 0, 0, 1, 0])
            .await
            .expect("write header");

        let err = reader.read_frame().await.expect_err("oversize");
        assert!(matches!(
            err,
            FramedTransportError::Codec(CodecError::FrameTooLarge { .. })
        ));
    }
}
