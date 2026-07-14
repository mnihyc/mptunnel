
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
