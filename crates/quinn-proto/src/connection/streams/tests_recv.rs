use bytes::Bytes;

use crate::{Dir, Side};

use super::*;

#[test]
fn reordered_frames_while_stopped() {
    const INITIAL_BYTES: u64 = 3;
    const INITIAL_OFFSET: u64 = 3;
    const RECV_WINDOW: u64 = 8;
    let mut s = Recv::new(RECV_WINDOW);
    let mut data_recvd = 0;
    // Receive bytes 3..6
    let (new_bytes, is_closed) = s
        .ingest(
            frame::Stream {
                id: StreamId::new(Side::Client, Dir::Uni, 0),
                offset: INITIAL_OFFSET,
                fin: false,
                data: Bytes::from_static(&[0; INITIAL_BYTES as usize]),
            },
            123,
            data_recvd,
            data_recvd + 1024,
        )
        .unwrap();
    data_recvd += new_bytes;
    assert_eq!(new_bytes, INITIAL_OFFSET + INITIAL_BYTES);
    assert!(!is_closed);

    let (credits, transmit) = s.stop().unwrap();
    assert!(transmit.should_transmit());
    assert_eq!(
        credits,
        INITIAL_OFFSET + INITIAL_BYTES,
        "full connection flow control credit is issued by stop"
    );

    let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
    assert!(!transmit.should_transmit());
    assert_eq!(
        max_stream_data, RECV_WINDOW,
        "stream flow control credit isn't issued by stop"
    );

    // Receive byte 7
    let (new_bytes, is_closed) = s
        .ingest(
            frame::Stream {
                id: StreamId::new(Side::Client, Dir::Uni, 0),
                offset: RECV_WINDOW - 1,
                fin: false,
                data: Bytes::from_static(&[0; 1]),
            },
            123,
            data_recvd,
            data_recvd + 1024,
        )
        .unwrap();
    data_recvd += new_bytes;
    assert_eq!(new_bytes, RECV_WINDOW - (INITIAL_OFFSET + INITIAL_BYTES));
    assert!(!is_closed);

    let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
    assert!(!transmit.should_transmit());
    assert_eq!(
        max_stream_data, RECV_WINDOW,
        "stream flow control credit isn't issued after stop"
    );

    // Receive bytes 0..3
    let (new_bytes, is_closed) = s
        .ingest(
            frame::Stream {
                id: StreamId::new(Side::Client, Dir::Uni, 0),
                offset: 0,
                fin: false,
                data: Bytes::from_static(&[0; INITIAL_OFFSET as usize]),
            },
            123,
            data_recvd,
            data_recvd + 1024,
        )
        .unwrap();
    assert_eq!(
        new_bytes, 0,
        "reordered frames don't issue connection-level flow control for stopped streams"
    );
    assert!(!is_closed);

    let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
    assert!(!transmit.should_transmit());
    assert_eq!(
        max_stream_data, RECV_WINDOW,
        "stream flow control credit isn't issued after stop"
    );
}
