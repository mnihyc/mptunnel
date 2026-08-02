use super::*;

#[test]
fn fragment_with_length() {
    let mut buf = SendBuffer::new();
    const MSG: &[u8] = b"Hello, world!";
    buf.write(MSG.into());
    // 0 byte offset => 19 bytes left => 13 byte data isn't enough
    // with 8 bytes reserved for length 11 payload bytes will fit
    assert_eq!(buf.poll_transmit(19), (0..11, true));
    assert_eq!(
        buf.poll_transmit(MSG.len() + 16 - 11),
        (11..MSG.len() as u64, true)
    );
    assert_eq!(
        buf.poll_transmit(58),
        (MSG.len() as u64..MSG.len() as u64, true)
    );
}

#[test]
fn fragment_without_length() {
    let mut buf = SendBuffer::new();
    const MSG: &[u8] = b"Hello, world with some extra data!";
    buf.write(MSG.into());
    // 0 byte offset => 19 bytes left => can be filled by 34 bytes payload
    assert_eq!(buf.poll_transmit(19), (0..19, false));
    assert_eq!(
        buf.poll_transmit(MSG.len() - 19 + 1),
        (19..MSG.len() as u64, false)
    );
    assert_eq!(
        buf.poll_transmit(58),
        (MSG.len() as u64..MSG.len() as u64, true)
    );
}

#[test]
fn reserves_encoded_offset() {
    let mut buf = SendBuffer::new();

    // Pretend we have more than 1 GB of data in the buffer
    let chunk: Bytes = Bytes::from_static(&[0; 1024 * 1024]);
    for _ in 0..1025 {
        buf.write(chunk.clone());
    }

    const SIZE1: u64 = 64;
    const SIZE2: u64 = 16 * 1024;
    const SIZE3: u64 = 1024 * 1024 * 1024;

    // Offset 0 requires no space
    assert_eq!(buf.poll_transmit(16), (0..16, false));
    buf.retransmit(0..16);
    assert_eq!(buf.poll_transmit(16), (0..16, false));
    let mut transmitted = 16u64;

    // Offset 16 requires 1 byte
    assert_eq!(
        buf.poll_transmit((SIZE1 - transmitted + 1) as usize),
        (transmitted..SIZE1, false)
    );
    buf.retransmit(transmitted..SIZE1);
    assert_eq!(
        buf.poll_transmit((SIZE1 - transmitted + 1) as usize),
        (transmitted..SIZE1, false)
    );
    transmitted = SIZE1;

    // Offset 64 requires 2 bytes
    assert_eq!(
        buf.poll_transmit((SIZE2 - transmitted + 2) as usize),
        (transmitted..SIZE2, false)
    );
    buf.retransmit(transmitted..SIZE2);
    assert_eq!(
        buf.poll_transmit((SIZE2 - transmitted + 2) as usize),
        (transmitted..SIZE2, false)
    );
    transmitted = SIZE2;

    // Offset 16384 requires requires 4 bytes
    assert_eq!(
        buf.poll_transmit((SIZE3 - transmitted + 4) as usize),
        (transmitted..SIZE3, false)
    );
    buf.retransmit(transmitted..SIZE3);
    assert_eq!(
        buf.poll_transmit((SIZE3 - transmitted + 4) as usize),
        (transmitted..SIZE3, false)
    );
    transmitted = SIZE3;

    // Offset 1GB requires 8 bytes
    assert_eq!(
        buf.poll_transmit(chunk.len() + 8),
        (transmitted..transmitted + chunk.len() as u64, false)
    );
    buf.retransmit(transmitted..transmitted + chunk.len() as u64);
    assert_eq!(
        buf.poll_transmit(chunk.len() + 8),
        (transmitted..transmitted + chunk.len() as u64, false)
    );
}

#[test]
fn multiple_segments() {
    let mut buf = SendBuffer::new();
    const MSG: &[u8] = b"Hello, world!";
    const MSG_LEN: u64 = MSG.len() as u64;

    const SEG1: &[u8] = b"He";
    buf.write(SEG1.into());
    const SEG2: &[u8] = b"llo,";
    buf.write(SEG2.into());
    const SEG3: &[u8] = b" w";
    buf.write(SEG3.into());
    const SEG4: &[u8] = b"o";
    buf.write(SEG4.into());
    const SEG5: &[u8] = b"rld!";
    buf.write(SEG5.into());

    assert_eq!(aggregate_unacked(&buf), MSG);

    assert_eq!(buf.poll_transmit(16), (0..8, true));
    assert_eq!(buf.get(0..5), SEG1);
    assert_eq!(buf.get(2..8), SEG2);
    assert_eq!(buf.get(6..8), SEG3);

    assert_eq!(buf.poll_transmit(16), (8..MSG_LEN, true));
    assert_eq!(buf.get(8..MSG_LEN), SEG4);
    assert_eq!(buf.get(9..MSG_LEN), SEG5);

    assert_eq!(buf.poll_transmit(42), (MSG_LEN..MSG_LEN, true));

    // Now drain the segments
    buf.ack(0..1);
    assert_eq!(aggregate_unacked(&buf), &MSG[1..]);
    buf.ack(0..3);
    assert_eq!(aggregate_unacked(&buf), &MSG[3..]);
    buf.ack(3..5);
    assert_eq!(aggregate_unacked(&buf), &MSG[5..]);
    buf.ack(7..9);
    assert_eq!(aggregate_unacked(&buf), &MSG[5..]);
    buf.ack(4..7);
    assert_eq!(aggregate_unacked(&buf), &MSG[9..]);
    buf.ack(0..MSG_LEN);
    assert_eq!(aggregate_unacked(&buf), &[] as &[u8]);
}

#[test]
fn retransmit() {
    let mut buf = SendBuffer::new();
    const MSG: &[u8] = b"Hello, world with extra data!";
    buf.write(MSG.into());
    // Transmit two frames
    assert_eq!(buf.poll_transmit(16), (0..16, false));
    assert_eq!(buf.poll_transmit(16), (16..23, true));
    // Lose the first, but not the second
    buf.retransmit(0..16);
    // Ensure we only retransmit the lost frame, then continue sending fresh data
    assert_eq!(buf.poll_transmit(16), (0..16, false));
    assert_eq!(buf.poll_transmit(16), (23..MSG.len() as u64, true));
    // Lose the second frame
    buf.retransmit(16..23);
    assert_eq!(buf.poll_transmit(16), (16..23, true));
}

#[test]
fn ack() {
    let mut buf = SendBuffer::new();
    const MSG: &[u8] = b"Hello, world!";
    buf.write(MSG.into());
    assert_eq!(buf.poll_transmit(16), (0..8, true));
    buf.ack(0..8);
    assert_eq!(aggregate_unacked(&buf), &MSG[8..]);
}

#[test]
fn reordered_ack() {
    let mut buf = SendBuffer::new();
    const MSG: &[u8] = b"Hello, world with extra data!";
    buf.write(MSG.into());
    assert_eq!(buf.poll_transmit(16), (0..16, false));
    assert_eq!(buf.poll_transmit(16), (16..23, true));
    buf.ack(16..23);
    assert_eq!(aggregate_unacked(&buf), MSG);
    buf.ack(0..16);
    assert_eq!(aggregate_unacked(&buf), &MSG[23..]);
    assert!(buf.acks.is_empty());
}

fn aggregate_unacked(buf: &SendBuffer) -> Vec<u8> {
    let mut result = Vec::new();
    for segment in buf.unacked_segments.iter() {
        result.extend_from_slice(&segment[..]);
    }
    result
}
