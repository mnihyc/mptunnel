//! Native ordering capability proofs, not an MPP repair-channel implementation.

use super::*;
use crate::{connection::State as ConnState, connection::Streams, SendStream, WriteError};

const WINDOW: u32 = 1024 * 1024;
const BULK: usize = 512 * 1024;

fn fixture() -> (StreamsState, StreamId, StreamId) {
    let mut state = StreamsState::new(
        Side::Server,
        2u32.into(),
        2u32.into(),
        u64::from(WINDOW),
        WINDOW.into(),
        WINDOW.into(),
    );
    state.set_params(&TransportParameters {
        initial_max_streams_bidi: 2u32.into(),
        initial_max_data: (WINDOW * 4).into(),
        initial_max_stream_data_bidi_remote: (WINDOW * 4).into(),
        ..TransportParameters::default()
    });
    let mut streams = Streams {
        state: &mut state,
        conn_state: &ConnState::Established,
    };
    let bulk = streams.open(Dir::Bi).unwrap();
    let repair = streams.open(Dir::Bi).unwrap();
    (state, bulk, repair)
}

fn write(
    state: &mut StreamsState,
    id: StreamId,
    priority: i32,
    data: &[u8],
) -> Result<usize, WriteError> {
    let mut pending = Retransmits::default();
    let mut stream = SendStream {
        id,
        state,
        pending: &mut pending,
        conn_state: &ConnState::Established,
    };
    stream.set_priority(priority).unwrap();
    stream.write(data)
}

#[test]
fn repair_ordering_same_stream_priority_cannot_overtake_unsent_bulk() {
    let (mut state, bulk, _) = fixture();
    assert_eq!(write(&mut state, bulk, 0, &vec![1; BULK]), Ok(BULK));
    assert_eq!(write(&mut state, bulk, 1, b"repair"), Ok(6));
    let mut packet = Vec::new();
    let frames = state.write_stream_frames(&mut packet, 1200, true);
    assert_eq!(frames[0].id, bulk);
    assert_eq!(frames[0].offsets.start, 0);
    assert!(frames[0].offsets.end < BULK as u64);
    assert_eq!(
        state.send[&bulk].as_ref().unwrap().pending.offset(),
        BULK as u64 + 6
    );
}

#[test]
fn repair_ordering_independent_priority_serves_before_unsent_bulk() {
    let (mut state, bulk, repair) = fixture();
    assert_eq!(write(&mut state, bulk, 0, &vec![1; BULK]), Ok(BULK));
    assert_eq!(write(&mut state, repair, 1, b"repair"), Ok(6));
    let mut packet = Vec::new();
    let frames = state.write_stream_frames(&mut packet, 1200, true);
    assert_eq!(frames[0].id, repair);
    assert_eq!(frames[0].offsets, 0..6);
    assert!(frames.iter().skip(1).any(|frame| frame.id == bulk));
    assert!(state.send[&bulk].as_ref().unwrap().is_pending());
}

#[test]
fn repair_ordering_independent_stream_does_not_mint_send_credit() {
    let (mut state, bulk, repair) = fixture();
    assert_eq!(
        write(&mut state, bulk, 0, &vec![1; WINDOW as usize]),
        Ok(WINDOW as usize)
    );
    assert_eq!(
        write(&mut state, repair, 1, b"repair"),
        Err(WriteError::Blocked)
    );
    let mut packet = Vec::new();
    let frames = state.write_stream_frames(&mut packet, 1200, true);
    let first = frames.into_iter().next().unwrap();
    assert_eq!(first.id, bulk);
    state.received_ack_of(first);
    assert_eq!(write(&mut state, repair, 1, b"repair"), Ok(6));
    packet.clear();
    assert_eq!(
        state.write_stream_frames(&mut packet, 1200, true)[0].id,
        repair
    );
}
