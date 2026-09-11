//! Actual STREAM frame producer checks; no ACK or invented progress publication.

use super::*;
use crate::{connection::State as ConnState, connection::Streams, SendStream, SendStreamProgress};

fn fixture(window: u32) -> (StreamsState, StreamId, StreamId) {
    let mut state = StreamsState::new(
        Side::Client,
        2u32.into(),
        2u32.into(),
        u64::from(window),
        window.into(),
        window.into(),
    );
    state.set_params(&TransportParameters {
        initial_max_streams_bidi: 2u32.into(),
        initial_max_data: window.into(),
        initial_max_stream_data_bidi_remote: window.into(),
        ..TransportParameters::default()
    });
    let mut streams = Streams {
        state: &mut state,
        conn_state: &ConnState::Established,
    };
    let first = streams.open(Dir::Bi).unwrap();
    let second = streams.open(Dir::Bi).unwrap();
    (state, first, second)
}

fn with_sender<T>(
    state: &mut StreamsState,
    id: StreamId,
    f: impl FnOnce(&mut SendStream<'_>) -> T,
) -> T {
    f(&mut SendStream {
        id,
        state,
        pending: &mut Retransmits::default(),
        conn_state: &ConnState::Established,
    })
}

fn snapshot(state: &mut StreamsState, id: StreamId) -> SendStreamProgress {
    with_sender(state, id, |sender| sender.packetization_progress().unwrap())
}

#[test]
fn send_progress_actual_packetizer_crosses_without_ack_and_ignores_retransmission() {
    // Four packet-sized pieces make below-end progress observable. This is a
    // fixture geometry, not a production limit or congestion configuration.
    let (mut state, id, _) = fixture(4096);
    with_sender(&mut state, id, |sender| {
        assert_eq!(sender.write(&[7; 4096]), Ok(4096));
        sender.request_packetization_notification(4096).unwrap();
    });
    assert_eq!(
        snapshot(&mut state, id),
        SendStreamProgress {
            accepted_end: 4096,
            first_unpacketized: 0
        }
    );
    assert!(state.poll().is_none());

    let mut packet = Vec::new();
    let frames = state.write_stream_frames(&mut packet, 1200, true);
    let first = frames.into_iter().next().unwrap();
    let below = snapshot(&mut state, id);
    assert!(0 < below.first_unpacketized && below.first_unpacketized < 4096);
    assert!(state.poll().is_none());

    state.retransmit(first.clone());
    packet.clear();
    // Exactly the first frame's original space: only its retransmission fits.
    let replay = state.write_stream_frames(&mut packet, 1200, true);
    assert_eq!(replay[0].offsets, first.offsets);
    assert_eq!(snapshot(&mut state, id), below);
    assert!(state.poll().is_none());

    while snapshot(&mut state, id).first_unpacketized < 4096 {
        packet.clear();
        assert!(!state
            .write_stream_frames(&mut packet, 1200, true)
            .is_empty());
    }
    assert_eq!(state.poll(), Some(StreamEvent::PacketizationChanged { id }));
    assert!(state.poll().is_none());
    // Native ACK accounting did not move; every byte remains unacknowledged.
    assert_eq!(state.unacked_data, 4096);
    assert_eq!(state.send[&id].as_ref().unwrap().pending.unacked(), 4096);
    with_sender(&mut state, id, |sender| {
        sender.request_packetization_notification(4096).unwrap();
    });
    assert!(state.send[&id]
        .as_ref()
        .unwrap()
        .packetization_target
        .is_none());
}

#[test]
fn send_progress_partial_acceptance_exact_targets_and_independent_streams() {
    let (mut state, id, other) = fixture(1024);
    with_sender(&mut state, other, |sender| {
        assert_eq!(sender.write(&[2; 32]), Ok(32));
        sender.set_priority(1).unwrap();
    });
    with_sender(&mut state, id, |sender| {
        assert_eq!(sender.write(&[1; 2048]), Ok(992));
        assert_eq!(sender.packetization_progress().unwrap().accepted_end, 992);
        // An unaccepted end must not create an unreachable observation arm.
        sender.request_packetization_notification(993).unwrap();
    });
    assert!(state.send[&id]
        .as_ref()
        .unwrap()
        .packetization_target
        .is_none());
    with_sender(&mut state, id, |sender| {
        sender.request_packetization_notification(992).unwrap();
    });
    let mut packet = Vec::new();
    let frames = state.write_stream_frames(&mut packet, 40, true);
    assert_eq!(frames[0].id, other);
    assert_eq!(snapshot(&mut state, id).first_unpacketized, 0);
    assert!(state.poll().is_none());

    // The smaller goal consumes one arm. A later observer rechecks/rearms.
    with_sender(&mut state, id, |sender| {
        sender.request_packetization_notification(1).unwrap();
    });
    packet.clear();
    state.write_stream_frames(&mut packet, 64, true);
    assert_eq!(state.poll(), Some(StreamEvent::PacketizationChanged { id }));
    assert!(snapshot(&mut state, id).first_unpacketized < 992);
    with_sender(&mut state, id, |sender| {
        sender.request_packetization_notification(992).unwrap();
    });
    packet.clear();
    state.write_stream_frames(&mut packet, 1200, true);
    assert_eq!(state.poll(), Some(StreamEvent::PacketizationChanged { id }));
}

#[test]
fn send_progress_reset_and_observer_removal_do_not_change_native_service() {
    let (mut state, id, other) = fixture(2048);
    for stream in [id, other] {
        with_sender(&mut state, stream, |sender| {
            assert_eq!(sender.write(&[3; 128]), Ok(128));
            sender.request_packetization_notification(128).unwrap();
        });
    }
    with_sender(&mut state, other, |sender| {
        sender.cancel_packetization_notification()
    });
    with_sender(&mut state, id, |sender| sender.reset(0u32.into()).unwrap());
    assert_eq!(state.poll(), Some(StreamEvent::PacketizationChanged { id }));
    with_sender(&mut state, id, |sender| {
        assert!(sender.packetization_progress().is_err())
    });
    let mut packet = Vec::new();
    let frames = state.write_stream_frames(&mut packet, 1200, true);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].id, other);
    assert_eq!(snapshot(&mut state, other).first_unpacketized, 128);
    assert!(state.poll().is_none());
}
