use super::*;
use crate::{
    ReadableError, RecvStream, SendStream, TransportErrorCode, WriteError,
    connection::State as ConnState, connection::Streams,
};
use bytes::Bytes;

fn make(side: Side) -> StreamsState {
    StreamsState::new(
        side,
        128u32.into(),
        128u32.into(),
        1024 * 1024,
        (1024 * 1024u32).into(),
        (1024 * 1024u32).into(),
    )
}

#[test]
fn trivial_flow_control() {
    let mut client = StreamsState::new(
        Side::Client,
        1u32.into(),
        1u32.into(),
        1024 * 1024,
        (1024 * 1024u32).into(),
        (1024 * 1024u32).into(),
    );
    let id = StreamId::new(Side::Server, Dir::Uni, 0);
    let initial_max = client.local_max_data;
    const MESSAGE_SIZE: usize = 2048;
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[0; MESSAGE_SIZE]),
                },
                2048
            )
            .unwrap(),
        ShouldTransmit(false)
    );
    assert_eq!(client.data_recvd, 2048);
    assert_eq!(client.local_max_data - initial_max, 0);

    let mut pending = Retransmits::default();
    let mut recv = RecvStream {
        id,
        state: &mut client,
        pending: &mut pending,
    };

    let mut chunks = recv.read(true).unwrap();
    assert_eq!(
        chunks.next(MESSAGE_SIZE).unwrap().unwrap().bytes.len(),
        MESSAGE_SIZE
    );
    assert!(chunks.next(0).unwrap().is_none());
    let should_transmit = chunks.finalize();
    assert!(should_transmit.0);
    assert!(pending.max_stream_id[Dir::Uni as usize]);
    assert_eq!(client.local_max_data - initial_max, MESSAGE_SIZE as u64);
}

#[test]
fn reset_flow_control() {
    let mut client = make(Side::Client);
    let id = StreamId::new(Side::Server, Dir::Uni, 0);
    let initial_max = client.local_max_data;
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: false,
                    data: Bytes::from_static(&[0; 2048]),
                },
                2048
            )
            .unwrap(),
        ShouldTransmit(false)
    );
    assert_eq!(client.data_recvd, 2048);
    assert_eq!(client.local_max_data - initial_max, 0);

    let mut pending = Retransmits::default();
    let mut recv = RecvStream {
        id,
        state: &mut client,
        pending: &mut pending,
    };

    let mut chunks = recv.read(true).unwrap();
    chunks.next(1024).unwrap();
    let _ = chunks.finalize();
    assert_eq!(client.local_max_data - initial_max, 1024);
    assert_eq!(
        client
            .received_reset(frame::ResetStream {
                id,
                error_code: 0u32.into(),
                final_offset: 4096u32.into(),
            })
            .unwrap(),
        ShouldTransmit(false)
    );

    assert_eq!(client.data_recvd, 4096);
    assert_eq!(client.local_max_data - initial_max, 4096);

    // Ensure reading after a reset doesn't issue redundant credit
    let mut recv = RecvStream {
        id,
        state: &mut client,
        pending: &mut pending,
    };
    let mut chunks = recv.read(true).unwrap();
    assert_eq!(
        chunks.next(1024).unwrap_err(),
        crate::ReadError::Reset(0u32.into())
    );
    let _ = chunks.finalize();
    assert_eq!(client.data_recvd, 4096);
    assert_eq!(client.local_max_data - initial_max, 4096);
}

#[test]
fn reset_after_empty_frame_flow_control() {
    let mut client = make(Side::Client);
    let id = StreamId::new(Side::Server, Dir::Uni, 0);
    let initial_max = client.local_max_data;
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id,
                    offset: 4096,
                    fin: false,
                    data: Bytes::from_static(&[0; 0]),
                },
                0
            )
            .unwrap(),
        ShouldTransmit(false)
    );
    assert_eq!(client.data_recvd, 4096);
    assert_eq!(client.local_max_data - initial_max, 0);
    assert_eq!(
        client
            .received_reset(frame::ResetStream {
                id,
                error_code: 0u32.into(),
                final_offset: 4096u32.into(),
            })
            .unwrap(),
        ShouldTransmit(false)
    );
    assert_eq!(client.data_recvd, 4096);
    assert_eq!(client.local_max_data - initial_max, 4096);
}

#[test]
fn duplicate_reset_flow_control() {
    let mut client = make(Side::Client);
    let id = StreamId::new(Side::Server, Dir::Uni, 0);
    assert_eq!(
        client
            .received_reset(frame::ResetStream {
                id,
                error_code: 0u32.into(),
                final_offset: 4096u32.into(),
            })
            .unwrap(),
        ShouldTransmit(false)
    );
    assert_eq!(client.data_recvd, 4096);
    assert_eq!(
        client
            .received_reset(frame::ResetStream {
                id,
                error_code: 0u32.into(),
                final_offset: 4096u32.into(),
            })
            .unwrap(),
        ShouldTransmit(false)
    );
    assert_eq!(client.data_recvd, 4096);
}

#[test]
fn recv_stopped() {
    let mut client = make(Side::Client);
    let id = StreamId::new(Side::Server, Dir::Uni, 0);
    let initial_max = client.local_max_data;
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: false,
                    data: Bytes::from_static(&[0; 32]),
                },
                32
            )
            .unwrap(),
        ShouldTransmit(false)
    );
    assert_eq!(client.local_max_data, initial_max);

    let mut pending = Retransmits::default();
    let mut recv = RecvStream {
        id,
        state: &mut client,
        pending: &mut pending,
    };

    recv.stop(0u32.into()).unwrap();
    assert_eq!(recv.pending.stop_sending.len(), 1);
    assert!(!recv.pending.max_data);

    assert!(recv.stop(0u32.into()).is_err());
    assert_eq!(recv.read(true).err(), Some(ReadableError::ClosedStream));
    assert_eq!(recv.read(false).err(), Some(ReadableError::ClosedStream));

    assert_eq!(client.local_max_data - initial_max, 32);
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id,
                    offset: 32,
                    fin: true,
                    data: Bytes::from_static(&[0; 16]),
                },
                16
            )
            .unwrap(),
        ShouldTransmit(false)
    );
    assert_eq!(client.local_max_data - initial_max, 48);
    assert!(!client.recv.contains_key(&id));
}

#[test]
fn stopped_reset() {
    let mut client = make(Side::Client);
    let id = StreamId::new(Side::Server, Dir::Uni, 0);
    // Server opens stream
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: false,
                    data: Bytes::from_static(&[0; 32])
                },
                32
            )
            .unwrap(),
        ShouldTransmit(false)
    );

    let mut pending = Retransmits::default();
    let mut recv = RecvStream {
        id,
        state: &mut client,
        pending: &mut pending,
    };

    recv.stop(0u32.into()).unwrap();
    assert_eq!(pending.stop_sending.len(), 1);
    assert!(!pending.max_data);

    // Server complies
    let prev_max = client.max_remote[Dir::Uni as usize];
    assert_eq!(
        client
            .received_reset(frame::ResetStream {
                id,
                error_code: 0u32.into(),
                final_offset: 32u32.into(),
            })
            .unwrap(),
        ShouldTransmit(false)
    );
    assert!(!client.recv.contains_key(&id), "stream state is freed");
    assert_eq!(client.max_remote[Dir::Uni as usize], prev_max + 1);
}

#[test]
fn send_stopped() {
    let mut server = make(Side::Server);
    server.set_params(&TransportParameters {
        initial_max_streams_uni: 1u32.into(),
        initial_max_data: 42u32.into(),
        initial_max_stream_data_uni: 42u32.into(),
        ..TransportParameters::default()
    });

    let (mut pending, state) = (Retransmits::default(), ConnState::Established);
    let id = Streams {
        state: &mut server,
        conn_state: &state,
    }
    .open(Dir::Uni)
    .unwrap();

    let mut stream = SendStream {
        id,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };

    let error_code = 0u32.into();
    stream.state.received_stop_sending(id, error_code);
    assert!(
        stream
            .state
            .events
            .contains(&StreamEvent::Stopped { id, error_code })
    );
    stream.state.events.clear();

    assert_eq!(stream.write(&[]), Err(WriteError::Stopped(error_code)));

    stream.reset(0u32.into()).unwrap();
    assert_eq!(stream.write(&[]), Err(WriteError::ClosedStream));

    // A duplicate frame is a no-op
    stream.state.received_stop_sending(id, error_code);
    assert!(stream.state.events.is_empty());
}

#[test]
fn final_offset_flow_control() {
    let mut client = make(Side::Client);
    assert_eq!(
        client
            .received_reset(frame::ResetStream {
                id: StreamId::new(Side::Server, Dir::Uni, 0),
                error_code: 0u32.into(),
                final_offset: VarInt::MAX,
            })
            .unwrap_err()
            .code,
        TransportErrorCode::FLOW_CONTROL_ERROR
    );
}

#[test]
fn stream_priority() {
    let mut server = make(Side::Server);
    server.set_params(&TransportParameters {
        initial_max_streams_bidi: 3u32.into(),
        initial_max_data: 10u32.into(),
        initial_max_stream_data_bidi_remote: 10u32.into(),
        ..TransportParameters::default()
    });

    let (mut pending, state) = (Retransmits::default(), ConnState::Established);
    let mut streams = Streams {
        state: &mut server,
        conn_state: &state,
    };

    let id_high = streams.open(Dir::Bi).unwrap();
    let id_mid = streams.open(Dir::Bi).unwrap();
    let id_low = streams.open(Dir::Bi).unwrap();

    let mut mid = SendStream {
        id: id_mid,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    mid.write(b"mid").unwrap();

    let mut low = SendStream {
        id: id_low,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    low.set_priority(-1).unwrap();
    low.write(b"low").unwrap();

    let mut high = SendStream {
        id: id_high,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    high.set_priority(1).unwrap();
    high.write(b"high").unwrap();

    let mut buf = Vec::with_capacity(40);
    let meta = server.write_stream_frames(&mut buf, 40, true);
    assert_eq!(meta[0].id, id_high);
    assert_eq!(meta[1].id, id_mid);
    assert_eq!(meta[2].id, id_low);

    assert!(!server.can_send_stream_data());
    assert_eq!(server.pending.len(), 0);
}

#[test]
fn requeue_stream_priority() {
    let mut server = make(Side::Server);
    server.set_params(&TransportParameters {
        initial_max_streams_bidi: 3u32.into(),
        initial_max_data: 1000u32.into(),
        initial_max_stream_data_bidi_remote: 1000u32.into(),
        ..TransportParameters::default()
    });

    let (mut pending, state) = (Retransmits::default(), ConnState::Established);
    let mut streams = Streams {
        state: &mut server,
        conn_state: &state,
    };

    let id_high = streams.open(Dir::Bi).unwrap();
    let id_mid = streams.open(Dir::Bi).unwrap();

    let mut mid = SendStream {
        id: id_mid,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    assert_eq!(mid.write(b"mid").unwrap(), 3);
    assert_eq!(server.pending.len(), 1);

    let mut high = SendStream {
        id: id_high,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    high.set_priority(1).unwrap();
    assert_eq!(high.write(&[0; 200]).unwrap(), 200);
    assert_eq!(server.pending.len(), 2);

    // Requeue the high priority stream to lowest priority. The initial send
    // still uses high priority since it's queued that way. After that it will
    // switch to low priority
    let mut high = SendStream {
        id: id_high,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    high.set_priority(-1).unwrap();

    let mut buf = Vec::with_capacity(1000);
    let meta = server.write_stream_frames(&mut buf, 40, true);
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0].id, id_high);

    // After requeuing we should end up with 2 priorities - not 3
    assert_eq!(server.pending.len(), 2);

    // Send the remaining data. The initial mid priority one should go first now
    let meta = server.write_stream_frames(&mut buf, 1000, true);
    assert_eq!(meta.len(), 2);
    assert_eq!(meta[0].id, id_mid);
    assert_eq!(meta[1].id, id_high);

    assert!(!server.can_send_stream_data());
    assert_eq!(server.pending.len(), 0);
}

#[test]
fn same_stream_priority() {
    for fair in [true, false] {
        let mut server = make(Side::Server);
        server.set_params(&TransportParameters {
            initial_max_streams_bidi: 3u32.into(),
            initial_max_data: 300u32.into(),
            initial_max_stream_data_bidi_remote: 300u32.into(),
            ..TransportParameters::default()
        });

        let (mut pending, state) = (Retransmits::default(), ConnState::Established);
        let mut streams = Streams {
            state: &mut server,
            conn_state: &state,
        };

        // a, b and c all have the same priority
        let id_a = streams.open(Dir::Bi).unwrap();
        let id_b = streams.open(Dir::Bi).unwrap();
        let id_c = streams.open(Dir::Bi).unwrap();

        let mut stream_a = SendStream {
            id: id_a,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        stream_a.write(&[b'a'; 100]).unwrap();

        let mut stream_b = SendStream {
            id: id_b,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        stream_b.write(&[b'b'; 100]).unwrap();

        let mut stream_c = SendStream {
            id: id_c,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        stream_c.write(&[b'c'; 100]).unwrap();

        let mut metas = vec![];
        let mut buf = Vec::with_capacity(1024);

        // loop until all the streams are written
        loop {
            let buf_len = buf.len();
            let meta = server.write_stream_frames(&mut buf, buf_len + 40, fair);
            if meta.is_empty() {
                break;
            }
            metas.extend(meta);
        }

        assert!(!server.can_send_stream_data());
        assert_eq!(server.pending.len(), 0);

        let stream_ids = metas.iter().map(|m| m.id).collect::<Vec<_>>();
        if fair {
            // When fairness is enabled, if we run out of buffer space to write out a stream,
            // the stream is re-queued after all the streams with the same priority.
            assert_eq!(
                stream_ids,
                vec![id_a, id_b, id_c, id_a, id_b, id_c, id_a, id_b, id_c]
            );
        } else {
            // When fairness is disabled the stream is re-queued before all the other streams
            // with the same priority.
            assert_eq!(
                stream_ids,
                vec![id_a, id_a, id_a, id_b, id_b, id_b, id_c, id_c, id_c]
            );
        }
    }
}

#[test]
fn unfair_priority_bump() {
    let mut server = make(Side::Server);
    server.set_params(&TransportParameters {
        initial_max_streams_bidi: 3u32.into(),
        initial_max_data: 300u32.into(),
        initial_max_stream_data_bidi_remote: 300u32.into(),
        ..TransportParameters::default()
    });

    let (mut pending, state) = (Retransmits::default(), ConnState::Established);
    let mut streams = Streams {
        state: &mut server,
        conn_state: &state,
    };

    // a, and b have the same priority, c has higher priority
    let id_a = streams.open(Dir::Bi).unwrap();
    let id_b = streams.open(Dir::Bi).unwrap();
    let id_c = streams.open(Dir::Bi).unwrap();

    let mut stream_a = SendStream {
        id: id_a,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    stream_a.write(&[b'a'; 100]).unwrap();

    let mut stream_b = SendStream {
        id: id_b,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    stream_b.write(&[b'b'; 100]).unwrap();

    let mut metas = vec![];
    let mut buf = Vec::with_capacity(1024);

    // Write the first chunk of stream_a
    let buf_len = buf.len();
    let meta = server.write_stream_frames(&mut buf, buf_len + 40, false);
    assert!(!meta.is_empty());
    metas.extend(meta);

    // Queue stream_c which has higher priority
    let mut stream_c = SendStream {
        id: id_c,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    stream_c.set_priority(1).unwrap();
    stream_c.write(&[b'b'; 100]).unwrap();

    // loop until all the streams are written
    loop {
        let buf_len = buf.len();
        let meta = server.write_stream_frames(&mut buf, buf_len + 40, false);
        if meta.is_empty() {
            break;
        }
        metas.extend(meta);
    }

    assert!(!server.can_send_stream_data());
    assert_eq!(server.pending.len(), 0);

    let stream_ids = metas.iter().map(|m| m.id).collect::<Vec<_>>();
    assert_eq!(
        stream_ids,
        // stream_c bumps stream_b but doesn't bump stream_a which had already been partly
        // written out
        vec![id_a, id_a, id_a, id_c, id_c, id_c, id_b, id_b, id_b]
    );
}

#[test]
fn stop_finished() {
    let mut client = make(Side::Client);
    let id = StreamId::new(Side::Server, Dir::Uni, 0);
    // Server finishes stream
    let _ = client
        .received(
            frame::Stream {
                id,
                offset: 0,
                fin: true,
                data: Bytes::from_static(&[0; 32]),
            },
            32,
        )
        .unwrap();
    let mut pending = Retransmits::default();
    let mut stream = RecvStream {
        id,
        state: &mut client,
        pending: &mut pending,
    };
    stream.stop(0u32.into()).unwrap();
    assert!(client.recv.get_mut(&id).is_none(), "stream is freed");
}

// Verify that a stream that's been reset doesn't cause the appearance of pending data
#[test]
fn reset_stream_cannot_send() {
    let mut server = make(Side::Server);
    server.set_params(&TransportParameters {
        initial_max_streams_uni: 1u32.into(),
        initial_max_data: 42u32.into(),
        initial_max_stream_data_uni: 42u32.into(),
        ..TransportParameters::default()
    });
    let (mut pending, state) = (Retransmits::default(), ConnState::Established);
    let mut streams = Streams {
        state: &mut server,
        conn_state: &state,
    };

    let id = streams.open(Dir::Uni).unwrap();
    let mut stream = SendStream {
        id,
        state: &mut server,
        pending: &mut pending,
        conn_state: &state,
    };
    stream.write(b"hello").unwrap();
    stream.reset(0u32.into()).unwrap();

    assert_eq!(pending.reset_stream, &[(id, 0u32.into())]);
    assert!(!server.can_send_stream_data());
}

#[test]
fn stream_limit_fixed() {
    let mut client = make(Side::Client);
    // Open streams 0-127
    assert_eq!(
        client.received(
            frame::Stream {
                id: StreamId::new(Side::Server, Dir::Uni, 127),
                offset: 0,
                fin: true,
                data: Bytes::from_static(&[]),
            },
            0
        ),
        Ok(ShouldTransmit(false))
    );
    // Try to open stream 128, exceeding limit
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 128),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            )
            .unwrap_err()
            .code,
        TransportErrorCode::STREAM_LIMIT_ERROR
    );

    // Free stream 127
    let mut pending = Retransmits::default();
    let mut stream = RecvStream {
        id: StreamId::new(Side::Server, Dir::Uni, 127),
        state: &mut client,
        pending: &mut pending,
    };
    stream.stop(0u32.into()).unwrap();

    // Open stream 128
    assert_eq!(
        client.received(
            frame::Stream {
                id: StreamId::new(Side::Server, Dir::Uni, 128),
                offset: 0,
                fin: true,
                data: Bytes::from_static(&[]),
            },
            0
        ),
        Ok(ShouldTransmit(false))
    );
}

#[test]
fn stream_limit_grows() {
    let mut client = make(Side::Client);
    // Open streams 0-127
    assert_eq!(
        client.received(
            frame::Stream {
                id: StreamId::new(Side::Server, Dir::Uni, 127),
                offset: 0,
                fin: true,
                data: Bytes::from_static(&[]),
            },
            0
        ),
        Ok(ShouldTransmit(false))
    );
    // Try to open stream 128, exceeding limit
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 128),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            )
            .unwrap_err()
            .code,
        TransportErrorCode::STREAM_LIMIT_ERROR
    );

    // Relax limit by one
    client.set_max_concurrent(Dir::Uni, 129u32.into());

    // Open stream 128
    assert_eq!(
        client.received(
            frame::Stream {
                id: StreamId::new(Side::Server, Dir::Uni, 128),
                offset: 0,
                fin: true,
                data: Bytes::from_static(&[]),
            },
            0
        ),
        Ok(ShouldTransmit(false))
    );
}

#[test]
fn stream_limit_shrinks() {
    let mut client = make(Side::Client);
    // Open streams 0-127
    assert_eq!(
        client.received(
            frame::Stream {
                id: StreamId::new(Side::Server, Dir::Uni, 127),
                offset: 0,
                fin: true,
                data: Bytes::from_static(&[]),
            },
            0
        ),
        Ok(ShouldTransmit(false))
    );

    // Tighten limit by one
    client.set_max_concurrent(Dir::Uni, 127u32.into());

    // Free stream 127
    let mut pending = Retransmits::default();
    let mut stream = RecvStream {
        id: StreamId::new(Side::Server, Dir::Uni, 127),
        state: &mut client,
        pending: &mut pending,
    };
    stream.stop(0u32.into()).unwrap();

    // Try to open stream 128, still exceeding limit
    assert_eq!(
        client
            .received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 128),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            )
            .unwrap_err()
            .code,
        TransportErrorCode::STREAM_LIMIT_ERROR
    );

    // Free stream 126
    assert_eq!(
        client.received_reset(frame::ResetStream {
            id: StreamId::new(Side::Server, Dir::Uni, 126),
            error_code: 0u32.into(),
            final_offset: 0u32.into(),
        }),
        Ok(ShouldTransmit(false))
    );
    let mut pending = Retransmits::default();
    let mut stream = RecvStream {
        id: StreamId::new(Side::Server, Dir::Uni, 126),
        state: &mut client,
        pending: &mut pending,
    };
    stream.stop(0u32.into()).unwrap();

    // Open stream 128
    assert_eq!(
        client.received(
            frame::Stream {
                id: StreamId::new(Side::Server, Dir::Uni, 128),
                offset: 0,
                fin: true,
                data: Bytes::from_static(&[]),
            },
            0
        ),
        Ok(ShouldTransmit(false))
    );
}

#[test]
fn remote_stream_capacity() {
    let mut client = make(Side::Client);
    for _ in 0..2 {
        client.set_max_concurrent(Dir::Uni, 200u32.into());
        client.set_max_concurrent(Dir::Bi, 201u32.into());
        assert_eq!(client.recv.len(), 200 + 201);
        assert_eq!(client.max_remote[Dir::Uni as usize], 200);
        assert_eq!(client.max_remote[Dir::Bi as usize], 201);
    }
}

#[test]
fn expand_receive_window() {
    let mut server = make(Side::Server);
    let new_receive_window = 2 * server.receive_window as u32;
    let expanded = server.set_receive_window(new_receive_window.into());
    assert!(expanded);
    assert_eq!(server.receive_window, new_receive_window as u64);
    assert_eq!(server.local_max_data, new_receive_window as u64);
    assert_eq!(server.receive_window_shrink_debt, 0);
    let prev_local_max_data = server.local_max_data;

    // credit, expecting all of them added to local_max_data
    let credits = 1024u64;
    let should_transmit = server.add_read_credits(credits);
    assert_eq!(server.receive_window_shrink_debt, 0);
    assert_eq!(server.local_max_data, prev_local_max_data + credits);
    assert!(should_transmit.should_transmit());
}

#[test]
fn shrink_receive_window() {
    let mut server = make(Side::Server);
    let new_receive_window = server.receive_window as u32 / 2;
    let prev_local_max_data = server.local_max_data;

    // shrink the receive_winbow, local_max_data is not expected to be changed
    let shrink_diff = server.receive_window - new_receive_window as u64;
    let expanded = server.set_receive_window(new_receive_window.into());
    assert!(!expanded);
    assert_eq!(server.receive_window, new_receive_window as u64);
    assert_eq!(server.local_max_data, prev_local_max_data);
    assert_eq!(server.receive_window_shrink_debt, shrink_diff);
    let prev_local_max_data = server.local_max_data;

    // credit twice, local_max_data does not change as it is absorbed by receive_window_shrink_debt
    let credits = 1024u64;
    for _ in 0..2 {
        let expected_receive_window_shrink_debt = server.receive_window_shrink_debt - credits;
        let should_transmit = server.add_read_credits(credits);
        assert_eq!(
            server.receive_window_shrink_debt,
            expected_receive_window_shrink_debt
        );
        assert_eq!(server.local_max_data, prev_local_max_data);
        assert!(!should_transmit.should_transmit());
    }

    // credit again which exceeds all remaining expected_receive_window_shrink_debt
    let credits = 1024 * 512;
    let prev_local_max_data = server.local_max_data;
    let expected_local_max_data =
        server.local_max_data + (credits - server.receive_window_shrink_debt);
    let _should_transmit = server.add_read_credits(credits);
    assert_eq!(server.receive_window_shrink_debt, 0);
    assert_eq!(server.local_max_data, expected_local_max_data);
    assert!(server.local_max_data > prev_local_max_data);

    // credit again, all should be added to local_max_data
    let credits = 1024 * 512;
    let expected_local_max_data = server.local_max_data + credits;
    let should_transmit = server.add_read_credits(credits);
    assert_eq!(server.receive_window_shrink_debt, 0);
    assert_eq!(server.local_max_data, expected_local_max_data);
    assert!(should_transmit.should_transmit());
}

#[test]
fn expand_send_window() {
    let mut server = make(Side::Server);

    let initial_send_window = server.send_window;
    let larger_send_window = initial_send_window * 2;

    // Set `initial_max_data` larger than `send_window` so we're limited by local flow control
    server.set_params(&TransportParameters {
        initial_max_data: VarInt::MAX,
        initial_max_stream_data_uni: VarInt::MAX,
        initial_max_streams_uni: VarInt::from_u32(100),
        ..TransportParameters::default()
    });

    assert_eq!(server.write_limit(), initial_send_window);
    assert_eq!(server.poll(), None);

    let mut retransmits = Retransmits::default();
    let conn_state = ConnState::Established;

    let stream_id = Streams {
        state: &mut server,
        conn_state: &conn_state,
    }
    .open(Dir::Uni)
    .expect("should be able to open a stream");

    let mut stream = SendStream {
        id: stream_id,
        state: &mut server,
        pending: &mut retransmits,
        conn_state: &conn_state,
    };

    // Check that the stream accepts `initial_send_window` bytes
    let initial_send_len = initial_send_window as usize;
    let data = vec![0xFFu8; initial_send_len];

    assert_eq!(stream.write(&data), Ok(initial_send_len));

    // Try to write the same data again, observe that it's blocked
    assert_eq!(stream.write(&data), Err(WriteError::Blocked));

    // Check that we get a `Writable` event after increasing the send window
    stream.state.set_send_window(larger_send_window);
    assert_eq!(
        stream.state.poll(),
        Some(StreamEvent::Writable { id: stream_id })
    );

    // Check that the stream accepts the exact same amount of data again
    assert_eq!(stream.write(&data), Ok(initial_send_len));
    assert_eq!(stream.write(&data), Err(WriteError::Blocked));

    assert_eq!(stream.state.poll(), None);

    // Ack the data
    stream.state.received_ack_of(frame::StreamMeta {
        id: stream_id,
        offsets: 0..larger_send_window,
        fin: false,
    });

    assert_eq!(
        stream.state.poll(),
        Some(StreamEvent::Writable { id: stream_id })
    );

    // Check that our full send window is available again
    assert_eq!(stream.write(&data), Ok(initial_send_len));
    assert_eq!(stream.write(&data), Ok(initial_send_len));
    assert_eq!(stream.write(&data), Err(WriteError::Blocked));
}

#[test]
fn shrink_send_window() {
    let mut server = make(Side::Server);

    let initial_send_window = server.send_window;
    let smaller_send_window = server.send_window / 2;

    // Set `initial_max_data` larger than `send_window` so we're limited by local flow control
    server.set_params(&TransportParameters {
        initial_max_data: VarInt::MAX,
        initial_max_stream_data_uni: VarInt::MAX,
        initial_max_streams_uni: VarInt::from_u32(100),
        ..TransportParameters::default()
    });

    assert_eq!(server.write_limit(), initial_send_window);
    assert_eq!(server.poll(), None);

    let mut retransmits = Retransmits::default();
    let conn_state = ConnState::Established;

    let stream_id = Streams {
        state: &mut server,
        conn_state: &conn_state,
    }
    .open(Dir::Uni)
    .expect("should be able to open a stream");

    let mut stream = SendStream {
        id: stream_id,
        state: &mut server,
        pending: &mut retransmits,
        conn_state: &conn_state,
    };

    let initial_send_len = initial_send_window as usize;

    let data = vec![0xFFu8; initial_send_len];

    // Assert that the full send window is accepted
    assert_eq!(stream.write(&data), Ok(initial_send_len));
    assert_eq!(stream.write(&data), Err(WriteError::Blocked));

    assert_eq!(stream.state.write_limit(), 0);
    assert_eq!(stream.state.poll(), None);

    // Shrink our send window, assert that it's still not writable
    stream.state.set_send_window(smaller_send_window);
    assert_eq!(stream.state.write_limit(), 0);
    assert_eq!(stream.state.poll(), None);

    // Assert that data is still not accepted
    assert_eq!(stream.write(&data), Err(WriteError::Blocked));

    // Ack some data, assert that writes are still not accepted due to outstanding sends
    stream.state.received_ack_of(frame::StreamMeta {
        id: stream_id,
        offsets: 0..smaller_send_window,
        fin: false,
    });

    assert_eq!(stream.write(&data), Err(WriteError::Blocked));

    // Ack the rest of the data
    stream.state.received_ack_of(frame::StreamMeta {
        id: stream_id,
        offsets: smaller_send_window..initial_send_window,
        fin: false,
    });

    // This should generate a `Writable` event
    assert_eq!(
        stream.state.poll(),
        Some(StreamEvent::Writable { id: stream_id })
    );
    assert_eq!(stream.state.write_limit(), smaller_send_window);

    // Assert that only `smaller_send_window` bytes are accepted
    assert_eq!(stream.write(&data), Ok(smaller_send_window as usize));
    assert_eq!(stream.write(&data), Err(WriteError::Blocked));
}
