use super::*;
use crate::transport::quic::QuicCarrierError;
use tokio::sync::oneshot;

#[test]
fn quic_write_interlock_defers_matching_terminal_frames_to_stream_owner() {
    let stream_id = StreamId(41);
    let (frames_tx, mut frames_rx) = mpsc::channel(4);

    for terminal in [
        Frame::StreamFin {
            stream_id,
            final_offset: 7,
        },
        Frame::StreamReset {
            stream_id,
            reason: crate::protocol::ResetReason::RemoteClosed,
        },
    ] {
        assert_eq!(
            try_route_client_udp_stream_frame_during_write(
                terminal.clone(),
                stream_id,
                &frames_tx,
            )
            .expect("route terminal frame"),
            Some(terminal),
        );
    }
    assert!(frames_rx.try_recv().is_err());
}

#[test]
fn quic_write_interlock_still_routes_nonterminal_stream_feedback() {
    let stream_id = StreamId(42);
    let (frames_tx, mut frames_rx) = mpsc::channel(1);
    let feedback = Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: Vec::new(),
    };

    assert_eq!(
        try_route_client_udp_stream_frame_during_write(feedback.clone(), stream_id, &frames_tx)
            .expect("route stream feedback"),
        None,
    );
    assert!(matches!(
        frames_rx.try_recv(),
        Ok(Ok(Frame::StreamAck {
            stream_id: received_stream_id,
            complete: false,
            ranges,
        })) if received_stream_id == stream_id && ranges.is_empty()
    ));
}

#[tokio::test]
async fn quic_write_interlock_preserves_terminal_before_clean_eof() {
    let stream_id = StreamId(43);
    let (input_tx, mut input_rx) = mpsc::channel(2);
    let (stream_frames_tx, mut stream_frames_rx) = mpsc::channel(1);
    let (release_write, write_released) = oneshot::channel::<()>();
    let (terminal_seen, terminal_deferred) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        let mut terminal_seen = Some(terminal_seen);
        let mut deferred_input = None;
        let (_, routed_frames) = super::super::io::await_udp_write_while_routing_stream_frames(
            async move {
                write_released.await.expect("release simulated QUIC write");
            },
            &mut input_rx,
            &mut deferred_input,
            |frame| {
                let routed = try_route_client_udp_stream_frame_during_write(
                    frame,
                    stream_id,
                    &stream_frames_tx,
                )?;
                if matches!(routed, Some(Frame::StreamFin { .. }))
                    && let Some(terminal_seen) = terminal_seen.take()
                {
                    let _ = terminal_seen.send(());
                }
                Ok(routed)
            },
        )
        .await;
        let following_input = input_rx.recv().await;
        (routed_frames, deferred_input, following_input)
    });

    input_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 9,
        }))
        .await
        .expect("queue terminal frame");
    terminal_deferred
        .await
        .expect("terminal frame reached ordering boundary");
    input_tx
        .send(Err(RuntimeError::QuicCarrier(
            QuicCarrierError::StreamFinished,
        )))
        .await
        .expect("queue clean QUIC EOF");
    release_write
        .send(())
        .expect("complete simulated QUIC write");

    let (routed_frames, deferred_input, following_input) = task.await.expect("join write wait");
    assert_eq!(routed_frames, 0);
    assert!(matches!(
        deferred_input,
        Some(Ok(Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 9,
        })) if received_stream_id == stream_id
    ));
    assert!(matches!(
        following_input,
        Some(Err(RuntimeError::QuicCarrier(
            QuicCarrierError::StreamFinished
        )))
    ));
    assert!(stream_frames_rx.try_recv().is_err());
}
