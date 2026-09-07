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
        assert!(matches!(
            try_route_client_udp_stream_frame_during_write(
                terminal.clone(),
                stream_id,
                &frames_tx,
            )
            .expect("route terminal frame"),
            CarrierInputRoute::Barrier(frame) if frame == terminal
        ));
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

    assert!(matches!(
        try_route_client_udp_stream_frame_during_write(feedback.clone(), stream_id, &frames_tx)
            .expect("route stream feedback"),
        CarrierInputRoute::Routed
    ));
    assert!(matches!(
        frames_rx.try_recv(),
        Ok(Ok(Frame::StreamAck {
            stream_id: received_stream_id,
            complete: false,
            ranges,
        })) if received_stream_id == stream_id && ranges.is_empty()
    ));
}

#[test]
fn quic_write_interlock_closed_product_recipient_is_retired_input() {
    let stream_id = StreamId(46);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    drop(frames_rx);
    assert!(matches!(
        try_route_client_udp_stream_frame_during_write(
            Frame::StreamAck {
                stream_id,
                complete: true,
                ranges: Vec::new(),
            },
            stream_id,
            &frames_tx,
        ),
        Ok(CarrierInputRoute::Routed)
    ));
}

#[tokio::test]
async fn quic_write_interlock_pending_product_recipient_retires_without_error() {
    let stream_id = StreamId(47);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    frames_tx.try_send(Ok(Frame::Ping { nonce: 1 })).unwrap();
    let route = try_route_client_udp_stream_frame_during_write(
        Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        },
        stream_id,
        &frames_tx,
    )
    .unwrap();
    let CarrierInputRoute::Mailbox(mut pending) = route else {
        panic!("full recipient must retain the exact frame");
    };
    drop(frames_rx);
    assert!(!pending.deliver().await);
}

#[tokio::test]
async fn quic_write_wait_retries_full_product_mailbox_before_write_completion() {
    let stream_id = StreamId(44);
    let (input_tx, mut input_rx) = mpsc::channel(1);
    let (stream_frames_tx, mut stream_frames_rx) = mpsc::channel(1);
    let occupied = Frame::StreamMaxData {
        stream_id,
        max_offset: 1,
    };
    let feedback = Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: Vec::new(),
    };
    stream_frames_tx
        .try_send(Ok(occupied.clone()))
        .expect("fill Product mailbox");
    let (release_write, write_released) = oneshot::channel::<()>();
    let (blocked_signal, blocked) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let mut blocked_signal = Some(blocked_signal);
        let mut deferred_input = None;
        let (_, routed) = super::super::io::await_udp_write_while_routing_stream_frames(
            async move {
                write_released.await.expect("release native write");
            },
            &mut input_rx,
            &mut deferred_input,
            |frame| {
                let result = try_route_client_udp_stream_frame_during_write(
                    frame,
                    stream_id,
                    &stream_frames_tx,
                )?;
                if matches!(result, CarrierInputRoute::Mailbox(_))
                    && let Some(signal) = blocked_signal.take()
                {
                    let _ = signal.send(());
                }
                Ok(result)
            },
        )
        .await;
        (routed, deferred_input)
    });
    input_tx
        .send(Ok(feedback.clone()))
        .await
        .expect("queue feedback");
    tokio::time::timeout(std::time::Duration::from_secs(1), blocked)
        .await
        .expect("observe actual mailbox pressure")
        .expect("blocked signal");
    assert_eq!(stream_frames_rx.recv().await.unwrap().unwrap(), occupied);
    let delivered = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        stream_frames_rx.recv(),
    )
    .await;
    assert!(
        !task.is_finished(),
        "retry must not cancel or complete the native write"
    );
    release_write
        .send(())
        .expect("release native write after capacity test");
    let (routed, deferred) = task.await.expect("join interlock");
    assert!(
        matches!(delivered, Ok(Some(Ok(ref frame))) if frame == &feedback),
        "Product mailbox capacity must retry feedback while native write is still pending; routed={routed}, retained={}",
        deferred.is_some(),
    );
    assert_eq!(routed, 1);
    assert!(deferred.is_none());
}

#[tokio::test]
async fn quic_write_wins_before_mailbox_capacity_preserves_exact_input() {
    let stream_id = StreamId(45);
    let (input_tx, mut input_rx) = mpsc::channel(1);
    let (stream_frames_tx, mut stream_frames_rx) = mpsc::channel(1);
    stream_frames_tx
        .try_send(Ok(Frame::Ping { nonce: 45 }))
        .unwrap();
    let feedback = Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: Vec::new(),
    };
    let (release_write, write_released) = oneshot::channel::<()>();
    let (blocked_signal, blocked) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let mut blocked_signal = Some(blocked_signal);
        let mut deferred_input = None;
        let (_, routed) = super::super::io::await_udp_write_while_routing_stream_frames(
            async move {
                write_released.await.unwrap();
            },
            &mut input_rx,
            &mut deferred_input,
            |frame| {
                let result = try_route_client_udp_stream_frame_during_write(
                    frame,
                    stream_id,
                    &stream_frames_tx,
                )?;
                if matches!(result, CarrierInputRoute::Mailbox(_))
                    && let Some(signal) = blocked_signal.take()
                {
                    let _ = signal.send(());
                }
                Ok(result)
            },
        )
        .await;
        (routed, deferred_input)
    });
    input_tx.send(Ok(feedback.clone())).await.unwrap();
    blocked.await.unwrap();
    release_write.send(()).unwrap();
    let (routed, deferred) = task.await.unwrap();
    assert_eq!(routed, 0);
    assert!(matches!(deferred, Some(Ok(frame)) if frame == feedback));
    assert!(matches!(
        stream_frames_rx.recv().await,
        Some(Ok(Frame::Ping { nonce: 45 }))
    ));
    assert!(
        stream_frames_rx.try_recv().is_err(),
        "write-wins must not duplicate retained feedback"
    );
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
                if matches!(routed, CarrierInputRoute::Barrier(Frame::StreamFin { .. }))
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
