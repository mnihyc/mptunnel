use super::{
    ReliablePathCommand, recv_reliable_path_command, recv_reliable_path_command_during_drain,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    reliable_path_command_writer_run_bytes, reliable_path_effective_frame_lane,
    reliable_path_frame_uses_priority_queue, try_recv_reliable_path_command,
};
use crate::protocol::{
    DatagramFlowId, Frame, PathId, PathUsage, ResetReason, StreamDemandHint, StreamId, TargetAddr,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::num::NonZeroU64;

fn stream_data_frame(stream_id: u64, bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(stream_id),
        offset: 0,
        payload: Bytes::from(vec![0; bytes]),
    }
}

fn datagram_data_frame(datagram_id: u64, bytes: usize) -> Frame {
    Frame::DatagramData {
        flow_id: DatagramFlowId(1),
        datagram_id: crate::protocol::DatagramId(datagram_id),
        ttl_ms: 1_000,
        payload: Bytes::from(vec![0; bytes]),
    }
}

#[test]
fn ordered_writer_flow_load_tracks_lane_changes_and_lifetime() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    assert_eq!(commands.active_flow_counts(), (0, 0));

    let throughput = commands.register_flow(TrafficClass::Throughput);
    let latency = commands.register_flow(TrafficClass::Latency);
    assert_eq!(commands.active_flow_counts(), (2, 1));

    throughput.set_lane(TrafficClass::Latency);
    assert_eq!(commands.active_flow_counts(), (2, 2));

    latency.deactivate();
    assert_eq!(commands.active_flow_counts(), (1, 1));
    drop(latency);
    assert_eq!(commands.active_flow_counts(), (1, 1));

    drop(throughput);
    assert_eq!(commands.active_flow_counts(), (0, 0));
}

#[test]
fn tcp_carrier_validation_data_is_typed_bounded_and_exactly_accounted() {
    const VALIDATION_ID: NonZeroU64 = NonZeroU64::new(17).expect("nonzero validation id");
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let frame = stream_data_frame(7, 4096);
    let expected_pending = crate::protocol::frame::reliable_path_frame_pacing_bytes(&frame);
    let expected_writer = crate::protocol::codec::encoded_frame_capacity_hint(&frame).max(1);

    let reservation = commands
        .try_reserve_tcp_carrier_validation_data(
            VALIDATION_ID,
            frame.clone(),
            TrafficClass::Throughput,
        )
        .expect("reserve one validation assignment");
    assert_eq!(
        commands.pending_bytes(),
        0,
        "reservation is not publication"
    );
    assert!(matches!(
        commands.try_reserve_tcp_carrier_validation_data(
            VALIDATION_ID,
            stream_data_frame(8, 1),
            TrafficClass::Throughput,
        ),
        Err(crate::runtime::RuntimeError::SenderServiceBlocked)
    ));

    reservation.commit();
    assert_eq!(commands.pending_bytes(), expected_pending as u64);
    let command =
        try_recv_reliable_path_command(&mut receivers).expect("published validation assignment");
    assert!(matches!(
        &command,
        ReliablePathCommand::SendTcpCarrierValidationData {
            validation_id: VALIDATION_ID,
            frame: received,
        } if received == &frame
    ));
    assert_eq!(
        reliable_path_command_pending_bytes(&command),
        expected_pending
    );
    assert_eq!(
        reliable_path_command_writer_run_bytes(&command),
        expected_writer
    );
    assert_eq!(commands.writer_pending_bytes(), expected_pending as u64);

    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    assert_eq!(commands.pending_bytes(), 0);
    assert_eq!(commands.writer_pending_bytes(), 0);
}

#[test]
fn tcp_carrier_validation_data_rejects_invalid_frame_and_lane() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    assert!(matches!(
        commands.try_reserve_tcp_carrier_validation_data(
            NonZeroU64::new(1).expect("nonzero validation id"),
            Frame::Ping { nonce: 1 },
            TrafficClass::Throughput,
        ),
        Err(crate::runtime::RuntimeError::Protocol(_))
    ));
    assert!(matches!(
        commands.try_reserve_tcp_carrier_validation_data(
            NonZeroU64::new(1).expect("nonzero validation id"),
            stream_data_frame(1, 1),
            TrafficClass::Latency,
        ),
        Err(crate::runtime::RuntimeError::Protocol(_))
    ));
    assert_eq!(commands.pending_bytes(), 0);

    let reservation = commands
        .try_reserve_tcp_carrier_validation_data(
            NonZeroU64::new(1).expect("nonzero validation id"),
            stream_data_frame(1, 1),
            TrafficClass::Throughput,
        )
        .expect("invalid attempts consume no bounded capacity");
    drop(reservation);
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
    assert_eq!(commands.pending_bytes(), 0);
}

#[tokio::test]
async fn tcp_carrier_validation_writer_boundary_is_bounded_zero_byte_fifo_work() {
    let validation_id = NonZeroU64::new(19).expect("nonzero validation id");
    let (commands, mut receivers) = reliable_path_command_channels(2);
    let frame = stream_data_frame(12, 4096);
    let frame_bytes =
        reliable_path_command_pending_bytes(&ReliablePathCommand::SendFrame(frame.clone()));
    commands
        .try_reserve_tcp_carrier_validation_data(
            validation_id,
            frame.clone(),
            TrafficClass::Throughput,
        )
        .expect("reserve preceding validation data")
        .commit();

    let mut boundary = Box::pin(commands.tcp_carrier_validation_writer_boundary(validation_id));
    tokio::select! {
        biased;
        result = boundary.as_mut() => panic!("writer completed an unconsumed boundary: {result:?}"),
        _ = std::future::ready(()) => {}
    }

    let preceding = recv_reliable_path_command(&mut receivers)
        .await
        .expect("preceding validation data");
    assert!(matches!(
        &preceding,
        ReliablePathCommand::SendTcpCarrierValidationData {
            validation_id: received_validation_id,
            frame: received_frame,
        } if *received_validation_id == validation_id && received_frame == &frame
    ));
    let marker = recv_reliable_path_command(&mut receivers)
        .await
        .expect("writer boundary follows preceding Product data");
    assert_eq!(reliable_path_command_pending_bytes(&marker), 0);
    assert_eq!(reliable_path_command_writer_run_bytes(&marker), 1);
    let completion = match marker {
        ReliablePathCommand::TcpCarrierValidationWriterBoundary {
            validation_id: received_validation_id,
            completion,
        } => {
            assert_eq!(received_validation_id, validation_id);
            completion
        }
        _ => panic!("expected typed validation writer boundary"),
    };
    assert_eq!(commands.pending_bytes(), frame_bytes as u64);
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&preceding));
    receivers.release_pending_command_bytes(0);
    assert_eq!(commands.pending_bytes(), 0);
    assert_eq!(commands.writer_pending_bytes(), 0);
    tokio::select! {
        biased;
        result = boundary.as_mut() => panic!("boundary completed before its writer: {result:?}"),
        _ = std::future::ready(()) => {}
    }

    let completed_at = std::time::Instant::now();
    completion
        .send(completed_at)
        .expect("boundary requester remains live");
    assert_eq!(
        boundary.await.expect("completed writer boundary"),
        completed_at
    );
}

#[tokio::test]
async fn tcp_carrier_validation_writer_boundary_fails_when_writer_closes() {
    let validation_id = NonZeroU64::new(23).expect("nonzero validation id");
    let (commands, receivers) = reliable_path_command_channels(1);
    let mut boundary = Box::pin(commands.tcp_carrier_validation_writer_boundary(validation_id));
    tokio::select! {
        biased;
        result = boundary.as_mut() => panic!("writer completed an unconsumed boundary: {result:?}"),
        _ = std::future::ready(()) => {}
    }

    drop(receivers);
    assert!(matches!(
        boundary.await,
        Err(crate::runtime::RuntimeError::ReliablePathSessionClosed)
    ));
}

#[tokio::test]
async fn path_drain_closes_admission_and_waits_for_preexisting_reservations() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let reservation = commands
        .try_reserve_tcp_carrier_validation_data(
            NonZeroU64::new(7).expect("nonzero validation id"),
            stream_data_frame(1, 1024),
            TrafficClass::Throughput,
        )
        .expect("pre-drain validation-data reservation");

    commands.begin_path_drain();
    assert!(matches!(
        commands.try_reserve_admitted_frame(stream_data_frame(2, 1024), TrafficClass::Throughput),
        Err(crate::runtime::error::RuntimeError::ReliablePathSessionClosed)
    ));
    assert!(matches!(
        commands.try_reserve_tcp_carrier_validation_data(
            NonZeroU64::new(8).expect("nonzero validation id"),
            stream_data_frame(2, 1024),
            TrafficClass::Throughput,
        ),
        Err(crate::runtime::error::RuntimeError::ReliablePathSessionClosed)
    ));
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id: StreamId(1),
                complete: false,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("crossing settlement control remains ordered after the application fence");
    receivers.close_for_path_drain();
    assert!(commands.is_closed(), "path drain stops all queue admission");
    let mut draining = Box::pin(recv_reliable_path_command_during_drain(&mut receivers));
    let control = draining
        .as_mut()
        .await
        .expect("crossing settlement control is preserved");
    drop(draining);
    assert!(matches!(
        control,
        ReliablePathCommand::SendFrame(Frame::StreamAck {
            stream_id: StreamId(1),
            ..
        })
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&control));

    let mut draining = Box::pin(recv_reliable_path_command_during_drain(&mut receivers));
    tokio::select! {
        biased;
        _ = draining.as_mut() => panic!("outstanding reservation ended path drain early"),
        _ = std::future::ready(()) => {}
    }

    reservation.commit();
    let command = draining.await.expect("pre-drain reservation is preserved");
    assert!(matches!(
        &command,
        ReliablePathCommand::SendTcpCarrierValidationData {
            validation_id,
            frame: Frame::StreamData {
                stream_id: StreamId(1),
                ..
            },
        } if *validation_id == NonZeroU64::new(7).expect("nonzero validation id")
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    assert!(
        recv_reliable_path_command_during_drain(&mut receivers)
            .await
            .is_none(),
        "terminal drain follows every accepted queue reservation"
    );
}

#[tokio::test]
async fn failed_path_termination_fences_admission_and_signals_terminal() {
    let (commands, receivers) = reliable_path_command_channels(1);
    let signal = receivers.path_drain_signal();

    commands.terminate_failed_path();
    signal.wait().await;

    assert!(signal.is_terminal());
    assert!(matches!(
        commands.try_reserve_admitted_frame(stream_data_frame(1, 1024), TrafficClass::Throughput),
        Err(crate::runtime::error::RuntimeError::ReliablePathSessionClosed)
    ));
}

#[test]
fn control_and_ack_frames_never_use_throughput_lane() {
    let priority_frames = [
        (
            Frame::OpenStream {
                stream_id: StreamId(1),
                target: TargetAddr::Domain {
                    host: "example.test".to_string(),
                    port: 443,
                },
                demand: StreamDemandHint::throughput(),
            },
            TrafficClass::Control,
        ),
        (
            Frame::PathStatus {
                path_id: PathId(1),
                sequence: 4,
                usage: PathUsage::Backup,
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamAck {
                stream_id: StreamId(1),
                complete: false,
                ranges: vec![],
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamMaxData {
                stream_id: StreamId(1),
                max_offset: 1024,
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamFin {
                stream_id: StreamId(1),
                final_offset: 64,
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamReset {
                stream_id: StreamId(1),
                reason: ResetReason::RemoteClosed,
            },
            TrafficClass::Control,
        ),
        (
            Frame::StreamDetach {
                stream_id: StreamId(1),
            },
            TrafficClass::Control,
        ),
        (
            Frame::DatagramFeedback {
                flow_id: DatagramFlowId(1),
                received: vec![],
            },
            TrafficClass::RealtimeDatagram,
        ),
        (
            Frame::DatagramClose {
                flow_id: DatagramFlowId(1),
            },
            TrafficClass::Control,
        ),
    ];

    for (frame, expected_lane) in priority_frames {
        let effective_lane = reliable_path_effective_frame_lane(&frame, TrafficClass::Throughput);
        assert_eq!(effective_lane, expected_lane);
        assert!(reliable_path_frame_uses_priority_queue(effective_lane));
    }
}

#[tokio::test]
async fn terminal_reset_and_close_uses_one_ordered_queue_item() {
    let stream_id = StreamId(2);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"bulk"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill ordered data queue");

    let (terminal_result, first) = tokio::join!(
        commands.send_stream_ordered_reset_and_close(
            stream_id,
            ResetReason::Refused,
            TrafficClass::Throughput,
        ),
        recv_reliable_path_command(&mut receivers),
    );
    assert!(
        terminal_result.is_ok(),
        "queue terminal transaction after prior data"
    );
    let first = first.expect("prior ordered data");
    assert!(matches!(
        &first,
        ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: data_stream_id,
            ..
        }) if *data_stream_id == stream_id
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&first));

    let terminal = recv_reliable_path_command(&mut receivers)
        .await
        .expect("single terminal transaction");
    assert!(matches!(
        &terminal,
        ReliablePathCommand::ResetAndCloseStream {
            stream_id: reset_stream_id,
            reason: ResetReason::Refused,
        } if *reset_stream_id == stream_id
    ));
    assert_eq!(
        commands.pending_bytes(),
        u64::try_from(reliable_path_command_pending_bytes(&terminal))
            .expect("terminal pacing debt fits queue metrics")
    );
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&terminal));
    assert_eq!(commands.pending_bytes(), 0);
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
}

#[tokio::test]
async fn control_close_discards_stale_stream_data_and_releases_queue_bytes() {
    let closed_stream = StreamId(20);
    let live_stream = StreamId(21);
    let (commands, mut receivers) = reliable_path_command_channels(4);

    commands
        .try_reserve_tcp_carrier_validation_data(
            NonZeroU64::new(9).expect("nonzero validation id"),
            stream_data_frame(closed_stream.0, 64 * 1024),
            TrafficClass::Throughput,
        )
        .expect("reserve validation data that becomes stale")
        .commit();
    commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame(live_stream.0, 64 * 1024),
            TrafficClass::Throughput,
        )
        .expect("queue live sibling data");
    commands
        .send_control(ReliablePathCommand::CloseStream(closed_stream))
        .await
        .expect("queue preempting close");
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(stream_id)) if stream_id == closed_stream
    ));

    let live = recv_reliable_path_command(&mut receivers)
        .await
        .expect("stale data is skipped in front of live sibling data");
    assert!(matches!(
        &live,
        ReliablePathCommand::SendFrame(Frame::StreamData { stream_id, .. })
            if *stream_id == live_stream
    ));
    assert_eq!(
        commands.pending_bytes(),
        reliable_path_command_pending_bytes(&live) as u64,
        "discarding stale data must return its queue byte charge"
    );
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&live));
    assert_eq!(commands.pending_bytes(), 0);
}

#[tokio::test]
async fn cancelling_waiting_terminal_reset_releases_queue_debt() {
    let stream_id = StreamId(3);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"bulk"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill ordered data queue");
    let queued_bytes = commands.pending_bytes();
    let terminal_bytes = u64::try_from(reliable_path_command_pending_bytes(
        &ReliablePathCommand::ResetAndCloseStream {
            stream_id,
            reason: ResetReason::Refused,
        },
    ))
    .expect("terminal pacing debt fits queue metrics");

    let mut terminal_send = Box::pin(commands.send_stream_ordered_reset_and_close(
        stream_id,
        ResetReason::Refused,
        TrafficClass::Throughput,
    ));
    tokio::select! {
        biased;
        _ = terminal_send.as_mut() => panic!("full queue admitted terminal transaction"),
        _ = std::future::ready(()) => {}
    }
    assert_eq!(
        commands.pending_bytes(),
        queued_bytes.saturating_add(terminal_bytes)
    );

    drop(terminal_send);
    assert_eq!(commands.pending_bytes(), queued_bytes);
    let queued = recv_reliable_path_command(&mut receivers)
        .await
        .expect("prior ordered data");
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&queued));
    assert_eq!(commands.pending_bytes(), 0);
}

#[tokio::test]
async fn deadline_admission_wakes_after_queue_release() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(datagram_data_frame(1, 512), TrafficClass::RealtimeDatagram)
        .expect("fill realtime queue");

    let waiting_commands = commands.clone();
    let waiting = tokio::spawn(async move {
        waiting_commands
            .enqueue_admitted_frame_until(
                datagram_data_frame(2, 512),
                TrafficClass::RealtimeDatagram,
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());

    let first = recv_reliable_path_command(&mut receivers)
        .await
        .expect("first realtime datagram");
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&first));
    tokio::time::timeout(std::time::Duration::from_millis(200), waiting)
        .await
        .expect("queue admission wakeup")
        .expect("admission task")
        .expect("realtime datagram admission");
    let datagram = recv_reliable_path_command(&mut receivers)
        .await
        .expect("admitted realtime datagram");
    assert!(matches!(
        datagram,
        ReliablePathCommand::SendFrame(Frame::DatagramData {
            datagram_id: crate::protocol::DatagramId(2),
            ..
        })
    ));
}

#[tokio::test]
async fn deadline_admission_expires_when_priority_headroom_stays_full() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(datagram_data_frame(2, 512), TrafficClass::RealtimeDatagram)
        .expect("fill bounded realtime queue");

    let result = commands
        .enqueue_admitted_frame_until(
            datagram_data_frame(3, 512),
            TrafficClass::RealtimeDatagram,
            tokio::time::Instant::now() + std::time::Duration::from_millis(20),
        )
        .await;

    assert!(matches!(
        result,
        Err(crate::runtime::RuntimeError::SenderServiceBlocked)
    ));
}

#[test]
fn reinjection_headroom_precedes_fresh_bulk_data() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let fresh = stream_data_frame(40, 4096);
    let repair = stream_data_frame(41, 4096);

    commands
        .try_enqueue_stream_ordered_frame(fresh, TrafficClass::Throughput)
        .expect("fill fresh-data queue");
    assert!(!commands.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput));
    assert!(commands.can_enqueue_reinjection_frame_now(&repair));
    commands
        .try_enqueue_reinjection_frame(repair, TrafficClass::Throughput)
        .expect("reinjection keeps independent bounded headroom");

    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: StreamId(41),
            ..
        }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: StreamId(40),
            ..
        }))
    ));
}

#[test]
fn reinjection_headroom_is_bounded() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_reinjection_frame(stream_data_frame(42, 4096), TrafficClass::Throughput)
        .expect("first reinjection uses bounded headroom");
    let second = stream_data_frame(43, 4096);

    assert!(!commands.can_enqueue_reinjection_frame_now(&second));
    assert!(matches!(
        commands.try_enqueue_reinjection_frame(second, TrafficClass::Throughput),
        Err(crate::runtime::RuntimeError::SenderServiceBlocked)
    ));
}
