use super::{
    ReliablePathCommand, recv_reliable_path_command, recv_reliable_path_command_during_drain,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    reliable_path_effective_frame_lane, reliable_path_frame_uses_priority_queue,
    try_recv_reliable_path_command,
};
use crate::protocol::{
    DatagramFlowId, Frame, PathId, PathUsage, ResetReason, StreamDemandHint, StreamId, TargetAddr,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;

fn stream_data_frame(stream_id: u64, bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(stream_id),
        offset: 0,
        payload: Bytes::from(vec![0; bytes]),
    }
}

fn datagram_data_frame(datagram_id: u64, bytes: usize) -> Frame {
    datagram_data_frame_for_flow(1, datagram_id, bytes)
}

fn datagram_data_frame_for_flow(flow_id: u64, datagram_id: u64, bytes: usize) -> Frame {
    Frame::DatagramData {
        flow_id: DatagramFlowId(flow_id),
        datagram_id: crate::protocol::DatagramId(datagram_id),
        ttl_ms: 1_000,
        payload: Bytes::from(vec![0; bytes]),
    }
}

#[test]
fn ordered_writer_flow_load_tracks_lane_changes_and_lifetime() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    assert_eq!(commands.active_flow_counts(), (0, 0));

    let throughput = commands.register_inactive_flow(TrafficClass::Throughput);
    throughput.activate();
    let latency = commands.register_inactive_flow(TrafficClass::Latency);
    latency.activate();
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
fn inactive_writer_flow_retains_lane_and_counts_only_while_demanding_service() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    let registration = commands.register_inactive_flow(TrafficClass::Throughput);
    assert!(!registration.is_active());
    assert_eq!(commands.active_flow_counts(), (0, 0));

    registration.set_lane(TrafficClass::Latency);
    assert_eq!(commands.active_flow_counts(), (0, 0));
    registration.activate();
    registration.activate();
    assert!(registration.is_active());
    assert_eq!(commands.active_flow_counts(), (1, 1));

    registration.set_lane(TrafficClass::Throughput);
    assert_eq!(commands.active_flow_counts(), (1, 0));
    registration.deactivate();
    registration.deactivate();
    assert_eq!(commands.active_flow_counts(), (0, 0));

    registration.set_lane(TrafficClass::Latency);
    registration.activate();
    assert_eq!(commands.active_flow_counts(), (1, 1));
    drop(registration);
    assert_eq!(commands.active_flow_counts(), (0, 0));
}

#[test]
fn writer_flow_lane_and_demand_transitions_are_concurrency_exact() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    for _ in 0..64 {
        let registration = commands.register_inactive_flow(TrafficClass::Throughput);
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let activate = registration.clone();
            let activate_start = start.clone();
            scope.spawn(move || {
                activate_start.wait();
                activate.activate();
            });
            let change_lane = registration.clone();
            scope.spawn(move || {
                start.wait();
                change_lane.set_lane(TrafficClass::Latency);
            });
        });
        assert_eq!(
            commands.active_flow_counts(),
            (1, 1),
            "activation and lane publication linearize to one exact latency flow"
        );

        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let deactivate = registration.clone();
            let deactivate_start = start.clone();
            scope.spawn(move || {
                deactivate_start.wait();
                deactivate.deactivate();
            });
            let change_lane = registration.clone();
            scope.spawn(move || {
                start.wait();
                change_lane.set_lane(TrafficClass::Throughput);
            });
        });
        assert_eq!(
            commands.active_flow_counts(),
            (0, 0),
            "deactivation releases the lane that owns the packed count without underflow"
        );
    }
}

#[tokio::test]
async fn path_drain_closes_admission_and_waits_for_preexisting_reservations() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let reservation = commands
        .try_reserve_admitted_frame(stream_data_frame(1, 1024), TrafficClass::Throughput)
        .expect("pre-drain data reservation");

    commands.begin_path_drain();
    assert!(!commands.product_admission_active());
    assert!(matches!(
        commands.try_reserve_admitted_frame(stream_data_frame(2, 1024), TrafficClass::Throughput),
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
    assert!(
        !commands.is_terminal(),
        "closed drain admission is not unexpected carrier failure"
    );
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
        ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: StreamId(1),
            ..
        })
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    assert!(
        recv_reliable_path_command_during_drain(&mut receivers)
            .await
            .is_none(),
        "terminal drain follows every accepted queue reservation"
    );
    assert!(receivers.finish_planned_path_retirement());
    assert!(commands.is_terminal());
    assert!(commands.is_planned_retirement());
}

#[tokio::test]
async fn path_drain_retains_exact_requalification_ack_recovery_control() {
    let (commands, mut receivers) = reliable_path_command_channels(2);
    commands.begin_path_drain();

    let ack = Frame::StreamRequalifyAck {
        stream_id: StreamId(3),
        probe_id: 7,
        offset: 4096,
        payload_bytes: 1024,
    };
    commands
        .try_enqueue_admitted_frame(ack.clone(), TrafficClass::Control)
        .expect(
            "carrier drain rejects new Product placement but retains recovery and ordered-control processing",
        );

    receivers.close_for_path_drain();
    assert!(matches!(
        recv_reliable_path_command_during_drain(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(frame)) if frame == ack
    ));
}

#[test]
fn failed_path_terminal_rejects_new_unbounded_retirement_handoffs() {
    let (commands, mut receivers) = reliable_path_command_channels(4);
    commands.terminate_failed_path();

    assert!(matches!(
        commands.retire_accepted_stream(StreamId(31)),
        Err(crate::runtime::error::RuntimeError::ReliablePathSessionClosed)
    ));
    assert!(matches!(
        commands.reset_accepted_stream(StreamId(32), ResetReason::Refused),
        Err(crate::runtime::error::RuntimeError::ReliablePathSessionClosed)
    ));
    assert!(matches!(
        commands.retire_datagram_attachment(33),
        Err(crate::runtime::error::RuntimeError::ReliablePathSessionClosed)
    ));
    assert!(matches!(
        commands.retire_server_datagram_flow(DatagramFlowId(34)),
        Err(crate::runtime::error::RuntimeError::ReliablePathSessionClosed)
    ));
    assert!(
        try_recv_reliable_path_command(&mut receivers).is_none(),
        "a terminal carrier cannot report ownership transfer to an unserviceable retirement lane",
    );
}

#[tokio::test]
async fn planned_drain_retains_unbounded_retirement_handoff() {
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let stream_id = StreamId(35);
    commands.begin_path_drain();
    commands
        .retire_accepted_stream(stream_id)
        .expect("planned drain retains queue-independent settlement handoff");

    receivers.close_for_path_drain();
    assert!(matches!(
        recv_reliable_path_command_during_drain(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach {
            stream_id: detached,
        })) if detached == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command_during_drain(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(closed)) if closed == stream_id
    ));
    assert!(
        recv_reliable_path_command_during_drain(&mut receivers)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn failed_path_termination_fences_admission_and_signals_terminal() {
    let (commands, receivers) = reliable_path_command_channels(1);
    let signal = receivers.path_drain_signal();

    commands.terminate_failed_path();
    signal.wait().await;

    assert!(signal.is_terminal());
    assert_eq!(
        signal.drain_started_at(),
        None,
        "native failure does not manufacture a planned-drain budget"
    );
    commands.begin_path_drain();
    assert_eq!(
        signal.drain_started_at(),
        None,
        "a late drain request cannot relabel terminal failure as planned retirement"
    );
    assert!(matches!(
        commands.try_reserve_admitted_frame(stream_data_frame(1, 1024), TrafficClass::Throughput),
        Err(crate::runtime::error::RuntimeError::ReliablePathSessionClosed)
    ));
}

#[tokio::test]
async fn terminal_failure_releases_a_full_non_product_control_waiter() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    commands
        .send_control(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
        .await
        .expect("fill bounded control queue");

    let mut waiting =
        Box::pin(commands.send_control(ReliablePathCommand::SendFrame(Frame::Pong { nonce: 2 })));
    tokio::select! {
        biased;
        result = waiting.as_mut() => panic!("full control queue accepted waiter early: {result:?}"),
        () = std::future::ready(()) => {}
    }

    commands.terminate_failed_path();
    tokio::select! {
        biased;
        result = waiting.as_mut() => assert!(
            result.is_err(),
            "known-terminal control admission unexpectedly succeeded",
        ),
        () = std::future::ready(()) => {
            panic!("known-terminal control waiter remained parked on queue capacity")
        }
    }
}

#[tokio::test(start_paused = true)]
async fn path_drain_deadline_starts_at_request_boundary_and_never_restarts() {
    let (commands, receivers) = reliable_path_command_channels(1);
    let signal = receivers.path_drain_signal();
    let retention = std::time::Duration::from_secs(10);
    let requested_at = tokio::time::Instant::now();

    commands.begin_path_drain();
    assert_eq!(signal.drain_started_at(), Some(requested_at));
    assert_eq!(
        signal.drain_deadline(retention),
        Some(requested_at + retention)
    );

    let deadline = signal.wait_for_drain_deadline(retention);
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        () = &mut deadline => panic!("drain expired at its request boundary"),
        () = std::future::ready(()) => {}
    }

    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    commands.begin_path_drain();
    assert_eq!(
        signal.drain_deadline(retention),
        Some(requested_at + retention),
        "duplicate drain request extended the absolute retention ceiling"
    );

    tokio::time::advance(std::time::Duration::from_millis(5_999)).await;
    tokio::select! {
        biased;
        () = &mut deadline => panic!("drain expired before its absolute ceiling"),
        () = std::future::ready(()) => {}
    }
    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    deadline.await;
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
                return_plan: Default::default(),
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
        .try_reserve_admitted_frame(
            stream_data_frame(closed_stream.0, 64 * 1024),
            TrafficClass::Throughput,
        )
        .expect("reserve data that becomes stale")
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
async fn server_datagram_retirement_discards_only_preaccepted_same_flow_work() {
    let retired_flow = DatagramFlowId(30);
    let sibling_flow = DatagramFlowId(31);
    let (commands, mut receivers) = reliable_path_command_channels(4);

    // Keep one reservation uncommitted across the retirement boundary to
    // prove the fence follows admission ownership rather than send timing.
    let crossing = commands
        .try_reserve_admitted_frame(
            datagram_data_frame_for_flow(retired_flow.0, 1, 4096),
            TrafficClass::RealtimeDatagram,
        )
        .expect("reserve same-flow work before retirement");
    commands
        .try_enqueue_admitted_frame(
            datagram_data_frame_for_flow(sibling_flow.0, 2, 2048),
            TrafficClass::RealtimeDatagram,
        )
        .expect("queue unrelated flow work");
    commands
        .retire_server_datagram_flow(retired_flow)
        .expect("publish queue-independent retirement");

    let close = recv_reliable_path_command(&mut receivers)
        .await
        .expect("retirement close");
    assert!(matches!(
        close,
        ReliablePathCommand::SendFrame(Frame::DatagramClose { flow_id })
            if flow_id == retired_flow
    ));

    crossing.commit();
    let sibling = recv_reliable_path_command(&mut receivers)
        .await
        .expect("unrelated queued work survives the flow fence");
    assert!(matches!(
        &sibling,
        ReliablePathCommand::SendFrame(Frame::DatagramData { flow_id, .. })
            if *flow_id == sibling_flow
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&sibling));
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
    assert_eq!(
        commands.pending_bytes(),
        0,
        "discarding retired work returns its queue-byte charge"
    );
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

#[tokio::test]
async fn frame_reservation_owns_byte_charge_from_reserve_through_writer_release() {
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let quantum = 64 * 1024;

    let cancelled = commands
        .try_reserve_admitted_frame(stream_data_frame(50, quantum), TrafficClass::Throughput)
        .expect("reserve one exact carrier command");
    assert_eq!(
        commands.pending_bytes(),
        quantum as u64,
        "reservation must precharge the carrier before Product publishes ownership"
    );
    let mut capacity_released = Box::pin(commands.capacity_notify().notified_owned());
    capacity_released.as_mut().enable();
    drop(cancelled);
    tokio::time::timeout(std::time::Duration::from_millis(200), capacity_released)
        .await
        .expect("dropping a precharged reservation wakes blocked admission");
    assert_eq!(
        commands.pending_bytes(),
        0,
        "dropping an unpublished reservation must return its charge"
    );

    let committed = commands
        .try_reserve_admitted_frame(stream_data_frame(51, quantum), TrafficClass::Throughput)
        .expect("reserve replacement carrier command");
    committed.commit();
    assert_eq!(
        commands.pending_bytes(),
        quantum as u64,
        "commit transfers, rather than duplicates or releases, byte ownership"
    );

    let command =
        try_recv_reliable_path_command(&mut receivers).expect("committed OriginalData command");
    assert_eq!(
        commands.pending_bytes(),
        quantum as u64,
        "dequeue retains the charge until the carrier writer releases it"
    );
    assert_eq!(commands.writer_pending_bytes(), quantum as u64);
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    assert_eq!(commands.pending_bytes(), 0);
    assert_eq!(commands.writer_pending_bytes(), 0);
}

#[test]
fn cloned_senders_pipeline_original_data_in_the_shared_bounded_queue() {
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let first = commands.clone();
    let second = commands.clone();
    let start = std::sync::Arc::new(std::sync::Barrier::new(2));
    let attempted = std::sync::Arc::new(std::sync::Barrier::new(2));
    let quantum = 64 * 1024;

    let (first_admitted, second_admitted) = std::thread::scope(|scope| {
        let first_start = start.clone();
        let first_attempted = attempted.clone();
        let first_thread = scope.spawn(move || {
            first_start.wait();
            let reservation = first.try_reserve_admitted_frame(
                stream_data_frame(60, quantum),
                TrafficClass::Throughput,
            );
            first_attempted.wait();
            match reservation {
                Ok(reservation) => {
                    reservation.commit();
                    true
                }
                Err(crate::runtime::RuntimeError::SenderServiceBlocked) => false,
                Err(error) => panic!("unexpected first reservation error: {error}"),
            }
        });

        let second_start = start.clone();
        let second_attempted = attempted.clone();
        let second_thread = scope.spawn(move || {
            second_start.wait();
            let reservation = second.try_reserve_admitted_frame(
                stream_data_frame(61, quantum),
                TrafficClass::Throughput,
            );
            second_attempted.wait();
            match reservation {
                Ok(reservation) => {
                    reservation.commit();
                    true
                }
                Err(crate::runtime::RuntimeError::SenderServiceBlocked) => false,
                Err(error) => panic!("unexpected second reservation error: {error}"),
            }
        });

        (
            first_thread.join().expect("first reservation thread"),
            second_thread.join().expect("second reservation thread"),
        )
    });

    assert_eq!(
        usize::from(first_admitted) + usize::from(second_admitted),
        2,
        "the shared bounded queue, not a one-quantum stop-and-wait lease, serializes Product actors"
    );
    assert_eq!(commands.pending_bytes(), (2 * quantum) as u64);
    for _ in 0..2 {
        let command = try_recv_reliable_path_command(&mut receivers)
            .expect("both admitted OriginalData commands remain bounded and ordered");
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    }
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
    assert_eq!(commands.pending_bytes(), 0);
}
