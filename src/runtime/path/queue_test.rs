use super::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_carrier_credit_bytes,
    reliable_path_command_channels, reliable_path_command_channels_with_send_credit,
    reliable_path_command_pending_bytes, reliable_path_effective_frame_lane,
    reliable_path_frame_uses_priority_queue, try_recv_reliable_path_command,
};
use crate::protocol::{
    DatagramFlowId, Frame, PathId, PathUsage, ResetReason, StreamDemandHint, StreamId, TargetAddr,
};
use crate::runtime::path::send_credit::{
    CarrierSendCredit, CarrierSendCreditSnapshot, CarrierSendCreditSource,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct TestCarrierSendCreditSource {
    limit_bytes: AtomicU64,
    committed_bytes: AtomicU64,
    closed: AtomicBool,
    notify: Arc<Notify>,
}

impl CarrierSendCreditSource for TestCarrierSendCreditSource {
    fn snapshot(&self) -> CarrierSendCreditSnapshot {
        CarrierSendCreditSnapshot {
            limit_bytes: self.limit_bytes.load(Ordering::Acquire),
            committed_bytes: self.committed_bytes.load(Ordering::Acquire),
            closed: self.closed.load(Ordering::Acquire),
        }
    }

    fn notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }
}

fn stream_data_frame(stream_id: u64, bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(stream_id),
        offset: 0,
        payload: Bytes::from(vec![0; bytes]),
    }
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
async fn control_close_discards_stale_stream_data_and_releases_carrier_credit() {
    let closed_stream = StreamId(20);
    let live_stream = StreamId(21);
    let source = Arc::new(TestCarrierSendCreditSource::default());
    source.limit_bytes.store(128 * 1024, Ordering::Release);
    let credit = CarrierSendCredit::new(source);
    let (commands, mut receivers) =
        reliable_path_command_channels_with_send_credit(4, credit.clone());
    let (observer, _observer_receivers) =
        reliable_path_command_channels_with_send_credit(1, credit);

    commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame(closed_stream.0, 64 * 1024),
            TrafficClass::Throughput,
        )
        .expect("queue data that becomes stale");
    commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame(live_stream.0, 64 * 1024),
            TrafficClass::Throughput,
        )
        .expect("queue live sibling data");
    assert!(!observer.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput));

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
    assert!(
        observer.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput),
        "discarding stale data must return its native carrier reservation"
    );
    receivers.release_pending_command_accounting(
        reliable_path_command_pending_bytes(&live),
        reliable_path_command_carrier_credit_bytes(&live),
    );
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
async fn connection_scoped_credit_is_shared_across_stream_queues() {
    let source = Arc::new(TestCarrierSendCreditSource::default());
    source.limit_bytes.store(14_600, Ordering::Release);
    let credit = CarrierSendCredit::new(source);
    let (first, mut first_rx) = reliable_path_command_channels_with_send_credit(4, credit.clone());
    let (second, _second_rx) = reliable_path_command_channels_with_send_credit(4, credit);

    first
        .try_enqueue_stream_ordered_frame(
            stream_data_frame(10, 64 * 1024),
            TrafficClass::Throughput,
        )
        .expect("first stream reserves the open native window");
    assert!(!second.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput));
    assert!(matches!(
        second.try_enqueue_stream_ordered_frame(
            stream_data_frame(11, 64 * 1024),
            TrafficClass::Throughput,
        ),
        Err(crate::runtime::RuntimeError::SenderServiceBlocked)
    ));

    let command = recv_reliable_path_command(&mut first_rx)
        .await
        .expect("first credited frame");
    assert!(!second.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput));
    first_rx.release_pending_command_accounting(
        reliable_path_command_pending_bytes(&command),
        reliable_path_command_carrier_credit_bytes(&command),
    );
    assert!(second.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput));
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

#[test]
fn reinjection_still_requires_native_carrier_credit() {
    let source = Arc::new(TestCarrierSendCreditSource::default());
    source.limit_bytes.store(64 * 1024, Ordering::Release);
    source.committed_bytes.store(64 * 1024, Ordering::Release);
    let (commands, _receivers) =
        reliable_path_command_channels_with_send_credit(4, CarrierSendCredit::new(source));
    let repair = stream_data_frame(44, 4096);

    assert!(!commands.can_enqueue_reinjection_frame_now(&repair));
    assert!(matches!(
        commands.try_enqueue_reinjection_frame(repair, TrafficClass::Throughput),
        Err(crate::runtime::RuntimeError::SenderServiceBlocked)
    ));
}

#[test]
fn dropped_atomic_frame_reservation_returns_carrier_credit() {
    let source = Arc::new(TestCarrierSendCreditSource::default());
    source.limit_bytes.store(64 * 1024, Ordering::Release);
    let credit = CarrierSendCredit::new(source);
    let (first, _first_rx) = reliable_path_command_channels_with_send_credit(4, credit.clone());
    let (second, _second_rx) = reliable_path_command_channels_with_send_credit(4, credit);

    let reservation = first
        .try_reserve_stream_ordered_frame(
            stream_data_frame(20, 64 * 1024),
            TrafficClass::Throughput,
        )
        .expect("reserve queue and carrier credit");
    assert!(!second.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput));
    drop(reservation);
    assert!(second.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput));
}

#[test]
fn zero_byte_control_remains_admissible_when_native_data_window_is_closed() {
    let source = Arc::new(TestCarrierSendCreditSource::default());
    source.limit_bytes.store(64 * 1024, Ordering::Release);
    source.committed_bytes.store(64 * 1024, Ordering::Release);
    let (commands, _receivers) =
        reliable_path_command_channels_with_send_credit(4, CarrierSendCredit::new(source));
    let ack = Frame::StreamAck {
        stream_id: StreamId(30),
        complete: false,
        ranges: Vec::new(),
    };

    assert!(commands.can_enqueue_frame_now(&ack, TrafficClass::Control));
    commands
        .try_enqueue_admitted_frame(ack, TrafficClass::Control)
        .expect("control does not consume carrier data credit");
    assert!(!commands.can_enqueue_stream_ordered_frame_now(TrafficClass::Throughput));
}
