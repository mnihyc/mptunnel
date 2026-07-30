use super::*;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::model::tcp_service::TcpServiceWriterLifecycle;
use crate::protocol::{PathId, PathMetricDirection, SessionId};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::runtime::tcp_service::{RequestTcpServiceControl, TcpServiceObserverRemoval};
use std::time::Duration;

fn opened_stream(
    stream_id: StreamId,
) -> (
    OpenedRemoteStream,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    opened_stream_at(stream_id, 0)
}

fn opened_stream_at(
    stream_id: StreamId,
    path_index: usize,
) -> (
    OpenedRemoteStream,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(4);
    let (frames_tx, frames_rx) = mpsc::channel(4);
    let stream = ReliablePathStream {
        stream_id,
        max_offset: limits.max_stream_window_bytes,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(0), commands, limits),
        frames: frames_rx.into(),
    };
    (OpenedRemoteStream::pending(stream, path_index), frames_tx)
}

#[tokio::test]
async fn carrier_failure_after_fin_remains_visible_to_attachment_owner() {
    let stream_id = StreamId(801);
    let (opened, frames_tx) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 8,
        }))
        .await
        .expect("queue FIN");
    frames_tx
        .send(Err(RuntimeError::ReliablePathSessionClosed))
        .await
        .expect("queue carrier failure");

    assert!(matches!(
        remotes.recv_event().await.expect("FIN event"),
        RequestRelayActorEvent::Frame(ReliableRelayRemoteFrame {
            frame:
        Ok(Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 8,
        }),
            ..
        }) if received_stream_id == stream_id
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), remotes.recv_event())
            .await
            .expect("carrier failure deadline")
            .expect("carrier failure event"),
        RequestRelayActorEvent::Frame(ReliableRelayRemoteFrame {
            frame: Err(RuntimeError::ReliablePathSessionClosed),
            ..
        })
    ));
}

#[tokio::test]
async fn tcp_service_controls_share_fifo_and_survive_zero_attachments() {
    let stream_id = StreamId(802);
    let (opened, _frames_tx) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let instance = remotes.path_instances()[0];
    let writer = remotes.tcp_service_writer();
    let lifecycle = TcpServiceWriterLifecycle::for_runtime_test(
        SessionId(8),
        2,
        PathMetricDirection::ClientToServer,
    );
    let first = ReliableRelayRemoteFrame {
        instance,
        frame: Ok(Frame::StreamMaxData {
            stream_id,
            max_offset: 1,
        }),
    };
    let second = ReliableRelayRemoteFrame {
        instance,
        frame: Ok(Frame::StreamMaxData {
            stream_id,
            max_offset: 2,
        }),
    };
    remotes
        .events_tx
        .send(RequestRelayActorEvent::Frame(first))
        .await
        .expect("queue first carrier frame");
    let (removed_tx, removed_rx) = tokio::sync::oneshot::channel();
    writer
        .send(RequestTcpServiceControl::Remove {
            lifecycle,
            receipt: removed_tx,
        })
        .await
        .expect("queue lifecycle boundary");
    remotes
        .events_tx
        .send(RequestRelayActorEvent::Frame(second))
        .await
        .expect("queue second carrier frame");

    assert!(matches!(
        remotes.try_recv_frame(),
        Some(ReliableRelayRemoteFrame {
            frame: Ok(Frame::StreamMaxData { max_offset: 1, .. }),
            ..
        })
    ));
    assert!(
        remotes.try_recv_frame().is_none(),
        "ready-frame batching stops at the lifecycle boundary"
    );
    match remotes.recv_event().await.expect("lifecycle event") {
        RequestRelayActorEvent::TcpService(control) => match *control {
            RequestTcpServiceControl::Remove {
                lifecycle: observed,
                receipt,
            } => {
                assert_eq!(observed, lifecycle);
                let _ = receipt.send(TcpServiceObserverRemoval::AlreadyAbsent);
            }
            _ => panic!("unexpected TCP service control"),
        },
        RequestRelayActorEvent::Frame(_) => panic!("control boundary was bypassed"),
    }
    assert_eq!(
        removed_rx.await.expect("removal receipt"),
        TcpServiceObserverRemoval::AlreadyAbsent
    );
    assert!(matches!(
        remotes.try_recv_frame(),
        Some(ReliableRelayRemoteFrame {
            frame: Ok(Frame::StreamMaxData { max_offset: 2, .. }),
            ..
        })
    ));

    remotes
        .remove_path_instance(instance)
        .expect("remove final attachment");
    let (disconnected_tx, disconnected_rx) = tokio::sync::oneshot::channel();
    writer
        .send(RequestTcpServiceControl::Remove {
            lifecycle,
            receipt: disconnected_tx,
        })
        .await
        .expect("queue disconnected cleanup");
    match remotes
        .recv_event()
        .await
        .expect("disconnected lifecycle event")
    {
        RequestRelayActorEvent::TcpService(control) => match *control {
            RequestTcpServiceControl::Remove { receipt, .. } => {
                let _ = receipt.send(TcpServiceObserverRemoval::AlreadyAbsent);
            }
            _ => panic!("unexpected disconnected TCP service control"),
        },
        RequestRelayActorEvent::Frame(_) => panic!("disconnected cleanup was bypassed"),
    }
    assert_eq!(
        disconnected_rx.await.expect("disconnected receipt"),
        TcpServiceObserverRemoval::AlreadyAbsent
    );
}

#[tokio::test]
async fn exhausted_attachment_incarnation_invalidates_the_changed_topology() {
    let stream_id = StreamId(803);
    let (first, _first_frames) = opened_stream_at(stream_id, 0);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    let (second, _second_frames) = opened_stream_at(stream_id, 1);
    assert_eq!(remotes.attach(second), ReliableRelayAttachOutcome::Attached);
    let stale_generation = remotes.membership_generation();
    let instances = remotes.path_instances();
    let [removed, survivor] = instances.as_slice() else {
        panic!("two exact attachments");
    };
    let removed = *removed;
    let survivor = *survivor;

    remotes.membership_generation = u64::MAX;
    remotes
        .remove_path_instance(removed)
        .expect("carrier loss remains unavoidable at identity exhaustion");

    assert_eq!(
        remotes.path_position_at_generation(stale_generation, survivor),
        None,
        "a changed topology can never retain stale placement authority"
    );
    assert_eq!(remotes.accepted_attachment_incarnation(), None);
    let (replacement, _replacement_frames) = opened_stream_at(stream_id, 0);
    assert_eq!(
        remotes.attach(replacement),
        ReliableRelayAttachOutcome::RejectedResourceLimit
    );
}
