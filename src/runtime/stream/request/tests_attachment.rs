use super::*;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::protocol::PathId;
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_priority_command,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
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
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(path_index as u16),
            commands,
            limits,
        ),
        frames: frames_rx.into(),
    };
    (OpenedRemoteStream::pending(stream, path_index), frames_tx)
}

#[tokio::test]
async fn reserved_attachment_adoption_preserves_exact_identity_without_double_allocation() {
    let stream_id = StreamId(800);
    let (initial, _initial_frames) = opened_stream_at(stream_id, 0);
    let mut remotes = ReliableRelayRemoteSet::new(initial, 4);
    let membership_before_adoption = remotes.membership_generation();

    let reservation = remotes.reserve_attachment_incarnation();
    let (candidate, _candidate_frames) = opened_stream_at(stream_id, 1);
    let expected = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        },
        path_instance_id: candidate.path_instance_id,
        attachment_id: reservation.attachment_id(),
    };
    let reservation = remotes
        .bind_validation_attachment(reservation, expected)
        .expect("bind exact validation attachment");

    assert!(remotes.validation_attachment_is_current(expected));
    assert!(!remotes.contains_path_instance(expected));
    assert!(remotes.contains_flight_owner_instance(expected));
    assert_eq!(
        remotes.membership_generation(),
        membership_before_adoption,
        "validation flight ownership is not ordinary membership",
    );

    assert_eq!(
        remotes
            .adopt_reserved_attachment(candidate, reservation)
            .expect("adopt acknowledged validation attachment"),
        ReliableRelayAttachOutcome::Attached,
    );
    assert_eq!(remotes.paths[1].instance(), expected);
    assert!(!remotes.validation_attachment_is_current(expected));
    assert!(remotes.contains_path_instance(expected));
    assert_eq!(
        remotes.membership_generation(),
        membership_before_adoption.wrapping_add(1),
        "one adoption is one membership transition",
    );

    let (ordinary, _ordinary_frames) = opened_stream_at(stream_id, 2);
    assert_eq!(
        remotes.attach(ordinary),
        ReliableRelayAttachOutcome::Attached
    );
    assert_eq!(
        remotes.paths[2].attachment_id,
        expected.attachment_id.wrapping_add(1),
        "adoption consumes the pre-reserved incarnation without allocating a second one",
    );
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
        remotes.recv_frame().await.expect("FIN frame").frame,
        Ok(Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 8,
        }) if received_stream_id == stream_id
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
            .await
            .expect("carrier failure deadline")
            .expect("carrier failure frame")
            .frame,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
}

#[tokio::test]
async fn receive_only_attachment_fans_in_data_and_feedback_without_request_authority() {
    let stream_id = StreamId(802);
    let (opened, _ordinary_frames) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let ordinary_membership = remotes.membership_generation();
    let ordinary_paths = remotes.path_keys();
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 2,
        },
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 7,
    };
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (frames_tx, frames) = mpsc::channel(4);

    assert!(remotes.attach_receive_only(instance, commands, frames));
    assert_eq!(remotes.membership_generation(), ordinary_membership);
    assert_eq!(remotes.path_keys(), ordinary_paths);
    assert_eq!(remotes.accepted_path_count(), 1);
    assert!(!remotes.contains_path_instance(instance));
    assert!(remotes.has_receive_feedback_output());

    let max_data = remotes.publish_max_data(4096);
    assert_eq!(max_data.published_offset, Some(4096));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
            stream_id: received_stream_id,
            max_offset: 4096,
        })) if received_stream_id == stream_id
    ));
    let ack = remotes.publish_stream_ack(
        1,
        vec![Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        }],
    );
    assert!(ack.published);
    assert!(!ack.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            stream_id: received_stream_id,
            complete: true,
            ..
        })) if received_stream_id == stream_id
    ));

    let data = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: bytes::Bytes::from_static(b"receive-only"),
    };
    frames_tx
        .send(Ok(data.clone()))
        .await
        .expect("queue receive-only data");
    let received = remotes.recv_frame().await.expect("receive-only input");
    assert_eq!(received.instance, instance);
    assert_eq!(received.frame.expect("receive-only frame"), data);
    assert!(remotes.remove_receive_only_instance(instance));
    assert!(!remotes.is_receive_only_instance(instance));
}
