use super::{ClientTcpDatagramOpenCancellation, ClientTcpDatagramState};
use crate::protocol::{DatagramFlowId, DatagramId, Frame, TargetAddr};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

#[tokio::test]
async fn cancelled_attachment_open_queues_close_after_open() {
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (frames, _frame_rx) = mpsc::channel(1);
    let (failure, _failure_rx) = oneshot::channel();
    let (response, _response_rx) = oneshot::channel();
    let attachment_id = 41;
    commands
        .send_control(ReliablePathCommand::OpenDatagramAttachment {
            attachment_id,
            frames,
            failure,
            open_deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            response,
        })
        .await
        .expect("queue attachment open");

    drop(ClientTcpDatagramOpenCancellation::new(
        commands,
        attachment_id,
    ));

    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::OpenDatagramAttachment {
            attachment_id: 41,
            ..
        })
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseDatagramAttachment {
            attachment_id: 41,
            response: None,
        })
    ));
}

#[tokio::test]
async fn inbound_datagram_is_timestamped_before_attachment_queueing() {
    let attachment_id = 7;
    let flow_id = DatagramFlowId(9);
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let (frames, mut frame_rx) = mpsc::channel(1);
    let (failure, _failure_rx) = oneshot::channel();
    let mut state = ClientTcpDatagramState::new(4, 4);
    state
        .attach(attachment_id, frames, failure)
        .expect("attach route");
    state.commit_open_flow(attachment_id, flow_id, target);

    let routed_after = tokio::time::Instant::now();
    state
        .route_inbound(Frame::DatagramData {
            flow_id,
            datagram_id: DatagramId(3),
            ttl_ms: 5,
            payload: Bytes::from_static(b"response"),
        })
        .expect("route response");
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let inbound = frame_rx
        .recv()
        .await
        .expect("queued response")
        .expect("valid response");

    assert!(inbound.received_at >= routed_after);
    assert!(inbound.received_at < tokio::time::Instant::now());
}
