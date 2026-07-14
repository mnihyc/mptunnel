use super::*;

#[tokio::test]
async fn server_tcp_path_input_frame_bypasses_queued_bulk_output() {
    let (tx, mut commands_rx) = reliable_path_command_channels(1);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        FlowLane::Throughput,
    )
    .expect("fill bulk output command queue");
    let (frame_tx, mut path_frames) = mpsc::channel(1);
    frame_tx
        .send(Ok(Frame::Ping { nonce: 7 }))
        .await
        .expect("queue inbound ping");

    match recv_server_tcp_path_event(&mut path_frames, &mut commands_rx)
        .await
        .expect("server path event")
        .expect("event")
    {
        ServerTcpPathEvent::Frame(Frame::Ping { nonce }) => assert_eq!(nonce, 7),
        _ => panic!("expected inbound frame before queued bulk output"),
    }
}
