use super::*;
use crate::model::path::CarrierPathInstanceId;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::prepared::PreparedNativeCommitmentInputs;
use bytes::Bytes;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::timeout;

/// The alternate delivery uses the real Product receiver/ACK producer. The late
/// Original uses authenticated H3 and its actual record-splitting writer. No
/// native offsets or framing overhead are reconstructed by this fixture.
#[tokio::test(flavor = "current_thread")]
async fn h3_operation_settlement_before_and_after_acceptance_gates_exact_ready_fifo() {
    let limits = CodecLimits::default();
    let mux = MuxLimits::default();
    let stream_id = StreamId(907);
    // One byte beyond the existing 12,000-byte QUIC Product record boundary.
    let payload = Bytes::from(vec![0x6d; 12_001]);
    let mut product_send = ReliableSendStream::new(stream_id, mux);
    let first = product_send.send_data(payload.clone()).unwrap();
    let second = product_send.send_data(payload.clone()).unwrap();
    let product_recv = Arc::new(Mutex::new(ReliableRecvStream::new(stream_id, mux)));
    let peer_product = product_recv.clone();
    let server = quic_transport::Endpoint::bind_server(
        "127.0.0.1:0".parse().unwrap(),
        &crate::transport::encrypted::test_server_tls_config(),
        quic_transport::test_candidate_verifier(),
        mux,
    )
    .await
    .unwrap();
    let address = server.local_addr().unwrap();
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let connection = server.accept().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        assert_eq!(
            quic_transport::read_frame(&mut recv, limits).await.unwrap(),
            Frame::Ping { nonce: 42 }
        );
        quic_transport::write_frame(&mut send, &Frame::Pong { nonce: 42 }, limits)
            .await
            .unwrap();
        let mut late_bytes = Vec::new();
        let mut control_tail_seen = false;
        while late_bytes.len() < 24_002 {
            match quic_transport::read_frame(&mut recv, limits).await.unwrap() {
                Frame::StreamData {
                    stream_id: actual,
                    offset,
                    payload,
                } => {
                    assert_eq!(actual, stream_id);
                    assert_eq!(offset, late_bytes.len() as u64);
                    late_bytes.extend_from_slice(&payload);
                    assert!(
                        peer_product
                            .lock()
                            .unwrap()
                            .receive_data(offset, payload)
                            .unwrap()
                            .delivered
                            .is_empty(),
                        "settled Original is a Product duplicate"
                    );
                }
                Frame::Ping { nonce: 43 } => {
                    assert_eq!(late_bytes.len(), 12_001);
                    control_tail_seen = true;
                }
                frame => panic!("unexpected H3 Product frame: {frame:?}"),
            }
        }
        assert_eq!(late_bytes, vec![0x6d; 24_002]);
        assert!(control_tail_seen);
        received_tx.send(()).unwrap();
        done_rx.await.unwrap();
    });
    let client = quic_transport::Endpoint::bind_client(
        "127.0.0.1:0".parse().unwrap(),
        &crate::transport::encrypted::test_client_tls_config(),
        quic_transport::test_candidate_selector(),
        mux,
    )
    .await
    .unwrap();
    let connection = client.connect(address).await.unwrap();
    let (stream, mut recv) = connection.open_bi().await.unwrap();
    let mut send = UdpPathSendStream {
        stream,
        native_commitment: None,
    };
    udp_path_write_frame(&mut send, &Frame::Ping { nonce: 42 }, limits)
        .await
        .unwrap();
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            quic_transport::read_frame(&mut recv, limits)
        )
        .await
        .unwrap()
        .unwrap(),
        Frame::Pong { nonce: 42 }
    );
    let commitment = send.bind_product_commitment(stream_id).unwrap();
    let (commands, mut writer) = reliable_path_command_channels(8);
    writer.bind_native_commitment(commitment.clone()).unwrap();
    let (independent_commands, mut independent_writer) = reliable_path_command_channels(8);
    let fifo = CarrierPathInstanceId::from_raw(907);
    let independent = CarrierPathInstanceId::from_raw(908);

    // First operation: alternate delivery and its positive ACK precede the
    // successful original API completion/operation association.
    let Frame::StreamData {
        offset, payload, ..
    } = &first
    else {
        unreachable!()
    };
    let delivered = product_recv
        .lock()
        .unwrap()
        .receive_data(*offset, payload.clone())
        .unwrap();
    assert_eq!(delivered.delivered.as_slice(), &[payload.clone()]);
    product_send
        .apply_ack(&product_recv.lock().unwrap().ack_ranges())
        .unwrap();
    let first_operation = [first, Frame::Ping { nonce: 43 }];
    let mut write = Box::pin(udp_path_write_frames(&mut send, &first_operation, limits));
    assert!(matches!(
        futures::poll!(&mut write),
        std::task::Poll::Ready(Ok(()))
    ));
    drop(write);
    let first_barrier = commitment
        .capture()
        .unwrap()
        .barrier(product_send.data_ack_frontier())
        .unwrap()
        .unwrap();

    // Second operation: positive ACK arrives after Native capture. The same
    // captured view must use current Product settlement without another query.
    let mut write = Box::pin(udp_path_write_frame(&mut send, &second, limits));
    assert!(matches!(
        futures::poll!(&mut write),
        std::task::Poll::Ready(Ok(()))
    ));
    drop(write);
    let captured = commitment.capture().unwrap();
    assert_eq!(
        captured
            .barrier(product_send.data_ack_frontier())
            .unwrap()
            .unwrap()
            .native_end(),
        first_barrier.native_end()
    );
    let Frame::StreamData {
        offset, payload, ..
    } = &second
    else {
        unreachable!()
    };
    let delivered = product_recv
        .lock()
        .unwrap()
        .receive_data(*offset, payload.clone())
        .unwrap();
    assert_eq!(delivered.delivered.as_slice(), &[payload.clone()]);
    product_send
        .apply_ack(&product_recv.lock().unwrap().ack_ranges())
        .unwrap();
    let barrier = captured
        .barrier(product_send.data_ack_frontier())
        .unwrap()
        .unwrap();
    assert!(barrier.native_end() > first_barrier.native_end());
    assert_eq!(product_send.reinjection_bytes(), 0);

    // Publish real writer capabilities only after recording successful writes.
    let receipt = writer.writer_ready_boundary(fifo).unwrap().receipt();
    let other_receipt = independent_writer
        .writer_ready_boundary(independent)
        .unwrap()
        .receipt();
    let inputs = PreparedNativeCommitmentInputs::capture([
        (fifo, commands.native_commitment()),
        (independent, independent_commands.native_commitment()),
    ]);
    let mut waits = Vec::new();
    assert_eq!(
        inputs.original_ready(
            vec![fifo, independent],
            product_send.data_ack_frontier(),
            &mut waits
        ),
        vec![independent]
    );
    assert!(receipt.is_current() && other_receipt.is_current());
    assert_eq!(waits.len(), 1);
    let mut crossing = Box::pin(barrier.wait_until_packetized());
    assert!(futures::poll!(&mut crossing).is_pending());
    timeout(Duration::from_secs(5), crossing)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(5), waits.pop().unwrap())
        .await
        .unwrap();
    timeout(Duration::from_secs(5), received_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        commitment
            .capture()
            .unwrap()
            .barrier(product_send.data_ack_frontier())
            .unwrap()
            .is_none()
    );
    let mut waits = Vec::new();
    let inputs = PreparedNativeCommitmentInputs::capture([(fifo, commands.native_commitment())]);
    assert_eq!(
        inputs.original_ready(vec![fifo], product_send.data_ack_frontier(), &mut waits),
        vec![fifo]
    );
    assert!(waits.is_empty());
    done_tx.send(()).unwrap();
    peer.await.unwrap();
}
