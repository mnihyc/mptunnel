use super::super::{
    Endpoint, MeasurementPhase, read_frame, write_frame, write_measurement_control,
};
use super::*;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, PathId};
use bytes::Bytes;
use std::time::{Duration, Instant};
use tokio::time::timeout;

#[tokio::test]
async fn quic_measurement_dedicated_writer_round_trips_declared_train() {
    let secret = b"0123456789abcdef0123456789abcdef";
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let path_id = PathId(3);
    let token = 0xabc_u64;
    let train_payload_bytes = 96 * 1024_u64;
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (receipt_consumed_tx, receipt_consumed_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read opener"),
            Frame::Ping { nonce: 1 }
        );
        write_frame(&mut send, &Frame::Pong { nonce: 1 }, limits)
            .await
            .expect("write opener response");
        let mut received = 0_u64;
        while received < train_payload_bytes {
            let frame = read_frame(&mut recv, limits)
                .await
                .expect("read capacity frame");
            let Frame::PathCapacityData {
                path_id: received_path_id,
                calibration_id,
                payload,
            } = frame
            else {
                panic!("dedicated capacity writer emitted a product frame");
            };
            assert_eq!(received_path_id, path_id);
            assert_eq!(calibration_id, token);
            received = received.saturating_add(payload.len() as u64);
        }
        assert_eq!(received, train_payload_bytes);
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read capacity finish"),
            Frame::PathCapacityFinish {
                path_id,
                calibration_id: token,
                payload_bytes: train_payload_bytes,
            }
        );
        assert!(matches!(
            write_measurement_control(&mut send, &Frame::Pong { nonce: 9 }, limits)
                .await
                .expect_err("product control must not bypass the ordinary writer"),
            QuicCarrierError::MeasurementRecordRequiresDedicatedWrite
        ));
        write_measurement_control(
            &mut send,
            &Frame::PathCapacityReceipt {
                path_id,
                calibration_id: token,
                received_payload_bytes: received,
            },
            limits,
        )
        .await
        .expect("write capacity receipt");
        // `write_all` queues into Quinn; retain the connection until the
        // peer consumes the receipt so endpoint teardown cannot overtake it.
        let _ = timeout(Duration::from_secs(5), receipt_consumed_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        secret,
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("write opener");
    assert_eq!(
        read_frame(&mut recv, limits)
            .await
            .expect("read opener response"),
        Frame::Pong { nonce: 1 }
    );
    assert!(matches!(
        write_frame(
            &mut send,
            &Frame::PathCapacityReceipt {
                path_id,
                calibration_id: token,
                received_payload_bytes: train_payload_bytes,
            },
            limits,
        )
        .await
        .expect_err("measurement control must not use the ordinary writer"),
        QuicCarrierError::MeasurementRecordRequiresDedicatedWrite
    ));
    // Quinn reports ACK-only datagrams through Controller::on_sent without
    // a matching on_ack, so native BIF is not an idle barrier. The exact
    // token receipt owns completion; this test exercises provisional I/O.
    let mut epoch = begin_measurement(
        &mut send,
        MeasurementSpec {
            token,
            train_payload_bytes,
            sample_floor_bytes: 64 * 1024,
            warmup_carrier_bytes: 32 * 1024,
            required_timed_carrier_bytes: 32 * 1024,
            expires_at: Instant::now() + Duration::from_secs(5),
            retention: Duration::from_secs(1),
        },
    )
    .await
    .expect("begin dedicated capacity train");
    assert!(matches!(
        epoch
            .write_data(&Frame::Ping { nonce: 11 }, limits)
            .await
            .expect_err("product data must not enter the measurement epoch"),
        QuicCarrierError::MeasurementRecordRequiresDedicatedWrite
    ));
    let chunk_bytes = 16 * 1024;
    let payload = Bytes::from(vec![0_u8; chunk_bytes]);
    let mut remaining = train_payload_bytes;
    while remaining > 0 {
        let payload_bytes = remaining.min(chunk_bytes as u64) as usize;
        epoch
            .write_data(
                &Frame::PathCapacityData {
                    path_id,
                    calibration_id: token,
                    payload: payload.slice(..payload_bytes),
                },
                limits,
            )
            .await
            .expect("write capacity data record");
        remaining -= payload_bytes as u64;
    }
    epoch
        .finish(
            &Frame::PathCapacityFinish {
                path_id,
                calibration_id: token,
                payload_bytes: train_payload_bytes,
            },
            limits,
        )
        .await
        .expect("finish dedicated capacity train");
    let metrics = connection.congestion_metrics();
    let epoch = metrics.measurement.expect("installed measurement epoch");
    assert_eq!(epoch.token, token);
    assert_eq!(epoch.written_payload_bytes, train_payload_bytes);
    assert_eq!(epoch.written_data_frame_count, 6);
    assert!(epoch.write_committed);
    assert_eq!(metrics.delivery_evidence_written_bytes, 0);
    assert!(send.encode_buffer.capacity() < train_payload_bytes as usize);

    let receipt = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("capacity receipt timeout")
        .expect("read capacity receipt");
    assert_eq!(
        receipt,
        Frame::PathCapacityReceipt {
            path_id,
            calibration_id: token,
            received_payload_bytes: train_payload_bytes,
        }
    );
    assert!(connection.confirm_measurement_receipt(token, train_payload_bytes, Instant::now(),));
    timeout(Duration::from_secs(5), async {
        loop {
            if connection
                .congestion_metrics()
                .measurement
                .is_some_and(|epoch| epoch.phase == MeasurementPhase::Complete)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("capacity receipt did not release carrier gate");
    let _ = receipt_consumed_tx.send(());

    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("capacity receiver timeout")
        .expect("capacity receiver task");
}
