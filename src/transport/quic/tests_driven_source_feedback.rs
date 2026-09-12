//! Diagnostic producer comparison, not Product scheduler or release acceptance.
use super::super::Endpoint;
use super::*;
use crate::mux::MuxLimits;
use crate::protocol::StreamId;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::timeout;

/// The same finite, one-operation H3 producer is either a separate task or polled
/// by the Native driver. No flags, controller policy, window or batch size change
/// between arms. Finite loopback supply cannot establish ordinary Internet service.
#[tokio::test(flavor = "current_thread")]
async fn native_driven_h3_feedback_spike() {
    for driven in [false, true] {
        let limits = CodecLimits::default();
        let mux_limits = MuxLimits::default();
        // One existing configured memory ceiling of data, reused by reference in
        // bounded chunks. This is finite probe volume, not an admission policy.
        let total = mux_limits.max_path_flight_bytes;
        let chunk = mux_limits.max_reliable_relay_chunk_bytes.min(total);
        let payload = Bytes::from(vec![0x73; chunk]);
        let server = Endpoint::bind_server(
            "127.0.0.1:0".parse().unwrap(),
            &crate::transport::encrypted::test_server_tls_config(),
            super::super::test_candidate_verifier(),
            mux_limits,
        )
        .await
        .expect("server endpoint");
        let server_addr = server.local_addr().unwrap();
        let (received_tx, received_rx) = tokio::sync::oneshot::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await.expect("accepted connection");
            let (mut send, mut recv) = connection.accept_bi().await.expect("accepted H3 stream");
            assert_eq!(
                read_frame(&mut recv, limits).await.unwrap(),
                Frame::Ping { nonce: 71 }
            );
            write_frame(&mut send, &Frame::Pong { nonce: 71 }, limits)
                .await
                .unwrap();
            let mut received = 0usize;
            while received < total {
                let Frame::StreamData {
                    stream_id,
                    offset,
                    payload,
                } = read_frame(&mut recv, limits).await.expect("real H3 body")
                else {
                    panic!("expected stream data");
                };
                assert_eq!(stream_id, StreamId(37));
                assert_eq!(offset, received as u64);
                assert!(payload.iter().all(|byte| *byte == 0x73));
                received += payload.len();
            }
            assert_eq!(received, total);
            received_tx.send(received).unwrap();
            stop_rx.await.unwrap();
        });
        let client = Endpoint::bind_client(
            "127.0.0.1:0".parse().unwrap(),
            &crate::transport::encrypted::test_client_tls_config(),
            super::super::test_candidate_selector(),
            mux_limits,
        )
        .await
        .expect("client endpoint");
        let connection = client.connect(server_addr).await.expect("connected");
        let (mut send, mut recv) = connection.open_bi().await.expect("H3 stream");
        write_frame(&mut send, &Frame::Ping { nonce: 71 }, limits)
            .await
            .unwrap();
        assert_eq!(
            read_frame(&mut recv, limits).await.unwrap(),
            Frame::Pong { nonce: 71 }
        );
        let native = send.connection.clone();
        let observer = send
            .native_progress_observer()
            .expect("exact native observer");
        let backlog = send.write_backlog.clone();
        let initial_sample = native
            .congestion_state()
            .latest_bandwidth_sample()
            .map(|sample| {
                (
                    sample.revision.get(),
                    sample.valid,
                    sample.source_round,
                    sample.app_limited,
                )
            });
        let source_native = native.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let source = Box::pin(async move {
            let mut offset = 0usize;
            let mut operations = 0usize;
            let mut samples = BTreeMap::new();
            let mut largest_operation = 0u64;
            while offset < total {
                let pending = observer.snapshot().expect("live producer");
                observer
                    .wait_until_packetized(pending.accepted_end)
                    .await
                    .expect("exact packetization handoff");
                let before = observer.snapshot().unwrap();
                assert!(before.first_unpacketized >= before.accepted_end);
                let len = chunk.min(total - offset);
                let frame = Frame::StreamData {
                    stream_id: StreamId(37),
                    offset: offset as u64,
                    payload: payload.slice(..len),
                };
                // The real H3 future and its accepted prefix stay in this source
                // across Pending. There is exactly one operation being resolved.
                write_frame(&mut send, &frame, limits)
                    .await
                    .expect("H3 operation");
                operations += 1;
                offset += len;
                let after = observer.snapshot().unwrap();
                let envelope = after.accepted_end - before.accepted_end;
                largest_operation = largest_operation.max(envelope);
                assert!(after.accepted_end - after.first_unpacketized <= envelope);
                assert_eq!(backlog.load(Ordering::Relaxed), 0);
                if let Some(sample) = source_native.congestion_state().latest_bandwidth_sample() {
                    samples.insert(
                        sample.revision.get(),
                        (sample.valid, sample.source_round, sample.app_limited),
                    );
                }
            }
            let last = observer.snapshot().unwrap();
            observer
                .wait_until_packetized(last.accepted_end)
                .await
                .unwrap();
            finish_stream(&mut send).await.unwrap();
            done_tx
                .send((operations, largest_operation, samples))
                .unwrap();
        });
        let separate = if driven {
            native
                .register_transmit_source(source)
                .expect("source registration");
            None
        } else {
            Some(tokio::spawn(source))
        };
        let (operations, largest_operation, samples) = timeout(Duration::from_secs(15), done_rx)
            .await
            .expect("finite producer timeout")
            .expect("producer result");
        assert_eq!(
            timeout(Duration::from_secs(5), received_rx)
                .await
                .unwrap()
                .unwrap(),
            total
        );
        if let Some(task) = separate {
            task.await.expect("separate source");
        }
        // Source exhaustion must remain visible to the ordinary classifier.
        timeout(Duration::from_secs(5), async {
            while !native.active_path_snapshot().app_limited {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real source exhaustion classified");
        let qualifying_rounds: std::collections::BTreeSet<_> = samples
            .values()
            .filter(|(valid, _, limited)| *valid && !*limited)
            .map(|(_, round, _)| *round)
            .collect();
        eprintln!(
            "native_source_feedback_case {}",
            serde_json::json!({
                "driven": driven, "bytes": total, "chunk_bytes": chunk,
                "operations": operations, "largest_native_envelope": largest_operation,
            "initial_sample": initial_sample,
                "distinct_valid_non_app_limited_rounds": qualifying_rounds,
                "samples": samples, "source_exhaustion_app_limited": true,
                "scope": "finite real H3 loopback producer; no Product claim or Internet acceptance proof"
            })
        );
        stop_tx.send(()).unwrap();
        server_task.await.expect("receiver task");
        connection.close();
        if driven {
            // Three distinct qualified send rounds are the ordinary Startup
            // plateau requirement; this checks eligibility, not plateau equality.
            assert!(
                qualifying_rounds.len() >= 3,
                "driver-owned producer did not restore sustained native eligibility"
            );
        }
    }
}
