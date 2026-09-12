use super::super::Endpoint;
use super::*;
use crate::mux::MuxLimits;
use crate::protocol::{
    CloseReason, DatagramFlowId, DatagramId, IpPacketId, IpTunnelId, StreamId, TargetAddr,
};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::timeout;

struct NativeSourceH3Fixture {
    _server: Endpoint,
    _client: Endpoint,
    client_connection: super::super::Connection,
    server_connection: super::super::Connection,
    send: SendStream,
    _recv: RecvStream,
    _server_send: SendStream,
    server_recv: RecvStream,
}

async fn native_source_h3_fixture() -> NativeSourceH3Fixture {
    // Restrict this stream's peer credit, keeping connection credit available
    // for a sibling. The test payload crosses both this limit and MPP records.
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024,
        ..MuxLimits::default()
    };
    let limits = CodecLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let (client_connection, server_connection) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            client.connect(server.local_addr().expect("server local addr")),
            server.accept()
        )
    })
    .await
    .expect("connection lifecycle timeout");
    let client_connection = client_connection.expect("client connection");
    let server_connection = server_connection.expect("server connection");
    let (client_stream, server_stream) = timeout(Duration::from_secs(5), async {
        tokio::join!(client_connection.open_bi(), server_connection.accept_bi())
    })
    .await
    .expect("request lifecycle timeout");
    let (mut send, mut recv) = client_stream.expect("client request");
    let (mut server_send, mut server_recv) = server_stream.expect("server request");
    timeout(Duration::from_secs(5), async {
        write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
            .await
            .expect("warmup ping");
        assert_eq!(
            read_frame(&mut server_recv, limits)
                .await
                .expect("warmup request"),
            Frame::Ping { nonce: 1 }
        );
        write_frame(&mut server_send, &Frame::Pong { nonce: 1 }, limits)
            .await
            .expect("warmup pong");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("warmup response"),
            Frame::Pong { nonce: 1 }
        );
    })
    .await
    .expect("warmup lifecycle timeout");
    NativeSourceH3Fixture {
        _server: server,
        _client: client,
        client_connection,
        server_connection,
        send,
        _recv: recv,
        _server_send: server_send,
        server_recv,
    }
}

/// Actual source ownership crosses two runtime workers here: Native has begun
/// a framed write, while the original owner cancels its handle on another
/// worker. The gates control finite poll/drop boundaries, not network policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_native_source_cancel_fences_actual_drop_and_preserves_h3_sibling() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::task::{Context, Poll};

    const LIFECYCLE_GUARD: Duration = Duration::from_secs(5);

    struct LastSourceDrop {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
        count: Arc<AtomicUsize>,
    }

    impl Drop for LastSourceDrop {
        fn drop(&mut self) {
            self.entered
                .take()
                .expect("one source destruction")
                .send(())
                .expect("source destruction observer");
            self.release
                .recv_timeout(LIFECYCLE_GUARD)
                .expect("release finite source destruction");
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ActualSource<F> {
        // Field order makes the marker run after the actual H3 future/stream.
        future: Pin<Box<F>>,
        _last_drop: LastSourceDrop,
    }

    impl<F: Future> Future for ActualSource<F> {
        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.get_mut().future.as_mut().poll(cx)
        }
    }

    let NativeSourceH3Fixture {
        _server,
        _client,
        client_connection,
        server_connection,
        mut send,
        _recv,
        _server_send,
        server_recv: _server_recv,
    } = native_source_h3_fixture().await;
    let limits = CodecLimits::default();
    let native = send.connection.clone();
    let domain = quinn::ExecutionDomain::default();
    native
        .bind_execution_domain(domain.clone())
        .expect("bind actual client driver");
    assert!(
        native
            .execution_domain()
            .expect("actual driver retains binding")
            .same_domain(&domain)
    );
    let (client_sibling, server_sibling) = timeout(LIFECYCLE_GUARD, async {
        tokio::join!(client_connection.open_bi(), server_connection.accept_bi())
    })
    .await
    .expect("sibling attachment lifecycle");
    let (mut sibling_send, mut sibling_recv) = client_sibling.expect("client sibling");
    let (mut peer_send, mut peer_recv) = server_sibling.expect("server sibling");
    let (poll_entered_tx, poll_entered_rx) = tokio::sync::oneshot::channel();
    let (poll_release_tx, poll_release_rx) = std::sync::mpsc::channel();
    let (drop_entered_tx, drop_entered_rx) = tokio::sync::oneshot::channel();
    let (drop_release_tx, drop_release_rx) = std::sync::mpsc::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = send
        .native_source_registration()
        .register(ActualSource {
            future: Box::pin(async move {
                write_frame(&mut send, &Frame::Ping { nonce: 2 }, limits)
                    .await
                    .expect("actual source framed write");
                poll_entered_tx
                    .send(std::thread::current().id())
                    .expect("actual Native source poll observer");
                poll_release_rx
                    .recv_timeout(LIFECYCLE_GUARD)
                    .expect("release finite actual source poll");
                std::future::pending::<()>().await;
            }),
            _last_drop: LastSourceDrop {
                entered: Some(drop_entered_tx),
                release: drop_release_rx,
                count: drops.clone(),
            },
        })
        .expect("register actual bound source");
    let source_worker = timeout(LIFECYCLE_GUARD, poll_entered_rx)
        .await
        .expect("actual source poll lifecycle")
        .expect("actual source entered Native poll");
    let (cancel_started_tx, cancel_started_rx) = tokio::sync::oneshot::channel();
    let cancel_returned = Arc::new(AtomicBool::new(false));
    let cancel_observed = cancel_returned.clone();
    let cancelled_drops = drops.clone();
    let cancel_domain = domain.clone();
    let cancel_task = tokio::spawn(async move {
        cancel_started_tx
            .send(std::thread::current().id())
            .expect("cancelling worker observer");
        // This exercises wrapper Drop without first polling the owner handle,
        // as can happen when a freshly spawned Product task is aborted.
        drop(cancel_domain.wrap(owner));
        assert_eq!(cancelled_drops.load(Ordering::SeqCst), 1);
        cancel_observed.store(true, Ordering::SeqCst);
    });
    let cancel_worker = timeout(LIFECYCLE_GUARD, cancel_started_rx)
        .await
        .expect("other worker cancellation lifecycle")
        .expect("other worker started cancellation");
    assert_ne!(source_worker, cancel_worker);
    assert!(!cancel_returned.load(Ordering::SeqCst));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    poll_release_tx.send(()).expect("finish actual source poll");
    timeout(LIFECYCLE_GUARD, drop_entered_rx)
        .await
        .expect("source destructor lifecycle")
        .expect("actual source fields destroyed before final marker");
    assert!(!cancel_returned.load(Ordering::SeqCst));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop_release_tx
        .send(())
        .expect("finish actual source destruction");
    timeout(LIFECYCLE_GUARD, cancel_task)
        .await
        .expect("cancellation completion lifecycle")
        .expect("cancelling worker did not panic");
    assert!(cancel_returned.load(Ordering::SeqCst));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(native.close_reason().is_none());

    let sibling = sibling_send
        .native_source_registration()
        .register(async move {
            write_frame(&mut sibling_send, &Frame::Ping { nonce: 42 }, limits)
                .await
                .expect("bound sibling write after cancellation");
            read_frame(&mut sibling_recv, limits)
                .await
                .expect("bound sibling response after cancellation")
        })
        .expect("register sibling on the same actual bound driver");
    let (response, ()) = timeout(LIFECYCLE_GUARD, async {
        tokio::join!(domain.wrap(sibling), async {
            assert_eq!(
                read_frame(&mut peer_recv, limits)
                    .await
                    .expect("actual sibling request"),
                Frame::Ping { nonce: 42 }
            );
            write_frame(&mut peer_send, &Frame::Pong { nonce: 42 }, limits)
                .await
                .expect("actual sibling response");
        })
    })
    .await
    .expect("bound sibling remains serviceable");
    assert_eq!(
        response.expect("bound sibling source live"),
        Frame::Pong { nonce: 42 }
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// This counts adapter operations, not Product claims. Every operation uses
/// real MPP framing and an exclusively owned H3 send half; native observation
/// only gates the next operation after the previous accepted envelope crosses.
#[tokio::test(flavor = "current_thread")]
async fn native_driven_h3_source_retains_partial_write_and_resumes_after_packetization() {
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;

    let NativeSourceH3Fixture {
        _server,
        _client,
        client_connection,
        server_connection,
        mut send,
        _recv,
        _server_send,
        mut server_recv,
    } = native_source_h3_fixture().await;
    let limits = CodecLimits::default();
    let payloads = [
        Bytes::from(vec![0x35; QUIC_STREAM_RECORD_PAYLOAD_BYTES * 2 + 17]),
        Bytes::from(vec![0xa7; QUIC_STREAM_RECORD_PAYLOAD_BYTES * 2 + 17]),
        Bytes::from_static(b"source available again"),
    ];
    let expected: Vec<u8> = payloads
        .iter()
        .flat_map(|bytes| bytes.iter().copied())
        .collect();
    let native = send.connection.clone();
    let observer = send
        .native_progress_observer()
        .expect("established observer");
    let write_backlog = send.write_backlog.clone();
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let source_started = started.clone();
    let source_completed = completed.clone();
    let source_backlog = write_backlog.clone();
    let (partial_tx, partial_rx) = tokio::sync::oneshot::channel();
    let (exhausted_tx, exhausted_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

    native
        .register_transmit_source(Box::pin(async move {
            let mut partial_tx = Some(partial_tx);
            let mut exhausted_tx = Some(exhausted_tx);
            let mut resume_rx = Some(resume_rx);
            let initial = observer.snapshot().expect("initial progress");
            observer
                .wait_until_packetized(initial.accepted_end)
                .await
                .expect("warmup packetized");
            let mut offset = 0;
            for (index, payload) in payloads.into_iter().enumerate() {
                if index == 2 {
                    let idle = observer.snapshot().expect("source exhaustion progress");
                    assert_eq!(idle.first_unpacketized, idle.accepted_end);
                    assert_eq!(source_backlog.load(Ordering::Relaxed), 0);
                    exhausted_tx
                        .take()
                        .expect("one exhaustion")
                        .send(idle)
                        .expect("exhaustion receiver");
                    // This is a real absence of source work, not native credit.
                    // Its unrelated wake must resume the registered future.
                    resume_rx
                        .take()
                        .expect("one resume")
                        .await
                        .expect("resume source");
                }
                let before = observer.snapshot().expect("operation boundary");
                assert_eq!(before.first_unpacketized, before.accepted_end);
                assert_eq!(source_started.load(Ordering::Relaxed), index);
                assert_eq!(source_completed.load(Ordering::Relaxed), index);
                source_started.fetch_add(1, Ordering::Relaxed);
                let payload_len = payload.len();
                let frame = Frame::StreamData {
                    stream_id: StreamId(7),
                    offset,
                    payload,
                };
                let mut write = Box::pin(write_frame(&mut send, &frame, limits));
                std::future::poll_fn(|cx| {
                    let result = write.as_mut().poll(cx);
                    if result.is_pending()
                        && let Some(notice) = partial_tx.take()
                    {
                        let partial = observer.snapshot().expect("partial native acceptance");
                        // The peer has not consumed the bulk body. A positive
                        // accepted prefix followed by Pending belongs to this
                        // same future until credit arrives; it is not refusal.
                        assert!(partial.accepted_end > before.accepted_end);
                        assert!(partial.accepted_end - before.accepted_end < payload_len as u64);
                        let charge = source_backlog.load(Ordering::Relaxed);
                        assert!(charge > 0);
                        notice
                            .send((before, partial, charge))
                            .expect("partial receiver");
                    }
                    result
                })
                .await
                .expect("retained H3 write completes");
                drop(write);
                source_completed.fetch_add(1, Ordering::Relaxed);
                assert_eq!(source_backlog.load(Ordering::Relaxed), 0);
                let accepted = observer.snapshot().expect("complete native envelope");
                assert!(accepted.accepted_end > before.accepted_end);
                let packetized = observer
                    .wait_until_packetized(accepted.accepted_end)
                    .await
                    .expect("exact operation packetization wake");
                assert_eq!(packetized.first_unpacketized, packetized.accepted_end);
                offset += payload_len as u64;
            }
            let final_progress = observer.snapshot().expect("final packetization");
            finish_stream(&mut send)
                .await
                .expect("finish finite source");
            finished_tx
                .send(final_progress)
                .expect("source completion receiver");
        }))
        .expect("register exclusive H3 source");

    let (before, partial, charge) = timeout(Duration::from_secs(5), partial_rx)
        .await
        .expect("partial write lifecycle timeout")
        .expect("partial write notice");
    assert!(partial.accepted_end > before.accepted_end);
    assert_eq!(started.load(Ordering::Relaxed), 1);
    assert_eq!(completed.load(Ordering::Relaxed), 0);
    assert_eq!(write_backlog.load(Ordering::Relaxed), charge);

    // No bulk read happens before this independent control round trip.
    let (mut control_send, mut control_recv) =
        timeout(Duration::from_secs(5), client_connection.open_bi())
            .await
            .expect("sibling open timeout")
            .expect("sibling request");
    timeout(Duration::from_secs(5), async {
        write_frame(&mut control_send, &Frame::Ping { nonce: 99 }, limits)
            .await
            .expect("sibling ping");
        let (mut peer_send, mut peer_recv) = server_connection
            .accept_bi()
            .await
            .expect("sibling accepted");
        assert_eq!(
            read_frame(&mut peer_recv, limits)
                .await
                .expect("sibling request"),
            Frame::Ping { nonce: 99 }
        );
        write_frame(&mut peer_send, &Frame::Pong { nonce: 99 }, limits)
            .await
            .expect("sibling pong");
        assert_eq!(
            read_frame(&mut control_recv, limits)
                .await
                .expect("sibling response"),
            Frame::Pong { nonce: 99 }
        );
    })
    .await
    .expect("sibling progress while bulk flow-blocked");
    assert_eq!(started.load(Ordering::Relaxed), 1);
    assert_eq!(completed.load(Ordering::Relaxed), 0);
    assert_eq!(write_backlog.load(Ordering::Relaxed), charge);

    let receiver = tokio::spawn(async move {
        let mut received = Vec::new();
        while received.len() < expected.len() {
            let Frame::StreamData {
                stream_id,
                offset,
                payload,
            } = read_frame(&mut server_recv, limits)
                .await
                .expect("bulk record")
            else {
                panic!("expected bulk STREAM_DATA")
            };
            assert_eq!(stream_id, StreamId(7));
            assert_eq!(offset, received.len() as u64);
            received.extend_from_slice(&payload);
        }
        assert_eq!(received, expected);
        server_recv
    });
    let exhausted = timeout(Duration::from_secs(5), exhausted_rx)
        .await
        .expect("finite source drain timeout")
        .expect("source exhausted");
    assert_eq!(exhausted.first_unpacketized, exhausted.accepted_end);
    assert_eq!(started.load(Ordering::Relaxed), 2);
    assert_eq!(completed.load(Ordering::Relaxed), 2);
    assert_eq!(write_backlog.load(Ordering::Relaxed), 0);
    // A registered source waiting for new input must still allow ordinary
    // native empty classification. This is not a BBR round-eligibility proof.
    timeout(Duration::from_secs(5), async {
        while !native.active_path_snapshot().app_limited {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("genuinely exhausted source can be application limited");
    resume_tx
        .send(())
        .expect("publish unrelated source availability");
    let finished = timeout(Duration::from_secs(5), finished_rx)
        .await
        .expect("resumed source timeout")
        .expect("finite source finished");
    assert_eq!(finished.first_unpacketized, finished.accepted_end);
    assert_eq!(started.load(Ordering::Relaxed), 3);
    assert_eq!(completed.load(Ordering::Relaxed), 3);
    assert_eq!(write_backlog.load(Ordering::Relaxed), 0);
    let _server_recv = timeout(Duration::from_secs(5), receiver)
        .await
        .expect("exact receiver timeout")
        .expect("exact receiver task");
    native.close(0u32.into(), b"finite H3 source complete");
}

struct NativeSourceDropNotice(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for NativeSourceDropNotice {
    fn drop(&mut self) {
        if let Some(notice) = self.0.take() {
            let _ = notice.send(());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_driven_h3_source_connection_close_drops_in_progress_write() {
    use std::future::Future;

    let NativeSourceH3Fixture {
        _server,
        _client,
        client_connection: _client_connection,
        server_connection: _server_connection,
        mut send,
        _recv,
        _server_send,
        server_recv: _server_recv,
    } = native_source_h3_fixture().await;
    let limits = CodecLimits::default();
    let native = send.connection.clone();
    let observer = send
        .native_progress_observer()
        .expect("established observer");
    let source_observer = observer.clone();
    let backlog = send.write_backlog.clone();
    let source_backlog = backlog.clone();
    let (partial_tx, partial_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    native
        .register_transmit_source(Box::pin(async move {
            let _drop_notice = NativeSourceDropNotice(Some(dropped_tx));
            let before = source_observer.snapshot().expect("initial native frontier");
            source_observer
                .wait_until_packetized(before.accepted_end)
                .await
                .expect("warmup packetized");
            let payload_len = QUIC_STREAM_RECORD_PAYLOAD_BYTES * 2 + 17;
            let frame = Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                payload: Bytes::from(vec![0x5c; payload_len]),
            };
            let mut partial_tx = Some(partial_tx);
            let mut write = Box::pin(write_frame(&mut send, &frame, limits));
            let result = std::future::poll_fn(|cx| {
                let result = write.as_mut().poll(cx);
                if result.is_pending()
                    && let Some(notice) = partial_tx.take()
                {
                    let partial = source_observer.snapshot().expect("partial native frontier");
                    assert!(partial.accepted_end > before.accepted_end);
                    assert!(partial.accepted_end - before.accepted_end < payload_len as u64);
                    assert!(source_backlog.load(Ordering::Relaxed) > 0);
                    notice.send(partial).expect("partial receiver");
                }
                result
            })
            .await;
            assert!(
                result.is_err(),
                "held peer credit cannot complete this operation"
            );
        }))
        .expect("register retained terminal source");
    let partial = timeout(Duration::from_secs(5), partial_rx)
        .await
        .expect("partial terminal write timeout")
        .expect("partial terminal write");
    assert!(partial.accepted_end > 0);
    assert!(backlog.load(Ordering::Relaxed) > 0);
    assert!(native.close_reason().is_none());
    native.close(0u32.into(), b"cancel retained H3 operation");
    timeout(Duration::from_secs(5), dropped_rx)
        .await
        .expect("driver source drop timeout")
        .expect("driver dropped source");
    assert_eq!(backlog.load(Ordering::Relaxed), 0);
    assert!(native.close_reason().is_some());
    assert!(observer.snapshot().is_err());
}

#[test]
fn quic_writer_splits_large_stream_data_below_product_scheduler() {
    let limits = CodecLimits::default();
    let payload = Bytes::from(vec![7u8; QUIC_STREAM_RECORD_PAYLOAD_BYTES * 2 + 17]);
    let mut packet = Vec::new();
    encode_quic_length_prefixed_frame(
        &Frame::StreamData {
            stream_id: StreamId(9),
            offset: 123,
            payload,
        },
        limits,
        &mut packet,
    )
    .expect("encode split stream data");

    let mut cursor = 0usize;
    let mut decoded = Vec::new();
    while cursor < packet.len() {
        let len = u32::from_be_bytes([
            packet[cursor],
            packet[cursor + 1],
            packet[cursor + 2],
            packet[cursor + 3],
        ]) as usize;
        cursor += FRAME_LEN_BYTES;
        let frame = decode_frame_bytes(
            Bytes::copy_from_slice(&packet[cursor..cursor + len]),
            limits,
        )
        .expect("decode split carrier record");
        decoded.push(frame);
        cursor += len;
    }

    assert_eq!(decoded.len(), 3);
    let mut expected_offset = 123u64;
    for frame in &decoded {
        let Frame::StreamData {
            stream_id,
            offset,
            payload,
        } = frame
        else {
            panic!("all split records must remain STREAM_DATA");
        };
        assert_eq!(*stream_id, StreamId(9));
        assert_eq!(*offset, expected_offset);
        expected_offset = expected_offset.saturating_add(payload.len() as u64);
        assert!(payload.len() <= QUIC_STREAM_RECORD_PAYLOAD_BYTES);
    }
}

fn encoded_h3_records(frames: &[Frame], limits: CodecLimits) -> Bytes {
    let mut packet = Vec::new();
    for frame in frames {
        encode_length_prefixed_frame(frame, limits, &mut packet).expect("encode H3 record");
    }
    Bytes::from(packet)
}

#[test]
fn ready_h3_stream_data_decode_is_zero_copy_for_one_record() {
    let limits = CodecLimits::default();
    let payload = Bytes::from_static(b"one contiguous record");
    let mut pending = encoded_h3_records(
        &[Frame::StreamData {
            stream_id: StreamId(7),
            offset: 11,
            payload: payload.clone(),
        }],
        limits,
    );
    let encoded_frame_len =
        u32::from_be_bytes([pending[0], pending[1], pending[2], pending[3]]) as usize;
    let payload_start = FRAME_LEN_BYTES + encoded_frame_len - payload.len();
    let encoded_payload_ptr = pending.slice(payload_start..).as_ptr();

    let decoded = decode_ready_h3_frame(&mut pending, limits)
        .expect("decode ready record")
        .expect("complete record");
    let Frame::StreamData {
        stream_id,
        offset,
        payload: decoded_payload,
    } = decoded
    else {
        panic!("record must remain STREAM_DATA");
    };
    assert_eq!(stream_id, StreamId(7));
    assert_eq!(offset, 11);
    assert_eq!(decoded_payload, payload);
    assert_eq!(decoded_payload.as_ptr(), encoded_payload_ptr);
    assert!(pending.is_empty());
}

#[test]
fn ready_h3_stream_data_coalesces_adjacent_records_from_one_chunk() {
    let limits = CodecLimits::default();
    let mut pending = encoded_h3_records(
        &[
            Frame::StreamData {
                stream_id: StreamId(9),
                offset: 100,
                payload: Bytes::from_static(b"abc"),
            },
            Frame::StreamData {
                stream_id: StreamId(9),
                offset: 103,
                payload: Bytes::from_static(b"defg"),
            },
            Frame::StreamData {
                stream_id: StreamId(9),
                offset: 107,
                payload: Bytes::from_static(b"hij"),
            },
        ],
        limits,
    );

    assert_eq!(
        decode_ready_h3_frame(&mut pending, limits).expect("decode ready batch"),
        Some(Frame::StreamData {
            stream_id: StreamId(9),
            offset: 100,
            payload: Bytes::from_static(b"abcdefghij"),
        })
    );
    assert!(pending.is_empty());
}

#[test]
fn ready_h3_stream_data_stops_at_semantic_and_codec_boundaries() {
    let limits = CodecLimits::default();
    let boundaries = [
        Frame::Ping { nonce: 1 },
        Frame::StreamFin {
            stream_id: StreamId(9),
            final_offset: 3,
        },
        Frame::StreamData {
            stream_id: StreamId(9),
            offset: 4,
            payload: Bytes::from_static(b"gap"),
        },
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 3,
            payload: Bytes::from_static(b"other"),
        },
    ];
    for boundary in boundaries {
        let first = Frame::StreamData {
            stream_id: StreamId(9),
            offset: 0,
            payload: Bytes::from_static(b"abc"),
        };
        let mut pending = encoded_h3_records(&[first.clone(), boundary.clone()], limits);
        assert_eq!(
            decode_ready_h3_frame(&mut pending, limits).expect("decode first record"),
            Some(first)
        );
        assert_eq!(
            decode_ready_h3_frame(&mut pending, limits).expect("decode preserved boundary"),
            Some(boundary)
        );
        assert!(pending.is_empty());
    }

    for bounded_limits in [
        CodecLimits {
            max_payload_bytes: 4,
            ..limits
        },
        CodecLimits {
            max_frame_bytes: 33,
            ..limits
        },
    ] {
        let first = Frame::StreamData {
            stream_id: StreamId(9),
            offset: 0,
            payload: Bytes::from_static(b"abc"),
        };
        let second = Frame::StreamData {
            stream_id: StreamId(9),
            offset: 3,
            payload: Bytes::from_static(b"de"),
        };
        let mut pending = encoded_h3_records(&[first.clone(), second.clone()], bounded_limits);
        assert_eq!(
            decode_ready_h3_frame(&mut pending, bounded_limits)
                .expect("decode frame below aggregate limit"),
            Some(first)
        );
        assert_eq!(
            decode_ready_h3_frame(&mut pending, bounded_limits)
                .expect("decode record preserved by aggregate limit"),
            Some(second)
        );
        assert!(pending.is_empty());
    }
}

#[test]
fn native_flow_registry_bounds_live_state_without_exhausting_on_churn() {
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let mut registry = DatagramFlowRegistry::new(2);

    // The global allocator is monotonic, so long-lived sequential churn
    // coalesces into bounded seen-ID ranges while only live flows consume the
    // concurrency limit.
    for value in 0..100_u64 {
        let flow_id = DatagramFlowId(value);
        registry
            .apply_transitions(&[Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
            }])
            .expect("open one live flow");
        assert_eq!(registry.active.len(), 1);
        assert!(registry.seen_ranges.len() <= registry.max_seen_ranges);
        registry
            .apply_transitions(&[Frame::DatagramClose { flow_id }])
            .expect("reliably close flow");
        assert!(registry.active.is_empty());
    }

    // A delayed, previously unseen allocation can fill a sparse gap and
    // coalesce it; an actually closed identity cannot be reopened.
    for value in [102_u64, 104, 103] {
        let flow_id = DatagramFlowId(value);
        registry
            .apply_transitions(&[Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
            }])
            .expect("out-of-order unseen flow remains valid");
        registry
            .apply_transitions(&[Frame::DatagramClose { flow_id }])
            .expect("close sparse flow");
    }

    registry
        .apply_transitions(&[
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(200),
                target: target.clone(),
            },
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(201),
                target: target.clone(),
            },
        ])
        .expect("fill the live-flow bound");
    assert!(matches!(
        registry.apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(202),
            target: target.clone(),
        }]),
        Err(QuicCarrierError::NativeDatagramFlowsExhausted)
    ));
    registry
        .apply_received_transitions(&[Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(202),
            target: target.clone(),
        }])
        .expect("receive side provisionally admits an over-cap OPEN");
    assert_eq!(
        registry.state(DatagramFlowId(202)),
        DatagramFlowState::Active,
    );
    assert_eq!(registry.active.len(), 3);
    registry
        .retain_refusal(DatagramFlowId(202), None, 2)
        .expect("runtime turns the excess candidate into a capacity refusal");
    assert_eq!(
        registry.state(DatagramFlowId(202)),
        DatagramFlowState::Refused,
    );
    assert_eq!(registry.active.len(), 2);
    registry
        .apply_transitions(&[Frame::DatagramClose {
            flow_id: DatagramFlowId(200),
        }])
        .expect("release one live flow");
    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(203),
            target: target.clone(),
        }])
        .expect("a new flow may consume the released live slot");
    assert_eq!(
        registry.state(DatagramFlowId(203)),
        DatagramFlowState::Active,
    );

    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(50),
            target,
        }])
        .expect("a terminal historical ID is ignored without failing the carrier");
    assert_eq!(
        registry.state(DatagramFlowId(50)),
        DatagramFlowState::Closed,
    );
}

#[test]
fn native_flow_registry_bounds_refusals_and_terminalizes_evictions() {
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let refused = DatagramFlowId(1);
    let mut registry = DatagramFlowRegistry::new(2);
    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: refused,
            target: target.clone(),
        }])
        .expect("open refused flow");
    registry
        .retain_refusal(refused, None, 2)
        .expect("mark refusal");
    assert_eq!(registry.state(refused), DatagramFlowState::Refused);
    assert!(registry.active.is_empty());

    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: refused,
            target: target.clone(),
        }])
        .expect("accept an in-flight repeated open");
    registry
        .retain_refusal(refused, None, 2)
        .expect("repeat refusal");
    assert_eq!(registry.state(refused), DatagramFlowState::Refused);

    let accepted = DatagramFlowId(2);
    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: accepted,
            target,
        }])
        .expect("refusal does not consume live capacity");
    assert_eq!(registry.state(accepted), DatagramFlowState::Active);

    let mut retained = std::collections::VecDeque::from([refused]);
    for value in 3..103 {
        let flow_id = DatagramFlowId(value);
        registry
            .apply_transitions(&[Frame::OpenDatagramFlow {
                flow_id,
                target: TargetAddr::Ip("127.0.0.1:53".parse().expect("denied target")),
            }])
            .expect("open denial-flood flow");
        let evicted = if retained.len() == 2 {
            retained.pop_front()
        } else {
            None
        };
        retained.push_back(flow_id);
        registry
            .retain_refusal(flow_id, evicted, 2)
            .expect("retain bounded denial");
        assert!(registry.refused.len() <= 2);
        assert!(registry.refused_lru.len() <= 2);
    }
    assert_eq!(
        registry.state(refused),
        DatagramFlowState::Closed,
        "the runtime-selected LRU eviction becomes terminal in transport",
    );
    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: refused,
            target: TargetAddr::Ip("127.0.0.1:53".parse().expect("terminal target")),
        }])
        .expect("ignore a terminal duplicate without failing the carrier");
    assert_eq!(registry.state(refused), DatagramFlowState::Closed);
    assert_eq!(registry.active.len(), 1, "accepted capacity is unchanged");

    registry
        .apply_transitions(&[Frame::DatagramClose { flow_id: refused }])
        .expect("close refused flow");
    assert_eq!(registry.state(refused), DatagramFlowState::Closed);
}

#[test]
fn native_receive_registry_allows_later_burst_candidate_after_earlier_denials() {
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let first = DatagramFlowId(10);
    let second = DatagramFlowId(11);
    let later = DatagramFlowId(12);
    let mut registry = DatagramFlowRegistry::new(2);
    registry
        .apply_received_transitions(&[
            Frame::OpenDatagramFlow {
                flow_id: first,
                target: target.clone(),
            },
            Frame::OpenDatagramFlow {
                flow_id: second,
                target: target.clone(),
            },
            Frame::OpenDatagramFlow {
                flow_id: later,
                target,
            },
        ])
        .expect("bounded reader queue provisionally admits the burst");
    assert_eq!(registry.active.len(), 3);
    registry
        .retain_refusal(first, None, 2)
        .expect("deny first candidate");
    registry
        .retain_refusal(second, None, 2)
        .expect("deny second candidate");
    assert_eq!(registry.active.len(), 1);
    assert_eq!(registry.state(later), DatagramFlowState::Active);
}

#[test]
fn native_ip_tunnel_registry_requires_one_open_ready_close_lifecycle() {
    let tunnel_id = IpTunnelId(7);
    let mut registry = IpTunnelRegistry::new();
    assert_eq!(registry.state(tunnel_id), IpTunnelState::Unknown);
    registry
        .apply_transitions(&[Frame::OpenIpTunnel { tunnel_id }])
        .expect("open tunnel association");
    assert_eq!(registry.state(tunnel_id), IpTunnelState::Open(tunnel_id));
    assert!(matches!(
        registry.apply_transitions(&[Frame::IpTunnelReady {
            tunnel_id: IpTunnelId(8),
            mtu: 1_400,
            addresses: Vec::new(),
        }]),
        Err(QuicCarrierError::InvalidNativeDatagram(_))
    ));
    registry
        .apply_transitions(&[Frame::IpTunnelReady {
            tunnel_id,
            mtu: 1_400,
            addresses: Vec::new(),
        }])
        .expect("ready matching tunnel association");
    assert_eq!(registry.state(tunnel_id), IpTunnelState::Ready(tunnel_id));
    registry
        .apply_transitions(&[Frame::IpTunnelClose {
            tunnel_id,
            reason: CloseReason::Normal,
        }])
        .expect("close matching tunnel association");
    assert_eq!(registry.state(tunnel_id), IpTunnelState::Closed(tunnel_id));
    assert!(matches!(
        registry.apply_transitions(&[Frame::OpenIpTunnel { tunnel_id }]),
        Err(QuicCarrierError::InvalidNativeDatagram(_))
    ));
}

#[tokio::test]
async fn quic_carrier_round_trips_product_frames() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        send.set_priority(1)
            .expect("set server QUIC stream priority");
        assert_eq!(
            send.priority().expect("read server QUIC stream priority"),
            1
        );
        match read_frame(&mut recv, limits)
            .await
            .expect("server read ping")
        {
            Frame::Ping { nonce } => {
                write_frame(&mut send, &Frame::Pong { nonce }, limits)
                    .await
                    .expect("server write pong");
                finish_stream(&mut send)
                    .await
                    .expect("server finish stream");
            }
            frame => panic!("unexpected frame: {frame:?}"),
        }
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("client stream");
    send.set_priority(1).expect("set QUIC stream priority");
    assert_eq!(send.priority().expect("read QUIC stream priority"), 1);
    write_frame(&mut send, &Frame::Ping { nonce: 42 }, limits)
        .await
        .expect("client write ping");
    assert_eq!(connection.congestion_metrics().pending_bytes, 0);
    assert!(!connection.is_closed());
    finish_stream(&mut send)
        .await
        .expect("client finish stream");
    let response = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("response timeout")
        .expect("client read pong");
    assert_eq!(response, Frame::Pong { nonce: 42 });
    let finished = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("stream finish timeout")
        .expect_err("server finished its QUIC send half");
    assert!(matches!(finished, QuicCarrierError::StreamFinished));
    let _ = client_done_tx.send(());

    server_task.await.expect("server task");
}

/// Keep one uninterrupted producer turn between API acceptance and observation:
/// the native driver cannot run inside that current-thread turn. The payload
/// crosses an MPP record boundary; native offsets come from Quinn, not a model
/// of HTTP/3 or Product framing overhead.
#[tokio::test(flavor = "current_thread")]
async fn accepted_h3_product_envelope_has_an_independent_packetization_wake() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let payload = Bytes::from(vec![0x6d; QUIC_STREAM_RECORD_PAYLOAD_BYTES + 1]);
    let server_payload = payload.clone();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("warmup ping"),
            Frame::Ping { nonce: 42 }
        );
        write_frame(&mut send, &Frame::Pong { nonce: 42 }, limits)
            .await
            .expect("warmup pong");
        let mut received = Vec::new();
        while received.len() < server_payload.len() {
            let Frame::StreamData {
                stream_id,
                offset,
                payload,
            } = read_frame(&mut recv, limits).await.expect("Product record")
            else {
                panic!("expected Product data");
            };
            assert_eq!(stream_id, StreamId(7));
            assert_eq!(offset, received.len() as u64);
            received.extend_from_slice(&payload);
        }
        assert_eq!(received.as_slice(), server_payload.as_ref());
        received_tx.send(()).expect("receiver confirmation");
        client_done_rx.await.expect("client completion");
    });
    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 42 }, limits)
        .await
        .expect("warmup ping");
    assert_eq!(
        timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
            .await
            .expect("warmup timeout")
            .expect("warmup pong"),
        Frame::Pong { nonce: 42 }
    );
    let observer = send
        .connection
        .observe_send_stream(send.request_stream_id)
        .expect("established native stream observer");
    let before = observer.snapshot().expect("pre-write native progress");
    let frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 0,
        payload,
    };
    let mut write = Box::pin(write_frame(&mut send, &frame, limits));
    assert!(matches!(
        futures::poll!(&mut write),
        std::task::Poll::Ready(Ok(()))
    ));
    drop(write);
    let accepted = observer.snapshot().expect("accepted native envelope");
    assert!(accepted.accepted_end > before.accepted_end);
    assert!(accepted.first_unpacketized < accepted.accepted_end);
    assert_eq!(connection.congestion_metrics().pending_bytes, 0);

    let mut crossing = Box::pin(observer.wait_until_packetized(accepted.accepted_end));
    assert!(futures::poll!(&mut crossing).is_pending());
    let packetized = timeout(Duration::from_secs(5), crossing)
        .await
        .expect("packetization wake timeout")
        .expect("packetization progress");
    assert!(packetized.first_unpacketized >= accepted.accepted_end);
    assert_eq!(packetized.accepted_end, accepted.accepted_end);
    timeout(Duration::from_secs(5), received_rx)
        .await
        .expect("receiver timeout")
        .expect("receiver confirmed exact Product bytes");
    client_done_tx.send(()).expect("client completion");
    server_task.await.expect("server task");
}

#[tokio::test]
async fn http_datagram_send_requires_an_open_request_send_side() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert!(matches!(
            read_frame(&mut recv, limits)
                .await
                .expect("read datagram flow open"),
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(9),
                ..
            }
        ));
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    write_frame(
        &mut send,
        &Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(9),
            target,
        },
        limits,
    )
    .await
    .expect("open native datagram flow");
    finish_stream(&mut send)
        .await
        .expect("finish request send side");

    assert!(matches!(
        write_frame(
            &mut send,
            &Frame::DatagramData {
                flow_id: DatagramFlowId(9),
                datagram_id: DatagramId(1),
                ttl_ms: 1_000,
                payload: Bytes::from_static(b"late"),
            },
            limits,
        )
        .await,
        Err(QuicCarrierError::H3StreamFinished)
    ));

    let _ = client_done_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn request_receive_fin_retires_native_datagram_route() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read request"),
            Frame::Ping { nonce: 1 }
        );
        assert!(matches!(
            read_frame(&mut recv, limits).await,
            Err(QuicCarrierError::StreamFinished)
        ));
        assert_eq!(
            connection.native_datagram_routing_counts().0,
            0,
            "a closed H3 receive side must not retain its datagram route"
        );
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("write request");
    finish_stream(&mut send).await.expect("finish request");

    server_task.await.expect("server task");
}

#[tokio::test]
async fn closed_request_datagrams_are_dropped_without_handoff_buffering() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (route_closed_tx, route_closed_rx) = tokio::sync::oneshot::channel();
    let (late_sent_tx, late_sent_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert!(matches!(
            read_frame(&mut recv, limits)
                .await
                .expect("read datagram flow open"),
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(9),
                ..
            }
        ));
        drop(recv);
        let before = connection.native_datagram_routing_counts();
        assert_eq!(before.0, 0);
        route_closed_tx.send(()).expect("publish closed route");
        late_sent_rx.await.expect("late datagram sent");

        let after = timeout(Duration::from_secs(1), async {
            loop {
                let after = connection.native_datagram_routing_counts();
                if after.1 > before.1 || after.2 > before.2 {
                    break after;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late datagram routing outcome");
        assert_eq!(
            after.1, before.1,
            "a closed request must not re-enter the pre-request handoff queue"
        );
        assert!(after.2 > before.2);
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(
        &mut send,
        &Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(9),
            target: TargetAddr::Ip("127.0.0.1:53".parse().expect("target")),
        },
        limits,
    )
    .await
    .expect("open native datagram flow");
    route_closed_rx.await.expect("server closed route");
    write_frame(
        &mut send,
        &Frame::DatagramData {
            flow_id: DatagramFlowId(9),
            datagram_id: DatagramId(1),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"late"),
        },
        limits,
    )
    .await
    .expect("send late native datagram");
    late_sent_tx.send(()).expect("publish late datagram");

    server_task.await.expect("server task");
}

#[tokio::test]
async fn stopped_quic_stream_write_keeps_the_shared_connection_available() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read opener"),
            Frame::Ping { nonce: 1 }
        );
        // Dropping an unread H3 receive half exercises the normal receiver
        // abandonment path and emits QUIC STOP_SENDING without relying on
        // h3-quinn's non-cancel-safe test-only stop wrapper.
        drop(recv);
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .expect("accept replacement stream");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read replacement"),
            Frame::Ping { nonce: 2 }
        );
        write_frame(&mut send, &Frame::Pong { nonce: 2 }, limits)
            .await
            .expect("write replacement response");
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("open carrier stream");
    assert_eq!(connection.congestion_metrics().pending_bytes, 0);
    assert!(!connection.is_closed());

    let err = timeout(Duration::from_secs(5), async {
        loop {
            match write_frame(&mut send, &Frame::Ping { nonce: 99 }, limits).await {
                Ok(()) => tokio::task::yield_now().await,
                Err(err) => break err,
            }
        }
    })
    .await
    .expect("HTTP/3 request cancellation timeout");
    assert!(matches!(err, QuicCarrierError::H3Stream(_)));
    let metrics = connection.congestion_metrics();
    assert_eq!(metrics.pending_bytes, 0);
    assert!(!connection.is_closed());

    let (mut replacement_send, mut replacement_recv) =
        connection.open_bi().await.expect("open replacement stream");
    write_frame(&mut replacement_send, &Frame::Ping { nonce: 2 }, limits)
        .await
        .expect("write replacement request");
    assert_eq!(
        read_frame(&mut replacement_recv, limits)
            .await
            .expect("read replacement response"),
        Frame::Pong { nonce: 2 }
    );

    let _ = client_done_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn cancelled_h3_request_write_retires_only_that_request_stream() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024,
        max_repair_bytes: 4 * 1024,
        max_reorder_bytes: 4 * 1024,
        max_datagram_queue_bytes: 4 * 1024,
        max_path_flight_bytes: 4 * 1024,
        max_reliable_relay_chunk_bytes: 4 * 1024,
        ..MuxLimits::default()
    };
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (server_ready_tx, server_ready_rx) = tokio::sync::oneshot::channel();
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read opener"),
            Frame::Ping { nonce: 1 }
        );
        let _ = server_ready_tx.send(());
        let _recv = recv;
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .expect("accept replacement stream");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read replacement"),
            Frame::Ping { nonce: 2 }
        );
        write_frame(&mut send, &Frame::Pong { nonce: 2 }, limits)
            .await
            .expect("write replacement response");
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("open carrier stream");
    timeout(Duration::from_secs(5), server_ready_rx)
        .await
        .expect("server ready timeout")
        .expect("server ready sender");

    let payload_len = 256 * 1024;
    let write_task = tokio::spawn(async move {
        write_frame(
            &mut send,
            &Frame::StreamData {
                stream_id: StreamId(9),
                offset: 0,
                payload: Bytes::from(vec![0x5a; payload_len]),
            },
            limits,
        )
        .await
    });
    timeout(Duration::from_secs(5), async {
        loop {
            if connection.congestion_metrics().pending_bytes > 0 {
                break;
            }
            assert!(!write_task.is_finished(), "constrained write must block");
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write did not enter backlog");

    write_task.abort();
    assert!(
        write_task
            .await
            .expect_err("aborted writer must be cancelled")
            .is_cancelled()
    );
    let metrics = connection.congestion_metrics();
    assert_eq!(metrics.pending_bytes, 0);
    assert!(!connection.is_closed());

    let (mut replacement_send, mut replacement_recv) =
        connection.open_bi().await.expect("open replacement stream");
    write_frame(&mut replacement_send, &Frame::Ping { nonce: 2 }, limits)
        .await
        .expect("write replacement request");
    assert_eq!(
        read_frame(&mut replacement_recv, limits)
            .await
            .expect("read replacement response"),
        Frame::Pong { nonce: 2 }
    );

    let _ = client_done_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn quic_carrier_batches_multiple_product_frames_per_write() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read first"),
            Frame::Ping { nonce: 1 }
        );
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read second"),
            Frame::Pong { nonce: 2 }
        );
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frames(
        &mut send,
        &[Frame::Ping { nonce: 1 }, Frame::Pong { nonce: 2 }],
        limits,
    )
    .await
    .expect("client write batch");
    finish_stream(&mut send)
        .await
        .expect("client finish stream");
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server task timeout")
        .expect("server task");
}

#[tokio::test]
async fn native_http_datagram_fragments_preserve_identity_without_reliable_hol() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let flow_id = DatagramFlowId(7);
    let request_id = DatagramId(11);
    let response_id = DatagramId(12);
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let request_payload = Bytes::from(vec![0x5a; 60_000]);
    let response_payload = Bytes::from_static(b"native response");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();

    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let expected_request = request_payload.clone();
    let expected_response = response_payload.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("accepted request");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read reliable open"),
            Frame::OpenDatagramFlow { flow_id, target }
        );

        let mut saw_native = false;
        let mut saw_reliable_ping = false;
        while !saw_native || !saw_reliable_ping {
            match read_frame(&mut recv, limits)
                .await
                .expect("read mixed traffic")
            {
                Frame::DatagramData {
                    flow_id: received_flow,
                    datagram_id,
                    ttl_ms,
                    payload,
                } => {
                    assert_eq!(received_flow, flow_id);
                    assert_eq!(datagram_id, request_id);
                    assert!(ttl_ms > 0 && ttl_ms <= 5_000);
                    assert_eq!(payload, expected_request);
                    saw_native = true;
                }
                Frame::Ping { nonce: 99 } => saw_reliable_ping = true,
                frame => panic!("unexpected mixed HTTP/3 frame: {frame:?}"),
            }
        }

        write_frame(
            &mut send,
            &Frame::DatagramData {
                flow_id,
                datagram_id: response_id,
                ttl_ms: 5_000,
                payload: expected_response,
            },
            limits,
        )
        .await
        .expect("write native response");
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("client request");
    write_frames(
        &mut send,
        &[
            Frame::OpenDatagramFlow {
                flow_id,
                target: TargetAddr::Ip("127.0.0.1:53".parse().expect("target")),
            },
            Frame::DatagramData {
                flow_id,
                datagram_id: request_id,
                ttl_ms: 5_000,
                payload: request_payload,
            },
            Frame::Ping { nonce: 99 },
        ],
        limits,
    )
    .await
    .expect("write reliable open, native payload, and independent control");

    match timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("native response timeout")
        .expect("native response")
    {
        Frame::DatagramData {
            flow_id: received_flow,
            datagram_id,
            ttl_ms,
            payload,
        } => {
            assert_eq!(received_flow, flow_id);
            assert_eq!(datagram_id, response_id);
            assert!(ttl_ms > 0 && ttl_ms <= 5_000);
            assert_eq!(payload, response_payload);
        }
        frame => panic!("unexpected native response frame: {frame:?}"),
    }
    let _ = client_done_tx.send(());
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server task timeout")
        .expect("server task");
}

#[tokio::test]
async fn native_ip_packets_require_ready_and_preserve_fragmented_identity() {
    // Native IP fragments are best effort. Guard every phase, rather than only
    // the final server join, so a lost packet cannot leave the fixture pending.
    // This is the existing five-second test bound, not a native expiry change.
    struct AbortOnDrop(tokio::task::JoinHandle<()>);

    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    let phases = Arc::new(Mutex::new(("not started", "not spawned")));
    let client_phase = |phase: &'static str| {
        phases.lock().expect("native IP test phases").0 = phase;
    };
    let scenario = async {
        let limits = CodecLimits::default();
        let mux_limits = MuxLimits::default();
        let tunnel_id = IpTunnelId(17);
        let request_id = IpPacketId(31);
        let response_id = IpPacketId(32);
        let request_payload = Bytes::from(vec![0x45; 60_000]);
        let response_payload = Bytes::from(vec![0x60; 32_000]);

        client_phase("binding server endpoint");
        let server = Endpoint::bind_server(
            "127.0.0.1:0".parse().expect("server addr"),
            &crate::transport::encrypted::test_server_tls_config(),
            super::super::test_candidate_verifier(),
            mux_limits,
        )
        .await
        .expect("server endpoint");
        let server_addr = server.local_addr().expect("server local addr");
        let expected_request = request_payload.clone();
        let expected_response = response_payload.clone();
        let server_phases = phases.clone();
        let mut server_task = AbortOnDrop(tokio::spawn(async move {
            let server_phase = |phase: &'static str| {
                server_phases.lock().expect("native IP test phases").1 = phase;
            };
            server_phase("accepting connection");
            let connection = server.accept().await.expect("accepted connection");
            server_phase("accepting request");
            let (mut send, mut recv) = connection.accept_bi().await.expect("accepted request");
            server_phase("reading reliable tunnel open");
            assert_eq!(
                read_frame(&mut recv, limits)
                    .await
                    .expect("read reliable tunnel open"),
                Frame::OpenIpTunnel { tunnel_id }
            );
            server_phase("writing reliable tunnel ready");
            write_frame(
                &mut send,
                &Frame::IpTunnelReady {
                    tunnel_id,
                    mtu: 65_535,
                    addresses: vec!["10.0.0.2".parse().expect("tunnel address")],
                },
                limits,
            )
            .await
            .expect("write reliable tunnel ready");
            server_phase("reading native request after writing ready");
            assert_eq!(
                read_frame(&mut recv, limits)
                    .await
                    .expect("read native IP packet"),
                Frame::IpPacket {
                    tunnel_id,
                    packet_id: request_id,
                    payload: expected_request,
                }
            );
            server_phase("writing native response after reassembling request");
            write_frame(
                &mut send,
                &Frame::IpPacket {
                    tunnel_id,
                    packet_id: response_id,
                    payload: expected_response,
                },
                limits,
            )
            .await
            .expect("write native IP response");
            server_phase("reading reliable close after response fragments locally accepted");
            assert_eq!(
                read_frame(&mut recv, limits)
                    .await
                    .expect("read reliable tunnel close"),
                Frame::IpTunnelClose {
                    tunnel_id,
                    reason: CloseReason::Normal,
                }
            );
            server_phase("finishing reliable response");
            finish_stream(&mut send)
                .await
                .expect("finish tunnel response");
            server_phase("complete");
        }));

        client_phase("binding client endpoint");
        let client = Endpoint::bind_client(
            "127.0.0.1:0".parse().expect("client addr"),
            &crate::transport::encrypted::test_client_tls_config(),
            super::super::test_candidate_selector(),
            mux_limits,
        )
        .await
        .expect("client endpoint");
        client_phase("connecting");
        let connection = client.connect(server_addr).await.expect("client connect");
        client_phase("opening request");
        let (mut send, mut recv) = connection.open_bi().await.expect("client request");
        client_phase("writing reliable tunnel open");
        write_frame(&mut send, &Frame::OpenIpTunnel { tunnel_id }, limits)
            .await
            .expect("write reliable tunnel open");
        client_phase("checking native request rejection before ready");
        assert!(matches!(
            write_frame(
                &mut send,
                &Frame::IpPacket {
                    tunnel_id,
                    packet_id: request_id,
                    payload: request_payload.clone(),
                },
                limits,
            )
            .await,
            Err(QuicCarrierError::InvalidNativeDatagram(_))
        ));
        client_phase("reading reliable tunnel ready");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read reliable tunnel ready"),
            Frame::IpTunnelReady {
                tunnel_id,
                mtu: 65_535,
                addresses: vec!["10.0.0.2".parse().expect("tunnel address")],
            }
        );
        client_phase("writing native request after reading ready");
        write_frame(
            &mut send,
            &Frame::IpPacket {
                tunnel_id,
                packet_id: request_id,
                payload: request_payload,
            },
            limits,
        )
        .await
        .expect("write native IP request");
        client_phase("reading native response after request fragments locally accepted");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read native IP response"),
            Frame::IpPacket {
                tunnel_id,
                packet_id: response_id,
                payload: response_payload,
            }
        );
        client_phase("writing reliable close after reassembling response");
        write_frame(
            &mut send,
            &Frame::IpTunnelClose {
                tunnel_id,
                reason: CloseReason::Normal,
            },
            limits,
        )
        .await
        .expect("write reliable tunnel close");
        client_phase("checking native packet rejection after close");
        assert!(matches!(
            write_frame(
                &mut send,
                &Frame::IpPacket {
                    tunnel_id,
                    packet_id: IpPacketId(33),
                    payload: Bytes::from_static(b"late"),
                },
                limits,
            )
            .await,
            Err(QuicCarrierError::InvalidNativeDatagram(_))
        ));
        client_phase("finishing reliable request");
        finish_stream(&mut send)
            .await
            .expect("finish tunnel request");
        client_phase("joining server task");
        (&mut server_task.0).await.expect("server task");
        client_phase("complete");
    };
    timeout(Duration::from_secs(5), scenario)
        .await
        .unwrap_or_else(|_| {
            let (client, server) = *phases.lock().expect("native IP test phases");
            panic!("native IP fixture timed out: client={client}; server={server}");
        });
}
