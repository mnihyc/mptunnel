//! Real-peer counterexamples for submitted, pre-first-MAX QUIC OPEN ownership.

use super::*;
use crate::protocol::ResetReason;

const OPEN_OWNERSHIP_GUARD: Duration = Duration::from_secs(5);

type PendingOpenEvents = mpsc::UnboundedReceiver<(StreamId, ClientUdpPendingOpenEvent)>;

fn observe_pending_opens(session: &ClientUdpPathSessionHandle) -> PendingOpenEvents {
    let (sender, receiver) = mpsc::unbounded_channel();
    *session
        .pending_open_events
        .lock()
        .expect("pending-open event lock") = Some(sender);
    receiver
}

async fn observe_until(
    events: &mut PendingOpenEvents,
    stream_id: StreamId,
    expected: ClientUdpPendingOpenEvent,
    observed: &mut Vec<ClientUdpPendingOpenEvent>,
) {
    tokio::time::timeout(OPEN_OWNERSHIP_GUARD, async {
        loop {
            let (id, event) = events.recv().await.expect("pending-open owner event");
            assert_eq!(id, stream_id, "fixture has one MPP open owner at a time");
            observed.push(event);
            if event == expected {
                return;
            }
        }
    })
    .await
    .expect("actual continuation transition timeout");
}

async fn assert_ordered_detach(
    recv: &mut UdpPathRecvStream,
    stream_id: StreamId,
    limits: CodecLimits,
) {
    assert_eq!(
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, udp_path_read_frame(recv, limits))
            .await
            .expect("DETACH native delivery timeout")
            .expect("peer receives ordered MPP DETACH"),
        Frame::StreamDetach { stream_id },
    );
    let result = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, udp_path_read_frame(recv, limits))
        .await
        .expect("native request close timeout");
    assert!(
        result
            .as_ref()
            .is_err_and(super::super::super::io::udp_path_input_finished)
    );
}

async fn retire_unopened_peer_repair(accepted: &AcceptedTestCarrier, limits: CodecLimits) {
    let (mut send, mut recv) =
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, accepted.connection.accept_bi())
            .await
            .expect("unopened repair native request timeout")
            .expect("accept allocated repair request");
    let frame = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, udp_path_read_frame(&mut recv, limits))
        .await
        .expect("unopened repair request releases without first MAX");
    assert!(
        frame.is_err(),
        "no MPP repair OPEN was submitted: {frame:?}"
    );
    send.cancel_pending_response();
}

async fn assert_real_sibling_exchange(
    fixture: &ClientOpenRaceFixture,
    accepted: &AcceptedTestCarrier,
) {
    let carrier = current_client_carrier(&fixture.session)
        .await
        .expect("same live carrier");
    let limits = fixture.server_context.codec_limits;
    tokio::time::timeout(OPEN_OWNERSHIP_GUARD, async {
        let (client, server) = tokio::join!(
            carrier.connection.open_bi(),
            accepted.connection.accept_bi(),
        );
        let (mut send, mut recv) = client.expect("sibling native open");
        let (mut peer_send, mut peer_recv) = server.expect("sibling native accept");
        udp_path_write_frame(&mut send, &Frame::Ping { nonce: 1200 }, limits)
            .await
            .expect("sibling request write");
        assert_eq!(
            udp_path_read_frame(&mut peer_recv, limits).await.unwrap(),
            Frame::Ping { nonce: 1200 }
        );
        udp_path_write_frame(&mut peer_send, &Frame::Pong { nonce: 1200 }, limits)
            .await
            .expect("sibling response write");
        assert_eq!(
            udp_path_read_frame(&mut recv, limits).await.unwrap(),
            Frame::Pong { nonce: 1200 }
        );
        super::super::super::io::udp_path_finish_stream(&mut send)
            .await
            .unwrap();
        super::super::super::io::udp_path_finish_stream(&mut peer_send)
            .await
            .unwrap();
        assert!(!carrier.connection.is_closed());
    })
    .await
    .expect("sibling request must retain real native credit and service");
}

#[tokio::test]
async fn submitted_open_cancellation_orders_detach_before_native_close() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let carrier = current_client_carrier(&fixture.session)
        .await
        .expect("established client carrier");
    let stream_id = StreamId(1200);
    let limits = fixture.server_context.codec_limits;
    let opening = spawn_test_open(&fixture.session, stream_id);
    // Keep both peer halves alive and withhold first MAX. A peer response-half
    // drop would instead introduce an H3 refusal into the tested boundary.
    let (_peer_send, mut peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .expect("actual submitted OPEN/MAX reached peer")
    .expect("read actual submitted OPEN/MAX");
    assert!(!opening.is_finished(), "first MAX remains withheld");
    opening.abort();
    assert!(
        opening
            .await
            .err()
            .is_some_and(|error| error.is_cancelled()),
        "caller cancellation must be joined"
    );

    let next = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut peer_recv, limits),
    )
    .await;
    assert!(
        matches!(next, Ok(Ok(Frame::StreamDetach { stream_id: detached })) if detached == stream_id),
        "fully submitted cancellation must deliver MPP DETACH before native EOF: {next:?}"
    );
    let eof = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut peer_recv, limits),
    )
    .await
    .expect("ordered native close follows DETACH");
    assert!(
        eof.as_ref()
            .is_err_and(super::super::super::io::udp_path_input_finished),
        "retired request must finish after its DETACH: {eof:?}"
    );
    assert!(!carrier.connection.is_closed());
    assert_eq!(
        current_client_carrier(&fixture.session)
            .await
            .expect("shared carrier remains installed")
            .path_instance_id,
        carrier.path_instance_id,
    );
}

#[tokio::test]
async fn parsed_open_reset_is_not_held_behind_physical_owner_mutex() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let stream_id = StreamId(1201);
    let limits = fixture.server_context.codec_limits;
    let mut opening = spawn_test_open(&fixture.session, stream_id);
    let (mut peer_send, _peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .expect("actual OPEN/MAX reached peer")
    .expect("peer reads pending OPEN/MAX");
    let owner = fixture.session.owner.connection.lock().await;
    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamReset {
            stream_id,
            reason: ResetReason::RemoteClosed,
        },
        limits,
    )
    .await
    .expect("peer publishes real logical terminal");

    let observed = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, &mut opening).await;
    let returned_before_unlock = observed.is_ok();
    drop(owner);
    // Settle the baseline too, so a failing conformance assertion does not
    // intentionally strand the open task behind the test's own mutex guard.
    let result = match observed {
        Ok(joined) => joined.expect("terminal open task join"),
        Err(_) => tokio::time::timeout(OPEN_OWNERSHIP_GUARD, opening)
            .await
            .expect("baseline settles after owner unlock")
            .expect("baseline open task join"),
    };
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
    ));
    assert!(
        returned_before_unlock,
        "a parsed logical RESET cannot await independent physical reconciliation"
    );
}

#[tokio::test]
async fn submitted_open_repeated_cancellation_releases_pair_credit_and_one_continuation() {
    let fixture = ClientOpenRaceFixture::new_with_limits(
        Arc::new(SystemCarrierNetworkProvider),
        ResourceLimits {
            // Exactly the authenticated control request and one native pair.
            // Every next pair proves the preceding native credit was returned.
            max_quic_concurrent_bidi_streams: 3,
            ..ResourceLimits::default()
        },
    )
    .await;
    let accepted = fixture.establish_current().await;
    let instance = current_client_carrier(&fixture.session)
        .await
        .unwrap()
        .path_instance_id;
    let limits = fixture.server_context.codec_limits;
    let mut events = observe_pending_opens(&fixture.session);
    for index in 0..3 {
        let stream_id = StreamId(1210 + index);
        let opening = spawn_test_open(&fixture.session, stream_id);
        let (mut peer_send, mut peer_recv) = tokio::time::timeout(
            OPEN_OWNERSHIP_GUARD,
            read_test_stream_open(&accepted.connection, stream_id, limits),
        )
        .await
        .expect("same-process repeated pair can obtain native credit")
        .unwrap();
        opening.abort();
        assert!(
            opening
                .await
                .err()
                .is_some_and(|error| error.is_cancelled())
        );
        retire_unopened_peer_repair(&accepted, limits).await;
        assert_ordered_detach(&mut peer_recv, stream_id, limits).await;
        peer_send.cancel_pending_response();
        drop((peer_send, peer_recv));
        let mut observed = Vec::new();
        observe_until(
            &mut events,
            stream_id,
            ClientUdpPendingOpenEvent::ContinuationExited,
            &mut observed,
        )
        .await;
        assert_eq!(
            observed
                .iter()
                .filter(|event| **event == ClientUdpPendingOpenEvent::ContinuationStarted)
                .count(),
            1
        );
        assert_eq!(
            observed
                .iter()
                .filter(|event| **event == ClientUdpPendingOpenEvent::Retiring)
                .count(),
            1
        );
        assert!(observed.contains(&ClientUdpPendingOpenEvent::RetirementFinished(true)));
        assert!(!observed.contains(&ClientUdpPendingOpenEvent::Accepted));
        assert_eq!(
            current_client_carrier(&fixture.session)
                .await
                .unwrap()
                .path_instance_id,
            instance
        );
        assert_real_sibling_exchange(&fixture, &accepted).await;
    }
}

#[tokio::test]
async fn submitted_open_expiry_transfers_retirement_without_extending_deadline() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let stream_id = StreamId(1220);
    let limits = fixture.server_context.codec_limits;
    let mut events = observe_pending_opens(&fixture.session);
    let opening = spawn_test_open(&fixture.session, stream_id);
    let (mut peer_send, mut peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .unwrap()
    .unwrap();
    // Advance the existing spawn_test_open ten-second deadline only after the
    // real peer confirms submission. This changes no production clock formula.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::time::resume();
    let result = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, opening)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(RuntimeError::PathOpenTimedOut)));
    retire_unopened_peer_repair(&accepted, limits).await;
    assert_ordered_detach(&mut peer_recv, stream_id, limits).await;
    peer_send.cancel_pending_response();
    let mut observed = Vec::new();
    observe_until(
        &mut events,
        stream_id,
        ClientUdpPendingOpenEvent::ContinuationExited,
        &mut observed,
    )
    .await;
    assert!(observed.contains(&ClientUdpPendingOpenEvent::RetirementFinished(true)));
}

#[tokio::test]
async fn submitted_open_success_uses_one_continuation_and_preserves_two_phase_credit() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let stream_id = StreamId(1230);
    let limits = fixture.server_context.codec_limits;
    let mut events = observe_pending_opens(&fixture.session);
    let opening = spawn_test_open(&fixture.session, stream_id);
    let (mut peer_send, mut peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .unwrap()
    .unwrap();
    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamMaxData {
            stream_id,
            max_offset: 0,
        },
        limits,
    )
    .await
    .unwrap();
    let mut opened = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, opening)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        opened.max_offset, 0,
        "zero is carrier admission, not target acceptance"
    );
    let (mut repair_send, mut repair_recv) =
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, accepted.connection.accept_bi())
            .await
            .unwrap()
            .unwrap();
    assert!(matches!(
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, udp_path_read_frame(&mut repair_recv, limits)).await.unwrap().unwrap(),
        Frame::OpenStreamRepair { stream_id: repaired, .. } if repaired == stream_id
    ));
    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamMaxData {
            stream_id,
            max_offset: 65_536,
        },
        limits,
    )
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, opened.frames.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Frame::StreamMaxData {
            stream_id,
            max_offset: 65_536
        },
    );
    assert_real_sibling_exchange(&fixture, &accepted).await;
    opened.retire_uncommitted().unwrap();
    assert_ordered_detach(&mut peer_recv, stream_id, limits).await;
    let mut observed = Vec::new();
    observe_until(
        &mut events,
        stream_id,
        ClientUdpPendingOpenEvent::ContinuationExited,
        &mut observed,
    )
    .await;
    assert_eq!(
        observed
            .iter()
            .filter(|event| **event == ClientUdpPendingOpenEvent::ContinuationStarted)
            .count(),
        1
    );
    assert_eq!(
        observed
            .iter()
            .filter(|event| **event == ClientUdpPendingOpenEvent::Accepted)
            .count(),
        1
    );
    assert!(!observed.contains(&ClientUdpPendingOpenEvent::Retiring));
    repair_send.cancel_pending_response();
}

#[tokio::test]
async fn submitted_open_session_retirement_ends_waiting_continuation() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let carrier = current_client_carrier(&fixture.session).await.unwrap();
    let stream_id = StreamId(1240);
    let limits = fixture.server_context.codec_limits;
    let mut events = observe_pending_opens(&fixture.session);
    let context = fixture.context.clone();
    let session = fixture.session.clone();
    let opening = tokio::spawn(async move {
        context
            .complete_session_operation(session.open_stream(
                stream_id,
                TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                TrafficClass::Latency,
                StreamDemandHint::Latency,
                Default::default(),
                tokio::time::Instant::now() + Duration::from_secs(10),
                65_536,
            ))
            .await
    });
    let (_peer_send, _peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .unwrap()
    .unwrap();
    fixture.context.retire_session(CloseReason::PolicyRejected);
    let result = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, opening)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    let mut observed = Vec::new();
    observe_until(
        &mut events,
        stream_id,
        ClientUdpPendingOpenEvent::ContinuationExited,
        &mut observed,
    )
    .await;
    tokio::time::timeout(OPEN_OWNERSHIP_GUARD, carrier.connection.wait_closed())
        .await
        .unwrap();
    assert!(!observed.contains(&ClientUdpPendingOpenEvent::Accepted));
}

#[tokio::test]
async fn submitted_open_native_failure_cannot_leave_a_continuation() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let stream_id = StreamId(1241);
    let limits = fixture.server_context.codec_limits;
    let mut events = observe_pending_opens(&fixture.session);
    let opening = spawn_test_open(&fixture.session, stream_id);
    let (_peer_send, _peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .unwrap()
    .unwrap();
    accepted.connection.close();
    let mut observed = Vec::new();
    observe_until(
        &mut events,
        stream_id,
        ClientUdpPendingOpenEvent::ContinuationExited,
        &mut observed,
    )
    .await;
    // The public open may now own its existing bounded physical retry. Cancel
    // that caller rather than granting it a new server or changing its policy.
    opening.abort();
    let _ = opening.await;
    assert!(!observed.contains(&ClientUdpPendingOpenEvent::Accepted));
}

#[tokio::test]
async fn parsed_open_reset_keeps_siblings_and_independent_native_retirement() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let carrier = current_client_carrier(&fixture.session).await.unwrap();
    let limits = fixture.server_context.codec_limits;
    let mut events = observe_pending_opens(&fixture.session);
    for (index, close_native) in [false, true].into_iter().enumerate() {
        let stream_id = StreamId(1250 + index as u64);
        let opening = spawn_test_open(&fixture.session, stream_id);
        let (mut peer_send, mut peer_recv) = tokio::time::timeout(
            OPEN_OWNERSHIP_GUARD,
            read_test_stream_open(&accepted.connection, stream_id, limits),
        )
        .await
        .unwrap()
        .unwrap();
        let owner = fixture.session.owner.connection.lock().await;
        udp_path_write_frame(
            &mut peer_send,
            &Frame::StreamReset {
                stream_id,
                reason: ResetReason::RemoteClosed,
            },
            limits,
        )
        .await
        .unwrap();
        let result = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, opening)
            .await
            .expect("parsed RESET must return while physical owner is locked")
            .unwrap();
        assert!(matches!(
            result,
            Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
        ));
        if close_native {
            // Closure happens after the logical result, while its independent
            // exact-owner observer still cannot acquire this mutex.
            accepted.connection.close();
            tokio::time::timeout(OPEN_OWNERSHIP_GUARD, carrier.connection.wait_closed())
                .await
                .unwrap();
            assert_eq!(
                fixture.context.authenticated_carriers.snapshot().live_count,
                1
            );
        }
        drop(owner);
        if close_native {
            wait_for_client_owner_absence(&fixture.session).await;
            tokio::time::timeout(OPEN_OWNERSHIP_GUARD, async {
                loop {
                    if fixture.context.authenticated_carriers.snapshot().live_count == 0
                        && fixture
                            .context
                            .peer_status
                            .carrier_count(fixture.context.session_id)
                            == 0
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("independent native observer releases exact physical registrations");
        } else {
            retire_unopened_peer_repair(&accepted, limits).await;
            assert_ordered_detach(&mut peer_recv, stream_id, limits).await;
            assert_real_sibling_exchange(&fixture, &accepted).await;
            assert_eq!(
                current_client_carrier(&fixture.session)
                    .await
                    .unwrap()
                    .path_instance_id,
                carrier.path_instance_id
            );
        }
        let mut observed = Vec::new();
        observe_until(
            &mut events,
            stream_id,
            ClientUdpPendingOpenEvent::ContinuationExited,
            &mut observed,
        )
        .await;
        assert!(!observed.contains(&ClientUdpPendingOpenEvent::Accepted));
    }
}

const BLOCKED_STREAM_WINDOW: usize = 4 * 1024;
const BLOCKED_CONNECTION_WINDOW: usize = 5 * BLOCKED_STREAM_WINDOW;
// Each resolved H3 header plus Ping must consume less than half a stream's
// initial native window (asserted below). This many unread bodies therefore
// offer more native credit demand than the complete connection receive window.
const BLOCKED_FILLER_COUNT: usize = BLOCKED_CONNECTION_WINDOW / (BLOCKED_STREAM_WINDOW / 2) + 1;

struct NativeReceiveBackpressure {
    writers: Vec<tokio::task::JoinHandle<Result<(), RuntimeError>>>,
    peers: Vec<(UdpPathSendStream, UdpPathRecvStream)>,
    observers: Vec<quinn::SendStreamObserver>,
}

impl NativeReceiveBackpressure {
    async fn fill(fixture: &ClientOpenRaceFixture, accepted: &AcceptedTestCarrier) -> Self {
        use std::future::Future;

        let carrier = current_client_carrier(&fixture.session).await.unwrap();
        let limits = fixture.server_context.codec_limits;
        let mut local_pairs = Vec::new();
        let mut peers = Vec::new();
        let mut observers = Vec::new();
        // Resolve all requests before any body fills connection flow control.
        // The caller already removed the target's unopened repair request from
        // the peer's accept queue, so these identities are unambiguous.
        for index in 0..BLOCKED_FILLER_COUNT {
            let (local, peer) = tokio::join!(
                carrier.connection.open_bi(),
                accepted.connection.accept_bi(),
            );
            let (mut send, recv) = local.unwrap();
            let (peer_send, mut peer_recv) = peer.unwrap();
            let opener = Frame::Ping {
                nonce: index as u64,
            };
            udp_path_write_frame(&mut send, &opener, limits)
                .await
                .unwrap();
            assert_eq!(
                udp_path_read_frame(&mut peer_recv, limits).await.unwrap(),
                opener
            );
            let observer = send.native_progress_observer_for_test().unwrap();
            assert!(observer.snapshot().unwrap().accepted_end < (BLOCKED_STREAM_WINDOW / 2) as u64);
            observers.push(observer);
            local_pairs.push((send, recv));
            peers.push((peer_send, peer_recv));
        }
        let mut result = Self {
            writers: Vec::new(),
            peers,
            observers,
        };
        let mut pending = Vec::new();
        for (index, (mut send, recv)) in local_pairs.into_iter().enumerate() {
            let (entered, observed) = tokio::sync::oneshot::channel();
            pending.push(observed);
            result.writers.push(tokio::spawn(async move {
                let _recv = recv;
                let write = async {
                    for part in 0..2 {
                        udp_path_write_frame(
                            &mut send,
                            &Frame::StreamData {
                                stream_id: StreamId(1300 + index as u64),
                                offset: (part * BLOCKED_STREAM_WINDOW) as u64,
                                payload: bytes::Bytes::from(vec![0x5a; BLOCKED_STREAM_WINDOW]),
                            },
                            limits,
                        )
                        .await?;
                    }
                    super::super::super::io::udp_path_finish_stream(&mut send).await
                };
                tokio::pin!(write);
                let mut entered = Some(entered);
                std::future::poll_fn(|cx| {
                    let result = write.as_mut().poll(cx);
                    if result.is_pending()
                        && let Some(entered) = entered.take()
                    {
                        let _ = entered.send(());
                    }
                    result
                })
                .await
            }));
        }
        for entered in pending {
            entered
                .await
                .expect("each real unread native write becomes Pending");
        }
        result.wait_quiescent(&carrier.connection).await;
        result
    }

    async fn wait_quiescent(&self, connection: &UdpPathConnection) {
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, async {
            let mut previous = None;
            loop {
                assert!(
                    !connection.is_closed(),
                    "fixture needs live native backpressure"
                );
                assert!(self.writers.iter().all(|writer| !writer.is_finished()));
                let progress: Vec<_> = self
                    .observers
                    .iter()
                    .map(|observer| observer.snapshot().unwrap())
                    .collect();
                // No accepted filler bytes remain before packet construction,
                // and the native controller has no outstanding in-flight work.
                // A Pending writer still below its initial stream credit is
                // the discriminant from only filling each stream separately.
                let offsets: Vec<_> = progress.iter().map(|value| value.accepted_end).collect();
                if progress
                    .iter()
                    .all(|value| value.first_unpacketized == value.accepted_end)
                    && progress
                        .iter()
                        .any(|value| value.accepted_end < BLOCKED_STREAM_WINDOW as u64)
                    && connection
                        .connection
                        .native_controller_shape_snapshot()
                        .bytes_in_flight()
                        == 0
                {
                    if previous.as_ref() == Some(&offsets) {
                        return;
                    }
                    previous = Some(offsets);
                } else {
                    previous = None;
                }
                // Let native wake recipients run once before calling this a
                // quiescent boundary; a just-received ACK alone is not enough.
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect(
            "unread peer bodies must reach real native backpressure after packet service settles",
        );
    }

    async fn release(mut self, limits: CodecLimits) {
        // All writers share connection credit. Freed credit may serve a
        // different filler, so each real peer consumer must remain pollable.
        futures::future::join_all(self.peers.drain(..).enumerate().map(
            |(index, (mut send, mut recv))| async move {
                for part in 0..2 {
                    assert_eq!(
                        udp_path_read_frame(&mut recv, limits).await.unwrap(),
                        Frame::StreamData {
                            stream_id: StreamId(1300 + index as u64),
                            offset: (part * BLOCKED_STREAM_WINDOW) as u64,
                            payload: bytes::Bytes::from(vec![0x5a; BLOCKED_STREAM_WINDOW]),
                        },
                    );
                }
                let eof = udp_path_read_frame(&mut recv, limits).await;
                assert!(
                    eof.as_ref()
                        .is_err_and(super::super::super::io::udp_path_input_finished)
                );
                send.cancel_pending_response();
            },
        ))
        .await;
        for writer in self.writers.drain(..) {
            writer.await.unwrap().unwrap();
        }
    }

    async fn join_after_native_end(mut self) {
        for writer in self.writers.drain(..) {
            assert!(
                writer.await.unwrap().is_err(),
                "unread writes must end through native failure"
            );
        }
        for (mut send, _recv) in self.peers.drain(..) {
            send.cancel_pending_response();
        }
    }
}

impl Drop for NativeReceiveBackpressure {
    fn drop(&mut self) {
        // Failed assertions must not detach the fixture's own traffic tasks.
        for writer in &self.writers {
            writer.abort();
        }
    }
}

#[derive(Clone, Copy)]
enum BlockedRetirementEnd {
    PeerReads,
    NativeClose,
    SessionRetirement,
    OwnerDrop,
}

async fn blocked_submitted_retirement(end: BlockedRetirementEnd) {
    let fixture = ClientOpenRaceFixture::new_with_limits(
        Arc::new(SystemCarrierNetworkProvider),
        ResourceLimits {
            max_frame_bytes: 2 * BLOCKED_STREAM_WINDOW,
            max_payload_bytes: BLOCKED_STREAM_WINDOW,
            max_stream_window_bytes: BLOCKED_STREAM_WINDOW as u64,
            max_repair_bytes: BLOCKED_STREAM_WINDOW,
            max_reorder_bytes: BLOCKED_STREAM_WINDOW,
            max_datagram_queue_bytes: BLOCKED_STREAM_WINDOW,
            max_path_flight_bytes: BLOCKED_STREAM_WINDOW,
            max_reliable_relay_chunk_bytes: BLOCKED_STREAM_WINDOW,
            max_quic_concurrent_bidi_streams: 1 + 2 + BLOCKED_FILLER_COUNT,
            ..ResourceLimits::default()
        },
    )
    .await;
    let accepted = fixture.establish_current().await;
    let carrier = current_client_carrier(&fixture.session).await.unwrap();
    let owner = Arc::downgrade(&fixture.session.owner);
    let stream_id = StreamId(1260);
    let limits = fixture.server_context.codec_limits;
    let mut events = observe_pending_opens(&fixture.session);
    let opening = spawn_test_open(&fixture.session, stream_id);
    let (mut peer_send, mut peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .unwrap()
    .unwrap();
    let (mut repair_send, mut repair_recv) =
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, accepted.connection.accept_bi())
            .await
            .unwrap()
            .unwrap();
    let fillers = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        NativeReceiveBackpressure::fill(&fixture, &accepted),
    )
    .await
    .expect("fixed native-credit fixture setup");
    assert!(
        !opening.is_finished(),
        "cancel the original live first-MAX owner"
    );
    opening.abort();
    assert!(
        opening
            .await
            .err()
            .is_some_and(|error| error.is_cancelled())
    );
    let mut observed = Vec::new();
    observe_until(
        &mut events,
        stream_id,
        ClientUdpPendingOpenEvent::RetirementPending,
        &mut observed,
    )
    .await;
    assert!(!observed.contains(&ClientUdpPendingOpenEvent::ContinuationExited));
    // This repair request never carried MPP OPEN. It must release now, while
    // the ordinary DETACH remains blocked, rather than joining its lifetime.
    assert!(
        tokio::time::timeout(
            OPEN_OWNERSHIP_GUARD,
            udp_path_read_frame(&mut repair_recv, limits)
        )
        .await
        .unwrap()
        .is_err()
    );
    repair_send.cancel_pending_response();
    drop((repair_send, repair_recv));
    fillers.wait_quiescent(&carrier.connection).await;
    while let Ok((id, event)) = events.try_recv() {
        assert_eq!(id, stream_id);
        assert!(
            !matches!(
                event,
                ClientUdpPendingOpenEvent::RetirementFinished(_)
                    | ClientUdpPendingOpenEvent::ContinuationExited
            ),
            "retirement completed without peer credit or native retirement"
        );
        observed.push(event);
    }
    match end {
        BlockedRetirementEnd::PeerReads => {
            tokio::time::timeout(OPEN_OWNERSHIP_GUARD, fillers.release(limits))
                .await
                .expect("peer reads return native flow credit");
            assert_ordered_detach(&mut peer_recv, stream_id, limits).await;
            observe_until(
                &mut events,
                stream_id,
                ClientUdpPendingOpenEvent::ContinuationExited,
                &mut observed,
            )
            .await;
            assert!(observed.contains(&ClientUdpPendingOpenEvent::RetirementFinished(true)));
            assert_real_sibling_exchange(&fixture, &accepted).await;
            assert_eq!(
                current_client_carrier(&fixture.session)
                    .await
                    .unwrap()
                    .path_instance_id,
                carrier.path_instance_id
            );
        }
        BlockedRetirementEnd::NativeClose => {
            accepted.connection.close();
            observe_until(
                &mut events,
                stream_id,
                ClientUdpPendingOpenEvent::ContinuationExited,
                &mut observed,
            )
            .await;
            tokio::time::timeout(OPEN_OWNERSHIP_GUARD, fillers.join_after_native_end())
                .await
                .unwrap();
        }
        BlockedRetirementEnd::SessionRetirement => {
            fixture.context.retire_session(CloseReason::PolicyRejected);
            observe_until(
                &mut events,
                stream_id,
                ClientUdpPendingOpenEvent::ContinuationExited,
                &mut observed,
            )
            .await;
            tokio::time::timeout(OPEN_OWNERSHIP_GUARD, fillers.join_after_native_end())
                .await
                .unwrap();
        }
        BlockedRetirementEnd::OwnerDrop => {
            drop(fixture);
            assert!(
                owner.upgrade().is_none(),
                "retirement continuation must not own the physical owner Arc"
            );
            observe_until(
                &mut events,
                stream_id,
                ClientUdpPendingOpenEvent::ContinuationExited,
                &mut observed,
            )
            .await;
            tokio::time::timeout(OPEN_OWNERSHIP_GUARD, fillers.join_after_native_end())
                .await
                .unwrap();
        }
    }
    peer_send.cancel_pending_response();
    assert_eq!(
        observed
            .iter()
            .filter(|event| **event == ClientUdpPendingOpenEvent::ContinuationStarted)
            .count(),
        1
    );
    assert!(!observed.contains(&ClientUdpPendingOpenEvent::Accepted));
    if !matches!(end, BlockedRetirementEnd::PeerReads) {
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, carrier.connection.wait_closed())
            .await
            .unwrap();
        assert!(
            !observed.contains(&ClientUdpPendingOpenEvent::RetirementFinished(true)),
            "native cancellation is not delivered DETACH"
        );
    }
}

#[tokio::test]
async fn submitted_open_blocked_retirement_finishes_after_real_peer_reads() {
    blocked_submitted_retirement(BlockedRetirementEnd::PeerReads).await;
}

#[tokio::test]
async fn submitted_open_blocked_retirement_exits_on_native_close() {
    blocked_submitted_retirement(BlockedRetirementEnd::NativeClose).await;
}

#[tokio::test]
async fn submitted_open_blocked_retirement_exits_on_session_retirement() {
    blocked_submitted_retirement(BlockedRetirementEnd::SessionRetirement).await;
}

#[tokio::test]
async fn submitted_open_blocked_retirement_does_not_retain_physical_owner() {
    blocked_submitted_retirement(BlockedRetirementEnd::OwnerDrop).await;
}

#[tokio::test]
async fn accepted_open_cancellation_before_instance_commit_orders_detach() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let carrier = current_client_carrier(&fixture.session)
        .await
        .expect("established exact carrier");
    let stream_id = StreamId(1270);
    let limits = fixture.server_context.codec_limits;
    let (mut reached, _resume) = install_accepted_open_pause(&fixture.session);
    let opening = spawn_test_open(&fixture.session, stream_id);
    let (mut peer_send, mut peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .expect("real OPEN and initial MAX arrive")
    .expect("peer reads submitted request");
    let parent_request_id = peer_send.request_stream_id();
    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamMaxData {
            stream_id,
            max_offset: 0,
        },
        limits,
    )
    .await
    .expect("real first MAX admits this carrier attachment");
    let (kind, instance) = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, reached.recv())
        .await
        .expect("accepted value reaches existing pre-commit pause")
        .expect("accepted-open observer remains alive");
    assert_eq!(kind, ClientUdpAcceptedOpenKind::Reliable);
    assert_eq!(instance, carrier.path_instance_id);

    // Keep both real peer halves alive. Reading the repair OPEN proves the
    // successful continuation owns the pair before we cancel its logical caller.
    let (_repair_send, mut repair_recv) =
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, accepted.connection.accept_bi())
            .await
            .expect("accepted continuation submits its repair request")
            .expect("peer accepts repair request");
    assert_eq!(
        tokio::time::timeout(
            OPEN_OWNERSHIP_GUARD,
            udp_path_read_frame(&mut repair_recv, limits)
        )
        .await
        .expect("real repair OPEN arrives")
        .expect("peer reads repair OPEN"),
        Frame::OpenStreamRepair {
            stream_id,
            parent_request_id,
        },
    );
    assert!(!opening.is_finished(), "physical commit remains paused");
    opening.abort();
    assert!(
        opening
            .await
            .err()
            .is_some_and(|error| error.is_cancelled()),
        "join the cancelled original caller before observing retirement"
    );

    let next = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut peer_recv, limits),
    )
    .await;
    let detached = matches!(
        &next,
        Ok(Ok(Frame::StreamDetach { stream_id: detached })) if *detached == stream_id
    );
    let eof = if detached {
        Some(
            tokio::time::timeout(
                OPEN_OWNERSHIP_GUARD,
                udp_path_read_frame(&mut peer_recv, limits),
            )
            .await,
        )
    } else {
        None
    };
    // Check this even when the old path has already produced EOF: a missing
    // DETACH must not be confused with physical failure or a dead sibling.
    assert!(!carrier.connection.is_closed());
    assert_eq!(
        current_client_carrier(&fixture.session)
            .await
            .expect("same physical owner remains installed")
            .path_instance_id,
        carrier.path_instance_id,
    );
    assert_real_sibling_exchange(&fixture, &accepted).await;
    assert!(
        detached,
        "accepted caller cancellation must retain DETACH before native EOF: {next:?}"
    );
    assert!(
        matches!(eof, Some(Ok(Err(ref error))) if super::super::super::io::udp_path_input_finished(error)),
        "exactly one DETACH must be followed by native EOF: {eof:?}"
    );
}

#[tokio::test]
async fn accepted_zero_max_reset_survives_physical_commit_expiry() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let carrier = current_client_carrier(&fixture.session)
        .await
        .expect("established exact carrier");
    let stream_id = StreamId(1280);
    let nonce = 1280;
    let limits = fixture.server_context.codec_limits;
    let (mut reached, resume) = install_accepted_open_pause(&fixture.session);
    let mut opening = spawn_test_open(&fixture.session, stream_id);
    let (mut peer_send, mut peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .expect("real OPEN and initial MAX arrive")
    .expect("peer reads submitted request");
    let parent_request_id = peer_send.request_stream_id();
    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamMaxData {
            stream_id,
            max_offset: 0,
        },
        limits,
    )
    .await
    .expect("real zero first MAX admits only the carrier attachment");
    let (kind, instance) = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, reached.recv())
        .await
        .expect("accepted value reaches existing pre-commit pause")
        .expect("accepted-open observer remains alive");
    assert_eq!(kind, ClientUdpAcceptedOpenKind::Reliable);
    assert_eq!(instance, carrier.path_instance_id);

    let (_repair_send, mut repair_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        accepted.connection.accept_bi(),
    )
    .await
    .expect("accepted continuation submits its repair request")
    .expect("peer accepts repair request");
    assert_eq!(
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, udp_path_read_frame(&mut repair_recv, limits))
            .await
            .expect("real repair OPEN arrives")
            .expect("peer reads repair OPEN"),
        Frame::OpenStreamRepair {
            stream_id,
            parent_request_id,
        },
    );

    // Keep the original caller and both peer halves alive. The real physical
    // commit cannot succeed while this exact owner mutex remains held.
    let owner = fixture.session.owner.connection.lock().await;
    assert_eq!(
        owner.as_ref().map(|slot| slot.carrier.path_instance_id),
        Some(carrier.path_instance_id),
    );
    resume.notify_one();
    assert!(
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, reached.recv())
            .await
            .expect("one-shot accepted pause exits before terminal injection")
            .is_none(),
        "the consumed hook closes its observer when the opener leaves the pause",
    );
    assert!(!opening.is_finished(), "actual physical commit remains pending");
    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamReset {
            stream_id,
            reason: ResetReason::RemoteClosed,
        },
        limits,
    )
    .await
    .expect("peer sends logical RESET after actual first MAX");
    udp_path_write_frame(&mut peer_send, &Frame::Ping { nonce }, limits)
        .await
        .expect("peer sends ordered semantic-routing witness");
    let first_reply = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut peer_recv, limits),
    )
    .await
    .expect("Pong or correctly published early terminal arrives")
    .expect("peer reads complete reply without cancelling its frame read");
    let reset_routed_before_expiry = match first_reply {
        Frame::Pong { nonce: replied } if replied == nonce => true,
        // A corrected terminal handoff may settle before replying to Ping.
        // This path still must return the exact RESET; it is not an expiry-loss
        // witness and therefore does not advance the test clock.
        Frame::StreamDetach { stream_id: detached } if detached == stream_id => false,
        frame => panic!("unexpected pre-expiry witness: {frame:?}"),
    };
    let expired_after_pong = reset_routed_before_expiry && !opening.is_finished();
    if expired_after_pong {
        // The ordered Pong proves RESET passed through the ordinary actor;
        // peer write completion alone would not establish raw queue admission.
        // Advance only the original helper's ten-second deadline, as in the
        // submitted-open expiry control, then restore real time immediately.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::time::resume();
    }
    let observed = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, &mut opening).await;
    let returned_before_unlock = observed.is_ok();
    drop(owner);
    // Even an unexpected result is settled before the final conformance
    // assertion. Never abort the opener to manufacture terminal loss.
    let result = match observed {
        Ok(joined) => joined.expect("original opening task join"),
        Err(_) => tokio::time::timeout(OPEN_OWNERSHIP_GUARD, opening)
            .await
            .expect("opening settles after fixture releases physical mutex")
            .expect("original opening task join after mutex release"),
    };
    let returned_error = match result {
        Ok(opened) => {
            opened
                .retire_uncommitted()
                .expect("retire an unexpected exact accepted result");
            None
        }
        Err(error) => Some(error),
    };

    if reset_routed_before_expiry {
        assert_ordered_detach(&mut peer_recv, stream_id, limits).await;
    } else {
        let eof = tokio::time::timeout(
            OPEN_OWNERSHIP_GUARD,
            udp_path_read_frame(&mut peer_recv, limits),
        )
        .await
        .expect("native EOF follows the already observed early DETACH");
        assert!(eof.as_ref().is_err_and(super::super::super::io::udp_path_input_finished));
    }
    assert!(!carrier.connection.is_closed());
    assert_eq!(
        current_client_carrier(&fixture.session)
            .await
            .expect("same physical carrier remains installed")
            .path_instance_id,
        carrier.path_instance_id,
    );
    assert_real_sibling_exchange(&fixture, &accepted).await;
    assert!(
        returned_before_unlock,
        "terminal result must settle independently of physical commit after its existing deadline",
    );
    assert!(
        matches!(
            returned_error.as_ref(),
            Some(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
        ),
        "post-first-MAX RESET must survive physical commit expiry: reset_routed_before_expiry={reset_routed_before_expiry} expired_after_pong={expired_after_pong} returned_error={returned_error:?}",
    );
}
