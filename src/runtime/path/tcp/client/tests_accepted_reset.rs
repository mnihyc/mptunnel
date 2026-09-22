use super::*;
use crate::config::{MppPerformanceConfig, ServerSecurityConfig};
use crate::outbound::OutboundConfig;
use crate::protocol::{CloseReason, Frame, PathUsage, UnderlayProtocol};
use crate::runtime::path::server_context::ServerPathContext;
use crate::transport::encrypted::EncryptedFramedStream;
use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};

const PEER_GUARD: Duration = Duration::from_secs(5);

async fn accept_authenticated_peer(
    listener: &TcpListener,
    context: &ServerPathContext,
    expected_session: crate::protocol::SessionId,
) -> (EncryptedFramedStream<TcpStream>, PathId) {
    let (socket, _) = listener.accept().await.expect("accept real TCP carrier");
    let mut framed = EncryptedFramedStream::accept(socket, &context.tls, context.codec_limits)
        .await
        .expect("accept protected TCP carrier");
    let binding = framed.tcp_admission_binding().expect("transport binding");
    let prelude = framed
        .read_tcp_admission()
        .await
        .expect("actual admission prelude");
    let authenticated = crate::runtime::path::tcp::admission::authenticate_prelude(
        &context.security,
        context.credential_admission.clone(),
        &prelude,
        &binding,
    )
    .expect("authenticate actual TCP prelude")
    .expect("known peer credential");
    let joined = authenticated
        .authenticate_path_join(
            UnderlayProtocol::Tcp,
            framed.read_frame().await.expect("actual path join"),
        )
        .expect("authenticate path join")
        .expect("valid path join");
    assert_eq!(joined.session_id, expected_session);
    assert!(context.accept_path_join_nonce(
        joined.session_id,
        joined.credential_id.clone(),
        joined.path_id,
        UnderlayProtocol::Tcp,
        joined.nonce,
        joined.issued_at_unix_secs,
        joined.verified_at_unix_secs,
    ));
    assert!(matches!(
        framed.read_frame().await.expect("client usage"),
        Frame::PathStatus { path_id, sequence: 0, .. } if path_id == joined.path_id
    ));
    framed
        .write_frame(&Frame::SessionReady)
        .await
        .expect("session ready");
    framed
        .write_frame(&Frame::PathStatus {
            path_id: joined.path_id,
            sequence: 0,
            usage: PathUsage::Available,
        })
        .await
        .expect("server usage");
    framed.flush().await.expect("flush authenticated readiness");
    (framed, joined.path_id)
}

async fn peer_read_open_and_max(peer: &mut EncryptedFramedStream<TcpStream>, expected: StreamId) {
    // The TCP OPEN producer writes its existing path evidence first.
    let metrics = peer.read_frame().await.expect("OPEN path metrics");
    assert!(matches!(metrics, Frame::PathMetrics { .. }), "{metrics:?}");
    assert!(matches!(peer.read_frame().await.expect("real OPEN"),
        Frame::OpenStream { stream_id, .. } if stream_id == expected));
    assert!(
        matches!(peer.read_frame().await.expect("initial receive MAX"),
        Frame::StreamMaxData { stream_id, max_offset } if stream_id == expected && max_offset > 0)
    );
}

#[tokio::test]
async fn tcp_accepted_slot_reset_survives_real_carrier_replacement() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("TCP test listener");
    let address = listener.local_addr().expect("test address");
    let secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
        .expect("test shared secret");
    let server = crate::runtime::node::server::new_identity_runtime(
        Vec::new(),
        OutboundConfig::Direct,
        crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        ServerSecurityConfig::for_test(secret.clone()),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    let context = ClientPathContext::new(
        // Planned replacement requires the declared pool to be occupied. This
        // ownership fixture establishes exactly one real carrier.
        vec![
            format!("tcp://{address}?max-tcp-carriers=1")
                .parse()
                .expect("real TCP path"),
        ],
        ClientSecurityConfig::for_test(secret),
        ResourceLimits::default(),
    )
    .expect("client TCP context");
    let handle = &context.tcp_sessions[0];
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(10);
    let (ready, (mut old_peer, old_path)) = tokio::time::timeout(PEER_GUARD, async {
        tokio::join!(
            handle.prepare_connection(deadline()),
            accept_authenticated_peer(&listener, &server.paths, context.session_id)
        )
    })
    .await
    .expect("authenticate initial real carrier");
    ready.expect("initial carrier ready");
    let old_instance = handle
        .connection_instance_id()
        .expect("actual predecessor identity");
    let stream_id = StreamId(1400);
    let mut opening = Box::pin(handle.open_stream_with_deadlines(
        stream_id,
        TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
        TrafficClass::Latency,
        StreamDemandHint::Latency,
        Default::default(),
        ClientTcpOpenDeadlines::fixed(deadline()),
        context.mux_limits.max_stream_window_bytes,
    ));
    tokio::time::timeout(PEER_GUARD, async {
        tokio::select! {
            () = peer_read_open_and_max(&mut old_peer, stream_id) => {},
            _ = &mut opening => panic!("open completes before peer first MAX"),
        }
    })
    .await
    .expect("submitted OPEN/MAX before response-slot exposure");

    // Do not poll the opener again until the real actor has published success
    // and then handled RESET. No synthetic response slot or parser call is used.
    for frame in [
        Frame::StreamMaxData {
            stream_id,
            max_offset: 0,
        },
        Frame::StreamReset {
            stream_id,
            reason: ResetReason::RemoteClosed,
        },
        Frame::Ping { nonce: 1400 },
    ] {
        old_peer
            .write_frame(&frame)
            .await
            .expect("peer writes actual terminal sequence");
    }
    old_peer.flush().await.expect("flush terminal sequence");
    assert_eq!(
        tokio::time::timeout(PEER_GUARD, old_peer.read_frame())
            .await
            .expect("ordered semantic routing witness")
            .expect("read full Pong"),
        Frame::Pong { nonce: 1400 }
    );

    let generation = handle.runtime.endpoint_policy.snapshot().generation;
    let (replacement, (mut new_peer, new_path)) = tokio::time::timeout(PEER_GUARD, async {
        tokio::try_join!(
            handle.replace_connection_for_endpoint_generation(
                deadline(),
                generation,
                address.port()
            ),
            async {
                Ok::<_, RuntimeError>(
                    accept_authenticated_peer(&listener, &server.paths, context.session_id).await,
                )
            }
        )
    })
    .await
    .expect("real authenticated planned replacement")
    .expect("publish actual successor");
    assert_eq!(replacement.predecessor_instance_id, old_instance);
    assert_eq!(replacement.predecessor_path_id, old_path);
    assert_eq!(replacement.successor_path_id, new_path);
    assert_ne!(replacement.successor_instance_id, old_instance);

    let reopened = Arc::new(AtomicBool::new(false));
    let observed_reopen = reopened.clone();
    let max_offset = context.mux_limits.max_stream_window_bytes;
    let peer_task = tokio::spawn(async move {
        loop {
            let frame = match new_peer.read_frame().await {
                Ok(frame) => frame,
                Err(_) => return,
            };
            let reply = match frame {
                Frame::OpenStream {
                    stream_id: opened, ..
                } => {
                    if opened == stream_id {
                        observed_reopen.store(true, Ordering::Release);
                        Some(Frame::StreamReset {
                            stream_id: opened,
                            reason: ResetReason::RemoteClosed,
                        })
                    } else {
                        Some(Frame::StreamMaxData {
                            stream_id: opened,
                            max_offset,
                        })
                    }
                }
                Frame::StreamData {
                    stream_id,
                    offset,
                    payload,
                } => Some(Frame::StreamData {
                    stream_id,
                    offset,
                    payload,
                }),
                Frame::Ping { nonce } => Some(Frame::Pong { nonce }),
                Frame::PathMetrics { .. }
                | Frame::StreamMaxData { .. }
                | Frame::StreamDetach { .. } => None,
                frame => panic!("unexpected successor peer frame: {frame:?}"),
            };
            if let Some(reply) = reply {
                new_peer
                    .write_frame(&reply)
                    .await
                    .expect("reply on real successor");
                new_peer.flush().await.expect("flush successor reply");
            }
        }
    });
    let result = tokio::time::timeout(PEER_GUARD, &mut opening)
        .await
        .expect("original caller observes settled response");
    drop(opening);
    let returned_error = match result {
        Ok(opened) => {
            opened
                .carrier
                .retire_uncommitted()
                .expect("retire unexpected successful reopen");
            None
        }
        Err(error) => Some(error),
    };
    assert_eq!(
        handle.connection_instance_id(),
        Some(replacement.successor_instance_id)
    );
    let mut sibling = tokio::time::timeout(
        PEER_GUARD,
        handle.open_stream_with_deadlines(
            StreamId(1401),
            TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
            TrafficClass::Latency,
            StreamDemandHint::Latency,
            Default::default(),
            ClientTcpOpenDeadlines::fixed(deadline()),
            context.mux_limits.max_stream_window_bytes,
        ),
    )
    .await
    .expect("sibling open guard")
    .expect("real successor accepts sibling");
    assert_eq!(
        sibling.carrier.path_instance_id,
        replacement.successor_instance_id
    );
    let data = Frame::StreamData {
        stream_id: StreamId(1401),
        offset: 0,
        payload: Bytes::from_static(b"alive"),
    };
    sibling
        .carrier
        .commands
        .try_enqueue_admitted_frame(data.clone(), TrafficClass::Latency)
        .expect("send real sibling payload");
    assert_eq!(
        tokio::time::timeout(PEER_GUARD, sibling.carrier.frames.recv())
            .await
            .expect("sibling payload guard")
            .expect("sibling input stays live")
            .expect("authenticated sibling response"),
        data
    );
    sibling
        .carrier
        .retire_uncommitted()
        .expect("retire sibling once");

    let (current_terminal, predecessor_terminal) = {
        let member = handle.member.lock().expect("actual member state");
        (
            member
                .current
                .as_ref()
                .expect("successor slot")
                .terminal
                .clone(),
            member
                .retiring_predecessor
                .as_ref()
                .map(|slot| slot.terminal.clone()),
        )
    };
    handle
        .runtime
        .state
        .session_lifecycle()
        .retire(CloseReason::Normal);
    tokio::time::timeout(PEER_GUARD, async {
        while !current_terminal.load(Ordering::Acquire)
            || predecessor_terminal
                .as_ref()
                .is_some_and(|value| !value.load(Ordering::Acquire))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both real actor lifetimes settle");
    drop(old_peer);
    peer_task.abort();
    let peer_result = peer_task.await;
    assert!(
        peer_result.is_ok()
            || peer_result
                .as_ref()
                .is_err_and(|error| error.is_cancelled())
    );
    let reopened = reopened.load(Ordering::Acquire);
    assert!(
        matches!(
            returned_error.as_ref(),
            Some(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
        ) && !reopened,
        "routed RESET must survive accepted response custody: returned_error={returned_error:?} reopened_on_successor={reopened}"
    );
}

#[tokio::test]
async fn tcp_accepted_stream_detach_reopens_same_id_on_live_carrier() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("TCP test listener");
    let address = listener.local_addr().expect("test address");
    let secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
        .expect("test shared secret");
    let server = crate::runtime::node::server::new_identity_runtime(
        Vec::new(),
        OutboundConfig::Direct,
        crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        ServerSecurityConfig::for_test(secret.clone()),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    let context = ClientPathContext::new(
        vec![
            format!("tcp://{address}?max-tcp-carriers=1")
                .parse()
                .expect("real TCP path"),
        ],
        ClientSecurityConfig::for_test(secret),
        ResourceLimits::default(),
    )
    .expect("client TCP context");
    let handle = &context.tcp_sessions[0];
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(10);
    let (ready, (mut peer, _path_id)) = tokio::time::timeout(PEER_GUARD, async {
        tokio::join!(
            handle.prepare_connection(deadline()),
            accept_authenticated_peer(&listener, &server.paths, context.session_id)
        )
    })
    .await
    .expect("authenticate real TCP carrier");
    ready.expect("initial carrier ready");
    let carrier_instance = handle
        .connection_instance_id()
        .expect("live carrier identity");

    let stream_id = StreamId(1500);
    let accept_max = context.mux_limits.max_stream_window_bytes;
    let (old_result, ()) = tokio::time::timeout(PEER_GUARD, async {
        tokio::join!(
            handle.open_stream_with_deadlines(
                stream_id,
                TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
                TrafficClass::Latency,
                StreamDemandHint::Latency,
                Default::default(),
                ClientTcpOpenDeadlines::fixed(deadline()),
                context.mux_limits.max_stream_window_bytes,
            ),
            async {
                peer_read_open_and_max(&mut peer, stream_id).await;
                peer.write_frame(&Frame::StreamMaxData {
                    stream_id,
                    max_offset: accept_max,
                })
                .await
                .expect("peer accepts initial attachment");
                peer.flush().await.expect("flush initial acceptance");
            },
        )
    })
    .await
    .expect("initial OPEN/MAX acceptance");
    let old = old_result.expect("initial stream accepted");
    assert_eq!(old.carrier.path_instance_id, carrier_instance);

    let sibling_id = StreamId(1501);
    let (sibling_result, ()) = tokio::time::timeout(PEER_GUARD, async {
        tokio::join!(
            handle.open_stream_with_deadlines(
                sibling_id,
                TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
                TrafficClass::Latency,
                StreamDemandHint::Latency,
                Default::default(),
                ClientTcpOpenDeadlines::fixed(deadline()),
                context.mux_limits.max_stream_window_bytes,
            ),
            async {
                peer_read_open_and_max(&mut peer, sibling_id).await;
                peer.write_frame(&Frame::StreamMaxData {
                    stream_id: sibling_id,
                    max_offset: accept_max,
                })
                .await
                .expect("peer accepts sibling attachment");
                peer.flush().await.expect("flush sibling acceptance");
            },
        )
    })
    .await
    .expect("sibling OPEN/MAX acceptance");
    let sibling = sibling_result.expect("sibling stream accepted");
    assert_eq!(sibling.carrier.path_instance_id, carrier_instance);

    // Keep the old command capability so a late duplicate retirement can be
    // issued after the replacement is live. The real peer must see DETACH
    // exactly once for the old attachment.
    let old_commands = old.carrier.commands.clone();
    old.carrier
        .retire_uncommitted()
        .expect("retire accepted predecessor");
    assert_eq!(
        tokio::time::timeout(PEER_GUARD, peer.read_frame())
            .await
            .expect("predecessor DETACH guard")
            .expect("predecessor carrier remains readable"),
        Frame::StreamDetach { stream_id }
    );

    let (replacement_result, ()) = tokio::time::timeout(PEER_GUARD, async {
        tokio::join!(
            handle.open_stream_with_deadlines(
                stream_id,
                TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
                TrafficClass::Latency,
                StreamDemandHint::Latency,
                Default::default(),
                ClientTcpOpenDeadlines::fixed(deadline()),
                context.mux_limits.max_stream_window_bytes,
            ),
            async {
                peer_read_open_and_max(&mut peer, stream_id).await;
                peer.write_frame(&Frame::StreamMaxData {
                    stream_id,
                    max_offset: accept_max,
                })
                .await
                .expect("peer accepts same-ID replacement");
                peer.flush().await.expect("flush same-ID acceptance");
            },
        )
    })
    .await
    .expect("same-ID replacement OPEN/MAX acceptance");
    let replacement = replacement_result.expect("same-ID replacement accepted");
    assert_eq!(replacement.carrier.path_instance_id, carrier_instance);
    assert_eq!(handle.connection_instance_id(), Some(carrier_instance));

    // A late retirement from the predecessor must not detach the live
    // replacement. Its data and the sibling's data both reach the same peer.
    old_commands
        .retire_accepted_stream(stream_id)
        .expect("late predecessor retirement is admitted");
    let replacement_data = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"replacement"),
    };
    replacement
        .carrier
        .commands
        .try_enqueue_admitted_frame(replacement_data.clone(), TrafficClass::Latency)
        .expect("queue replacement data");
    assert_eq!(
        tokio::time::timeout(PEER_GUARD, peer.read_frame())
            .await
            .expect("replacement data guard")
            .expect("replacement peer remains readable"),
        replacement_data
    );
    let sibling_data = Frame::StreamData {
        stream_id: sibling_id,
        offset: 0,
        payload: Bytes::from_static(b"sibling"),
    };
    sibling
        .carrier
        .commands
        .try_enqueue_admitted_frame(sibling_data.clone(), TrafficClass::Latency)
        .expect("queue sibling data");
    assert_eq!(
        tokio::time::timeout(PEER_GUARD, peer.read_frame())
            .await
            .expect("sibling data guard")
            .expect("sibling peer remains readable"),
        sibling_data
    );

    replacement
        .carrier
        .retire_uncommitted()
        .expect("retire replacement once");
    sibling
        .carrier
        .retire_uncommitted()
        .expect("retire sibling once");
    handle
        .runtime
        .state
        .session_lifecycle()
        .retire(CloseReason::Normal);
}

#[path = "tests_initial_retention.rs"]
mod initial_retention;
