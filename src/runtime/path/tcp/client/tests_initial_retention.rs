//! Real authenticated TCP producers exercise the finite initial coordinator.
use super::*;
use crate::model::path::RelayPathKey;
use crate::protocol::StreamReturnPlan;
use crate::runtime::relay::open::{open_remote_stream_until, reliable_initial_open_timeout};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

type Peer = EncryptedFramedStream<TcpStream>;

struct Fixture {
    context: ClientPathContext,
    peers: Vec<Peer>,
}

impl Fixture {
    async fn new() -> Self {
        let mut listeners = Vec::new();
        let mut paths = Vec::new();
        for _ in 0..2 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            paths.push(
                format!("tcp://{address}?max-tcp-carriers=1")
                    .parse()
                    .unwrap(),
            );
            listeners.push(listener);
        }
        let secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).unwrap();
        let server = crate::runtime::node::server::new_identity_runtime(
            Vec::new(),
            OutboundConfig::Direct,
            crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
            ServerSecurityConfig::for_test(secret.clone()),
            MppPerformanceConfig::default(),
            ResourceLimits::default(),
        );
        let context = ClientPathContext::new(
            paths,
            ClientSecurityConfig::for_test(secret),
            ResourceLimits::default(),
        )
        .unwrap();
        let mut peers = Vec::new();
        for (index, listener) in listeners.iter().enumerate() {
            let (ready, (peer, _)) = tokio::time::timeout(PEER_GUARD, async {
                tokio::join!(
                    context.tcp_sessions[index]
                        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(10)),
                    accept_authenticated_peer(listener, &server.paths, context.session_id),
                )
            })
            .await
            .expect("authenticate both real candidates");
            ready.unwrap();
            peers.push(peer);
        }
        Self { context, peers }
    }
}

async fn submitted_peer(mut peer: Peer, index: usize) -> (Peer, usize, StreamId, StreamReturnPlan) {
    assert!(matches!(
        peer.read_frame().await.unwrap(),
        Frame::PathMetrics { .. }
    ));
    let (stream_id, plan) = match peer.read_frame().await.unwrap() {
        Frame::OpenStream {
            stream_id,
            return_plan,
            ..
        } => (stream_id, return_plan),
        frame => panic!("expected actual CREATE, got {frame:?}"),
    };
    assert_eq!(plan.phase, crate::protocol::StreamAttachmentPhase::Create);
    assert!(
        matches!(peer.read_frame().await.unwrap(), Frame::StreamMaxData { stream_id: id, max_offset } if id == stream_id && max_offset > 0)
    );
    (peer, index, stream_id, plan)
}

async fn grant(peer: &mut Peer, stream_id: StreamId, max_offset: u64) {
    peer.write_frame(&Frame::StreamMaxData {
        stream_id,
        max_offset,
    })
    .await
    .unwrap();
    peer.flush().await.unwrap();
}

async fn routed(peer: &mut Peer, nonce: u64) {
    peer.write_frame(&Frame::Ping { nonce }).await.unwrap();
    peer.flush().await.unwrap();
    assert_eq!(peer.read_frame().await.unwrap(), Frame::Pong { nonce });
}

async fn detached(peer: &mut Peer, stream_id: StreamId) {
    loop {
        match peer.read_frame().await.unwrap() {
            Frame::StreamDetach { stream_id: id } => {
                assert_eq!(id, stream_id);
                return;
            }
            Frame::Ping { nonce } => {
                peer.write_frame(&Frame::Pong { nonce }).await.unwrap();
                peer.flush().await.unwrap();
            }
            Frame::PathMetrics { .. } | Frame::PathProofData { .. } => {}
            frame => panic!("unexpected retirement output: {frame:?}"),
        }
    }
}

async fn settle_actors(context: &ClientPathContext) {
    let terminals: Vec<_> = context
        .tcp_sessions
        .iter()
        .map(|handle| {
            handle
                .member
                .lock()
                .unwrap()
                .current
                .as_ref()
                .unwrap()
                .terminal
                .clone()
        })
        .collect();
    context.tcp_sessions[0]
        .runtime
        .state
        .session_lifecycle()
        .retire(CloseReason::Normal);
    tokio::time::timeout(PEER_GUARD, async {
        while terminals
            .iter()
            .any(|terminal| !terminal.load(Ordering::Acquire))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all real carrier actor owners settle");
}

async fn retained_race(original_wins: bool) {
    let Fixture { context, peers } = Fixture::new().await;
    let identities: Vec<_> = context
        .tcp_sessions
        .iter()
        .map(|handle| handle.connection_instance_id())
        .collect();
    let mut submitted = FuturesUnordered::new();
    for (index, peer) in peers.into_iter().enumerate() {
        submitted.push(submitted_peer(peer, index));
    }
    let opening = open_remote_stream_until(
        &context,
        TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
        TrafficClass::Latency,
        tokio::time::Instant::now() + Duration::from_secs(10),
    );
    tokio::pin!(opening);
    let peers = async {
        let (mut first, first_index, stream_id, first_plan) = submitted.next().await.unwrap();
        // Actual second OPEN/MAX, not elapsed time, proves the nominal successor entered.
        let (mut second, second_index, second_id, second_plan) = submitted.next().await.unwrap();
        assert_eq!(second_id, stream_id);
        assert_ne!(first_plan.candidate_ordinal, second_plan.candidate_ordinal);
        assert_eq!(first_plan.candidate_total, 2);
        assert_eq!(second_plan.candidate_total, 2);
        let (winner, winner_index) = if original_wins {
            (&mut first, first_index)
        } else {
            (&mut second, second_index)
        };
        grant(
            winner,
            stream_id,
            context.mux_limits.max_stream_window_bytes,
        )
        .await;
        (first, second, stream_id, winner_index)
    };
    let (opened, (mut first, mut second, stream_id, winner_index)) =
        tokio::time::timeout(PEER_GUARD, async { tokio::join!(&mut opening, peers) })
            .await
            .expect("actual retained/fallback peer race settles");
    let opened = opened.expect("the selected actual grant wins");
    assert_eq!(opened.path_index(), winner_index);
    assert_eq!(opened.stream().stream_id, stream_id);
    opened.retire_uncommitted();
    tokio::time::timeout(PEER_GUARD, async {
        tokio::join!(
            detached(&mut first, stream_id),
            detached(&mut second, stream_id)
        );
    })
    .await
    .expect("winner and submitted loser each retire through DETACH");
    for (handle, identity) in context.tcp_sessions.iter().zip(identities) {
        assert_eq!(handle.connection_instance_id(), identity);
    }
    settle_actors(&context).await;
}

#[tokio::test]
async fn tcp_initial_retained_original_wins_after_actual_fallback_submission() {
    retained_race(true).await;
}

#[tokio::test]
async fn tcp_initial_fallback_wins_and_retires_original() {
    retained_race(false).await;
}

#[tokio::test]
async fn tcp_initial_zero_max_before_nominal_decision_suppresses_fallback() {
    let Fixture { context, peers } = Fixture::new().await;
    let mut submitted = FuturesUnordered::new();
    for (index, peer) in peers.into_iter().enumerate() {
        submitted.push(submitted_peer(peer, index));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let opening = open_remote_stream_until(
        &context,
        TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
        TrafficClass::Latency,
        deadline,
    );
    tokio::pin!(opening);
    let (mut peer, index, stream_id, _) = tokio::time::timeout(PEER_GUARD, async {
        tokio::select! { first = submitted.next() => first.unwrap(), _ = &mut opening => panic!("no peer MAX yet"), }
    }).await.unwrap();
    grant(&mut peer, stream_id, 0).await;
    // The logical opener stays unpolled while the actual actor handles MAX/Ping.
    tokio::time::timeout(PEER_GUARD, routed(&mut peer, 1510))
        .await
        .unwrap();
    let nominal = reliable_initial_open_timeout(
        &context,
        RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        },
        true,
    );
    tokio::time::pause();
    tokio::time::advance(nominal).await;
    assert!(
        opening.as_mut().now_or_never().is_none(),
        "zero admission is not logical success"
    );
    assert_eq!(
        context.reliable_selection_passes_for_test(),
        1,
        "actual first MAX suppresses timer-driven next CREATE"
    );
    tokio::time::resume();
    grant(
        &mut peer,
        stream_id,
        context.mux_limits.max_stream_window_bytes,
    )
    .await;
    let opened = tokio::time::timeout(PEER_GUARD, &mut opening)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(opened.path_index(), index);
    opened.retire_uncommitted();
    tokio::time::timeout(PEER_GUARD, detached(&mut peer, stream_id))
        .await
        .unwrap();
    drop(submitted);
    settle_actors(&context).await;
}

async fn logical_deadline_with_terminal(reset: bool) {
    let Fixture { context, peers } = Fixture::new().await;
    let mut submitted = FuturesUnordered::new();
    for (index, peer) in peers.into_iter().enumerate() {
        submitted.push(submitted_peer(peer, index));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let opening = open_remote_stream_until(
        &context,
        TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
        TrafficClass::Latency,
        deadline,
    );
    tokio::pin!(opening);
    let (mut peer, _, stream_id, _) = tokio::time::timeout(PEER_GUARD, async {
        tokio::select! { first = submitted.next() => first.unwrap(), _ = &mut opening => panic!("no peer MAX yet"), }
    }).await.unwrap();
    grant(&mut peer, stream_id, 0).await;
    if reset {
        peer.write_frame(&Frame::StreamReset {
            stream_id,
            reason: ResetReason::RemoteClosed,
        })
        .await
        .unwrap();
    }
    tokio::time::timeout(PEER_GUARD, routed(&mut peer, 1511))
        .await
        .unwrap();
    // Advance only after actual authenticated processing. No production budget is changed.
    tokio::time::pause();
    tokio::time::advance(deadline.saturating_duration_since(tokio::time::Instant::now())).await;
    let result = opening.await;
    tokio::time::resume();
    let error = match result {
        Ok(opened) => {
            opened.retire_uncommitted();
            panic!("no positive target grant");
        }
        Err(error) => error,
    };
    if reset {
        assert!(
            matches!(error, RuntimeError::RemoteReset(ResetReason::RemoteClosed)),
            "{error:?}"
        );
    } else {
        assert!(
            matches!(
                error,
                RuntimeError::OutboundConnect(
                    crate::outbound::OutboundConnectError::ConnectTimeout
                )
            ),
            "{error:?}"
        );
    }
    drop(submitted);
    settle_actors(&context).await;
}

#[tokio::test]
async fn tcp_initial_logical_deadline_is_not_path_nominal_timeout() {
    logical_deadline_with_terminal(false).await;
}

#[tokio::test]
async fn tcp_initial_routed_reset_precedes_logical_deadline() {
    logical_deadline_with_terminal(true).await;
}
