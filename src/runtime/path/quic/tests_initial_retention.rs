//! Full logical coordinator with two authenticated native QUIC carriers.
use super::*;
use crate::model::path::RelayPathKey;
use crate::protocol::{
    PathMetricDirection, StreamAttachmentPhase, StreamReturnPlan, UnderlayProtocol,
};
use crate::runtime::relay::open::{open_remote_stream_until, reliable_initial_open_timeout};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

async fn submitted_request(
    connection: &UdpPathConnection,
    index: usize,
    limits: CodecLimits,
) -> (
    UdpPathSendStream,
    UdpPathRecvStream,
    usize,
    StreamId,
    StreamReturnPlan,
) {
    let (send, mut recv) = connection.accept_bi().await.unwrap();
    let (stream_id, plan) = match udp_path_read_frame(&mut recv, limits).await.unwrap() {
        Frame::OpenStream {
            stream_id,
            return_plan,
            ..
        } => (stream_id, return_plan),
        frame => panic!("expected actual CREATE: {frame:?}"),
    };
    assert_eq!(plan.phase, StreamAttachmentPhase::Create);
    assert!(
        matches!(udp_path_read_frame(&mut recv, limits).await.unwrap(),
        Frame::StreamMaxData { stream_id: id, max_offset } if id == stream_id && max_offset > 0)
    );
    (send, recv, index, stream_id, plan)
}

async fn retained_native_race(original_wins: bool) {
    let fixture = ClientOpenRaceFixture::new_with_path_count(
        Arc::new(SystemCarrierNetworkProvider),
        ResourceLimits::default(),
        2,
    )
    .await;
    let first_carrier = fixture.establish_current().await;
    let accepting = fixture.spawn_server_accept();
    fixture.context.udp_sessions[1]
        .prepare_connection(tokio::time::Instant::now() + OPEN_OWNERSHIP_GUARD)
        .await
        .unwrap();
    let second_carrier = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, accepting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let accepted = [first_carrier, second_carrier];
    let mut identities = Vec::new();
    for session in fixture.context.udp_sessions.iter() {
        identities.push(
            current_client_carrier(session)
                .await
                .unwrap()
                .path_instance_id,
        );
    }
    let limits = fixture.server_context.codec_limits;
    let mut events: Vec<_> = fixture
        .context
        .udp_sessions
        .iter()
        .map(observe_pending_opens)
        .collect();
    let mut submitted = FuturesUnordered::new();
    for (index, carrier) in accepted.iter().enumerate() {
        submitted.push(submitted_request(&carrier.connection, index, limits));
    }
    let opening = open_remote_stream_until(
        &fixture.context,
        TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
        TrafficClass::Latency,
        tokio::time::Instant::now() + Duration::from_secs(10),
    );
    tokio::pin!(opening);
    let peers = async {
        let (mut first_send, first_recv, first_index, stream_id, first_plan) =
            submitted.next().await.unwrap();
        let (mut second_send, second_recv, second_index, second_id, second_plan) =
            submitted.next().await.unwrap();
        assert_eq!(second_id, stream_id);
        assert_eq!(first_plan.candidate_total, 2);
        assert_eq!(second_plan.candidate_total, 2);
        assert_ne!(first_plan.candidate_ordinal, second_plan.candidate_ordinal);
        let (winner, index) = if original_wins {
            (&mut first_send, first_index)
        } else {
            (&mut second_send, second_index)
        };
        udp_path_write_frame(
            winner,
            &Frame::StreamMaxData {
                stream_id,
                max_offset: fixture.context.mux_limits.max_stream_window_bytes,
            },
            limits,
        )
        .await
        .unwrap();
        (
            first_send,
            first_recv,
            second_send,
            second_recv,
            stream_id,
            index,
        )
    };
    let (result, (first_send, mut first_recv, second_send, mut second_recv, stream_id, winner)) =
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, async {
            tokio::join!(&mut opening, peers)
        })
        .await
        .expect("actual native original/fallback grant settles");
    let opened = result.expect("actual accepted candidate wins");
    assert_eq!(opened.path_index(), winner);
    assert_eq!(opened.stream().stream_id, stream_id);
    let parent_request_id = if original_wins {
        first_send.request_stream_id()
    } else {
        second_send.request_stream_id()
    };
    // Acceptance transfers the repair pair to the ordinary continuation. Its
    // real producer always submits this repair OPEN; only the loser stayed
    // unopened. Observe the exact parent before requesting winner retirement.
    let (mut repair_send, mut repair_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        accepted[winner].connection.accept_bi(),
    )
    .await
    .expect("accepted continuation exposes its repair request")
    .unwrap();
    assert_eq!(
        tokio::time::timeout(
            OPEN_OWNERSHIP_GUARD,
            udp_path_read_frame(&mut repair_recv, limits)
        )
        .await
        .expect("accepted repair OPEN arrives")
        .unwrap(),
        Frame::OpenStreamRepair {
            stream_id,
            parent_request_id
        },
        "repair ownership names the actual winning ordinary native request",
    );
    // The logical opener publishes exactly one opening metrics frame, unlike
    // the lower-level pending-open fixtures. Observe that producer before
    // retirement; the strict DETACH/EOF checks below still reject extra frames.
    let winner_recv = if original_wins {
        &mut first_recv
    } else {
        &mut second_recv
    };
    let opening_metrics = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(winner_recv, limits),
    )
    .await
    .expect("accepted logical opener publishes its opening metrics")
    .unwrap();
    let Frame::PathMetrics { metrics } = opening_metrics else {
        panic!("expected the winner's single opening metrics frame: {opening_metrics:?}");
    };
    assert_eq!(metrics.path_id, accepted[winner].registration.path_id());
    assert_eq!(metrics.underlay, accepted[winner].registration.underlay());
    assert_eq!(metrics.underlay, UnderlayProtocol::Udp);
    assert_eq!(metrics.direction, PathMetricDirection::ClientToServer);
    opened.retire_uncommitted();
    tokio::join!(
        assert_ordered_detach(&mut first_recv, stream_id, limits),
        assert_ordered_detach(&mut second_recv, stream_id, limits),
    );
    let repair_end = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut repair_recv, limits),
    )
    .await
    .expect("accepted repair request ends with its ordinary owner");
    assert!(
        repair_end
            .as_ref()
            .is_err_and(super::super::super::super::io::udp_path_input_finished),
        "accepted repair must finish after its exact OPEN, without another MPP frame: {repair_end:?}"
    );
    repair_send.cancel_pending_response();
    for (index, carrier) in accepted.iter().enumerate() {
        if index != winner {
            retire_unopened_peer_repair(carrier, limits).await;
        }
        let mut observed = Vec::new();
        observe_until(
            &mut events[index],
            stream_id,
            ClientUdpPendingOpenEvent::ContinuationExited,
            &mut observed,
        )
        .await;
        let count = |expected| observed.iter().filter(|event| **event == expected).count();
        assert_eq!(count(ClientUdpPendingOpenEvent::Submitted), 1);
        assert_eq!(count(ClientUdpPendingOpenEvent::ContinuationStarted), 1);
        assert_eq!(count(ClientUdpPendingOpenEvent::ContinuationExited), 1);
        assert_eq!(
            count(ClientUdpPendingOpenEvent::Accepted),
            usize::from(index == winner)
        );
        assert_eq!(
            count(ClientUdpPendingOpenEvent::Retiring),
            usize::from(index != winner)
        );
        assert_eq!(
            count(ClientUdpPendingOpenEvent::RetirementFinished(true)),
            usize::from(index != winner)
        );
        assert_eq!(
            count(ClientUdpPendingOpenEvent::RetirementFinished(false)),
            0
        );
    }
    for (session, identity) in fixture.context.udp_sessions.iter().zip(identities) {
        let current = current_client_carrier(session).await.unwrap();
        assert_eq!(current.path_instance_id, identity);
        assert!(!current.connection.is_closed());
    }
    assert_real_sibling_exchange(&fixture, &accepted[0]).await;
}

#[tokio::test]
async fn quic_initial_retained_original_wins_after_actual_fallback_submission() {
    retained_native_race(true).await;
}

#[tokio::test]
async fn quic_initial_fallback_wins_and_retires_original() {
    retained_native_race(false).await;
}

#[tokio::test]
async fn quic_initial_singleton_accepts_first_max_after_setup_allowance() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let mut events = observe_pending_opens(&fixture.session);
    let limits = fixture.server_context.codec_limits;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let opening = open_remote_stream_until(
        &fixture.context,
        TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
        TrafficClass::Latency,
        deadline,
    );
    tokio::pin!(opening);
    let (mut peer_send, mut peer_recv, index, stream_id, plan) =
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, async {
            tokio::select! {
                request = submitted_request(&accepted.connection, 0, limits) => request,
                _ = &mut opening => panic!("single-candidate open completed before peer MAX"),
            }
        })
        .await
        .expect("single candidate submits its native CREATE/MAX");
    let parent_request_id = peer_send.request_stream_id();
    assert_eq!(plan.candidate_total, 1);
    assert_eq!(plan.candidate_ordinal, 0);

    let setup = reliable_initial_open_timeout(
        &fixture.context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index,
        },
        false,
    );
    tokio::time::pause();
    tokio::time::advance(setup + Duration::from_millis(1)).await;
    assert!(
        opening.as_mut().now_or_never().is_none(),
        "a fully submitted final candidate remains live beyond its setup allowance"
    );
    tokio::time::resume();

    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamMaxData {
            stream_id,
            max_offset: fixture.context.mux_limits.max_stream_window_bytes,
        },
        limits,
    )
    .await
    .expect("peer submits late first MAX");
    let opened = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, &mut opening)
        .await
        .expect("late first MAX settles before the original logical deadline")
        .expect("late first MAX admits the still-live final candidate");
    assert_eq!(opened.path_index(), index);
    assert_eq!(opened.stream().stream_id, stream_id);
    assert_eq!(fixture.context.reliable_selection_passes_for_test(), 1);

    let (mut repair_send, mut repair_recv) =
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
        .expect("repair OPEN arrives")
        .expect("peer reads repair OPEN"),
        Frame::OpenStreamRepair {
            stream_id,
            parent_request_id,
        },
        "the continuation is bound to this exact successful CREATE",
    );
    // The logical opener publishes its opening metrics after acceptance.
    // Consume that exact frame before asserting strict DETACH/EOF ordering.
    let opening_metrics = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut peer_recv, limits),
    )
    .await
    .expect("accepted logical opener publishes its opening metrics")
    .expect("peer reads opening metrics");
    let Frame::PathMetrics { metrics } = opening_metrics else {
        panic!("expected the winner's single opening metrics frame: {opening_metrics:?}");
    };
    assert_eq!(metrics.path_id, accepted.registration.path_id());
    assert_eq!(metrics.underlay, UnderlayProtocol::Udp);
    assert_eq!(metrics.direction, PathMetricDirection::ClientToServer);
    opened.retire_uncommitted();
    assert_ordered_detach(&mut peer_recv, stream_id, limits).await;
    let repair_end = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut repair_recv, limits),
    )
    .await
    .expect("repair receive half retires with its exact ordinary owner");
    assert!(
        repair_end
            .as_ref()
            .is_err_and(crate::runtime::path::quic::io::udp_path_input_finished),
        "the successful repair request closes without extra MPP frames: {repair_end:?}"
    );
    repair_send.cancel_pending_response();

    let mut observed = Vec::new();
    observe_until(
        &mut events,
        stream_id,
        ClientUdpPendingOpenEvent::ContinuationExited,
        &mut observed,
    )
    .await;
    let count = |expected| observed.iter().filter(|event| **event == expected).count();
    assert_eq!(count(ClientUdpPendingOpenEvent::Submitted), 1);
    assert_eq!(count(ClientUdpPendingOpenEvent::ContinuationStarted), 1);
    assert_eq!(count(ClientUdpPendingOpenEvent::Accepted), 1);
    assert_eq!(count(ClientUdpPendingOpenEvent::ContinuationExited), 1);
    assert_real_sibling_exchange(&fixture, &accepted).await;
}

#[tokio::test]
async fn quic_initial_singleton_reset_after_setup_allowance_is_not_swallowed() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let mut events = observe_pending_opens(&fixture.session);
    let limits = fixture.server_context.codec_limits;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let opening = open_remote_stream_until(
        &fixture.context,
        TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
        TrafficClass::Latency,
        deadline,
    );
    tokio::pin!(opening);
    let (mut peer_send, mut peer_recv, index, stream_id, plan) =
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, async {
            tokio::select! {
                request = submitted_request(&accepted.connection, 0, limits) => request,
                _ = &mut opening => panic!("single-candidate open completed before peer terminal"),
            }
        })
        .await
        .expect("single candidate submits its native CREATE/MAX");
    assert_eq!(plan.candidate_total, 1);
    let setup = reliable_initial_open_timeout(
        &fixture.context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index,
        },
        false,
    );
    tokio::time::pause();
    tokio::time::advance(setup + Duration::from_millis(1)).await;
    assert!(
        opening.as_mut().now_or_never().is_none(),
        "the submitted request remains pending before its logical deadline"
    );
    tokio::time::resume();

    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamReset {
            stream_id,
            reason: ResetReason::Refused,
        },
        limits,
    )
    .await
    .expect("peer submits authenticated target refusal");
    let result = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, &mut opening)
        .await
        .expect("authenticated RESET is handled before T");
    let error = match result {
        Ok(opened) => {
            opened.retire_uncommitted();
            panic!("a refused target must not be reported as success");
        }
        Err(error) => error,
    };
    assert!(
        matches!(error, RuntimeError::RemoteReset(ResetReason::Refused)),
        "the exact target refusal survives retention: {error:?}"
    );
    assert_ordered_detach(&mut peer_recv, stream_id, limits).await;
    retire_unopened_peer_repair(&accepted, limits).await;
    let mut observed = Vec::new();
    observe_until(
        &mut events,
        stream_id,
        ClientUdpPendingOpenEvent::ContinuationExited,
        &mut observed,
    )
    .await;
    let count = |expected| observed.iter().filter(|event| **event == expected).count();
    assert_eq!(count(ClientUdpPendingOpenEvent::Submitted), 1);
    assert_eq!(count(ClientUdpPendingOpenEvent::ContinuationStarted), 1);
    assert_eq!(count(ClientUdpPendingOpenEvent::ContinuationExited), 1);
    assert_eq!(fixture.context.reliable_selection_passes_for_test(), 1);
    assert_real_sibling_exchange(&fixture, &accepted).await;
}
