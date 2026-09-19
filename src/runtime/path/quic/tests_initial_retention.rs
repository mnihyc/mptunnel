//! Full logical coordinator with two authenticated native QUIC carriers.
use super::*;
use crate::protocol::{StreamAttachmentPhase, StreamReturnPlan};
use crate::runtime::relay::open::open_remote_stream_until;
use futures::StreamExt;
use futures::stream::FuturesUnordered;

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
    let (result, (_first_send, mut first_recv, _second_send, mut second_recv, stream_id, winner)) =
        tokio::time::timeout(OPEN_OWNERSHIP_GUARD, async {
            tokio::join!(&mut opening, peers)
        })
        .await
        .expect("actual native original/fallback grant settles");
    let opened = result.expect("actual accepted candidate wins");
    assert_eq!(opened.path_index(), winner);
    assert_eq!(opened.stream().stream_id, stream_id);
    opened.retire_uncommitted();
    tokio::join!(
        assert_ordered_detach(&mut first_recv, stream_id, limits),
        assert_ordered_detach(&mut second_recv, stream_id, limits),
    );
    for carrier in &accepted {
        retire_unopened_peer_repair(carrier, limits).await;
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
