use super::*;

#[tokio::test]
async fn terminal_udp_denial_remains_silent_for_the_association_lifetime() {
    let (request_tx, request_rx) = mpsc::channel(2);
    let (completion_tx, mut completion_rx) = mpsc::channel(2);
    let (_cancel_tx, cancelled) = tokio::sync::watch::channel(false);
    let target = TargetAddr::Domain {
        host: "denied.example".to_string(),
        port: 443,
    };
    let initial = UdpEdgeRequest {
        target: target.clone(),
        payload: Bytes::from_static(b"first"),
        ttl_ms: 1_000,
        metadata: 9_u8,
    };
    let lane = tokio::spawn(run_silent_udp_denial_lane(
        7,
        9_u8,
        request_rx,
        completion_tx,
        cancelled,
        initial,
    ));
    request_tx
        .send(UdpEdgeRequest {
            target: target.clone(),
            payload: Bytes::from_static(b"second"),
            ttl_ms: 1_000,
            metadata: 9_u8,
        })
        .await
        .expect("second datagram");

    for _ in 0..2 {
        assert!(matches!(
            completion_rx.recv().await.expect("silent completion"),
            UdpEdgeCompletion::Discarded { lane_id: 7 }
        ));
    }
    assert!(
        !lane.is_finished(),
        "the denied association must cache its terminal policy outcome"
    );
    drop(request_tx);
    lane.await.expect("denied lane task");
}
