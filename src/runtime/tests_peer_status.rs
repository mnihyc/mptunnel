use super::*;
use crate::protocol::{
    PathId, PathMetricDirection, PathMetrics, PathUsage, PeerPathState, UnderlayProtocol,
};

fn status(path_id: u16) -> PeerPathStatus {
    PeerPathStatus {
        state: PeerPathState::Active,
        usage: PathUsage::Available,
        metrics: PathMetrics {
            path_id: PathId(path_id),
            underlay: UnderlayProtocol::Tcp,
            direction: PathMetricDirection::ClientToServer,
            metric_epoch: 1,
            metric_age_us: 0,
            rate_valid_for_us: 0,
            rate_observed: false,
            srtt_us: 10_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 10_000_000,
            pacing_rate_bps: 10_000_000,
            pacing_rate_observed: false,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight_observed: true,
            queue_observed: true,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 0,
            app_limited: true,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
    }
}

fn status_for(underlay: UnderlayProtocol, path_id: u16) -> PeerPathStatus {
    let mut status = status(path_id);
    status.metrics.underlay = underlay;
    status
}

#[test]
fn disabled_responder_does_not_sample_local_paths() {
    let broker = PeerStatusBroker::new(false);
    assert!(!broker.allows_incoming());
    let carrier = broker.register(SessionId(7));
    let frame = carrier.response_frame(9, CodecLimits::default(), || {
        panic!("disabled response sampled paths")
    });
    assert_eq!(
        frame,
        Frame::PeerStatusResponse {
            request_id: 9,
            code: PeerStatusCode::Disabled,
            paths: Vec::new(),
        }
    );
}

#[test]
fn response_reports_unavailable_instead_of_exceeding_codec_limit() {
    let broker = PeerStatusBroker::new(true);
    let carrier = broker.register(SessionId(8));
    let limits = CodecLimits {
        max_frame_bytes: 64,
        ..CodecLimits::default()
    };
    assert_eq!(
        carrier.response_frame(10, limits, || Some(vec![status(1)])),
        Frame::PeerStatusResponse {
            request_id: 10,
            code: PeerStatusCode::Unavailable,
            paths: Vec::new(),
        }
    );
}

#[test]
fn responder_policy_is_independent_of_live_sessions() {
    let broker = PeerStatusBroker::new(true);
    assert!(broker.allows_incoming());
    assert!(broker.session_ids().is_empty());
}

#[test]
fn scoped_responder_authorizes_only_selected_sessions_and_global_overrides() {
    let broker = PeerStatusBroker::with_scoped_incoming(false, true);
    assert!(broker.allows_incoming());
    let denied = broker.register_with_incoming(SessionId(20), false);
    let allowed = broker.register_with_incoming(SessionId(21), true);
    assert_eq!(
        denied.response_frame(1, CodecLimits::default(), || {
            panic!("denied scoped response sampled paths")
        }),
        Frame::PeerStatusResponse {
            request_id: 1,
            code: PeerStatusCode::Disabled,
            paths: Vec::new(),
        }
    );
    assert!(matches!(
        allowed.response_frame(2, CodecLimits::default(), || Some(vec![status(1)])),
        Frame::PeerStatusResponse {
            code: PeerStatusCode::Ok,
            ..
        }
    ));

    let global = PeerStatusBroker::with_scoped_incoming(true, false);
    let carrier = global.register_with_incoming(SessionId(22), false);
    assert!(matches!(
        carrier.response_frame(3, CodecLimits::default(), || Some(vec![status(2)])),
        Frame::PeerStatusResponse {
            code: PeerStatusCode::Ok,
            ..
        }
    ));
}

#[test]
fn incoming_requests_are_rate_limited_across_session_carriers() {
    let broker = PeerStatusBroker::new(true);
    let first = broker.register(SessionId(12));
    let second = broker.register(SessionId(12));
    assert_eq!(broker.carrier_count(SessionId(12)), 2);
    assert!(matches!(
        first.response_frame(1, CodecLimits::default(), || Some(vec![status(1)])),
        Frame::PeerStatusResponse {
            code: PeerStatusCode::Ok,
            ..
        }
    ));
    assert_eq!(
        second.response_frame(2, CodecLimits::default(), || {
            panic!("rate-limited request sampled paths")
        }),
        Frame::PeerStatusResponse {
            request_id: 2,
            code: PeerStatusCode::Unavailable,
            paths: Vec::new(),
        }
    );
}

#[test]
fn unrepresentable_snapshot_reports_unavailable() {
    let broker = PeerStatusBroker::new(true);
    let carrier = broker.register(SessionId(13));
    assert_eq!(
        carrier.response_frame(3, CodecLimits::default(), || None),
        Frame::PeerStatusResponse {
            request_id: 3,
            code: PeerStatusCode::Unavailable,
            paths: Vec::new(),
        }
    );
}

#[tokio::test]
async fn response_identity_is_causal_registration_owned_and_underlay_scoped() {
    let broker = PeerStatusBroker::new(true);
    let session_id = SessionId(14);
    let mut predecessor =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(47), 2, None);

    let old_request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let old_request_id = predecessor.recv_request().await.expect("old request");
    assert!(predecessor.receive_response(
        old_request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Tcp, 47)],
    ));
    let old_result = old_request
        .await
        .expect("old request task")
        .expect("old result");
    assert_eq!(
        old_result.local_path_index(UnderlayProtocol::Tcp, PathId(47)),
        Some(2)
    );

    let mut successor =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(47), 3, None);
    let udp = broker.register_path(session_id, UnderlayProtocol::Udp, PathId(47), 8, None);
    drop(predecessor);

    assert_eq!(
        broker.local_path_index(session_id, UnderlayProtocol::Tcp, PathId(47)),
        Some(3),
        "stale predecessor cleanup must not erase its authenticated successor"
    );
    assert_eq!(
        broker.local_path_index(session_id, UnderlayProtocol::Udp, PathId(47)),
        Some(8),
        "equal numeric PathIds on different underlays are independent"
    );
    assert_eq!(
        broker
            .latest(session_id)
            .expect("cached old result")
            .local_path_index(UnderlayProtocol::Tcp, PathId(47)),
        Some(2),
        "authenticated reuse must not relabel a cached response"
    );

    let new_request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let new_request_id = successor.recv_request().await.expect("new request");
    assert!(successor.receive_response(
        new_request_id,
        PeerStatusCode::Ok,
        vec![
            status_for(UnderlayProtocol::Tcp, 47),
            status_for(UnderlayProtocol::Udp, 47),
        ],
    ));
    let new_result = new_request
        .await
        .expect("new request task")
        .expect("new result");
    assert_eq!(
        new_result.local_path_index(UnderlayProtocol::Tcp, PathId(47)),
        Some(3)
    );
    assert_eq!(
        new_result.local_path_index(UnderlayProtocol::Udp, PathId(47)),
        Some(8)
    );
    assert_eq!(
        old_result.local_path_index(UnderlayProtocol::Tcp, PathId(47)),
        Some(2),
        "an already returned result remains immutable"
    );

    drop(successor);
    assert_eq!(
        broker.local_path_index(session_id, UnderlayProtocol::Tcp, PathId(47)),
        None
    );
    assert_eq!(
        broker.local_path_index(session_id, UnderlayProtocol::Udp, PathId(47)),
        Some(8)
    );
    assert_eq!(
        new_result.local_path_index(UnderlayProtocol::Tcp, PathId(47)),
        Some(3),
        "registration retirement cannot rewrite a completed response"
    );

    drop(udp);
    assert!(broker.session_ids().is_empty());
}

#[tokio::test]
async fn response_identity_survives_unrelated_carrier_retirement_in_flight() {
    let broker = PeerStatusBroker::new(true);
    let session_id = SessionId(16);
    let mut request_carrier =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(0), 0, None);
    let retired_carrier =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(1), 1, None);

    let request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let request_id = request_carrier.recv_request().await.expect("peer request");

    // Reproduce the management race: the peer has already received the
    // request and may have sampled PathId 1, while the corresponding local
    // carrier retires before that response reaches this endpoint.
    drop(retired_carrier);
    assert_eq!(
        broker.local_path_index(session_id, UnderlayProtocol::Tcp, PathId(1)),
        None
    );
    assert!(request_carrier.receive_response(
        request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Tcp, 1)],
    ));

    let result = request.await.expect("request task").expect("peer result");
    assert_eq!(
        result.local_path_index(UnderlayProtocol::Tcp, PathId(1)),
        Some(1),
        "an in-flight retirement must not erase the causal PathId mapping"
    );
}

#[tokio::test]
async fn retired_identity_lives_until_complete_peer_snapshot_proves_it_absent() {
    let broker = PeerStatusBroker::new(true);
    let session_id = SessionId(18);
    let mut request_carrier =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(0), 0, None);
    let failed_carrier =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(1), 1, None);
    drop(failed_carrier);

    let retained_request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let retained_request_id = request_carrier
        .recv_request()
        .await
        .expect("retained mapping request");
    assert!(request_carrier.receive_response(
        retained_request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Tcp, 1)],
    ));
    let retained = retained_request
        .await
        .expect("retained request task")
        .expect("retained result");
    assert_eq!(
        retained.local_path_index(UnderlayProtocol::Tcp, PathId(1)),
        Some(1),
        "local teardown must not erase an identity the peer still reports"
    );

    let cleanup_request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let cleanup_request_id = request_carrier
        .recv_request()
        .await
        .expect("cleanup request");
    assert!(request_carrier.receive_response(cleanup_request_id, PeerStatusCode::Ok, Vec::new(),));
    cleanup_request
        .await
        .expect("cleanup request task")
        .expect("cleanup result");

    let after_cleanup_request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let after_cleanup_request_id = request_carrier
        .recv_request()
        .await
        .expect("post-cleanup request");
    assert!(request_carrier.receive_response(
        after_cleanup_request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Tcp, 1)],
    ));
    let after_cleanup = after_cleanup_request
        .await
        .expect("post-cleanup request task")
        .expect("post-cleanup result");
    assert_eq!(
        after_cleanup.local_path_index(UnderlayProtocol::Tcp, PathId(1)),
        None,
        "a complete peer snapshot that omits the retired PathId must release its tombstone"
    );
}

#[tokio::test]
async fn retired_and_live_generations_in_one_slot_keep_their_own_ports() {
    let broker = PeerStatusBroker::new(true);
    let session_id = SessionId(19);
    let mut request_carrier =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(0), 0, None);
    let predecessor =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(41), 2, Some(7443));
    let successor =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(42), 2, Some(7444));

    assert_eq!(
        broker.live_path_active_port(session_id, UnderlayProtocol::Tcp, PathId(41)),
        Some(7443),
        "a draining predecessor retains its exact live port during overlap"
    );
    assert_eq!(
        broker.live_path_active_port(session_id, UnderlayProtocol::Tcp, PathId(42)),
        Some(7444),
        "a current successor must resolve by exact PathId during overlap"
    );

    let request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let request_id = request_carrier.recv_request().await.expect("peer request");
    assert!(request_carrier.receive_response(
        request_id,
        PeerStatusCode::Ok,
        vec![
            status_for(UnderlayProtocol::Tcp, 41),
            status_for(UnderlayProtocol::Tcp, 42),
        ],
    ));

    let result = request.await.expect("request task").expect("peer result");
    assert_eq!(
        result.local_path_index(UnderlayProtocol::Tcp, PathId(41)),
        Some(2)
    );
    assert_eq!(
        result.local_active_port(UnderlayProtocol::Tcp, PathId(41)),
        Some(7443),
        "a retained predecessor owns its last exact carrier port"
    );
    assert_eq!(
        result.local_path_retired(UnderlayProtocol::Tcp, PathId(41)),
        Some(false),
        "the overlapping predecessor is still an exact live assignment"
    );
    assert_eq!(
        result.local_path_index(UnderlayProtocol::Tcp, PathId(42)),
        Some(2)
    );
    assert_eq!(
        result.local_active_port(UnderlayProtocol::Tcp, PathId(42)),
        Some(7444),
        "a successor in the same configured slot owns its distinct port"
    );
    assert_eq!(
        result.local_path_retired(UnderlayProtocol::Tcp, PathId(42)),
        Some(false),
        "the authenticated successor is live at request dispatch"
    );

    drop(predecessor);
    assert_eq!(
        broker.live_path_active_port(session_id, UnderlayProtocol::Tcp, PathId(41)),
        None,
        "a retired predecessor tombstone cannot be presented as live"
    );
    assert_eq!(
        broker.live_path_active_port(session_id, UnderlayProtocol::Tcp, PathId(42)),
        Some(7444),
        "retiring the predecessor cannot erase the live successor"
    );
    drop(successor);
    let _reused =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(43), 2, Some(7445));
    assert_eq!(
        broker.live_path_active_port(session_id, UnderlayProtocol::Tcp, PathId(43)),
        Some(7445),
        "a newly authenticated generation owns its own live port"
    );
}

#[tokio::test]
async fn response_identity_is_not_relabelled_by_in_flight_path_id_reuse() {
    let broker = PeerStatusBroker::new(true);
    let session_id = SessionId(17);
    let mut request_carrier =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(0), 0, None);
    let predecessor =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(47), 2, Some(7443));

    let request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let request_id = request_carrier.recv_request().await.expect("peer request");

    drop(predecessor);
    let _successor =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(47), 3, Some(7444));
    assert_eq!(
        broker.local_path_index(session_id, UnderlayProtocol::Tcp, PathId(47)),
        Some(3)
    );
    assert!(request_carrier.receive_response(
        request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Tcp, 47)],
    ));

    let result = request.await.expect("request task").expect("peer result");
    assert_eq!(
        result.local_path_index(UnderlayProtocol::Tcp, PathId(47)),
        Some(2),
        "a successor authenticated after the request must not relabel its response"
    );
    assert_eq!(
        result.local_active_port(UnderlayProtocol::Tcp, PathId(47)),
        Some(7443),
        "the response must retain the predecessor port captured at request dispatch"
    );
}

#[tokio::test]
async fn stale_path_metadata_handle_cannot_overwrite_successor_port() {
    let broker = PeerStatusBroker::new(true);
    let session_id = SessionId(23);
    let mut request_carrier =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(0), 0, None);
    let predecessor =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(47), 2, Some(7443));
    let stale_metadata = predecessor
        .path_metadata_handle()
        .expect("predecessor path metadata");
    let _successor =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(47), 2, Some(7444));
    assert!(
        !stale_metadata.set_active_port(7999),
        "a superseded registration cannot mutate the live assignment"
    );

    let request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let request_id = request_carrier.recv_request().await.expect("peer request");
    assert!(request_carrier.receive_response(
        request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Tcp, 47)],
    ));
    let result = request.await.expect("request task").expect("peer result");
    assert_eq!(
        result.local_active_port(UnderlayProtocol::Tcp, PathId(47)),
        Some(7444)
    );
}

#[tokio::test]
async fn path_port_update_before_dispatch_is_visible_and_after_dispatch_is_frozen() {
    let broker = PeerStatusBroker::new(true);
    let session_id = SessionId(24);
    let mut request_carrier =
        broker.register_path(session_id, UnderlayProtocol::Tcp, PathId(0), 0, None);
    let quic = broker.register_path(session_id, UnderlayProtocol::Udp, PathId(9), 4, Some(7443));
    let metadata = quic.path_metadata_handle().expect("QUIC path metadata");
    assert!(metadata.set_active_port(7444));

    let request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let request_id = request_carrier.recv_request().await.expect("peer request");
    assert!(metadata.set_active_port(7445));
    assert!(request_carrier.receive_response(
        request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Udp, 9)],
    ));
    let frozen = request.await.expect("request task").expect("peer result");
    assert_eq!(
        frozen.local_active_port(UnderlayProtocol::Udp, PathId(9)),
        Some(7444),
        "a migration completed after dispatch cannot rewrite the pending request"
    );

    let next_request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let next_request_id = request_carrier
        .recv_request()
        .await
        .expect("next peer request");
    assert!(request_carrier.receive_response(
        next_request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Udp, 9)],
    ));
    let updated = next_request
        .await
        .expect("next request task")
        .expect("next peer result");
    assert_eq!(
        updated.local_active_port(UnderlayProtocol::Udp, PathId(9)),
        Some(7445),
        "the next dispatch observes the completed migration"
    );
}

#[tokio::test]
async fn scoped_server_registration_uses_the_same_response_identity_snapshot() {
    let broker = PeerStatusBroker::with_scoped_incoming(false, true);
    let session_id = SessionId(15);
    let mut server_carrier = broker.register_path_with_incoming(
        session_id,
        UnderlayProtocol::Udp,
        PathId(9),
        4,
        None,
        true,
    );
    let request = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(session_id).await })
    };
    let request_id = server_carrier.recv_request().await.expect("server request");
    assert!(server_carrier.receive_response(
        request_id,
        PeerStatusCode::Ok,
        vec![status_for(UnderlayProtocol::Udp, 9)],
    ));
    let result = request
        .await
        .expect("server request task")
        .expect("server result");
    assert_eq!(
        result.local_path_index(UnderlayProtocol::Udp, PathId(9)),
        Some(4)
    );
    assert_eq!(
        result.local_active_port(UnderlayProtocol::Udp, PathId(9)),
        None,
        "server registrations do not fabricate a client destination port"
    );
}

#[tokio::test]
async fn request_is_exactly_correlated_and_cached() {
    let broker = PeerStatusBroker::new(true);
    let mut carrier = broker.register(SessionId(7));
    let requester = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(7)).await })
    };
    let request_id = carrier.recv_request().await.expect("request");
    assert!(!carrier.receive_response(
        request_id.wrapping_add(1),
        PeerStatusCode::Ok,
        vec![status(1)],
    ));
    assert!(carrier.receive_response(request_id, PeerStatusCode::Ok, vec![status(2)]));

    let result = requester.await.expect("request task").expect("result");
    assert_eq!(result.request_id, request_id);
    assert_eq!(result.paths, vec![status(2)]);
    assert_eq!(
        broker.latest(SessionId(7)).expect("latest").paths,
        result.paths
    );
}

#[tokio::test]
async fn one_request_per_session_and_registration_drop_cleans_state() {
    let broker = PeerStatusBroker::with_timeout(true, Duration::from_millis(50));
    let mut carrier = broker.register(SessionId(7));
    let first = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(7)).await })
    };
    let _request_id = carrier.recv_request().await.expect("request");
    assert_eq!(
        broker.request(SessionId(7)).await,
        Err(PeerStatusRequestError::RequestInProgress)
    );
    drop(carrier);
    assert_eq!(
        first.await.expect("request task"),
        Err(PeerStatusRequestError::SessionUnavailable)
    );
    assert!(broker.session_ids().is_empty());
}

#[tokio::test]
async fn timed_out_request_releases_the_session_slot() {
    let broker = PeerStatusBroker::with_timeout(true, Duration::from_millis(5));
    let mut carrier = broker.register(SessionId(7));
    let first = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(7)).await })
    };
    let _stale = carrier.recv_request().await.expect("stale request");
    assert_eq!(
        first.await.expect("first request task"),
        Err(PeerStatusRequestError::TimedOut)
    );
    let second = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(7)).await })
    };
    let request_id = carrier.recv_request().await.expect("second request");
    assert!(carrier.receive_response(request_id, PeerStatusCode::Unavailable, vec![status(1)]));
    let result = second.await.expect("request task").expect("result");
    assert_eq!(result.code, PeerStatusCode::Unavailable);
    assert!(result.paths.is_empty());
}

#[tokio::test]
async fn successful_control_carrier_is_retained_and_timeout_rotates_to_an_alternate() {
    let broker = PeerStatusBroker::with_timeout(true, Duration::from_millis(20));
    let mut first = broker.register(SessionId(9));
    let mut second = broker.register(SessionId(9));

    let initial = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(9)).await })
    };
    let request_id = first.recv_request().await.expect("first carrier request");
    assert!(first.receive_response(request_id, PeerStatusCode::Ok, vec![status(1)]));
    initial
        .await
        .expect("request task")
        .expect("initial result");

    let timed_out = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(9)).await })
    };
    let _ = first.recv_request().await.expect("preferred request");
    assert_eq!(
        timed_out.await.expect("request task"),
        Err(PeerStatusRequestError::TimedOut)
    );

    let alternate = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(9)).await })
    };
    let request_id = second
        .recv_request()
        .await
        .expect("alternate carrier request");
    assert!(second.receive_response(request_id, PeerStatusCode::Unavailable, vec![status(2)]));
    let alternate = alternate
        .await
        .expect("request task")
        .expect("alternate result");
    assert_eq!(alternate.code, PeerStatusCode::Unavailable);
    assert!(alternate.paths.is_empty());

    let retained = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(9)).await })
    };
    let request_id = second
        .recv_request()
        .await
        .expect("retained carrier request");
    assert!(second.receive_response(request_id, PeerStatusCode::Disabled, Vec::new()));
    assert_eq!(
        retained
            .await
            .expect("request task")
            .expect("retained result")
            .code,
        PeerStatusCode::Disabled
    );

    let cancelled = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(9)).await })
    };
    let _ = second
        .recv_request()
        .await
        .expect("request before cancellation");
    cancelled.abort();
    assert!(
        cancelled
            .await
            .expect_err("cancelled request task")
            .is_cancelled()
    );

    let after_cancel = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.request(SessionId(9)).await })
    };
    let request_id = second
        .recv_request()
        .await
        .expect("retained request after cancellation");
    assert!(second.receive_response(request_id, PeerStatusCode::Ok, vec![status(3)]));
    assert_eq!(
        after_cancel
            .await
            .expect("request task")
            .expect("result after cancellation")
            .paths,
        vec![status(3)]
    );
}
