use super::*;
use crate::protocol::{PeerStatusCode, UnderlayProtocol};
use crate::runtime::peer_status::PeerStatusBroker;
use crate::runtime::relay::open::open_remote_stream;

const PEER_STATUS_E2E_TIMEOUT: Duration = Duration::from_secs(5);

async fn hold_tcp_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind peer-status target");
    let address = listener.local_addr().expect("peer-status target address");
    let task = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept peer-status target");
        std::future::pending::<()>().await;
    });
    (address, task)
}

async fn request_and_assert_server_path(
    context: &ClientPathContext,
    expected_underlay: UnderlayProtocol,
) {
    let result = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        context.peer_status.request(context.session_id),
    )
    .await
    .expect("peer-status actor round trip timed out")
    .expect("peer-status actor round trip failed");

    assert_eq!(result.session_id, context.session_id);
    assert_ne!(result.request_id, 0);
    assert_eq!(result.code, PeerStatusCode::Ok);
    assert_eq!(result.paths.len(), 1);
    let path = &result.paths[0];
    assert_eq!(path.metrics.path_id, PathId(0));
    assert_eq!(path.metrics.underlay, expected_underlay);
    assert_eq!(path.metrics.direction, PathMetricDirection::ServerToClient);
    assert_eq!(
        context
            .peer_status
            .latest(context.session_id)
            .expect("cache authenticated peer response"),
        result
    );
}

async fn abort_task<T>(task: tokio::task::JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_peer_status_round_trips_through_tcp_carrier_actors() {
    let path = reserve_tcp_path().await;
    let listener = bind_listener(&path).await.expect("bind TCP carrier");
    let local_path = ServerLocalPath::new(0, path.clone());
    let ServerIdentityRuntime {
        mut paths,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    paths.peer_status = PeerStatusBroker::new(true);
    let server_context = paths.clone();
    let relay = tokio::spawn(
        reliable_relay
            .expect("L4 test server has a reliable relay")
            .run(),
    );
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept TCP carrier");
        handle_server_path(stream, local_path, paths).await
    });
    let (target_addr, target) = hold_tcp_target().await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    probe_client_paths(&context, Duration::from_secs(2)).await;

    let remote = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        open_remote_stream(&context, TargetAddr::Ip(target_addr), TrafficClass::Latency),
    )
    .await
    .expect("TCP carrier open timed out")
    .expect("open TCP carrier stream");
    assert_eq!(
        server_context.peer_status.carrier_count(context.session_id),
        1
    );
    assert_eq!(context.authenticated_carriers.snapshot().live_count, 1);

    request_and_assert_server_path(&context, UnderlayProtocol::Tcp).await;

    drop(remote);
    drop(context);
    abort_task(server).await;
    abort_task(relay).await;
    abort_task(target).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_peer_status_round_trips_through_quic_carrier_actors() {
    let path = reserve_udp_path().await;
    let ServerIdentityRuntime {
        mut paths,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    paths.peer_status = PeerStatusBroker::new(true);
    let server_context = paths.clone();
    let endpoint = bind_server_udp_endpoint(&path, &paths)
        .await
        .expect("bind QUIC carrier");
    let local_path = ServerLocalPath::new(0, path.clone());
    let relay = tokio::spawn(
        reliable_relay
            .expect("L4 test server has a reliable relay")
            .run(),
    );
    let server = tokio::spawn(run_server_udp_listener(endpoint, local_path, paths));
    let (target_addr, target) = hold_tcp_target().await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");

    let remote = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        open_remote_stream(&context, TargetAddr::Ip(target_addr), TrafficClass::Latency),
    )
    .await
    .expect("QUIC carrier open timed out")
    .expect("open QUIC carrier stream");
    assert_eq!(
        server_context.peer_status.carrier_count(context.session_id),
        1
    );
    assert_eq!(context.authenticated_carriers.snapshot().live_count, 1);

    request_and_assert_server_path(&context, UnderlayProtocol::Udp).await;

    drop(remote);
    drop(context);
    abort_task(server).await;
    abort_task(relay).await;
    abort_task(target).await;
}
