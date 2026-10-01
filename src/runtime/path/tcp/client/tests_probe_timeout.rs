use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::protocol::PathId;
use crate::runtime::path::{ClientPathContext, WebhookProbeTrigger};
use crate::runtime::webhook::EventPublisher;
use crate::webhook::{EventKind, EventMatcher, EventMatcherBranch};

// Poll the real disconnected actor once, then close the caller's receiver
// BEFORE letting the actor observe the elapsed transport deadline. This forces
// the formerly racy ordering without scheduler timing assumptions.
async fn close_probe_caller(runtime: &ClientTcpPathSessionRuntime, at_deadline: bool) {
    let (heartbeat_publication, published) = watch::channel(None);
    let mut state = ClientTcpPathSessionState {
        connection: None,
        heartbeat_publication,
        heartbeat_drain_observer: Arc::new(ClientTcpHeartbeatDrainObserver {
            published,
            draining: AtomicBool::new(false),
        }),
        streams: ClientTcpPathStreams::new(),
        closed_streams: RecentIdCache::new(runtime.closed_stream_cache_capacity),
        datagrams: ClientTcpDatagramState::new(
            runtime.mux_limits.max_streams,
            runtime.closed_stream_cache_capacity,
        ),
    };
    let mut readiness = ClientTcpCarrierReadiness::new(
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        runtime.carrier_groups.clone(),
    );
    let (response, caller) = tokio::sync::oneshot::channel();
    let interval = Duration::from_secs(1);
    let handling = handle_disconnected_client_tcp_command(
        ReliablePathCommand::PrepareConnection {
            open_deadline: tokio::time::Instant::now() + interval,
            endpoint_generation: runtime.endpoint_policy.snapshot().generation,
            probe_trigger: Some(WebhookProbeTrigger::Periodic),
            response,
        },
        runtime,
        &mut state,
        &mut readiness,
    );
    tokio::pin!(handling);
    std::future::poll_fn(|cx| {
        assert!(handling.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    if at_deadline {
        tokio::time::advance(interval).await;
    }
    drop(caller);
    handling.await;
}

fn context(endpoint: &str) -> ClientPathContext {
    let path = endpoint.parse().unwrap();
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).unwrap(),
    );
    ClientPathContext::new(vec![path], security, ResourceLimits::default()).unwrap()
}

#[tokio::test(start_paused = true)]
async fn expired_probe_caller_commits_failure_for_every_tcp_pool_member() {
    // TCP is accepted by the kernel, but no server authenticates it.
    let stalled = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let context = context(&format!("tcp://{}", stalled.local_addr().unwrap()));
    assert_eq!(context.tcp_sessions.len(), 3);
    let (publisher, capture) = EventPublisher::test_capture(
        &[EventKind::PathStateChanged, EventKind::PathProbeCompleted],
        32,
    );
    context.attach_webhook_publisher(publisher);
    for _ in 0..2 {
        for (index, handle) in context.tcp_sessions.iter().enumerate() {
            let runtime = handle.runtime.for_carrier(PathId(index as u16), None);
            close_probe_caller(&runtime, true).await;
        }
    }
    let events = capture.snapshot();
    let transitions = events
        .iter()
        .filter(|event| event["event"]["type"] == "path.state_changed")
        .collect::<Vec<_>>();
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0]["change"]["from"], "unknown");
    assert_eq!(transitions[0]["change"]["to"], "down");
    let probes = events
        .iter()
        .filter(|event| event["event"]["type"] == "path.probe_completed")
        .collect::<Vec<_>>();
    assert_eq!(probes.len(), 6);
    assert!(probes.iter().all(|event| {
        event["probe"]["outcome"] == "failure" && event["probe"]["applied"] == true
    }));
    assert!(
        probes[..3]
            .iter()
            .all(|event| event["probe"]["state_at_start"] == "unknown")
    );
    assert!(
        probes[3..]
            .iter()
            .all(|event| event["probe"]["state_at_start"] == "down")
    );
    let when = EventMatcher {
        branches: vec![
            EventMatcherBranch {
                events: vec![EventKind::PathStateChanged],
                from: vec!["up".into()],
                to: vec!["down".into()],
                ..Default::default()
            },
            EventMatcherBranch {
                events: vec![EventKind::PathProbeCompleted],
                probe_state_at_start: vec!["down".into()],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert!(!when.matches(EventKind::PathStateChanged, transitions[0]));
    assert_eq!(
        probes
            .iter()
            .filter(|event| when.matches(EventKind::PathProbeCompleted, event))
            .count(),
        3,
        "each subsequent failed recovery check matches the shipped rule"
    );
    assert_eq!(capture.dropped(), 0);
}

#[tokio::test(start_paused = true)]
async fn early_probe_caller_cancellation_does_not_commit_failure() {
    let stalled = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let context = context(&format!(
        "tcp://{}?max-tcp-carriers=1",
        stalled.local_addr().unwrap()
    ));
    let (publisher, capture) = EventPublisher::test_capture(
        &[EventKind::PathStateChanged, EventKind::PathProbeCompleted],
        8,
    );
    context.attach_webhook_publisher(publisher);
    let runtime = context.tcp_sessions[0].runtime.for_carrier(PathId(0), None);
    close_probe_caller(&runtime, false).await;
    let events = capture.snapshot();
    assert_eq!(events.len(), 1, "cancellation must not invent an outage");
    assert_eq!(events[0]["event"]["type"], "path.probe_completed");
    assert_eq!(events[0]["probe"]["outcome"], "cancelled");
    assert_eq!(events[0]["probe"]["applied"], false);
    assert_eq!(events[0]["path"]["state"], "unknown");
    assert_eq!(capture.dropped(), 0);
}
