use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::performance::MppPerformanceConfig;
use crate::product::{FlowContext, InboundId, Network, OutboundId, PrincipalId, ProtocolTarget};
use crate::runtime::path::ClientPathContext;
use crate::runtime::product_policy::{ClientIngressRouter, ClientRoute};
use crate::runtime::telemetry::{
    ProductFlowOriginKind, ProductFlowScope, ProductFlowSource, RuntimeTelemetry,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::test(start_paused = true)]
async fn idle_close_publication_wait_is_bounded() {
    let close = std::future::pending::<Result<(), RuntimeError>>();
    let publication = tokio::spawn(bounded_idle_close_publication(
        Duration::from_millis(50),
        close,
    ));
    tokio::task::yield_now().await;
    assert!(!publication.is_finished());

    tokio::time::advance(Duration::from_millis(49)).await;
    tokio::task::yield_now().await;
    assert!(!publication.is_finished());

    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(matches!(
        publication.await.expect("bounded publication task"),
        Err(IdleClosePublicationError::TimedOut)
    ));
}

#[tokio::test]
async fn idle_close_transport_failure_is_diagnostic_not_a_flow_outcome() {
    let result = bounded_idle_close_publication(Duration::from_secs(1), async {
        Err(RuntimeError::Protocol(
            "deterministic idle close publication failure",
        ))
    })
    .await;
    assert!(matches!(
        result,
        Err(IdleClosePublicationError::Failed(RuntimeError::Protocol(
            "deterministic idle close publication failure"
        )))
    ));
}

async fn offline_edge_test_plan(
    target: &TargetAddr,
) -> (ClientOutboundPlan, Vec<OpenedUdpOutbound>) {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("edge test secret"),
    );
    let context = ClientPathContext::new(
        vec!["quic://127.0.0.1:16190".parse().expect("edge test path")],
        security,
        ResourceLimits::default(),
    )
    .expect("edge test context");
    let router = ClientIngressRouter::single_for_test(context, MppPerformanceConfig::default())
        .expect("edge test router");
    let ClientRoute::Open(plan) = router
        .route_udp(
            target,
            "198.51.100.8:41000".parse().expect("edge test source"),
            PrincipalId::parse("anonymous").expect("edge test principal"),
            InboundId::parse("local-socks").expect("edge test inbound"),
        )
        .expect("edge test route")
    else {
        panic!("edge test route must open");
    };
    let mut held_product_flows = Vec::new();
    loop {
        match plan.open_udp(target).await {
            Ok(opened) => held_product_flows.push(opened),
            Err(RuntimeError::ProductAdmission(_)) => break,
            Err(error) => panic!("unexpected terminal-fixture setup failure: {error}"),
        }
    }
    (plan, held_product_flows)
}

fn edge_test_mux_limits(queue_slots: usize) -> MuxLimits {
    MuxLimits {
        max_payload_bytes: 1,
        max_datagram_queue_bytes: queue_slots,
        max_streams: queue_slots,
        ..MuxLimits::default()
    }
}

fn edge_test_request(target: &TargetAddr, payload: &'static [u8]) -> UdpEdgeRequest<u8> {
    UdpEdgeRequest {
        target: target.clone(),
        payload: Bytes::from_static(payload),
        ttl_ms: 1_000,
        metadata: 9,
    }
}

fn edge_test_retirement_gate() -> Arc<std::sync::Mutex<UdpEdgeRetirementGate>> {
    Arc::new(std::sync::Mutex::new(UdpEdgeRetirementGate {
        accepting: true,
        activity: ProductFlowActivity::new(),
    }))
}

#[tokio::test(start_paused = true)]
async fn admitted_boundary_payload_rearms_before_the_idle_fence() {
    let target = TargetAddr::Ip("203.0.113.20:443".parse().expect("edge test target"));
    let timeout = Some(Duration::from_secs(5));
    let (requests, mut receiver) = mpsc::channel(1);
    let retirement = edge_test_retirement_gate();
    let activity = retirement.lock().expect("retirement gate").activity.clone();
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(activity.is_idle(timeout));

    try_send_udp_edge_request(&requests, &retirement, edge_test_request(&target, b""))
        .expect("pre-fence payload is accepted");
    assert!(matches!(
        fence_udp_edge_idle(timeout, &mut receiver, &retirement),
        UdpEdgeIdleFence::Active
    ));
    let request = receiver.try_recv().expect("accepted payload remains owned");

    assert!(
        request.payload.is_empty(),
        "empty application datagrams are activity"
    );
    assert!(
        !activity.is_idle(timeout),
        "claimed payload must rearm idleness"
    );
    assert!(!requests.is_closed());
    assert!(retirement.lock().expect("retirement gate").accepting);
}

#[tokio::test(start_paused = true)]
async fn idle_fence_rejects_postfence_payload_intact_for_a_successor() {
    let target = TargetAddr::Ip("203.0.113.21:443".parse().expect("edge test target"));
    let timeout = Some(Duration::from_secs(5));
    let (retired_sender, mut retired_receiver) = mpsc::channel(1);
    let retired_gate = edge_test_retirement_gate();
    tokio::time::advance(Duration::from_secs(5)).await;

    assert!(matches!(
        fence_udp_edge_idle(timeout, &mut retired_receiver, &retired_gate,),
        UdpEdgeIdleFence::Retired
    ));
    let request = match try_send_udp_edge_request(
        &retired_sender,
        &retired_gate,
        edge_test_request(&target, b"successor"),
    ) {
        Err(mpsc::error::TrySendError::Closed(request)) => request,
        Err(mpsc::error::TrySendError::Full(_)) => panic!("retired lane reported queue pressure"),
        Ok(()) => panic!("post-fence payload entered the retired lane"),
    };

    let (successor_sender, mut successor_receiver) = mpsc::channel(1);
    let successor_gate = edge_test_retirement_gate();
    try_send_udp_edge_request(&successor_sender, &successor_gate, request)
        .expect("exact rejected payload is accepted by successor");
    let accepted = successor_receiver
        .try_recv()
        .expect("successor owns the exact payload");
    assert_eq!(accepted.payload, Bytes::from_static(b"successor"));
    assert_eq!(accepted.metadata, 9);
    assert!(retired_sender.is_closed());
}

fn native_udp_edge_test_product_flow(target: &TargetAddr) -> OpenedProductFlow {
    let flow = FlowContext::without_source(
        Network::Udp,
        ProtocolTarget::parse_authority(&target.authority()).expect("native edge test target"),
        PrincipalId::parse("native-edge-user").expect("native edge test principal"),
        InboundId::parse("native-edge-inbound").expect("native edge test inbound"),
    );
    let scope = ProductFlowScope::from_flow(
        ProductFlowOriginKind::LocalInbound,
        &flow,
        OutboundId::parse("native-edge-outbound").expect("native edge test outbound"),
        None,
        ProductFlowSource::local_peer(([127, 0, 0, 1], 0).into()),
    );
    let telemetry = RuntimeTelemetry::new(2);
    OpenedProductFlow::native_udp_for_test(scope, &telemetry)
}

struct BlockingTerminalNativeUdpIo {
    recv_entered: Option<oneshot::Sender<()>>,
    recv_release: Arc<(Mutex<bool>, Condvar)>,
}

struct CountingNativeUdpIo {
    first_send_entered: Option<oneshot::Sender<()>>,
    sends: Arc<AtomicUsize>,
}

impl NativeUdpIo for CountingNativeUdpIo {
    async fn send_payload(&mut self, payload: &[u8]) -> Result<usize, RuntimeError> {
        let previous = self.sends.fetch_add(1, Ordering::AcqRel);
        if previous == 0
            && let Some(entered) = self.first_send_entered.take()
        {
            let _ = entered.send(());
        }
        Ok(payload.len())
    }

    async fn recv_payload(&mut self, _buffer: &mut [u8]) -> Result<usize, RuntimeError> {
        std::future::pending().await
    }
}

impl NativeUdpIo for BlockingTerminalNativeUdpIo {
    async fn send_payload(&mut self, payload: &[u8]) -> Result<usize, RuntimeError> {
        Ok(payload.len())
    }

    async fn recv_payload(&mut self, _buffer: &mut [u8]) -> Result<usize, RuntimeError> {
        self.recv_entered
            .take()
            .expect("native receive is polled once")
            .send(())
            .expect("native receive observer");
        let (released, wake) = &*self.recv_release;
        let mut released = released.lock().expect("native receive release lock");
        while !*released {
            released = wake.wait(released).expect("native receive release wait");
        }
        Err(RuntimeError::Protocol(
            "deterministic native UDP receive failure",
        ))
    }
}

async fn wait_for_request_to_leave_lane_queue(lanes: &[UdpEdgeLane<u8>], lane_id: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let lane = lanes
                .iter()
                .find(|lane| lane.lane_id == lane_id)
                .expect("observed UDP edge lane");
            if lane.requests.is_closed() || lane.requests.capacity() == lane.requests.max_capacity()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("UDP edge actor must consume its accepted request");
}

async fn wait_for_all_lane_tasks_to_finish(lanes: &[UdpEdgeLane<u8>]) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while lanes.iter().any(|lane| {
            !lane
                .handle
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("UDP edge actors must finish after report capacity becomes available");
}

fn offline_completion_lane(completion: &UdpEdgeCompletion<u8>) -> usize {
    match completion {
        UdpEdgeCompletion::Sent {
            lane_id,
            metadata: 9,
            result: Err(error),
            ..
        } if matches!(error.as_ref(), RuntimeError::ProductAdmission(_)) => *lane_id,
        UdpEdgeCompletion::Sent { result, .. } => {
            panic!("offline edge request produced the wrong send result: {result:?}")
        }
        UdpEdgeCompletion::Discarded { .. } => {
            panic!("offline edge request was discarded instead of completed")
        }
        UdpEdgeCompletion::Received { .. } => {
            panic!("offline edge request unexpectedly received a response")
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_receive_failure_settles_request_admitted_while_receive_is_in_flight() {
    let target = TargetAddr::Ip("203.0.113.15:443".parse().expect("native edge test target"));
    let (plan, _held_product_flows) = offline_edge_test_plan(&target).await;
    let limits = edge_test_mux_limits(2);
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<u8>>(udp_edge_completion_queue(limits));
    let (request_tx, request_rx) = mpsc::channel(udp_edge_queue_slots(limits));
    let (cancel, cancelled) = tokio::sync::watch::channel(false);
    let (recv_entered_tx, recv_entered_rx) = oneshot::channel();
    let recv_release = Arc::new((Mutex::new(false), Condvar::new()));
    let socket = BlockingTerminalNativeUdpIo {
        recv_entered: Some(recv_entered_tx),
        recv_release: Arc::clone(&recv_release),
    };
    let lane_id = 44;
    let initial = edge_test_request(&target, b"initial");
    let retirement = edge_test_retirement_gate();
    let activity = retirement.lock().expect("retirement gate").activity.clone();
    let handle = tokio::spawn(run_native_udp_edge_lane(
        lane_id,
        9,
        socket,
        limits,
        request_rx,
        completion_tx.clone(),
        cancelled,
        initial,
        None,
        native_udp_edge_test_product_flow(&target),
        None,
        activity,
        retirement.clone(),
    ));
    let mut lanes = vec![UdpEdgeLane {
        lane_id,
        metadata: 9,
        pending: 1,
        requests: request_tx,
        retirement,
        cancel,
        handle: Some(handle),
    }];
    let mut next_lane_id = lane_id + 1;

    tokio::time::timeout(std::time::Duration::from_secs(1), recv_entered_rx)
        .await
        .expect("native receive must be polled")
        .expect("native receive actor must remain live");
    assert!(
        dispatch_udp_edge_request(
            &mut lanes,
            &mut next_lane_id,
            &plan,
            limits,
            &completion_tx,
            edge_test_request(&target, b"buffered"),
        )
        .is_ok(),
        "the real lane queue must accept the request while receive is in flight"
    );
    assert_eq!(lanes[0].pending, 2);

    {
        let (released, wake) = &*recv_release;
        *released.lock().expect("native receive release lock") = true;
        wake.notify_one();
    }

    let initial_completion =
        tokio::time::timeout(std::time::Duration::from_secs(1), completion_rx.recv())
            .await
            .expect("initial native send completion timeout")
            .expect("initial native send completion");
    assert!(matches!(
        &initial_completion,
        UdpEdgeCompletion::Sent {
            lane_id: 44,
            target: completed_target,
            metadata: 9,
            result: Ok(()),
        } if completed_target == &target
    ));
    finish_udp_edge_completion(&mut lanes, &initial_completion);
    assert_eq!(lanes[0].pending, 1);

    let terminal_completion =
        tokio::time::timeout(std::time::Duration::from_secs(1), completion_rx.recv())
            .await
            .expect("accepted buffered request must receive its terminal completion")
            .expect("terminal native receive completion channel");
    match &terminal_completion {
        UdpEdgeCompletion::Sent {
            lane_id: completed_lane,
            target: completed_target,
            metadata,
            result: Err(error),
        } => {
            assert_eq!(*completed_lane, lane_id);
            assert_eq!(completed_target, &target);
            assert_eq!(*metadata, 9);
            assert_eq!(
                error.to_string(),
                RuntimeError::Protocol("deterministic native UDP receive failure").to_string(),
                "the buffered request must retain the actual receive terminal cause"
            );
        }
        _ => panic!("buffered request produced the wrong terminal completion"),
    }
    finish_udp_edge_completion(&mut lanes, &terminal_completion);
    assert_eq!(lanes[0].pending, 0);
    wait_for_all_lane_tasks_to_finish(&lanes).await;
    assert!(matches!(
        completion_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    close_udp_edge_lanes(lanes).await;
}

#[tokio::test(start_paused = true)]
async fn idle_fence_fails_stale_queued_payload_without_sending_it() {
    let target = TargetAddr::Ip("203.0.113.16:443".parse().expect("native edge test target"));
    let limits = edge_test_mux_limits(1);
    let (completion_tx, mut completion_rx) = mpsc::channel(1);
    completion_tx
        .try_send(UdpEdgeCompletion::Discarded { lane_id: 90 })
        .expect("hold initial completion publication");
    let (request_tx, request_rx) = mpsc::channel(1);
    let (_cancel, cancelled) = tokio::sync::watch::channel(false);
    let (first_send_entered, first_send_observed) = oneshot::channel();
    let sends = Arc::new(AtomicUsize::new(0));
    let retirement = edge_test_retirement_gate();
    let activity = retirement.lock().expect("retirement gate").activity.clone();
    let handle = tokio::spawn(run_native_udp_edge_lane(
        45,
        9,
        CountingNativeUdpIo {
            first_send_entered: Some(first_send_entered),
            sends: sends.clone(),
        },
        limits,
        request_rx,
        completion_tx,
        cancelled,
        edge_test_request(&target, b"initial"),
        None,
        native_udp_edge_test_product_flow(&target),
        Some(Duration::from_secs(5)),
        activity,
        retirement.clone(),
    ));

    first_send_observed.await.expect("initial send started");
    try_send_udp_edge_request(
        &request_tx,
        &retirement,
        edge_test_request(&target, b"stale"),
    )
    .expect("source queue accepts the second payload before retirement");
    tokio::time::advance(Duration::from_secs(5)).await;

    assert!(matches!(
        completion_rx.recv().await,
        Some(UdpEdgeCompletion::Discarded { lane_id: 90 })
    ));
    assert!(matches!(
        completion_rx.recv().await,
        Some(UdpEdgeCompletion::Sent {
            lane_id: 45,
            result: Ok(()),
            ..
        })
    ));
    let terminal = completion_rx
        .recv()
        .await
        .expect("stale accepted payload terminal completion");
    assert!(matches!(
        terminal,
        UdpEdgeCompletion::Sent {
            lane_id: 45,
            result: Err(error),
            ..
        } if matches!(error.as_ref(), RuntimeError::ProductIdleTimeout)
    ));
    assert_eq!(
        sends.load(Ordering::Acquire),
        1,
        "an expired queued payload must fail exactly once, not reach the socket"
    );
    handle.await.expect("idle-retired native UDP lane");
}

#[tokio::test]
async fn every_accepted_offline_request_completes_across_a_full_report_queue() {
    let target = TargetAddr::Ip("203.0.113.10:443".parse().expect("edge test target"));
    let (plan, _held_product_flows) = offline_edge_test_plan(&target).await;
    let limits = edge_test_mux_limits(2);
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<u8>>(udp_edge_completion_queue(limits));
    completion_tx
        .try_send(UdpEdgeCompletion::Discarded { lane_id: 90 })
        .expect("first completion filler");
    completion_tx
        .try_send(UdpEdgeCompletion::Discarded { lane_id: 91 })
        .expect("second completion filler");
    let mut lanes = Vec::new();
    let mut next_lane_id = 0;

    assert!(
        dispatch_udp_edge_request(
            &mut lanes,
            &mut next_lane_id,
            &plan,
            limits,
            &completion_tx,
            edge_test_request(&target, b"a"),
        )
        .is_ok(),
        "first request must be accepted"
    );
    wait_for_request_to_leave_lane_queue(&lanes, 0).await;
    assert!(
        !lanes[0]
            .handle
            .as_ref()
            .expect("first lane actor")
            .is_finished(),
        "the full completion queue must hold the first actor in terminal reporting"
    );

    assert!(
        dispatch_udp_edge_request(
            &mut lanes,
            &mut next_lane_id,
            &plan,
            limits,
            &completion_tx,
            edge_test_request(&target, b"b"),
        )
        .is_ok(),
        "second request must fit the remaining configured slot"
    );
    assert_eq!(
        lanes.iter().map(|lane| lane.pending).sum::<usize>(),
        2,
        "both accepted requests remain owned until their completions are observed"
    );

    assert!(matches!(
        completion_rx.recv().await,
        Some(UdpEdgeCompletion::Discarded { lane_id: 90 })
    ));
    assert!(matches!(
        completion_rx.recv().await,
        Some(UdpEdgeCompletion::Discarded { lane_id: 91 })
    ));
    wait_for_all_lane_tasks_to_finish(&lanes).await;

    let first_completion = completion_rx
        .try_recv()
        .expect("first accepted request completion");
    let second_completion = completion_rx
        .try_recv()
        .expect("second accepted request completion");
    let first_lane_id = offline_completion_lane(&first_completion);
    let second_lane_id = offline_completion_lane(&second_completion);
    let mut completed_lanes = [first_lane_id, second_lane_id];
    completed_lanes.sort_unstable();
    assert_eq!(
        completed_lanes,
        [0, 1],
        "terminal reporting must retain the first lane and use only one bounded successor"
    );
    finish_udp_edge_completion(&mut lanes, &first_completion);
    finish_udp_edge_completion(&mut lanes, &second_completion);
    assert!(reap_finished_udp_edge_lane_instance(
        &mut lanes,
        first_lane_id
    ));
    assert_eq!(
        lanes.iter().map(|lane| lane.lane_id).collect::<Vec<_>>(),
        vec![second_lane_id],
        "an internal terminal completion may reap only its exact actor"
    );
    assert!(reap_finished_udp_edge_lane_instance(
        &mut lanes,
        second_lane_id
    ));
    assert!(matches!(
        completion_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    close_udp_edge_lanes(lanes).await;
}

#[tokio::test]
async fn single_slot_terminal_reporter_holds_capacity_until_task_finishes() {
    let target = TargetAddr::Ip("203.0.113.11:443".parse().expect("edge test target"));
    let (plan, _held_product_flows) = offline_edge_test_plan(&target).await;
    let limits = edge_test_mux_limits(1);
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<u8>>(udp_edge_completion_queue(limits));
    completion_tx
        .try_send(UdpEdgeCompletion::Discarded { lane_id: 90 })
        .expect("completion filler");
    let mut lanes = Vec::new();
    let mut next_lane_id = 0;

    assert!(
        dispatch_udp_edge_request(
            &mut lanes,
            &mut next_lane_id,
            &plan,
            limits,
            &completion_tx,
            edge_test_request(&target, b"a"),
        )
        .is_ok(),
        "first request must be accepted"
    );
    wait_for_request_to_leave_lane_queue(&lanes, 0).await;
    assert!(
        !lanes[0]
            .handle
            .as_ref()
            .expect("terminal reporting actor")
            .is_finished()
    );

    let rejected = dispatch_udp_edge_request(
        &mut lanes,
        &mut next_lane_id,
        &plan,
        limits,
        &completion_tx,
        edge_test_request(&target, b"blocked"),
    )
    .expect_err("terminal reporting must retain the only configured slot");
    assert_eq!(rejected.payload, Bytes::from_static(b"blocked"));
    assert_eq!(lanes.len(), 1);
    assert_eq!(lanes[0].lane_id, 0);
    assert_eq!(lanes[0].pending, 1);
    assert_eq!(next_lane_id, 1, "no successor may open before completion");

    assert!(matches!(
        completion_rx.recv().await,
        Some(UdpEdgeCompletion::Discarded { lane_id: 90 })
    ));
    let first_completion = completion_rx.recv().await.expect("first completion");
    finish_udp_edge_completion(&mut lanes, &first_completion);
    assert_eq!(offline_completion_lane(&first_completion), 0);
    wait_for_all_lane_tasks_to_finish(&lanes).await;
    assert_eq!(lanes[0].pending, 0);
    assert!(
        lanes[0]
            .handle
            .as_ref()
            .expect("completed terminal actor")
            .is_finished(),
        "only the completed actor boundary releases terminal-reporting ownership"
    );
    close_udp_edge_lanes(lanes).await;
}

#[tokio::test]
async fn genuinely_finished_terminal_actor_is_reaped_before_replacement() {
    let target = TargetAddr::Ip("203.0.113.12:443".parse().expect("edge test target"));
    let (plan, _held_product_flows) = offline_edge_test_plan(&target).await;
    let limits = edge_test_mux_limits(1);
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<u8>>(udp_edge_completion_queue(limits));
    completion_tx
        .try_send(UdpEdgeCompletion::Discarded { lane_id: 90 })
        .expect("completion filler");
    let mut lanes = Vec::new();
    let mut next_lane_id = 0;

    assert!(
        dispatch_udp_edge_request(
            &mut lanes,
            &mut next_lane_id,
            &plan,
            limits,
            &completion_tx,
            edge_test_request(&target, b"before-finish"),
        )
        .is_ok(),
        "first request must be accepted"
    );
    wait_for_request_to_leave_lane_queue(&lanes, 0).await;
    assert!(
        !lanes[0]
            .handle
            .as_ref()
            .expect("terminal reporting actor")
            .is_finished()
    );
    assert!(matches!(
        completion_rx.recv().await,
        Some(UdpEdgeCompletion::Discarded { lane_id: 90 })
    ));
    let first_completion = completion_rx.recv().await.expect("first completion");
    finish_udp_edge_completion(&mut lanes, &first_completion);
    assert_eq!(offline_completion_lane(&first_completion), 0);
    wait_for_all_lane_tasks_to_finish(&lanes).await;
    assert!(
        lanes[0]
            .handle
            .as_ref()
            .expect("completed terminal actor")
            .is_finished(),
        "replacement is attempted only after the real actor has finished"
    );

    assert!(
        dispatch_udp_edge_request(
            &mut lanes,
            &mut next_lane_id,
            &plan,
            limits,
            &completion_tx,
            edge_test_request(&target, b"replacement"),
        )
        .is_ok(),
        "a genuinely finished actor must be reaped before replacement"
    );
    assert_eq!(lanes.len(), 1);
    assert_eq!(lanes[0].lane_id, 1);
    assert_eq!(lanes[0].pending, 1);
    assert_eq!(next_lane_id, 2);

    let replacement_completion = completion_rx
        .recv()
        .await
        .expect("replacement request completion");
    finish_udp_edge_completion(&mut lanes, &replacement_completion);
    assert_eq!(offline_completion_lane(&replacement_completion), 1);
    wait_for_all_lane_tasks_to_finish(&lanes).await;
    close_udp_edge_lanes(lanes).await;
}

#[tokio::test]
async fn terminal_actor_reports_every_request_accepted_before_its_failure_boundary() {
    let target = TargetAddr::Ip("203.0.113.13:443".parse().expect("edge test target"));
    let (plan, _held_product_flows) = offline_edge_test_plan(&target).await;
    let limits = edge_test_mux_limits(2);
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<u8>>(udp_edge_completion_queue(limits));
    let mut lanes = Vec::new();
    let mut next_lane_id = 0;

    // This current-thread test does not poll the spawned actor between these
    // calls, so both successful admissions belong to the same lane before its
    // offline open result closes the receiver.
    for payload in [b"a".as_slice(), b"b".as_slice()] {
        assert!(
            dispatch_udp_edge_request(
                &mut lanes,
                &mut next_lane_id,
                &plan,
                limits,
                &completion_tx,
                edge_test_request(&target, payload),
            )
            .is_ok()
        );
    }
    assert_eq!(lanes.len(), 1);
    assert_eq!(lanes[0].pending, 2);

    for _ in 0..2 {
        let completion = completion_rx
            .recv()
            .await
            .expect("accepted request terminal completion");
        assert_eq!(offline_completion_lane(&completion), 0);
        finish_udp_edge_completion(&mut lanes, &completion);
    }
    wait_for_all_lane_tasks_to_finish(&lanes).await;
    assert_eq!(lanes[0].pending, 0);
    assert!(matches!(
        completion_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    close_udp_edge_lanes(lanes).await;
}

#[tokio::test]
async fn external_expiry_removes_all_overlapping_lanes_for_its_exact_metadata() {
    let target = TargetAddr::Ip("203.0.113.14:443".parse().expect("edge test target"));
    let (plan, _held_product_flows) = offline_edge_test_plan(&target).await;
    let limits = edge_test_mux_limits(2);
    let (completion_tx, _completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<u8>>(udp_edge_completion_queue(limits));
    completion_tx
        .try_send(UdpEdgeCompletion::Discarded { lane_id: 90 })
        .expect("first completion filler");
    completion_tx
        .try_send(UdpEdgeCompletion::Discarded { lane_id: 91 })
        .expect("second completion filler");
    let mut lanes = Vec::new();
    let mut next_lane_id = 0;

    assert!(
        dispatch_udp_edge_request(
            &mut lanes,
            &mut next_lane_id,
            &plan,
            limits,
            &completion_tx,
            edge_test_request(&target, b"reporter"),
        )
        .is_ok()
    );
    wait_for_request_to_leave_lane_queue(&lanes, 0).await;
    assert!(
        dispatch_udp_edge_request(
            &mut lanes,
            &mut next_lane_id,
            &plan,
            limits,
            &completion_tx,
            edge_test_request(&target, b"successor"),
        )
        .is_ok()
    );
    assert_eq!(lanes.len(), 2, "reporter and successor must overlap");
    assert!(remove_udp_edge_lane(&mut lanes, &9));
    assert!(
        lanes.is_empty(),
        "external generation expiry owns and removes every overlapping actor"
    );
}

#[tokio::test]
async fn external_expiry_transfers_cleanup_to_lane_actor_instead_of_aborting_it() {
    let (requests, _requests_rx) = mpsc::channel(1);
    let retirement = edge_test_retirement_gate();
    let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
    let (cleanup_tx, cleanup_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        cancelled.changed().await.expect("retirement signal");
        assert!(*cancelled.borrow());
        let _ = cleanup_tx.send(());
    });
    let mut lanes = vec![UdpEdgeLane {
        lane_id: 77,
        metadata: 9_u8,
        pending: 0,
        requests,
        retirement,
        cancel,
        handle: Some(handle),
    }];

    assert!(remove_udp_edge_lane(&mut lanes, &9));
    assert!(lanes.is_empty());
    tokio::time::timeout(Duration::from_secs(1), cleanup_rx)
        .await
        .expect("lane cleanup deadline")
        .expect("lane cleanup ran to completion");
}

#[tokio::test]
async fn owner_cancellation_leaves_the_udp_lane_actor_to_finish_cleanup() {
    let (requests, _requests_rx) = mpsc::channel(1);
    let retirement = edge_test_retirement_gate();
    let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
    let (cleanup_tx, cleanup_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        cancelled.changed().await.expect("retirement signal");
        assert!(*cancelled.borrow());
        let _ = cleanup_tx.send(());
    });
    let lane = UdpEdgeLane {
        lane_id: 78,
        metadata: 9_u8,
        pending: 0,
        requests,
        retirement,
        cancel,
        handle: Some(handle),
    };

    drop(lane);
    tokio::time::timeout(Duration::from_secs(1), cleanup_rx)
        .await
        .expect("detached cleanup deadline")
        .expect("lane actor was not aborted with its owner");
}

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
    let retirement = edge_test_retirement_gate();
    let activity = retirement.lock().expect("retirement gate").activity.clone();
    let lane = tokio::spawn(run_silent_udp_denial_lane(
        7,
        9_u8,
        request_rx,
        completion_tx,
        cancelled,
        initial,
        None,
        activity,
        retirement,
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
