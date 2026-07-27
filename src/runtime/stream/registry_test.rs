use super::*;
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS};
use crate::model::path::{CarrierPathKey, PathPolicy};
use crate::mux::MuxLimits;
use crate::protocol::{OffsetRange, PathMetricDirection, PathUsage};
use crate::runtime::path::commands::{
    reliable_path_command_channels, try_recv_reliable_path_priority_command,
};
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::path::{
    PathProofObservation, ServerLocalPathProperties, ServerStreamPathAttachment,
};
use std::net::SocketAddr;
use std::sync::Barrier;
use std::time::{Duration, Instant};

fn constrained_registry(
    max_streams: usize,
    max_paths_per_session: usize,
) -> Arc<ServerReliableStreamRegistry> {
    let (accepted, _accepted_rx) = mpsc::unbounded_channel();
    let limits = MuxLimits {
        max_streams,
        ..MuxLimits::default()
    };
    Arc::new(ServerReliableStreamRegistry::with_accept_sender(
        max_streams,
        max_paths_per_session,
        accepted,
        limits,
    ))
}

fn path_proof_observation(proof_id: u64, elapsed: Duration) -> PathProofObservation {
    PathProofObservation {
        proof_id,
        elapsed,
        sent_at: Instant::now()
            .checked_sub(elapsed)
            .expect("test proof send instant"),
    }
}

fn native_quic_test_metrics(path_id: PathId) -> PathMetrics {
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Udp,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 0,
        delivery_rate_bps: 1,
        pacing_rate_bps: 1,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
        inflight_hi_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        data_sample_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
    }
}

#[test]
fn carrier_admission_enforces_logical_identity_and_cross_carrier_session_cap() {
    let registry = constrained_registry(8, 2);
    let port = registry.path_port();
    let session_id = SessionId(600);
    let tcp = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            ServerLocalPathProperties::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("first TCP carrier");
    let duplicate = port.register_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
        PrincipalPermit::for_test("test-peer"),
    );
    assert!(
        duplicate
            .expect_err("same logical carrier must be unique")
            .to_string()
            .contains("duplicate server logical carrier path")
    );
    let udp = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
            PathId(0),
            ServerLocalPathProperties::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("second carrier across QUIC underlay");
    let over_limit = port.register_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
        PrincipalPermit::for_test("test-peer"),
    );
    assert!(
        over_limit
            .expect_err("cross-carrier session limit")
            .to_string()
            .contains("server session carrier path limit reached")
    );

    drop(tcp);
    let replacement = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(1),
            ServerLocalPathProperties::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("dropping one carrier recovers the per-session slot");
    assert_eq!(port.management_snapshot().paths.len(), 2);
    drop(replacement);
    drop(udp);
    assert!(port.management_snapshot().sessions.is_empty());
}

#[test]
fn failed_global_and_session_admission_roll_back_and_recover_exactly() {
    let path_bounded = constrained_registry(2, 1);
    let path_port = path_bounded.path_port();
    let first = path_port
        .register_carrier_path(
            SessionId(610),
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("first"),
        )
        .expect("first global path");
    let second = path_port
        .register_carrier_path(
            SessionId(611),
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("second"),
        )
        .expect("second global path");
    assert!(
        path_port
            .register_carrier_path(
                SessionId(612),
                UnderlayProtocol::Tcp,
                PathId(0),
                Default::default(),
                PrincipalPermit::for_test("third"),
            )
            .expect_err("global carrier ceiling")
            .to_string()
            .contains("server global carrier path limit reached")
    );
    drop(first);
    let recovered = path_port
        .register_carrier_path(
            SessionId(612),
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("third"),
        )
        .expect("global path slot recovers after exact drop");
    drop(recovered);
    drop(second);

    let session_bounded = constrained_registry(2, 4);
    let session_port = session_bounded.path_port();
    let first = session_port
        .register_carrier_path(
            SessionId(620),
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("first"),
        )
        .expect("first authenticated session");
    let second = session_port
        .register_carrier_path(
            SessionId(621),
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("second"),
        )
        .expect("second authenticated session");
    assert!(
        session_port
            .register_carrier_path(
                SessionId(622),
                UnderlayProtocol::Tcp,
                PathId(0),
                Default::default(),
                PrincipalPermit::for_test("third"),
            )
            .expect_err("authenticated session ceiling")
            .to_string()
            .contains("server authenticated session limit reached")
    );
    assert_eq!(
        session_port.management_snapshot().paths.len(),
        2,
        "failed session admission must roll back its carrier reservation"
    );
    drop(first);
    let recovered = session_port
        .register_carrier_path(
            SessionId(622),
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("third"),
        )
        .expect("authenticated session slot recovers after final reference drops");
    drop(recovered);
    drop(second);
    assert!(session_port.management_snapshot().sessions.is_empty());
}

#[test]
fn stale_carrier_retirement_cannot_release_its_replacement() {
    let registry = constrained_registry(2, 1);
    let port = registry.path_port();
    let session_id = SessionId(630);
    let first = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("first carrier generation");
    let stale_identity = ServerCarrierPathIdentity {
        session_id,
        underlay: first.underlay(),
        path_id: first.path_id(),
        path_instance_id: first.path_instance_id(),
    };
    drop(first);
    let replacement = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("replacement carrier generation");

    registry.retire_carrier_path(stale_identity);
    let snapshot = port.management_snapshot();
    assert_eq!(snapshot.paths.len(), 1);
    assert_eq!(
        snapshot.paths[0].path_instance_id,
        replacement.path_instance_id(),
        "stale retirement must be an exact-instance no-op"
    );
    assert!(
        port.register_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect_err("replacement still owns the logical identity")
        .to_string()
        .contains("duplicate server logical carrier path")
    );
}

#[tokio::test]
async fn reconnect_waits_for_bounded_ordered_retirement_under_backpressure() {
    let registry = constrained_registry(1, 1);
    let port = registry.path_port();
    let session_id = SessionId(640);
    let stream_id = StreamId(1);
    let registration = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("initial carrier");
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            lane: TrafficClass::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };
    let mut stream = accepted.take_stream();
    let key = CarrierPathKey {
        underlay: registration.underlay(),
        path_id: registration.path_id(),
    };
    let output_incarnation = match &stream.output {
        ReliablePathStreamOutput::Switchable(binding) => {
            binding
                .sender_path_targets(TrafficClass::Throughput, 1)
                .first()
                .expect("response output")
                .observation
                .incarnation
        }
        ReliablePathStreamOutput::Fixed(_) => panic!("expected switchable response output"),
    };
    let mut backpressured = false;
    for offset in 0..10_000u64 {
        let frame = Frame::StreamAck {
            stream_id,
            complete: false,
            ranges: vec![OffsetRange {
                start: offset,
                end: offset + 1,
            }],
        };
        if matches!(
            port.try_route_frame(&registration, stream_id, frame)
                .expect("route ACK"),
            ServerStreamFrameRoute::Backpressured(_)
        ) {
            backpressured = true;
            break;
        }
    }
    assert!(backpressured, "test must saturate the ordered actor queue");

    port.detach_path(&registration, stream_id)
        .expect("first explicit detach");
    port.detach_path(&registration, stream_id)
        .expect("replayed explicit detach shares pending lifecycle");
    drop(registration);
    assert!(
        port.register_carrier_path(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect_err("retiring logical carrier retains its bounded admission slot")
        .to_string()
        .contains("duplicate server logical carrier path")
    );

    tokio::time::timeout(Duration::from_secs(1), async {
        while stream.has_output_incarnation(key, output_incarnation) {
            // Once PathDetached is applied recv_frame waits for another
            // product frame; a short cancellation exposes the state change.
            let _ = tokio::time::timeout(Duration::from_millis(10), stream.recv_frame()).await;
        }
    })
    .await
    .expect("the one ordered detach must drain through actor backpressure");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !port.management_snapshot().paths.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordered retirement must finish after queue capacity recovers");
    let replacement = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("reconnect after ordered retirement");
    drop(replacement);
}

#[test]
fn parallel_carrier_admission_and_drop_has_no_lock_order_deadlock() {
    const WORKERS: usize = 8;
    const ITERATIONS: usize = 200;
    let registry = constrained_registry(WORKERS, 1);
    let barrier = Arc::new(Barrier::new(WORKERS + 1));
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let mut workers = Vec::new();
    for worker in 0..WORKERS {
        let registry = registry.clone();
        let barrier = barrier.clone();
        let done_tx = done_tx.clone();
        workers.push(std::thread::spawn(move || {
            let port = registry.path_port();
            barrier.wait();
            for _ in 0..ITERATIONS {
                let registration = port
                    .register_carrier_path(
                        SessionId(700 + worker as u64),
                        UnderlayProtocol::Tcp,
                        PathId(0),
                        Default::default(),
                        PrincipalPermit::for_test("test-peer"),
                    )
                    .expect("parallel exact carrier admission");
                drop(registration);
            }
            done_tx.send(()).expect("completion receiver");
        }));
    }
    drop(done_tx);
    barrier.wait();
    for _ in 0..WORKERS {
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parallel carrier admission must not deadlock");
    }
    for worker in workers {
        worker.join().expect("carrier admission worker");
    }
    assert!(registry.path_port().management_snapshot().paths.is_empty());
}

#[tokio::test]
async fn new_stream_acceptance_precedes_validation_on_its_opening_carrier() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(700);
    let stream_id = StreamId(8);
    let path_id = PathId(2);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(8);
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            lane: TrafficClass::Latency,
            attachment: ServerStreamPathAttachment {
                path_registration: registration,
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };

    assert_eq!(
        accepted.stream().max_offset,
        0,
        "OPEN_STREAM alone must not manufacture reverse-direction send credit"
    );
    accepted
        .accept_opening_path()
        .await
        .expect("publish opening acceptance and validation");
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(crate::runtime::path::commands::ReliablePathCommand::SendFrame(
            Frame::StreamMaxData {
                stream_id: accepted_stream_id,
                ..
            }
        )) if accepted_stream_id == stream_id
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(crate::runtime::path::commands::ReliablePathCommand::SendFrame(
            Frame::PathProofData {
                path_id: proof_path_id,
                payload,
                ..
            }
        )) if proof_path_id == path_id && !payload.is_empty()
    ));
}

#[test]
fn unacknowledged_carrier_validation_can_be_retried() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(1));
    let port = registry.path_port();
    let registration = port.register_test_carrier_path(
        SessionId(699),
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    assert!(
        registration
            .path_validation_challenge(MuxLimits::default())
            .is_some(),
        "an unvalidated carrier emits a challenge",
    );
    assert!(
        registration
            .path_validation_challenge(MuxLimits::default())
            .is_some(),
        "an unacknowledged challenge must not suppress a later retry",
    );

    port.record_path_proof_success(
        &registration,
        path_proof_observation(1, Duration::from_millis(1)),
    );
    assert!(
        registration
            .path_validation_challenge(MuxLimits::default())
            .is_none(),
        "a validated carrier must not emit another challenge",
    );
}

#[test]
fn accepted_stream_does_not_extend_carrier_registration_lifetime() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(1));
    let port = registry.path_port();
    let session_id = SessionId(698);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(2);
    let _accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id: StreamId(7),
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            lane: TrafficClass::Latency,
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };

    drop(registration);
    assert!(
        port.management_snapshot().paths.is_empty(),
        "the carrier actor, not an accepted product stream, owns registration lifetime",
    );
}

#[test]
fn late_open_and_closed_output_replacement_inherit_path_evidence() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(16));
    let session_id = SessionId(701);
    let stream_id = StreamId(9);
    let path_id = PathId(3);
    let port = registry.path_port();
    let local_policy = PathPolicy {
        backup: true,
        bulk_allowed: false,
        ..PathPolicy::default()
    };
    let metrics = native_quic_test_metrics(path_id);
    let mut startup_metrics = metrics;
    startup_metrics.srtt_us = 333_000;
    startup_metrics.confidence_ppm = 0;
    startup_metrics.has_ack_derived_data_sample = false;
    startup_metrics.data_sample_count = 0;
    startup_metrics.data_sample_bytes = 0;
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties {
            config_ordinal: 4,
            policy: local_policy,
            initial_metrics: Some(startup_metrics),
        },
    );
    let path_instance_id = registration.path_instance_id();
    port.record_peer_path_usage(&registration, 7, PathUsage::Backup);
    port.record_local_path_metrics(&registration, metrics, false);
    let stored_before_proof = registry
        .path_metrics
        .lock()
        .expect("test path metrics lock")
        .get(&(session_id, UnderlayProtocol::Udp, path_id, path_instance_id))
        .copied()
        .expect("stored native path metrics");
    let proof = path_proof_observation(1, Duration::from_millis(12));
    port.record_path_proof_success(&registration, proof);
    let stored_after_proof = registry
        .path_metrics
        .lock()
        .expect("test path metrics lock")
        .get(&(session_id, UnderlayProtocol::Udp, path_id, path_instance_id))
        .copied()
        .expect("native metrics survive path proof");
    assert_eq!(stored_after_proof.metrics, stored_before_proof.metrics);
    assert_eq!(stored_after_proof.source, stored_before_proof.source);
    assert_eq!(
        stored_after_proof.native_drain_observed,
        stored_before_proof.native_drain_observed
    );
    assert_eq!(
        stored_after_proof.recorded_at,
        stored_before_proof.recorded_at
    );

    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, mut first_receivers) = reliable_path_command_channels(8);
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            lane: TrafficClass::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };
    let stream = accepted.stream();
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable response binding");
    };
    let inherited = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key.path_id == path_id)
        .expect("inherited carrier target");
    assert!(inherited.observation.has_bulk_rate_evidence);
    assert!(inherited.observation.has_path_proof_evidence);
    assert_eq!(inherited.observation.snapshot.confidence, 1.0);
    assert_eq!(
        inherited.observation.snapshot.srtt_ms, 12.0,
        "validation RTT is a fallback without replacing native capacity evidence",
    );
    assert_eq!(
        inherited.observation.snapshot.peer_usage,
        Some(PathUsage::Backup)
    );
    assert_eq!(
        inherited.observation.snapshot.policy, local_policy,
        "response scheduling must inherit the accepting listener's local policy",
    );
    assert!(
        try_recv_reliable_path_priority_command(&mut first_receivers).is_none(),
        "a validated carrier must not be probed again for each product stream",
    );

    port.record_peer_path_usage(&registration, 6, PathUsage::Available);
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .into_iter()
            .find(|target| target.observation.key.path_id == path_id)
            .expect("live usage target")
            .observation
            .snapshot
            .peer_usage,
        Some(PathUsage::Backup),
        "stale directional usage must not replace the latest sequence"
    );

    drop(first_receivers);
    let generation = binding.response_model_generation();
    let (replacement_commands, mut replacement_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: target.clone(),
                lane: TrafficClass::Throughput,
                attachment: ServerStreamPathAttachment {
                    path_registration: registration.clone(),
                    commands: replacement_commands,
                    max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                },
                mux_limits: MuxLimits::default(),
            },)
            .expect("replace closed response output"),
        ServerReliableStreamOpen::Existing
    ));
    assert_eq!(
        binding.response_model_generation(),
        generation + 1,
        "replacement membership and inherited evidence publish as one model generation",
    );
    let replacement = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key.path_id == path_id)
        .expect("replacement carrier target");
    assert!(replacement.observation.has_bulk_rate_evidence);
    assert!(replacement.observation.has_path_proof_evidence);
    assert_eq!(replacement.observation.snapshot.confidence, 1.0);
    assert_eq!(
        replacement.observation.snapshot.peer_usage,
        Some(PathUsage::Backup)
    );
    assert!(
        try_recv_reliable_path_priority_command(&mut replacement_receivers).is_none(),
        "reattachment to the same validated carrier must not enqueue another proof",
    );

    port.record_peer_path_usage(&registration, 8, PathUsage::Available);
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .into_iter()
            .find(|target| target.observation.key.path_id == path_id)
            .expect("updated usage target")
            .observation
            .snapshot
            .peer_usage,
        Some(PathUsage::Available)
    );
}

#[test]
fn replacement_carrier_does_not_inherit_retired_path_proof() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(16));
    let port = registry.path_port();
    let session_id = SessionId(702);
    let stream_id = StreamId(10);
    let path_id = PathId(4);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let first_registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        Default::default(),
    );
    port.record_path_proof_success(
        &first_registration,
        path_proof_observation(2, Duration::from_millis(20)),
    );
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            lane: TrafficClass::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: first_registration.clone(),
                commands: first_commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };
    let ReliablePathStreamOutput::Switchable(binding) = &accepted.stream().output else {
        panic!("expected switchable response binding");
    };
    assert!(
        binding.sender_path_targets(TrafficClass::Throughput, 1)[0]
            .observation
            .has_path_proof_evidence
    );

    let first_instance = first_registration.path_instance_id();
    drop(first_registration);
    let replacement_registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        Default::default(),
    );
    assert_ne!(
        replacement_registration.path_instance_id(),
        first_instance,
        "a replacement must have a distinct physical-path identity",
    );
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
                lane: TrafficClass::Throughput,
                attachment: ServerStreamPathAttachment {
                    path_registration: replacement_registration.clone(),
                    commands: replacement_commands,
                    max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                },
                mux_limits: MuxLimits::default(),
            })
            .expect("attach replacement response path"),
        ServerReliableStreamOpen::Existing
    ));
    let replacement = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|candidate| {
            candidate.observation.path_instance_id == replacement_registration.path_instance_id()
        })
        .expect("replacement response target");
    assert!(
        !replacement.observation.has_path_proof_evidence,
        "proof from a retired carrier instance must not validate its replacement",
    );

    port.record_path_proof_success(
        &replacement_registration,
        path_proof_observation(3, Duration::from_millis(25)),
    );
    assert!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .into_iter()
            .find(|candidate| {
                candidate.observation.path_instance_id
                    == replacement_registration.path_instance_id()
            })
            .expect("validated replacement response target")
            .observation
            .has_path_proof_evidence
    );
}

#[test]
fn peer_status_snapshot_is_session_scoped_and_tracks_registration_lifetime() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(8));
    let port = registry.path_port();
    let first_session = SessionId(801);
    let second_session = SessionId(802);
    let path_id = PathId(4);
    let mut first_metrics = native_quic_test_metrics(path_id);
    first_metrics.delivery_rate_bps = 111;
    let mut second_metrics = native_quic_test_metrics(path_id);
    second_metrics.delivery_rate_bps = 222;
    let first = port.register_test_carrier_path(
        first_session,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties {
            config_ordinal: 0,
            policy: PathPolicy {
                backup: true,
                ..PathPolicy::default()
            },
            initial_metrics: Some(first_metrics),
        },
    );
    let _second = port.register_test_carrier_path(
        second_session,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties {
            config_ordinal: 1,
            policy: PathPolicy::default(),
            initial_metrics: Some(second_metrics),
        },
    );
    port.record_local_path_metrics(&first, first_metrics, false);

    let paths = port.peer_status_snapshot(first_session);
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].metrics.delivery_rate_bps, 111);
    assert_eq!(paths[0].usage, PathUsage::Backup);
    assert_eq!(paths[0].state, PeerPathState::Active);

    first.set_state(PeerPathState::Draining);
    assert_eq!(
        port.peer_status_snapshot(first_session)[0].state,
        PeerPathState::Draining
    );
    let management = port.management_snapshot();
    let managed = management
        .paths
        .iter()
        .find(|path| path.session_id == first_session)
        .expect("managed path");
    assert_eq!(managed.state, PeerPathState::Draining);
    assert_eq!(managed.configured_index, 0);
    assert!(managed.policy.backup);
    assert_eq!(managed.usage, None);
    assert_eq!(managed.source, Some("local_sender"));
    assert_eq!(managed.metrics.expect("metrics").delivery_rate_bps, 111);
    assert!(managed.path_instance_id.as_u64() > 0);
    assert!(
        management
            .sessions
            .iter()
            .any(|session| session.session_id == first_session)
    );
    drop(first);
    assert!(port.peer_status_snapshot(first_session).is_empty());
    assert_eq!(
        port.peer_status_snapshot(second_session)[0]
            .metrics
            .delivery_rate_bps,
        222
    );
}

#[tokio::test]
async fn server_stream_try_route_preserves_bounded_backpressure() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(901);
    let stream_id = StreamId(33);
    let path_id = PathId(0);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            lane: TrafficClass::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };
    let mut stream = accepted.take_stream();
    let first = Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: vec![OffsetRange { start: 0, end: 1 }],
    };
    assert!(matches!(
        port.try_route_frame(&registration, stream_id, first.clone()),
        Ok(ServerStreamFrameRoute::Routed)
    ));
    assert_eq!(stream.recv_frame().await.expect("routed frame"), first);

    let mut backpressured = None;
    for offset in 1..10_000u64 {
        let frame = Frame::StreamAck {
            stream_id,
            complete: false,
            ranges: vec![OffsetRange {
                start: offset,
                end: offset + 1,
            }],
        };
        match port
            .try_route_frame(&registration, stream_id, frame)
            .expect("try route frame")
        {
            ServerStreamFrameRoute::Routed => {}
            ServerStreamFrameRoute::Backpressured(frame) => {
                backpressured = Some(frame);
                break;
            }
        }
    }
    let backpressured = backpressured.expect("bounded stream queue must report pressure");
    let _ = stream.recv_frame().await.expect("release one queue slot");
    assert!(matches!(
        port.try_route_frame(&registration, stream_id, backpressured),
        Ok(ServerStreamFrameRoute::Routed)
    ));
}

#[tokio::test]
async fn routed_request_data_updates_feedback_ingress_on_the_same_stream_event_snapshot() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(905);
    let stream_id = StreamId(37);
    let mux_limits = MuxLimits::default();
    let tcp = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let udp = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let mut accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            lane: TrafficClass::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: tcp.clone(),
                commands: tcp_commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };
    let mut stream = accepted.take_stream();
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
                lane: TrafficClass::Throughput,
                attachment: ServerStreamPathAttachment {
                    path_registration: udp.clone(),
                    commands: udp_commands,
                    max_frame_payload_bytes: mux_limits.max_payload_bytes,
                },
                mux_limits,
            })
            .expect("attach QUIC response output"),
        ServerReliableStreamOpen::Existing
    ));
    assert_eq!(
        stream.request_feedback_underlay(),
        Some(UnderlayProtocol::Udp),
        "the latest accepted request ingress starts as the return-path hint"
    );

    let ack = Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: vec![OffsetRange { start: 0, end: 1 }],
    };
    port.route_frame(&tcp, stream_id, ack.clone())
        .await
        .expect("route feedback frame");
    assert_eq!(stream.recv_frame().await.expect("receive feedback"), ack);
    assert_eq!(
        stream.request_feedback_underlay(),
        Some(UnderlayProtocol::Udp),
        "response feedback must not claim request-data ingress"
    );

    let data = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: bytes::Bytes::from_static(b"request"),
    };
    assert!(matches!(
        port.try_route_frame(&tcp, stream_id, data.clone()),
        Ok(ServerStreamFrameRoute::Routed)
    ));
    assert_eq!(
        stream.recv_frame().await.expect("receive request data"),
        data
    );
    assert_eq!(
        stream.request_feedback_underlay(),
        Some(UnderlayProtocol::Tcp),
        "the route snapshot must account the exact carrier that supplied request data"
    );

    let fin = Frame::StreamFin {
        stream_id,
        final_offset: 7,
    };
    port.route_frame(&udp, stream_id, fin.clone())
        .await
        .expect("route request FIN");
    assert_eq!(stream.recv_frame().await.expect("receive request FIN"), fin);
    assert_eq!(
        stream.request_feedback_underlay(),
        Some(UnderlayProtocol::Udp),
        "request FIN updates the return-path hint through the same route snapshot"
    );
}

#[test]
fn full_actor_queue_keeps_detach_fifo_without_runtime_context() {
    let session_id = SessionId(904);
    let stream_id = StreamId(36);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        session_id,
        key.underlay,
        key.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let target = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .next()
        .expect("response output");
    let path_instance_id = target.observation.path_instance_id;
    let output_incarnation = target.observation.incarnation;
    let ack = Frame::StreamAck {
        stream_id,
        complete: true,
        ranges: vec![OffsetRange { start: 0, end: 1 }],
    };
    let (events, mut actor_input) = mpsc::channel(1);
    events
        .try_send(ServerReliableStreamEvent::Frame(ack.clone()))
        .expect("fill actor queue");
    assert_eq!(
        binding.begin_path_detach(key, path_instance_id),
        Some(ResponsePathDetachOutcome::Begun(output_incarnation))
    );

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let drain_barrier = barrier.clone();
    let drainer = std::thread::spawn(move || {
        drain_barrier.wait();
        std::thread::sleep(Duration::from_millis(50));
        let first = actor_input.blocking_recv().expect("queued ACK");
        (first, actor_input)
    });
    barrier.wait();
    queue_ordered_path_detach(
        events,
        binding.clone(),
        key,
        path_instance_id,
        output_incarnation,
    );

    let (first, mut actor_input) = drainer.join().expect("actor drainer");
    assert!(matches!(first, ServerReliableStreamEvent::Frame(frame) if frame == ack));
    let second = actor_input.blocking_recv().expect("ordered detach");
    assert!(matches!(
        second,
        ServerReliableStreamEvent::PathDetached {
            key: detached_key,
            path_instance_id: detached_instance,
            output_incarnation: detached_incarnation,
        } if detached_key == key
            && detached_instance == path_instance_id
            && detached_incarnation == output_incarnation
    ));
    assert!(binding.has_output_incarnation(key, output_incarnation));
    binding.complete_path_detach(key, path_instance_id, output_incarnation);
    assert!(!binding.has_output_incarnation(key, output_incarnation));
}

#[tokio::test]
async fn queued_ack_precedes_following_path_detach_at_stream_actor() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(903);
    let stream_id = StreamId(35);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            lane: TrafficClass::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };
    let mut stream = accepted.take_stream();
    let key = CarrierPathKey {
        underlay: registration.underlay(),
        path_id: registration.path_id(),
    };
    let output_incarnation = match &stream.output {
        ReliablePathStreamOutput::Switchable(binding) => {
            binding
                .sender_path_targets(TrafficClass::Throughput, 1)
                .first()
                .expect("response output")
                .observation
                .incarnation
        }
        ReliablePathStreamOutput::Fixed(_) => panic!("expected switchable response output"),
    };
    let ack = Frame::StreamAck {
        stream_id,
        complete: true,
        ranges: vec![OffsetRange {
            start: 0,
            end: 32_971,
        }],
    };
    port.route_frame(&registration, stream_id, ack.clone())
        .await
        .expect("route ACK");
    port.detach_path(&registration, stream_id)
        .expect("queue path detach");

    assert!(
        !stream.has_live_output(),
        "detaching output must stop accepting new sends immediately"
    );
    assert!(stream.has_output_incarnation(key, output_incarnation));
    assert_eq!(stream.recv_frame().await.expect("receive ACK"), ack);
    assert!(
        stream.has_output_incarnation(key, output_incarnation),
        "the following detach must not overtake ACK handling"
    );

    let next = Frame::StreamMaxData {
        stream_id,
        max_offset: 65_536,
    };
    port.route_frame(&registration, stream_id, next.clone())
        .await
        .expect("route next frame");
    assert_eq!(stream.recv_frame().await.expect("receive next frame"), next);
    assert!(
        !stream.has_output_incarnation(key, output_incarnation),
        "the actor applies detach immediately after earlier input"
    );
}

#[tokio::test]
async fn late_frame_after_relay_exit_does_not_close_shared_carrier() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(902);
    let stream_id = StreamId(34);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            lane: TrafficClass::Latency,
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted) => accepted,
        _ => panic!("expected new response stream"),
    };

    drop(accepted.take_stream());
    let late_fin = Frame::StreamFin {
        stream_id,
        final_offset: 0,
    };
    port.route_frame(&registration, stream_id, late_fin.clone())
        .await
        .expect("late frame is scoped to the retired stream");
    assert!(matches!(
        port.try_route_frame(&registration, stream_id, late_fin),
        Ok(ServerStreamFrameRoute::Routed)
    ));
}
