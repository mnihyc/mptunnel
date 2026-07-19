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
use std::time::{Duration, Instant};

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

#[tokio::test]
async fn new_stream_acceptance_precedes_validation_on_its_opening_carrier() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(700);
    let stream_id = StreamId(8);
    let path_id = PathId(2);
    let registration = port.register_carrier_path(
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
    let registration = port.register_carrier_path(
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
    let registration = port.register_carrier_path(
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
    let registration = port.register_carrier_path(
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
    let first_registration = port.register_carrier_path(
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
    let replacement_registration = port.register_carrier_path(
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
    let first = port.register_carrier_path(
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
    let _second = port.register_carrier_path(
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
    let registration = port.register_carrier_path(
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
        Some(output_incarnation)
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
    let registration = port.register_carrier_path(
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
    let registration = port.register_carrier_path(
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
