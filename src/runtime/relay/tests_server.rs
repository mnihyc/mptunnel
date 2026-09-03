use super::*;
use crate::config::ProductPolicyConfig;
use crate::model::capacity::MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
use crate::model::path::CarrierPathKey;
use crate::model::timing::{ReliableDataAckGapTiming, transport_pto_from_snapshot};
use crate::mux::stream::validate_stream_ack;
use crate::outbound::OutboundConfig;
use crate::performance::ResourceLimits;
use crate::product::{
    EgressAction, InboundId, InitialDemand, RouteAction, RouteMatchSpec, RouteRuleSpec, RouteStage,
    RuleId,
};
use crate::protocol::frame::stream_ack_contiguous_frontier;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, StreamDemandHint, TargetAddr};
use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistry};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_command,
};
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::path::{
    ServerLocalPathProperties, ServerStreamOpenOutcome, ServerStreamOpenRequest,
    ServerStreamPathAttachment,
};
use crate::runtime::stream::ReliablePathStreamOutput;
use crate::runtime::stream::response::{
    ResponseStreamAttachOutcome, ResponseStreamBinding, ServerPathMetricsSource,
};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn tail_recovery_candidate(start: u64, sent_at: Instant) -> ReliableRelayTailRecoveryCandidate {
    ReliableRelayTailRecoveryCandidate::Untracked {
        start,
        end: start + 64,
        sent_at,
    }
}

fn tracked_tail_recovery_candidate(
    start: u64,
    sent_at: Instant,
    underlay: UnderlayProtocol,
) -> ReliableRelayTailRecoveryCandidate {
    ReliableRelayTailRecoveryCandidate::Tracked(ResponseDataAckRecoveryCandidate {
        start,
        end: start + 64,
        key: CarrierPathKey {
            underlay,
            path_id: PathId(0),
        },
        output_incarnation: 1,
        sent_at,
    })
}

#[test]
fn server_ack_gap_timer_uses_the_evaluation_epoch() {
    let now = Instant::now();
    let observed_at = now - Duration::from_millis(2);
    let deadline = now - Duration::from_millis(1);

    assert_eq!(
        server_ack_gap_timer_deadline(Some(deadline), observed_at),
        Some(tokio::time::Instant::from_std(deadline)),
        "a deadline future at evaluation must remain armed even if it crosses during synchronous loop work",
    );
    assert_eq!(
        server_ack_gap_timer_deadline(Some(deadline), deadline),
        None,
        "a deadline already due at evaluation belongs to the current evaluation rather than a new timer",
    );
}

#[test]
fn live_owner_wake_requires_both_cause_and_shared_epoch_without_losing_due_state() {
    let observed_at = Instant::now();
    let cause_deadline = tokio::time::Instant::from_std(observed_at + Duration::from_millis(10));
    let epoch_deadline = observed_at + Duration::from_millis(20);

    assert_eq!(
        server_live_owner_recovery_wake(Some(cause_deadline), Some(epoch_deadline), 0, observed_at,),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: Some(epoch_deadline),
        },
        "live-owner eligibility is the conjunction of the cause clock and shared epoch",
    );
    assert_eq!(
        server_live_owner_recovery_wake(
            Some(cause_deadline),
            Some(epoch_deadline),
            0,
            observed_at + Duration::from_millis(15),
        ),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: Some(epoch_deadline),
        },
        "an accepted live-owner attempt retains the already-due cause and wakes exactly at its immutable successor epoch",
    );

    let due_at = observed_at + Duration::from_millis(30);
    assert_eq!(
        server_live_owner_recovery_wake(Some(cause_deadline), Some(epoch_deadline), 0, due_at),
        LiveOwnerRecoveryWake {
            due: true,
            deadline: None,
        },
        "a due epoch remains an explicit evaluation fact instead of disappearing with its past timer",
    );
    assert_eq!(
        server_live_owner_recovery_wake(None, Some(epoch_deadline), 0, due_at),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: None,
        },
        "the shared epoch is only a gate and cannot wake without a retained recovery cause",
    );
}

#[test]
fn optional_live_owner_service_does_not_wait_for_the_frontier_floor_epoch() {
    let observed_at = Instant::now();
    let due_cause = tokio::time::Instant::from_std(observed_at - Duration::from_millis(1));
    let epoch_deadline = observed_at + Duration::from_millis(20);

    assert_eq!(
        server_live_owner_recovery_wake(Some(due_cause), Some(epoch_deadline), 1, observed_at),
        LiveOwnerRecoveryWake {
            due: true,
            deadline: Some(epoch_deadline),
        },
        "cumulative optional credit remains usable while the over-credit floor token is closed",
    );
}

#[test]
fn server_ack_gap_capacity_wait_is_limited_to_unready_multipath_gaps() {
    assert!(server_ack_gap_capacity_wait_arm_active(true, true));
    assert!(!server_ack_gap_capacity_wait_arm_active(false, true));
    assert!(!server_ack_gap_capacity_wait_arm_active(true, false));

    assert!(server_ack_gap_missing_target_wait_active(true, true, false));
    assert!(!server_ack_gap_missing_target_wait_active(
        false, true, false,
    ));
    assert!(!server_ack_gap_missing_target_wait_active(
        true, false, false,
    ));
    assert!(!server_ack_gap_missing_target_wait_active(true, true, true));
}

#[tokio::test]
async fn prearmed_server_ack_gap_capacity_wait_retains_release_before_select_poll() {
    let capacity = Arc::new(tokio::sync::Notify::new());
    let wait = arm_carrier_capacity_notifies(vec![capacity.clone()])
        .expect("one server reinjection-capacity notification");

    // This is the exact negative-selection race: the actor has already armed
    // the carrier notification, target selection reports no current target,
    // and the writer releases capacity before `select!` polls the waiter.
    assert!(server_ack_gap_missing_target_wait_active(true, true, false));
    capacity.notify_waiters();

    tokio::time::timeout(Duration::from_millis(50), wait)
        .await
        .expect("a pre-armed server ACK-gap capacity release must not be lost");
}

#[tokio::test]
async fn prearmed_server_response_capacity_wait_retains_release_before_credit_check() {
    let capacity = Arc::new(tokio::sync::Notify::new());
    let wait = arm_response_sender_capacity_wait(vec![capacity.clone()])
        .expect("one server response carrier-capacity notification");

    // This is the exact response-writer race: the actor has armed the edge,
    // synchronous credit revalidation observes a full handoff, and the writer
    // releases it before `select!` polls the retained waiter.
    capacity.notify_waiters();

    tokio::time::timeout(Duration::from_millis(50), wait)
        .await
        .expect("a pre-armed response-writer release must not fall back to the retry timer");
}

#[tokio::test]
async fn stream_owned_requalification_ack_capacity_release_wakes_an_idle_response_actor() {
    let limits = MuxLimits::default();
    let session_id = SessionId(714);
    let stream_id = StreamId(714);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let binding = ResponseStreamBinding::new_with_limits(
        session_id,
        key.underlay,
        key.path_id,
        commands.clone(),
        TrafficClass::Throughput,
        limits,
    );
    let path_instance_id = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .first()
        .expect("initial response attachment")
        .observation
        .path_instance_id;
    commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 714 }, TrafficClass::Control)
        .expect("fill the only return control queue");

    let (frames_tx, frames_rx) = mpsc::channel(4);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: key.underlay,
        max_frame_payload_bytes: limits.max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx.into(),
    };
    let outbound_id = crate::product::OutboundId::parse("test-direct").expect("outbound ID");
    let outbound_registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: outbound_id.clone(),
            config: OutboundConfig::Direct,
            connect_timeout: Duration::from_secs(1),
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("outbound registry");
    let router = ClientIngressRouter::new(
        &ProductPolicyConfig {
            generation: 1,
            routes: vec![RouteRuleSpec::new(
                RuleId::parse("default").expect("route ID"),
                RouteMatchSpec::default(),
                RouteAction::allow_restricted(
                    EgressAction::Outbound(outbound_id),
                    None,
                    InitialDemand::Automatic,
                ),
            )],
        },
        outbound_registry,
    )
    .expect("router");
    let context = Arc::new(ServerReliableRelayContext {
        router,
        inbound: InboundId::parse("test-inbound").expect("inbound ID"),
        performance: MppPerformanceConfig::default(),
        mux_limits: limits,
        max_paths_per_session: crate::performance::ResourceLimits::default().max_paths,
        session_retention_timeout: Duration::from_secs(60),
        flow_idle_timeout: None,
        telemetry: RuntimeTelemetry::new(1),
    });
    let (application, relay_side) = tokio::io::duplex(4096);
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_reliable_stream(
            relay_side,
            path_stream,
            &relay_context,
            session_id,
            crate::runtime::stream::SessionSendBuffer::from_limits(limits),
        )
        .await
    });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }

    let probe = crate::model::requalification::StreamRequalificationProbe {
        id: 1,
        offset: 4096,
        payload_bytes: 512,
    };
    binding
        .accept_request_requalification_probe(key, path_instance_id, stream_id, probe)
        .expect("the idle response actor owns the blocked exact ACK");
    assert!(binding.has_pending_request_requalification_ack());

    let filler = recv_reliable_path_command(&mut receivers)
        .await
        .expect("queued filler command");
    let filler_bytes = reliable_path_command_pending_bytes(&filler);
    assert!(matches!(
        filler,
        ReliablePathCommand::SendFrame(Frame::Ping { nonce: 714 })
    ));
    receivers.release_pending_command_bytes(filler_bytes);

    tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            let command = recv_reliable_path_command(&mut receivers)
                .await
                .expect("live return command queue");
            let pending_bytes = reliable_path_command_pending_bytes(&command);
            let exact_ack = matches!(
                command,
                ReliablePathCommand::SendFrame(Frame::StreamRequalifyAck {
                    stream_id: ack_stream_id,
                    probe_id: 1,
                    offset: 4096,
                    payload_bytes: 512,
                }) if ack_stream_id == stream_id
            );
            receivers.release_pending_command_bytes(pending_bytes);
            if exact_ack {
                break;
            }
        }
    })
    .await
    .expect("capacity release wakes the idle actor's stream-owned ACK retry");
    assert!(!binding.has_pending_request_requalification_ack());

    relay.abort();
    let _ = relay.await;
    drop(application);
    drop(frames_tx);
}

#[tokio::test]
async fn exact_requalification_capacity_release_wakes_an_open_idle_source() {
    let limits = MuxLimits::default();
    let session_id = SessionId(713);
    let stream_id = StreamId(713);
    let tcp = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let quic = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        session_id,
        tcp.underlay,
        tcp.path_id,
        tcp_commands,
        TrafficClass::Throughput,
        limits,
    );
    let (quic_commands, mut quic_receivers) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            quic.underlay,
            quic.path_id,
            quic_commands.clone(),
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let quic_identity = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == quic)
        .map(|target| ServerReinjectionOutputIdentity {
            key: quic,
            incarnation: target.observation.incarnation,
        })
        .expect("attached QUIC response target");
    assert!(binding.mark_output_stale(quic_identity, TrafficClass::Throughput,));
    quic_commands
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id: StreamId(999),
                offset: 0,
                payload: Bytes::from_static(b"fill-stale-quic"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill the exact stale target queue");

    let (_frames_tx, frames_rx) = mpsc::channel(4);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: limits.max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx.into(),
    };
    let outbound_id = crate::product::OutboundId::parse("test-direct").expect("outbound ID");
    let outbound_registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: outbound_id.clone(),
            config: OutboundConfig::Direct,
            connect_timeout: Duration::from_secs(1),
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("outbound registry");
    let router = ClientIngressRouter::new(
        &ProductPolicyConfig {
            generation: 1,
            routes: vec![RouteRuleSpec::new(
                RuleId::parse("default").expect("route ID"),
                RouteMatchSpec::default(),
                RouteAction::allow_restricted(
                    EgressAction::Outbound(outbound_id),
                    None,
                    InitialDemand::Automatic,
                ),
            )],
        },
        outbound_registry,
    )
    .expect("router");
    let context = Arc::new(ServerReliableRelayContext {
        router,
        inbound: InboundId::parse("test-inbound").expect("inbound ID"),
        performance: MppPerformanceConfig::default(),
        mux_limits: limits,
        max_paths_per_session: crate::performance::ResourceLimits::default().max_paths,
        session_retention_timeout: Duration::from_secs(60),
        flow_idle_timeout: None,
        telemetry: RuntimeTelemetry::new(1),
    });
    let (mut application, relay_side) = tokio::io::duplex(4096);
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_reliable_stream(
            relay_side,
            path_stream,
            &relay_context,
            session_id,
            crate::runtime::stream::SessionSendBuffer::from_limits(limits),
        )
        .await
    });

    application
        .write_all(b"retained response source")
        .await
        .expect("seed one retained response range");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let command = recv_reliable_path_command(&mut tcp_receivers)
                .await
                .expect("healthy TCP command queue");
            let pending_bytes = reliable_path_command_pending_bytes(&command);
            let original = matches!(
                command,
                ReliablePathCommand::SendFrame(Frame::StreamData { ref payload, .. })
                    if payload == &Bytes::from_static(b"retained response source")
            );
            tcp_receivers.release_pending_command_bytes(pending_bytes);
            if original {
                break;
            }
        }
    })
    .await
    .expect("healthy TCP must accept the retained source");

    // Let the actor observe the full stale queue and enter its idle read wait.
    // The application endpoint deliberately stays open but sends no more data.
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    loop {
        let command = recv_reliable_path_command(&mut quic_receivers)
            .await
            .expect("stale QUIC command queue");
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let filler = matches!(
            command,
            ReliablePathCommand::SendFrame(Frame::StreamData {
                stream_id: StreamId(999),
                ..
            })
        );
        quic_receivers.release_pending_command_bytes(pending_bytes);
        if filler {
            break;
        }
    }

    let probe = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            let command = recv_reliable_path_command(&mut quic_receivers)
                .await
                .expect("stale QUIC command queue remains live");
            let pending_bytes = reliable_path_command_pending_bytes(&command);
            let probe = matches!(
                command,
                ReliablePathCommand::SendFrame(Frame::StreamRequalifyData { .. })
            );
            quic_receivers.release_pending_command_bytes(pending_bytes);
            if probe {
                break;
            }
        }
    })
    .await;

    relay.abort();
    let _ = relay.await;
    drop(application);
    assert!(
        probe.is_ok(),
        "the exact stale-target capacity release must wake requalification while the open local source is idle",
    );
}

#[test]
fn server_completion_waits_for_every_live_ack_publication() {
    let mut publication = ServerAckPublicationState::default();
    publication.record_status(1, true, true);
    assert!(
        !publication.current_generation_is_fully_published(),
        "one accepted copy cannot retire retained state for a blocked attachment"
    );

    publication.record_status(1, true, false);
    assert!(publication.current_generation_is_fully_published());
}

async fn assert_post_resolution_denial_is_logical_stream_local(
    denial: RouteAction,
    silently_dropped: bool,
) {
    let limits = MuxLimits::default();
    let outbound_id = crate::product::OutboundId::parse("post-dns-direct").expect("outbound ID");
    let dns = crate::dns::DnsGeneration::from_test_answers(HashMap::from([(
        "denied.example".to_string(),
        vec!["8.8.8.8".parse().expect("post-resolution address")],
    )]));
    let outbound_registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: outbound_id.clone(),
            config: OutboundConfig::Direct,
            connect_timeout: Duration::from_secs(1),
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        dns,
    )
    .expect("post-resolution registry");
    let router = ClientIngressRouter::new(
        &ProductPolicyConfig {
            generation: 1,
            routes: vec![
                RouteRuleSpec::new(
                    RuleId::parse("resolved-denial").expect("route ID"),
                    RouteMatchSpec {
                        destination_cidrs: vec!["8.8.8.8/32".parse().expect("route CIDR")],
                        stages: vec![RouteStage::PostResolution],
                        ..RouteMatchSpec::default()
                    },
                    denial,
                ),
                RouteRuleSpec::new(
                    RuleId::parse("provisional-direct").expect("route ID"),
                    RouteMatchSpec::default(),
                    RouteAction::allow(
                        EgressAction::Outbound(outbound_id),
                        None,
                        InitialDemand::Automatic,
                    ),
                ),
            ],
        },
        outbound_registry,
    )
    .expect("post-resolution router");
    let context = Arc::new(ServerReliableRelayContext {
        router,
        inbound: InboundId::parse("test-inbound").expect("inbound ID"),
        performance: MppPerformanceConfig::default(),
        mux_limits: limits,
        max_paths_per_session: crate::performance::ResourceLimits::default().max_paths,
        session_retention_timeout: Duration::from_secs(1),
        flow_idle_timeout: None,
        telemetry: RuntimeTelemetry::new(4),
    });
    let (registry, mut accepted_rx) = ServerReliableStreamRegistry::new_accepting_with_limits(
        limits,
        crate::performance::ResourceLimits::default().max_paths,
    );
    let port = registry.path_port();
    let session_id = SessionId(if silently_dropped { 722 } else { 721 });
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let (commands, mut command_rx) = reliable_path_command_channels(8);
    let denied_stream_id = StreamId(1);
    let sibling_stream_id = StreamId(2);
    for (stream_id, target) in [
        (
            denied_stream_id,
            TargetAddr::Domain {
                host: "denied.example".to_string(),
                port: 443,
            },
        ),
        (
            sibling_stream_id,
            TargetAddr::Ip("1.1.1.1:443".parse().expect("sibling target")),
        ),
    ] {
        assert!(matches!(
            port.open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
                initial_demand: StreamDemandHint::Latency,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: registration.clone(),
                    commands: commands.clone(),
                    max_frame_payload_bytes: limits.max_payload_bytes,
                },
                mux_limits: limits,
            })
            .await
            .expect("open logical stream"),
            ServerStreamOpenOutcome::New(_)
        ));
    }
    for expected_stream_id in [denied_stream_id, sibling_stream_id] {
        let admission = recv_reliable_path_command(&mut command_rx)
            .await
            .expect("zero-credit carrier admission");
        let pending_bytes = reliable_path_command_pending_bytes(&admission);
        assert!(matches!(
            admission,
            ReliablePathCommand::SendFrame(Frame::StreamMaxData {
                stream_id,
                max_offset: 0,
            }) if stream_id == expected_stream_id
        ));
        command_rx.release_pending_command_bytes(pending_bytes);
    }
    let denied = accepted_rx.recv().await.expect("denied accepted stream");
    let sibling = accepted_rx.recv().await.expect("sibling accepted stream");
    assert_eq!(registry.management_snapshot().active_streams, 2);

    relay_accepted_stream(context, denied)
        .await
        .expect("apply post-resolution denial");
    let first = tokio::time::timeout(
        Duration::from_secs(1),
        recv_reliable_path_command(&mut command_rx),
    )
    .await
    .expect("logical denial command timeout")
    .expect("logical denial command");
    let terminal = if silently_dropped {
        first
    } else {
        let pending_bytes = reliable_path_command_pending_bytes(&first);
        assert!(matches!(
            first,
            ReliablePathCommand::SendFrame(Frame::PathProofData {
                path_id: proof_path_id,
                payload,
                ..
            }) if proof_path_id == PathId(0) && !payload.is_empty()
        ));
        command_rx.release_pending_command_bytes(pending_bytes);
        recv_reliable_path_command(&mut command_rx)
            .await
            .expect("logical rejection terminal command")
    };
    if silently_dropped {
        assert!(
            matches!(
                terminal,
                ReliablePathCommand::CloseStream(stream_id) if stream_id == denied_stream_id
            ),
            "drop must detach locally without a refusal frame"
        );
    } else {
        assert!(matches!(
            terminal,
            ReliablePathCommand::ResetAndCloseStream {
                stream_id,
                reason: ResetReason::Refused,
            } if stream_id == denied_stream_id
        ));
    }
    assert_eq!(
        registry.management_snapshot().active_streams,
        1,
        "post-resolution denial must retire only its logical stream",
    );
    assert_eq!(sibling.stream().stream_id, sibling_stream_id);
}

#[tokio::test]
async fn post_resolution_reject_and_drop_preserve_sibling_logical_streams() {
    assert_post_resolution_denial_is_logical_stream_local(RouteAction::reject(), false).await;
    assert_post_resolution_denial_is_logical_stream_local(RouteAction::drop(), true).await;
}

#[tokio::test]
async fn server_relay_expires_only_after_its_absolute_no_output_interval() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(611);
    let (commands, command_receivers) = reliable_path_command_channels(4);
    drop(command_receivers);
    let binding = ResponseStreamBinding::new(
        SessionId(611),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        TrafficClass::Latency,
    );
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: limits.max_stream_window_bytes,
        lane: TrafficClass::Latency,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx.into(),
    };
    let id = crate::product::OutboundId::parse("test-direct").expect("outbound");
    let outbound_registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: id.clone(),
            config: OutboundConfig::Direct,
            connect_timeout: Duration::from_secs(1),
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("registry");
    let router = ClientIngressRouter::new(
        &ProductPolicyConfig {
            generation: 1,
            routes: vec![RouteRuleSpec::new(
                RuleId::parse("default").expect("route ID"),
                RouteMatchSpec::default(),
                RouteAction::allow_restricted(
                    EgressAction::Outbound(id),
                    None,
                    InitialDemand::Automatic,
                ),
            )],
        },
        outbound_registry,
    )
    .expect("router");
    let context = ServerReliableRelayContext {
        router,
        inbound: InboundId::parse("test-inbound").expect("inbound ID"),
        performance: MppPerformanceConfig::default(),
        mux_limits: limits,
        max_paths_per_session: crate::performance::ResourceLimits::default().max_paths,
        session_retention_timeout: Duration::from_millis(100),
        flow_idle_timeout: None,
        telemetry: RuntimeTelemetry::new(1),
    };
    let (application, relay_side) = tokio::io::duplex(4096);
    let send_buffer = crate::runtime::stream::SessionSendBuffer::from_limits(limits);
    let mut relay = Box::pin(relay_reliable_stream(
        relay_side,
        path_stream,
        &context,
        SessionId(611),
        send_buffer,
    ));

    assert!(
        tokio::time::timeout(Duration::from_millis(30), relay.as_mut())
            .await
            .is_err(),
        "server relay expired before its configured retention interval"
    );
    let result = tokio::time::timeout(Duration::from_secs(1), relay.as_mut())
        .await
        .expect("server retention expiry");
    assert!(matches!(result, Err(RuntimeError::SessionRetentionTimeout)));

    drop(application);
    drop(frames_tx);
}

#[tokio::test]
async fn server_relay_applies_path_detach_after_request_half_close_without_response_flight() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(612);
    let session_id = SessionId(612);
    let path_id = PathId(0);
    let id = crate::product::OutboundId::parse("test-direct").expect("outbound");
    let outbound_registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: id.clone(),
            config: OutboundConfig::Direct,
            connect_timeout: Duration::from_secs(1),
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("registry");
    let router = ClientIngressRouter::new(
        &ProductPolicyConfig {
            generation: 1,
            routes: vec![RouteRuleSpec::new(
                RuleId::parse("default").expect("route ID"),
                RouteMatchSpec::default(),
                RouteAction::allow_restricted(
                    EgressAction::Outbound(id),
                    None,
                    InitialDemand::Automatic,
                ),
            )],
        },
        outbound_registry,
    )
    .expect("router");
    let context = Arc::new(ServerReliableRelayContext {
        router,
        inbound: InboundId::parse("test-inbound").expect("inbound ID"),
        performance: MppPerformanceConfig::default(),
        mux_limits: limits,
        max_paths_per_session: crate::performance::ResourceLimits::default().max_paths,
        // This test must observe lifecycle progress without relying on Product
        // expiry to close the actor's ordered event receiver.
        session_retention_timeout: Duration::from_secs(60),
        flow_idle_timeout: None,
        telemetry: RuntimeTelemetry::new(1),
    });
    let (registry, mut accepted_rx) = ServerReliableStreamRegistry::new_accepting_with_limits(
        limits,
        crate::performance::ResourceLimits::default().max_paths,
    );
    let port = registry.path_port();
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let path_instance_id = registration.path_instance_id();
    let (commands, mut command_rx) = reliable_path_command_channels(8);
    assert!(matches!(
        port.open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip("127.0.0.1:443".parse().expect("target")),
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: limits.max_payload_bytes,
            },
            mux_limits: limits,
        })
        .await
        .expect("open logical stream"),
        ServerStreamOpenOutcome::New(_)
    ));
    let admission = recv_reliable_path_command(&mut command_rx)
        .await
        .expect("zero-credit carrier admission");
    let admission_bytes = reliable_path_command_pending_bytes(&admission);
    assert!(matches!(
        admission,
        ReliablePathCommand::SendFrame(Frame::StreamMaxData {
            stream_id: admitted_stream,
            max_offset: 0,
        }) if admitted_stream == stream_id
    ));
    command_rx.release_pending_command_bytes(admission_bytes);

    let mut accepted = accepted_rx.recv().await.expect("accepted stream");
    let stream_retirement = accepted.supervise();
    let session_send_buffer = accepted.session_send_buffer();
    let path_stream = accepted.take_stream();
    let binding = match &path_stream.output {
        ReliablePathStreamOutput::Switchable(binding) => binding.clone(),
        ReliablePathStreamOutput::Fixed(_) => panic!("server relay output must be switchable"),
    };
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id,
    };
    let output_incarnation = binding
        .sender_path_targets(TrafficClass::Latency, 1)
        .into_iter()
        .find(|target| target.observation.path_instance_id == path_instance_id)
        .expect("opening output")
        .observation
        .incarnation;
    drop(accepted);

    let (mut application, relay_side) = tokio::io::duplex(4096);
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_reliable_stream(
            relay_side,
            path_stream,
            &relay_context,
            session_id,
            session_send_buffer,
        )
        .await
    });
    port.route_frame(
        &registration,
        stream_id,
        Frame::StreamFin {
            stream_id,
            final_offset: 0,
        },
    )
    .await
    .expect("route request FIN");
    let mut byte = [0_u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), application.read(&mut byte))
            .await
            .expect("request half-close propagation timeout")
            .expect("read request half-close"),
        0,
        "request FIN must half-close the target before carrier retirement",
    );
    let carrier_retirement = registration.begin_retirement();
    tokio::time::timeout(Duration::from_secs(1), carrier_retirement.wait())
        .await
        .expect("carrier retirement must not wait for Product expiry or target closure");
    assert!(
        !binding.has_output_incarnation(key, output_incarnation),
        "ordered PathDetached must be applied before carrier retirement completes",
    );
    assert!(
        !relay.is_finished(),
        "path detach must preserve the response-only Product stream",
    );

    relay.abort();
    let _ = relay.await;
    stream_retirement.retire().await;
    drop(application);
}

fn record_server_delivery_evidence(binding: &ResponseStreamBinding, key: CarrierPathKey) {
    record_server_delivery_evidence_with_srtt(binding, key, 40_000);
}

fn record_server_delivery_evidence_with_srtt(
    binding: &ResponseStreamBinding,
    key: CarrierPathKey,
    srtt_us: u32,
) {
    binding.update_path_metrics(
        key,
        PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            rate_valid_for_us: 10_000_000,
            rate_observed: true,
            srtt_us,
            rttvar_us: 5_000,
            jitter_us: 5_000,
            delivery_rate_bps: 100_000_000,
            pacing_rate_bps: 100_000_000,
            pacing_rate_observed: true,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight_observed: false,
            queue_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 1,
            data_sample_bytes: 65_536,
        },
        ServerPathMetricsSource::LocalSender,
    );
}

#[test]
fn ambiguous_prefix_ack_cannot_withdraw_a_fresh_response_tail_beyond_the_horizon() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(613);
    let quic = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let tcp = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (quic_commands, _quic_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(613),
        quic.underlay,
        quic.path_id,
        quic_commands,
        TrafficClass::Throughput,
    );
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            tcp.underlay,
            tcp.path_id,
            tcp_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.mark_output_path_proven_for_test(quic);
    binding.mark_output_path_proven_for_test(tcp);
    record_server_delivery_evidence_with_srtt(&binding, quic, 1_000);
    record_server_delivery_evidence_with_srtt(&binding, tcp, 1_000);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: quic.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let quic_identity = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == quic)
        .map(|target| ServerReinjectionOutputIdentity {
            key: quic,
            incarnation: target.observation.incarnation,
        })
        .expect("attached QUIC response output");
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let prefix = send_stream
        .send_data(Bytes::from_static(b"acked-prefix"))
        .expect("send ACKed prefix");
    let horizon = reliable_stream_frame_extent(&prefix)
        .expect("prefix extent")
        .1;
    let tail = send_stream
        .send_data(Bytes::from_static(b"silent-tail"))
        .expect("send silent tail");
    let tail_end = reliable_stream_frame_extent(&tail).expect("tail extent").1;
    binding.record_original_flight(quic, &prefix);
    binding.record_reinjected_flight(tcp, &prefix);
    binding.record_original_flight(quic, &tail);
    let ack_ranges = [OffsetRange::new(0, horizon).expect("prefix ACK range")];
    let _ = send_stream.apply_ack(&ack_ranges);
    let ack_release = binding.release_normalized_acked_ranges(&ack_ranges);
    assert!(
        ack_release.path_progress_outputs.is_empty(),
        "delivery of an overlapping original and reinjection has no exact owner attribution",
    );
    let mut staleness = ReliableResponsePathStaleness::default();
    let candidates = path_stream.data_ack_recovery_candidates(horizon, TrafficClass::Throughput);
    assert!(
        candidates.is_empty(),
        "the fresh tail begins at the complete ACK horizon and is not an authoritative omission",
    );
    assert!(!mark_response_path_staleness(
        &mut staleness,
        &path_stream,
        &candidates,
        ack_release.path_progress_outputs.as_slice(),
        TrafficClass::Throughput,
    ));
    assert_eq!(staleness.next_deadline(), None);
    assert!(!binding.output_is_stale(quic_identity));
    let later_horizon_candidates =
        path_stream.data_ack_recovery_candidates(tail_end, TrafficClass::Throughput);
    assert_eq!(later_horizon_candidates.len(), 1);
    assert_eq!(
        response_recovery_output_identity(later_horizon_candidates[0]),
        quic_identity,
        "the same retained tail becomes eligible only when a later complete horizon covers it",
    );
}

#[test]
fn reliable_recv_progress_sends_exact_tcp_sparse_deltas_without_delaying_feedback() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_path_flight_bytes: 64 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 180.0, 500_000_000.0);
    let udp = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 500_000_000.0);

    for (path, request_sparse, compact) in
        [(tcp, true, true), (tcp, false, false), (udp, true, false)]
    {
        let mut recv_stream = ReliableRecvStream::new(StreamId(24), mux_limits);
        let mut progress = ReliableRecvProgress::default();
        let mut sparse_progress = RequestTcpSparseAckProgress::default();
        recv_stream
            .receive_data(0, Bytes::from(vec![0x11; 1024]))
            .expect("contiguous prefix");
        assert!(progress.should_send_ack(
            &recv_stream,
            Some(path),
            TrafficClass::Throughput,
            mux_limits,
            false,
        ));
        assert_eq!(sparse_progress.ack_frames(&recv_stream, false).len(), 1);

        let mut frames = Vec::new();
        for offset in [8192, 32768, 16384, 12288] {
            recv_stream
                .receive_data(offset, Bytes::from(vec![0x22; 1024]))
                .expect("sparse range");
            assert!(
                progress.should_send_ack(
                    &recv_stream,
                    Some(path),
                    TrafficClass::Throughput,
                    mux_limits,
                    false,
                ),
                "range-shape feedback cadence must not be weakened"
            );
            frames = sparse_progress.ack_frames(
                &recv_stream,
                request_sparse && path.underlay == UnderlayProtocol::Tcp,
            );
        }
        assert_eq!(frames.len(), 1);
        let Frame::StreamAck {
            complete, ranges, ..
        } = &frames[0]
        else {
            panic!("receive progress must emit STREAM_ACK");
        };
        assert_eq!(*complete, !compact);
        assert_eq!(ranges.len(), if compact { 1 } else { 5 });
        assert_eq!(
            ranges.first().map(|range| range.start),
            Some(if compact { 12288 } else { 0 })
        );
        assert_eq!(
            ranges.last().map(|range| range.start),
            Some(if compact { 12288 } else { 32768 })
        );
        if compact {
            assert_eq!(ranges[0], OffsetRange::new(12288, 13312).unwrap());
        }
    }
}

#[test]
fn tail_stall_reinjection_retransmits_same_frontier_only_after_stall_evidence() {
    let stream_id = StreamId(34);
    let (commands, _receivers) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new(
        SessionId(34),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        TrafficClass::Throughput,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let frame = Frame::StreamData {
        stream_id,
        offset: 128,
        payload: Bytes::from_static(b"frontier"),
    };
    binding.record_original_flight(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        &frame,
    );
    let later_frame = Frame::StreamData {
        stream_id,
        offset: 136,
        payload: Bytes::from_static(b"later"),
    };

    let (without_stall, blocked_offset) = prefix_reinjection_frames_with_available_output(
        &path_stream,
        vec![frame.clone(), later_frame.clone()],
        false,
    );
    assert!(without_stall.is_empty());
    assert_eq!(blocked_offset, Some(128));

    let (with_stall, blocked_offset) = prefix_reinjection_frames_with_available_output(
        &path_stream,
        vec![frame, later_frame],
        true,
    );
    assert_eq!(blocked_offset, None);
    assert_eq!(with_stall.len(), 1);
    assert!(matches!(
        &with_stall[0],
        Frame::StreamData {
            offset: 128,
            payload,
            ..
        } if payload.as_ref() == b"frontier"
    ));
}

#[test]
fn response_sender_wait_state_blocks_immediately_without_carrier_credit() {
    let now = tokio::time::Instant::now();
    let retry_delay = Duration::from_millis(10);

    let state = response_sender_wait_state(true, true, false, None, now, retry_delay);

    assert!(state.blocked);
    assert!(!state.ready);
    assert!(state.subscribe_capacity);
    assert_eq!(state.retry_at, Some(now + retry_delay));
}

#[test]
fn response_sender_wait_state_allows_admission_when_carrier_has_credit() {
    let now = tokio::time::Instant::now();
    let retry_delay = Duration::from_millis(10);

    let state = response_sender_wait_state(true, true, true, None, now, retry_delay);

    assert!(!state.blocked);
    assert!(state.ready);
    assert!(
        !state.subscribe_capacity,
        "product-ordering pressure is handled by sender admission, not carrier pipe exhaustion"
    );
    assert_eq!(state.retry_at, None);
}

#[test]
fn response_sender_wait_state_preserves_pending_retry_with_carrier_credit() {
    let now = tokio::time::Instant::now();
    let retry_delay = Duration::from_millis(10);
    let retry_at = now + retry_delay;

    let state = response_sender_wait_state(true, true, true, Some(retry_at), now, retry_delay);

    assert!(state.blocked);
    assert!(!state.ready);
    assert!(state.subscribe_capacity);
    assert_eq!(state.retry_at, Some(retry_at));
}

#[test]
fn tail_timer_reinjection_allows_only_authoritative_or_failed_original_reinjection() {
    assert!(
        stream_tail_timer_reinjection_allowed(false, true),
        "after the original-transmission path output is gone, the remaining output is the failover path even though it is no longer a second live alternative"
    );
    assert!(!stream_tail_timer_reinjection_allowed(false, false));
    assert!(
        stream_tail_timer_reinjection_allowed(true, false),
        "authoritative ACK-frontier tail reinjection may use a live alternate"
    );
}

#[test]
fn incomplete_ack_chunks_after_a_snapshot_do_not_extend_its_negative_authority() {
    let limits = MuxLimits::default();
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(StreamId(312), limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x11; 1024]))
        .expect("send response data before snapshot");

    let mut authoritative = AuthoritativeStreamAckSnapshot::default();
    let complete_prefix = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let complete_ack =
        validate_stream_ack(true, complete_prefix.to_vec(), send_stream.next_offset())
            .expect("complete prefix stays within assigned data");
    send_stream
        .apply_validated_ack(&complete_ack)
        .expect("complete ACK fits retained send chunks");
    update_reinjection_authoritative_ack_snapshot(&mut authoritative, &complete_ack);

    for value in [0x22, 0x33] {
        send_stream
            .send_data(Bytes::from(vec![value; 1024]))
            .expect("send response data after snapshot");
    }
    let incomplete_progress = [OffsetRange {
        start: 1024,
        end: 3072,
    }];
    let incomplete_ack = validate_stream_ack(
        false,
        incomplete_progress.to_vec(),
        send_stream.next_offset(),
    )
    .expect("incomplete progress stays within assigned data");
    send_stream
        .apply_validated_ack(&incomplete_ack)
        .expect("incomplete ACK fits retained send chunks");
    update_reinjection_authoritative_ack_snapshot(&mut authoritative, &incomplete_ack);

    assert_eq!(
        authoritative.ranges(),
        &[OffsetRange {
            start: 0,
            end: 1024,
        }]
    );
    assert!(authoritative.complete());
    assert_eq!(authoritative.horizon(), Some(1024));
    assert_eq!(send_stream.data_ack_frontier(), 3072);
    assert_eq!(
        reliable_relay_current_data_ack_outstanding_bytes(
            TrafficClass::Throughput,
            &send_stream,
            send_stream.data_ack_frontier(),
        ),
        0,
        "positive incomplete ACK chunks must not leave stale tail-guard debt",
    );
}

#[test]
fn response_source_staging_uses_exact_retained_product_debt_in_every_lane() {
    let limits = MuxLimits {
        max_path_flight_bytes: 2 * 1024 * 1024,
        max_repair_bytes: 8 * 1024 * 1024,
        max_reorder_bytes: 8 * 1024 * 1024,
        max_stream_window_bytes: 8 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let aggregate_product_window = 4 * 1024 * 1024;
    let retained_product_debt = aggregate_product_window - 32 * 1024;
    let queued_original_data = 16 * 1024;

    for lane in [TrafficClass::Latency, TrafficClass::Throughput] {
        assert_eq!(
            reliable_relay_response_source_staging_headroom(
                lane,
                aggregate_product_window,
                retained_product_debt,
                queued_original_data,
            ),
            16 * 1024,
            "retained exact Product O and queued OriginalData consume the same aggregate P before assignment in {lane:?}",
        );
        assert_eq!(
            reliable_relay_response_source_staging_headroom(
                lane,
                aggregate_product_window,
                0,
                queued_original_data,
            ),
            aggregate_product_window - queued_original_data,
            "MPP DataACK release must reopen source reads in {lane:?}",
        );
    }

    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(StreamId(313), limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x44; 32 * 1024]))
        .expect("retain exact Product bytes");
    for lane in [TrafficClass::Latency, TrafficClass::Throughput] {
        assert_eq!(
            reliable_relay_current_data_ack_outstanding_bytes(lane, &send_stream, 0),
            32 * 1024,
        );
    }
    let ack = validate_stream_ack(
        true,
        vec![OffsetRange {
            start: 0,
            end: 32 * 1024,
        }],
        send_stream.next_offset(),
    )
    .expect("exact retained range ACK");
    send_stream
        .apply_validated_ack(&ack)
        .expect("DataACK releases exact retained bytes");
    for lane in [TrafficClass::Latency, TrafficClass::Throughput] {
        assert_eq!(
            reliable_relay_current_data_ack_outstanding_bytes(lane, &send_stream, 0),
            0,
        );
    }
}

#[test]
fn tail_reinjection_uses_single_pto_stall_timeout() {
    let original_sent_at = Instant::now();
    let deadline = reliable_relay_tail_reinjection_deadline(original_sent_at, None, None);
    let expected =
        tokio::time::Instant::from_std(original_sent_at + transport_pto_from_snapshot(None));

    assert_eq!(deadline, expected);
}

#[test]
fn tail_reinjection_timer_is_lane_neutral_after_stall_evidence() {
    assert!(
        reliable_relay_tail_reinjection_timer_active(64, true, false),
        "a complete stalled original-transmission path suffix must use bounded alternate-output reinjection in every reliable lane"
    );
    assert!(
        reliable_relay_tail_reinjection_timer_active(64, false, true),
        "failed-original-transmission path correctness reinjection must not depend on the product lane"
    );
    assert!(
        !reliable_relay_tail_reinjection_timer_active(64, false, false),
        "an outstanding suffix without an eligible alternate must remain with its carrier"
    );
    assert!(
        !reliable_relay_tail_reinjection_timer_active(0, true, true),
        "a fully acknowledged stream must not arm the reinjection timer"
    );
}

#[tokio::test]
async fn latency_tail_reinjection_dispatches_suffix_on_distinct_reinjection_without_fin() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(118);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, mut original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(118),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x41, 0x42, 0x43] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]))
            .expect("seed original-transmission path response data");
        binding.record_original_flight(original_key, &frame);
    }
    let ack_ranges = [OffsetRange { start: 0, end: 128 }];
    let _ = send_stream.apply_ack(&ack_ranges);
    assert_eq!(send_stream.next_offset(), 192);
    assert_eq!(send_stream.reinjection_bytes(), 64);
    assert!(reliable_relay_tail_reinjection_timer_active(
        send_stream.reinjection_bytes(),
        true,
        false,
    ));

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(118),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(128);
    let outcome = enqueue_reliable_tail_reinjection_with_ack_horizon(
        &mut response_sender,
        &path_stream,
        &[],
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        Some(128),
        None,
        TrafficClass::Latency,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        Some(Instant::now()),
        Some(Instant::now()),
        128,
    );
    assert_eq!(outcome.queued, 1);
    assert!(!outcome.pending);

    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("latency tail reinjection must dispatch on a distinct output");
    assert_eq!(dispatch.lane, ReliableWorkClass::Reinjection);
    assert_eq!(dispatch.selected_path, Some(reinjection_key));

    let reinjection_frame = match try_recv_reliable_path_command(&mut reinjection_receivers) {
        Some(ReliablePathCommand::SendFrame(frame)) => {
            assert!(matches!(
                &frame,
                Frame::StreamData {
                    offset: 128,
                    payload,
                    ..
                } if payload.len() == 64
            ));
            frame
        }
        _ => panic!("expected the nonterminal 64-byte reinjected suffix"),
    };
    assert!(try_recv_reliable_path_command(&mut original_receivers).is_none());
    let original_outputs = binding.original_flight_outputs_overlapping_frame(&reinjection_frame);
    assert_eq!(original_outputs.len(), 1);
    assert_eq!(original_outputs[0].0, original_key);
    assert!(
        binding.has_output_incarnation(original_outputs[0].0, original_outputs[0].1),
        "reinjection must preserve the exact original-output attribution",
    );
    assert!(path_stream.has_recent_reinjection_overlap(&reinjection_frame));
}

#[test]
fn sparse_authoritative_ack_reinjects_the_lowest_live_path_gap() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(119);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(119),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands.clone(),
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    record_server_delivery_evidence(&binding, reinjection_key);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x41, 0x42, 0x43, 0x44] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]))
            .expect("seed original-transmission path response data");
        binding.record_original_flight(original_key, &frame);
    }
    let ack_ranges = [
        OffsetRange { start: 0, end: 64 },
        OffsetRange {
            start: 128,
            end: 192,
        },
    ];
    let _ = send_stream.apply_ack(&ack_ranges);
    assert_eq!(stream_ack_contiguous_frontier(&ack_ranges), 64);
    assert!(!stream_ack_is_authoritative_contiguous_prefix(
        true,
        &ack_ranges,
        64,
    ));
    assert_eq!(send_stream.reinjection_bytes(), 128);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(119),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Latency,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        64,
    );

    assert_eq!(outcome.queued, 1);
    assert!(!outcome.pending);
    assert_eq!(response_sender.bytes(), 64);
}

#[tokio::test]
async fn sparse_ack_failed_original_reinjection_starts_at_lowest_hole() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Latency,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x51, 0x52, 0x53, 0x54] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]))
            .expect("seed failed-original-transmission path response data");
        binding.record_original_flight(original_key, &frame);
    }
    let ack_ranges = [
        OffsetRange { start: 0, end: 64 },
        OffsetRange {
            start: 128,
            end: 192,
        },
    ];
    let _ = send_stream.apply_ack(&ack_ranges);
    binding.release_normalized_acked_ranges(&ack_ranges);
    binding.detach(original_key, &original_commands);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(120),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Latency,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        64,
    );
    assert!(outcome.queued > 0);
    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("dispatch lowest failed-original-transmission path hole");
    assert_eq!(dispatch.selected_path, Some(reinjection_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut reinjection_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 64,
            payload,
            ..
        })) if payload.len() == 64
    ));
}

#[test]
fn tail_reinjection_repeats_after_one_recovery_interval_without_progress() {
    let original_sent_at = Instant::now();
    let last_reinjection = original_sent_at + transport_pto_from_snapshot(None);
    let deadline =
        reliable_relay_tail_reinjection_deadline(original_sent_at, Some(last_reinjection), None);
    let expected =
        tokio::time::Instant::from_std(last_reinjection + transport_pto_from_snapshot(None));

    assert_eq!(deadline, expected);
}

#[test]
fn tail_reinjection_deadline_does_not_move_with_metrics_for_the_same_gap() {
    let original_sent_at = Instant::now();
    let fast_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 20.0, 1.0);
    let slow_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 800.0, 1.0);
    let mut timer = ReliableRelayTailReinjectionTimer::default();

    let candidate = tail_recovery_candidate(0, original_sent_at);
    let armed = timer.observe(
        Some(candidate),
        original_sent_at,
        Some(fast_snapshot),
        false,
    );
    let refreshed = timer.observe(
        Some(candidate),
        original_sent_at,
        Some(slow_snapshot),
        false,
    );

    assert_eq!(refreshed, armed);
}

#[test]
fn data_ack_recovery_deadline_shortens_but_does_not_postpone_tail_timer() {
    let original_sent_at = Instant::now();
    let slow_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 800.0, 1.0);
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let candidate = tail_recovery_candidate(0, original_sent_at);

    let generic_deadline = timer.observe(
        Some(candidate),
        original_sent_at,
        Some(slow_snapshot),
        false,
    );
    let recovery_deadline = original_sent_at + Duration::from_millis(200);
    assert!(tokio::time::Instant::from_std(recovery_deadline) < generic_deadline);

    timer.arm_recovery_deadline(candidate, recovery_deadline);
    assert_eq!(
        timer.observe(
            Some(candidate),
            original_sent_at,
            Some(slow_snapshot),
            false,
        ),
        tokio::time::Instant::from_std(recovery_deadline)
    );

    timer.arm_recovery_deadline(candidate, original_sent_at + Duration::from_millis(300));
    assert_eq!(
        timer.observe(
            Some(candidate),
            original_sent_at,
            Some(slow_snapshot),
            false,
        ),
        tokio::time::Instant::from_std(recovery_deadline)
    );
}

#[test]
fn tail_reinjection_timer_clears_without_an_authoritative_candidate() {
    let sent_at = Instant::now();
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let _ = timer.observe(
        Some(tail_recovery_candidate(0, sent_at)),
        sent_at,
        None,
        false,
    );
    assert!(timer.candidate.is_some());
    assert!(timer.deadline.is_some());

    let _ = timer.observe(None, sent_at, None, false);

    assert_eq!(timer.candidate, None);
    assert_eq!(timer.deadline, None);
    assert_eq!(timer.last_attempt_at, None);
}

#[test]
fn tail_reinjection_candidate_uses_the_latest_flight_or_data_ack_progress_time() {
    let first_original_sent_at = Instant::now();
    let first_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 20.0, 1.0);
    let next_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 800.0, 1.0);
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let first_deadline = timer.observe(
        Some(tail_recovery_candidate(0, first_original_sent_at)),
        first_original_sent_at,
        Some(first_snapshot),
        false,
    );

    let next_original_sent_at = first_original_sent_at + Duration::from_secs(1);
    let next_candidate = tail_recovery_candidate(64, next_original_sent_at);
    let progress_deadline = timer.observe(
        Some(next_candidate),
        next_original_sent_at,
        Some(next_snapshot),
        false,
    );
    assert!(progress_deadline > first_deadline);

    let attempted_at = next_original_sent_at + Duration::from_secs(1);
    timer.record_attempt_at(attempted_at);
    let attempt_deadline = timer.observe(
        Some(next_candidate),
        next_original_sent_at,
        Some(first_snapshot),
        false,
    );
    assert_eq!(
        attempt_deadline,
        reliable_relay_tail_reinjection_deadline(
            next_original_sent_at,
            Some(attempted_at),
            Some(first_snapshot),
        )
    );
}

#[test]
fn new_original_flight_does_not_inherit_pre_send_data_ack_stall_time() {
    let data_ack_progress_at = Instant::now();
    let original_sent_at = data_ack_progress_at + Duration::from_millis(250);
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let deadline = timer.observe(
        Some(tail_recovery_candidate(0, original_sent_at)),
        data_ack_progress_at,
        None,
        false,
    );

    assert_eq!(
        deadline,
        tokio::time::Instant::from_std(original_sent_at + transport_pto_from_snapshot(None)),
        "recovery time begins when the blocking range exists, not when the stream last had no data",
    );
}

#[test]
fn untracked_data_ack_progress_rearms_live_path_recovery_for_an_old_original_flight() {
    let original_sent_at = Instant::now() - Duration::from_secs(2);
    let data_ack_progress_at = Instant::now();
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let deadline = timer.observe(
        Some(tail_recovery_candidate(64, original_sent_at)),
        data_ack_progress_at,
        None,
        false,
    );

    assert_eq!(
        deadline,
        tokio::time::Instant::from_std(data_ack_progress_at + transport_pto_from_snapshot(None)),
        "live-path Data ACK progress starts a new connection-level recovery interval"
    );
}

#[test]
fn proof_exact_completion_deadline_prevents_slow_progress_from_postponing_old_frontier() {
    let now = Instant::now();
    let original_sent_at = now - Duration::from_secs(2);
    let owner = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 2_000_000.0);
    let candidate = tracked_tail_recovery_candidate(64, original_sent_at, UnderlayProtocol::Tcp);

    let legacy_deadline = reliable_relay_tail_reinjection_deadline(now, None, Some(owner));
    assert!(legacy_deadline > tokio::time::Instant::from_std(now));

    let measured_alternate_completion = Duration::from_millis(55);
    let exact_deadline = reliable_data_ack_recovery_deadline(
        Some(original_sent_at),
        Some(UnderlayProtocol::Tcp),
        Some(owner),
        Some(measured_alternate_completion),
    )
    .expect("measured alternate beats native owner recovery");
    assert!(
        exact_deadline < now,
        "the newly exposed range is already overdue"
    );

    let mut exact = ReliableRelayTailReinjectionTimer::default();
    let preserved = exact.observe(Some(candidate), now, Some(owner), false);
    println!(
        "old-frontier sent_age_ms={} legacy_due_in_ms={} exact_due_age_ms={} completion_rescue_due=true",
        now.saturating_duration_since(original_sent_at).as_millis(),
        legacy_deadline
            .saturating_duration_since(tokio::time::Instant::from_std(now))
            .as_millis(),
        now.saturating_duration_since(exact_deadline).as_millis(),
    );
    assert_eq!(preserved, tokio::time::Instant::from_std(exact_deadline));
    assert!(preserved <= tokio::time::Instant::from_std(now));
}

#[test]
fn tracked_old_frontier_assignments_keep_exact_age_across_multiple_progress_events() {
    let now = Instant::now();
    let owner = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 2_000_000.0);
    let recovery = reliable_relay_tail_reinjection_delay(Some(owner));
    let assignments = [
        (0_u64, now - Duration::from_secs(3)),
        (64_u64, now - Duration::from_secs(2)),
        (128_u64, now - Duration::from_secs(1)),
    ];

    for (index, (start, sent_at)) in assignments.into_iter().enumerate() {
        let progress_at = now + Duration::from_millis((index as u64 + 1) * 10);
        let mut timer = ReliableRelayTailReinjectionTimer::default();
        let deadline = timer.observe(
            Some(tracked_tail_recovery_candidate(
                start,
                sent_at,
                UnderlayProtocol::Tcp,
            )),
            progress_at,
            Some(owner),
            false,
        );
        assert_eq!(
            deadline,
            tokio::time::Instant::from_std(sent_at + recovery),
            "frontier movement must not rewrite the next tracked flight's assignment epoch",
        );
        assert!(deadline <= tokio::time::Instant::from_std(progress_at));
    }
}

#[test]
fn tracked_fresh_frontier_and_untracked_fallback_do_not_fire_early() {
    let now = Instant::now();
    let owner = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 2_000_000.0);
    let recovery = reliable_relay_tail_reinjection_delay(Some(owner));

    let mut tracked = ReliableRelayTailReinjectionTimer::default();
    let tracked_deadline = tracked.observe(
        Some(tracked_tail_recovery_candidate(
            0,
            now,
            UnderlayProtocol::Tcp,
        )),
        now + Duration::from_millis(10),
        Some(owner),
        false,
    );
    assert_eq!(
        tracked_deadline,
        tokio::time::Instant::from_std(now + recovery),
    );
    assert!(tracked_deadline > tokio::time::Instant::from_std(now));

    let progress_at = now + Duration::from_millis(50);
    let mut untracked = ReliableRelayTailReinjectionTimer::default();
    let untracked_deadline = untracked.observe(
        Some(ReliableRelayTailRecoveryCandidate::Untracked {
            start: 0,
            end: 64,
            sent_at: now,
        }),
        progress_at,
        Some(owner),
        false,
    );
    assert_eq!(
        untracked_deadline,
        tokio::time::Instant::from_std(progress_at + recovery),
        "unknown assignment ownership retains the conservative progress bound",
    );

    let mut failed_owner = ReliableRelayTailReinjectionTimer::default();
    let failed_deadline = failed_owner.observe(
        Some(tracked_tail_recovery_candidate(
            0,
            now - Duration::from_secs(1),
            UnderlayProtocol::Tcp,
        )),
        progress_at,
        Some(owner),
        true,
    );
    assert_eq!(
        failed_deadline,
        tokio::time::Instant::from_std(progress_at),
        "confirmed owner failure retains the existing immediate failover clock",
    );
}

#[test]
fn tracked_frontier_repair_is_paced_across_candidate_changes_without_duplicate_burst() {
    let now = Instant::now();
    let owner = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 2_000_000.0);
    let recovery = reliable_relay_tail_reinjection_delay(Some(owner));
    let mut timer = ReliableRelayTailReinjectionTimer::default();

    let first =
        tracked_tail_recovery_candidate(0, now - Duration::from_secs(2), UnderlayProtocol::Tcp);
    assert!(
        timer.observe(Some(first), now, Some(owner), false) <= tokio::time::Instant::from_std(now)
    );
    timer.record_attempt_at(now);

    let next =
        tracked_tail_recovery_candidate(64, now - Duration::from_secs(1), UnderlayProtocol::Tcp);
    let next_progress_at = now + Duration::from_millis(10);
    let next_deadline = timer.observe(Some(next), next_progress_at, Some(owner), false);
    assert_eq!(
        next_deadline,
        tokio::time::Instant::from_std(next_progress_at + recovery),
        "after one repair, frontier progress starts a full quiet interval before the next repair",
    );
    let same_deadline = timer.observe(Some(next), next_progress_at, Some(owner), false);
    assert_eq!(same_deadline, next_deadline);
}

#[test]
fn actor_flight_ledger_preserves_first_age_then_paces_next_frontier_from_ack_progress() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(123);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(123),
        original_key.underlay,
        original_key.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x51, 0x52] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]))
            .expect("seed tracked OriginalData flight");
        binding.record_original_flight(original_key, &frame);
    }

    let first = path_stream
        .data_ack_recovery_candidate(0)
        .expect("first tracked flight is reachable from the actor ledger");
    assert_eq!((first.start, first.end), (0, 64));
    let recovery = reliable_relay_tail_reinjection_delay(None);
    let first_observed_at = first.sent_at + Duration::from_secs(2);
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    assert_eq!(
        timer.observe(
            Some(ReliableRelayTailRecoveryCandidate::Tracked(first)),
            first_observed_at,
            None,
            false,
        ),
        tokio::time::Instant::from_std(first.sent_at + recovery),
        "the first tracked repair inherits the immutable ledger assignment age",
    );
    timer.record_attempt_at(first_observed_at);

    let acknowledged = [OffsetRange { start: 0, end: 64 }];
    let _ = send_stream.apply_ack(&acknowledged);
    path_stream.release_normalized_acked_ranges(&acknowledged);
    let next_frontier = send_stream.data_ack_frontier();
    assert_eq!(next_frontier, 64);
    let next = path_stream
        .data_ack_recovery_candidate(next_frontier)
        .expect("ACK progress exposes the next tracked ledger flight");
    assert_eq!((next.start, next.end), (64, 128));
    let ack_progress_at = first_observed_at + Duration::from_millis(10);
    timer.arm_recovery_deadline(
        ReliableRelayTailRecoveryCandidate::Tracked(next),
        next.sent_at + recovery,
    );
    assert_eq!(
        timer.observe(
            Some(ReliableRelayTailRecoveryCandidate::Tracked(next)),
            ack_progress_at,
            None,
            false,
        ),
        tokio::time::Instant::from_std(ack_progress_at + recovery),
        "after a repair, the newly exposed frontier waits one quiet interval after Data ACK progress",
    );
}

#[test]
fn failed_original_retry_keeps_pacing_across_ack_progress() {
    let original_sent_at = Instant::now() - Duration::from_secs(2);
    let attempted_at = Instant::now();
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let first = tracked_tail_recovery_candidate(0, original_sent_at, UnderlayProtocol::Tcp);
    let _ = timer.observe(Some(first), original_sent_at, None, true);
    timer.record_attempt_at(attempted_at);

    let next = tracked_tail_recovery_candidate(64, original_sent_at, UnderlayProtocol::Tcp);
    timer.arm_recovery_deadline(next, original_sent_at + transport_pto_from_snapshot(None));
    let deadline = timer.observe(Some(next), attempted_at, None, true);

    assert_eq!(
        deadline,
        tokio::time::Instant::from_std(attempted_at + transport_pto_from_snapshot(None)),
        "ACK progress on a failed carrier must not trigger an unpaced retry cascade"
    );
}

#[test]
fn empty_tail_reinjection_scan_retries_after_one_recovery_interval() {
    let sent_at = Instant::now();
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let candidate = tail_recovery_candidate(0, sent_at);
    let _ = timer.observe(Some(candidate), sent_at, None, false);

    let scan_started_at = Instant::now();
    timer.record_scan();
    let retry_deadline = timer.observe(Some(candidate), sent_at, None, false);

    assert!(
        retry_deadline
            >= tokio::time::Instant::from_std(
                scan_started_at + reliable_relay_tail_reinjection_delay(None),
            ),
        "an empty scan remains time-wakeable when carrier capacity changes without a model update",
    );
}

#[test]
fn live_tail_reinjection_timer_uses_blocking_original_snapshot() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(110);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let fast_alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(110),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            fast_alternate.underlay,
            fast_alternate.path_id,
            alternate_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let slow_original_metrics = PathMetrics {
        path_id: original_key.path_id,
        underlay: original_key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        rate_valid_for_us: 10_000_000,
        rate_observed: true,
        srtt_us: 500_000,
        rttvar_us: 60_000,
        jitter_us: 60_000,
        delivery_rate_bps: 80_000_000,
        pacing_rate_bps: 80_000_000,
        pacing_rate_observed: true,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight_observed: false,
        queue_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 0,
        inflight_hi_bytes: 0,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 1,
        data_sample_bytes: 65_536,
    };
    let fast_alternate_metrics = PathMetrics {
        path_id: fast_alternate.path_id,
        underlay: fast_alternate.underlay,
        srtt_us: 25_000,
        rttvar_us: 2_000,
        jitter_us: 2_000,
        delivery_rate_bps: 200_000_000,
        pacing_rate_bps: 200_000_000,
        ..slow_original_metrics
    };
    binding.update_path_metrics(
        original_key,
        slow_original_metrics,
        ServerPathMetricsSource::LocalSender,
    );
    binding.update_path_metrics(
        fast_alternate,
        fast_alternate_metrics,
        ServerPathMetricsSource::LocalSender,
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let original_frame = Frame::StreamData {
        stream_id,
        offset: 1024,
        payload: Bytes::from(vec![0x55; 65_536]),
    };
    binding.record_original_flight(original_key, &original_frame);

    let snapshot = path_stream
        .tail_reinjection_snapshot(1024, TrafficClass::Throughput, 65_536)
        .expect("blocking original-transmission path path is still attached");

    assert_eq!(snapshot.id, original_key.path_id);
    assert_eq!(snapshot.underlay, original_key.underlay);
    assert!(
        transport_pto_from_snapshot(Some(snapshot))
            > transport_pto_from_snapshot(
                path_stream.send_path_snapshot(TrafficClass::Throughput, 65_536)
            ),
        "tail reinjection timing must follow the blocking OriginalData path, not the fastest attached alternate"
    );
}

#[test]
fn failed_original_tail_reinjection_is_immediate_after_original_path_detaches() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(111);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(111),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x51; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let original_sent_at = Instant::now();
    let generic_deadline = reliable_relay_tail_reinjection_deadline(original_sent_at, None, None);
    let failover_deadline = reliable_relay_effective_tail_reinjection_deadline(
        original_sent_at,
        None,
        None,
        reliable_failed_original_tail_reinjection_ready(
            &path_stream.failed_original_recovery_state(),
            &send_stream,
        ),
    );

    assert_eq!(
        failover_deadline,
        tokio::time::Instant::from_std(original_sent_at),
        "detached-original-transmission path tail reinjection should not wait a generic PTO before failing over"
    );
    assert!(
        failover_deadline < generic_deadline,
        "failed-original-transmission path reinjection timing must be faster than live-original-transmission path tail reinjection"
    );
}

#[test]
fn failed_original_tail_reinjection_retry_uses_single_pto_not_persistent_backoff() {
    let original_sent_at = Instant::now();
    let last_reinjection = original_sent_at + Duration::from_millis(1);
    let slow_stale_original = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 20.0, 1.0);

    let deadline = reliable_relay_effective_tail_reinjection_deadline(
        original_sent_at,
        Some(last_reinjection),
        Some(slow_stale_original),
        true,
    );
    let expected =
        tokio::time::Instant::from_std(last_reinjection + transport_pto_from_snapshot(None));

    assert_eq!(
        deadline, expected,
        "failed-original-transmission path reinjection may fire immediately once, then retries one bounded reinjection quantum per PTO; persistent backoff is for live original-transmission path congestion recovery, not detached-original-transmission path failover"
    );
}

#[test]
fn live_tail_reinjection_is_one_product_quantum_for_every_underlay() {
    let limits = MuxLimits::default();
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let path = PathSnapshot::new(PathId(3), underlay, 180.0, 500_000_000.0);
        let base =
            adaptive_reliable_relay_reinjection_bytes(Some(path), TrafficClass::Throughput, limits);
        assert_eq!(
            reliable_critical_tail_reinjection_limit_bytes(base, limits.max_repair_bytes, limits,),
            base,
            "live-tail recovery must not synthesize a transport-sized flight above native recovery",
        );
    }
}

#[test]
fn live_tail_stall_reinjection_is_not_queued_even_with_optional_budget() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(98);
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let reinjection_debt = base_limit.saturating_mul(8);
    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(98),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 1,
        },
    );
    let initial_budget = response_sender.reinjection_extra_budget_remaining(limits);
    assert!(initial_budget > 0);

    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(0), commands, limits),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x32; reinjection_debt]))
        .expect("original-transmission path data");
    let ack_frontier = base_limit as u64;

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: ack_frontier,
        }],
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 1,
        },
        path_stream.max_frame_payload_bytes,
        ack_frontier,
    );

    assert_eq!(
        outcome.queued, 0,
        "live contiguous original-transmission path-tail bytes are neither ACK-gap nor final-tail correctness reinjection"
    );
    assert!(!outcome.pending);
    assert!(
        !outcome.has_reinjection_attempt(),
        "an empty scan must wait for carrier-state change without rewriting the recovery clock"
    );
}

#[test]
fn failed_original_tail_reinjection_uses_remaining_output_after_persistent_stall() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(99);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(99),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x42; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(99),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "a detached original-transmission path path turns a persistent contiguous tail into failover reinjection on the remaining output"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_original_tail_reinjection_queues_one_bounded_target_flight() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(121);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(121),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let unresolved_payload_len = reliable_relay_buffer_len(limits)
        .saturating_add(MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
        .min(limits.max_payload_bytes);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x52; unresolved_payload_len]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(121),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(outcome.queued, 1);
    assert!(
        response_sender.bytes() > MAX_RELIABLE_SERVICE_QUANTUM_BYTES,
        "path failure recovery must not serialize a modeled target flight into 64 KiB PTO steps"
    );
    assert!(
        response_sender.bytes() <= limits.max_path_flight_bytes.min(limits.max_repair_bytes),
        "failed-path reinjection remains bounded by the configured product flight envelope"
    );
}

#[test]
fn unknown_original_tail_reinjection_uses_remaining_output_after_persistent_stall() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(119);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(119),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(original_key, &original_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(119),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "when retained original-transmission path bytes have no live original-transmission path and no path-flight record, persistent tail reinjection must still use a live survivor instead of deadlocking"
    );
    assert!(!outcome.pending);
}

#[tokio::test]
async fn unknown_original_tail_reinjection_dispatches_as_path_failure_reinjection() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, mut failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(original_key, &original_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(120),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );
    assert_eq!(outcome.queued, 1);

    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect(
            "unknown-original-transmission path tail reinjection must be failover-dispatchable",
        );

    assert_eq!(dispatch.lane, ReliableWorkClass::Reinjection);
    assert_eq!(dispatch.selected_path, Some(failover_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut failover_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[test]
fn live_original_without_data_ack_waits_for_authoritative_gap() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(121);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(121),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands.clone(),
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: reinjection_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x48; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(121),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "no ACK frontier is not an authoritative product gap; live-original-transmission path recovery must wait for ACK progress, failed-original-transmission path evidence, or terminal-tail reinjection"
    );
    assert!(!outcome.pending);
}

#[test]
fn live_original_without_data_ack_does_not_probe_prefix() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(122);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(122),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: reinjection_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let total = reliable_relay_buffer_len(limits).saturating_mul(4);
    let mut remaining = total;
    while remaining > 0 {
        let chunk = remaining.min(limits.max_payload_bytes);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x48; chunk]))
            .expect("prepare original-transmission path data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit original-transmission path data");
        binding.record_original_flight(original_key, &frame);
        remaining = remaining.saturating_sub(chunk);
    }

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(122),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "no-frontier live-original-transmission path data may still be in carrier recovery and must not become product ReinjectedData"
    );
    assert!(!outcome.pending);
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn unknown_original_tail_reinjection_without_ack_frontier_waits() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(original_key, &original_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x44; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(120),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "unknown-original-transmission path reinjection needs an ACK frontier; without one it can duplicate the entire startup tail and inflate overhead"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_original_tail_reinjection_does_not_duplicate_queued_reinjection_range() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(109);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(109),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(109),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);
    let performance = MppPerformanceConfig {
        optional_reinjection_budget_percent: 5,
    };

    let first = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        performance,
        path_stream.max_frame_payload_bytes,
        1024,
    );
    let queued_bytes_after_first = response_sender.bytes();
    let second = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        performance,
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(first.queued, 1);
    assert!(!first.pending);
    assert_eq!(
        second.queued, 0,
        "tail reinjection must not enqueue the same ReinjectedData range while it is already queued"
    );
    assert!(
        second.pending,
        "already queued ReinjectedData should count as a pending reinjection attempt so the tail timer backs off"
    );
    assert_eq!(response_sender.bytes(), queued_bytes_after_first);
}

#[test]
fn failed_original_tail_scan_skips_an_overlapped_range_but_repairs_a_later_range() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(229);
    let failed_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let surviving_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (failed_commands, _failed_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(229),
        failed_key.underlay,
        failed_key.path_id,
        failed_commands.clone(),
        TrafficClass::Throughput,
    );
    let (surviving_commands, _surviving_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            surviving_key.underlay,
            surviving_key.path_id,
            surviving_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: surviving_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let first = send_stream
        .prepare_data(Bytes::from(vec![0x41; 64]))
        .expect("prepare first failed range");
    send_stream
        .commit_prepared_data(&first)
        .expect("commit first failed range");
    let middle = send_stream
        .prepare_data(Bytes::from(vec![0x42; 64]))
        .expect("prepare surviving range");
    send_stream
        .commit_prepared_data(&middle)
        .expect("commit surviving range");
    let later = send_stream
        .prepare_data(Bytes::from(vec![0x43; 64]))
        .expect("prepare later failed range");
    send_stream
        .commit_prepared_data(&later)
        .expect("commit later failed range");
    binding.record_original_flight(failed_key, &first);
    binding.record_original_flight(surviving_key, &middle);
    binding.record_original_flight(failed_key, &later);
    binding.detach(failed_key, &failed_commands);

    let mut response_sender = ServerResponseSenderService::new(SessionId(229), stream_id);
    response_sender.enqueue_critical_reinjection_frame_with_cause(
        first,
        RelaySendCause::PathFailureReinjection,
    );
    let queued_before = response_sender.bytes();
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig::default(),
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(outcome.queued, 1);
    assert!(outcome.pending);
    assert_eq!(
        response_sender.bytes(),
        queued_before + 64,
        "an overlap in the first failed range cannot suppress disjoint later failed-owner repair",
    );
}

#[test]
fn tail_reinjection_defers_live_inflight_reinjection_to_the_accepted_copy_wake() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(127);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(127),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x49; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    let inflight_reinjection = send_stream
        .retransmission_frames_after_ack_frontier(&ack_ranges, 1024)
        .into_iter()
        .next()
        .expect("expected frontier reinjection frame");
    binding.record_reinjected_flight(reinjection_key, &inflight_reinjection);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(127),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 0,
        "live in-flight ReinjectedData for the same range must not be stacked"
    );
    assert!(
        !outcome.pending,
        "the generic tail timer must not claim ownership of an accepted-copy wait"
    );
    assert!(
        binding
            .earliest_reinjection_suppression_deadline()
            .is_some(),
        "the immutable accepted-copy wake owns reevaluation of this live repair",
    );
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn persistent_tail_reinjection_defers_a_live_copy_to_its_accepted_copy_wake() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(105);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(105),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x48; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.record_reinjected_flight(reinjection_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(105),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 0,
        "persistent tail reinjection must not stack another copy while a live ReinjectedData flight already covers the frontier range"
    );
    assert!(
        !outcome.pending,
        "the generic persistent-tail timer must not claim an accepted-copy wait"
    );
    assert!(
        binding
            .earliest_reinjection_suppression_deadline()
            .is_some(),
        "the immutable accepted-copy wake owns reevaluation of this live repair",
    );
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn stale_live_reinjection_flight_allows_terminal_tail_retry_on_a_distinct_output() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(106);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let retry_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(106),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (retry_commands, _retry_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            retry_key.underlay,
            retry_key.path_id,
            retry_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x4a; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    let inflight_reinjection = send_stream
        .retransmission_frames_after_ack_frontier(&ack_ranges, 1024)
        .into_iter()
        .next()
        .expect("expected frontier reinjection frame");
    binding.record_reinjected_flight(reinjection_key, &inflight_reinjection);
    binding.age_reinjected_flights_for_test(
        reliable_relay_tail_reinjection_delay(None).saturating_add(Duration::from_millis(1)),
    );

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(106),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "expired native suppression may recover on a distinct exact output without minting same-target K"
    );
    assert!(
        !outcome.pending,
        "stale ReinjectedData should be retried instead of keeping the tail timer backed off"
    );
    assert!(response_sender.bytes() > 0);
}

#[tokio::test]
async fn live_tail_reinjection_uses_repair_headroom_before_new_data() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(127);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(127),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(1);
    let reinjection_commands_for_fill = reinjection_commands.clone();
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.mark_output_path_proven_for_test(original_key);
    binding.mark_output_path_proven_for_test(reinjection_key);
    reinjection_commands_for_fill
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 4096,
                payload: Bytes::from_static(b"queued"),
            },
            TrafficClass::Throughput,
        )
        .expect("test setup fills alternate data queue");

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x55; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(127),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);
    let optional_budget = response_sender.reinjection_extra_event_budget_remaining(limits);
    assert!(optional_budget > 0);
    response_sender.record_reinjection_for_test(optional_budget);
    assert_eq!(
        response_sender.reinjection_extra_event_budget_remaining(limits),
        0,
        "fixture must distinguish critical live-tail authority from optional repair credit",
    );
    response_sender.enqueue_data_for_lane(
        Bytes::from_static(b"new response data"),
        TrafficClass::Throughput,
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(outcome.queued, 1);
    assert!(!outcome.pending);
    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("repair remains dispatchable despite a full fresh-data queue");
    assert_eq!(dispatch.lane, ReliableWorkClass::Reinjection);
    assert_eq!(dispatch.selected_path, Some(reinjection_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut reinjection_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 1024,
            ..
        }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut reinjection_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 4096,
            ..
        }))
    ));

    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("new data follows the dispatched repair");
    assert_eq!(dispatch.lane, ReliableWorkClass::Data);
}

#[tokio::test]
async fn persistent_tail_reinjection_waits_when_distinct_alternate_lacks_repair_headroom() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(124);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, mut original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(124),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(1);
    let reinjection_commands_for_fill = reinjection_commands.clone();
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x55; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(124),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );
    assert_eq!(outcome.queued, 1);

    reinjection_commands_for_fill
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id,
                offset: 4096,
                payload: Bytes::from_static(b"queued"),
            },
            TrafficClass::Throughput,
        )
        .expect("test setup fills alternate repair headroom");

    let dispatch = response_sender.dispatch_next_with_data_ack_outstanding(
        &path_stream,
        &mut send_stream,
        TrafficClass::Throughput,
        limits,
        0,
    );

    assert!(matches!(dispatch, Err(RuntimeError::SenderServiceBlocked)));
    assert!(
        try_recv_reliable_path_command(&mut original_receivers).is_none(),
        "live-original-transmission path tail reinjection must wait rather than retransmit on its original-transmission path"
    );
    let completed = [OffsetRange {
        start: 0,
        end: send_stream.next_offset(),
    }];
    let _ = send_stream.apply_ack(&completed);
    path_stream.release_normalized_acked_ranges(&completed);
    response_sender.release_normalized_acked_reinjections(&completed);
    assert!(
        response_sender.is_empty(),
        "original-transmission path ACK progress must remove a blocked queued live-tail reinjection before FIN or later data"
    );
}

#[tokio::test]
async fn live_owner_final_tail_does_not_become_path_failure_when_alternate_lacks_credit() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(125);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, mut original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(125),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(1);
    let reinjection_commands_for_fill = reinjection_commands.clone();
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x56; 192]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [
        OffsetRange { start: 0, end: 64 },
        OffsetRange {
            start: 80,
            end: 128,
        },
    ];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(125),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(128);
    binding.age_original_flights_for_test(Duration::from_secs(1));

    let accepted_at = Instant::now();
    let outcome = enqueue_live_response_final_tail_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &ack_ranges,
        64,
        limits,
        accepted_at,
    );
    assert_eq!(outcome.blocked_frontier_offset, None);
    assert_eq!(outcome.frontier_limit, 16);
    assert!(
        outcome.service_limit > outcome.frontier_limit,
        "available optional credit may remain after the exact lowest owner-uniform FIN frontier",
    );
    assert_eq!(
        outcome.queued, 1,
        "one bound decision cannot cross the acknowledged coverage hole into a disjoint owner interval",
    );
    assert_eq!(response_sender.bytes(), outcome.frontier_limit);
    assert!(
        !response_sender.live_owner_frontier_floor_ready(accepted_at),
        "accepted live-owner FIN-tail work must consume the gap/tail shared epoch",
    );

    reinjection_commands_for_fill
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id,
                offset: 192,
                payload: Bytes::from_static(b"queued"),
            },
            TrafficClass::Latency,
        )
        .expect("test setup fills alternate repair headroom");

    assert!(matches!(
        response_sender.dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
            limits,
            0,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        try_recv_reliable_path_command(&mut original_receivers).is_none(),
        "a live owner's FIN tail cannot acquire path-failure authority to retransmit on its original carrier",
    );
    response_sender.clear_queued_work_for_test();
    assert!(
        try_recv_reliable_path_command(&mut reinjection_receivers).is_some(),
        "fixture must release the alternate carrier's command credit",
    );
    let remaining_optional_credit =
        response_sender.reinjection_extra_event_budget_remaining(limits);
    response_sender.record_reinjection_for_test(remaining_optional_credit);
    assert_eq!(
        response_sender.reinjection_extra_event_budget_remaining(limits),
        0,
        "same-epoch non-renewal requires isolating the over-credit floor from independently valid optional service",
    );
    let same_epoch = enqueue_live_response_final_tail_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &ack_ranges,
        64,
        limits,
        Instant::now(),
    );
    assert_eq!(
        same_epoch.queued, 0,
        "queue removal and renewed carrier credit cannot mint a second FIN-tail attempt in the same epoch",
    );
}

#[tokio::test]
async fn bound_response_fin_capacity_release_wakes_and_retries_the_exact_tail() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(230);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternate_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(230),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        TrafficClass::Throughput,
    );
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            alternate_key.underlay,
            alternate_key.path_id,
            alternate_commands.clone(),
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    for key in [owner_key, alternate_key] {
        binding.mark_output_path_proven_for_test(key);
        record_server_delivery_evidence(&binding, key);
    }
    alternate_commands
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id,
                offset: 4096,
                payload: Bytes::from_static(b"full"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill the sole alternate native queue");

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let tail = send_stream
        .prepare_data(Bytes::from(vec![0x55; 4096]))
        .expect("prepare final response tail");
    send_stream
        .commit_prepared_data(&tail)
        .expect("commit final response tail");
    binding.record_original_flight(owner_key, &tail);
    binding.age_original_flights_for_test(Duration::from_secs(1));
    let mut response_sender = ServerResponseSenderService::new(SessionId(230), stream_id);

    // The actor arms this exact edge before synchronous target selection.
    let wait = arm_carrier_capacity_notifies(path_stream.response_recovery_capacity_notifies())
        .expect("bound FIN has an alternate carrier capacity edge");
    let blocked = enqueue_live_response_final_tail_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &[],
        4096,
        limits,
        Instant::now(),
    );
    assert_eq!(blocked.queued, 0);
    assert!(blocked.blocked_for_carrier_capacity);

    let filler = try_recv_reliable_path_command(&mut alternate_receivers)
        .expect("release the full alternate queue");
    alternate_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&filler));
    tokio::time::timeout(Duration::from_millis(50), wait)
        .await
        .expect("the pre-armed bound-FIN capacity wake cannot be lost");

    let retried = enqueue_live_response_final_tail_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &[],
        4096,
        limits,
        Instant::now(),
    );
    assert_eq!(retried.queued, 1);
    assert!(!retried.blocked_for_carrier_capacity);
}

#[test]
fn response_fin_keeps_its_exact_decide_target_across_metric_churn() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(226);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let selected_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let challenger_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(226),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        TrafficClass::Throughput,
    );
    let (selected_commands, mut selected_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            selected_key.underlay,
            selected_key.path_id,
            selected_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let (challenger_commands, mut challenger_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            challenger_key.underlay,
            challenger_key.path_id,
            challenger_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    for key in [owner_key, selected_key, challenger_key] {
        binding.mark_output_path_proven_for_test(key);
    }
    record_server_delivery_evidence_with_srtt(&binding, owner_key, 80_000);
    record_server_delivery_evidence_with_srtt(&binding, selected_key, 10_000);
    record_server_delivery_evidence_with_srtt(&binding, challenger_key, 100_000);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x5a; 4096]))
        .expect("prepare final response tail");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit final response tail");
    binding.record_original_flight(owner_key, &frame);
    binding.age_original_flights_for_test(Duration::from_secs(1));

    let mut zero_authority_sender = ServerResponseSenderService::new(SessionId(226), stream_id);
    let zero_authority = enqueue_live_response_final_tail_reinjection(
        &mut zero_authority_sender,
        &path_stream,
        &send_stream,
        &[],
        0,
        limits,
        Instant::now(),
    );
    assert_eq!(zero_authority.queued, 0);
    assert_eq!(zero_authority.frontier_limit, 0);
    assert_eq!(
        zero_authority_sender.completion_tail_owner_fallback_deadline(),
        None
    );
    for zero_resource_limits in [
        MuxLimits {
            max_repair_bytes: 0,
            ..limits
        },
        MuxLimits {
            max_path_flight_bytes: 0,
            ..limits
        },
    ] {
        let mut zero_resource_sender = ServerResponseSenderService::new(SessionId(226), stream_id);
        let outcome = enqueue_live_response_final_tail_reinjection(
            &mut zero_resource_sender,
            &path_stream,
            &send_stream,
            &[],
            4096,
            zero_resource_limits,
            Instant::now(),
        );
        assert_eq!(outcome.queued, 0);
        assert_eq!(outcome.frontier_limit, 0);
        assert_eq!(
            zero_resource_sender.completion_tail_owner_fallback_deadline(),
            None,
            "zero Product authority cannot manufacture M=1 or mutate its owner epoch",
        );
    }

    let mut response_sender = ServerResponseSenderService::new(SessionId(226), stream_id);
    let response_targets = binding.sender_path_targets(TrafficClass::Throughput, 4096);
    let identity_for = |key| {
        response_targets
            .iter()
            .find(|candidate| candidate.observation.key == key)
            .map(|candidate| ServerReinjectionOutputIdentity {
                key,
                incarnation: candidate.observation.incarnation,
            })
            .expect("attached response identity")
    };
    let owner_identity = identity_for(owner_key);
    let selected_identity = identity_for(selected_key);
    let owner_interval = reliable_data_retransmission_interval(
        Some(owner_key.underlay),
        path_stream.response_output_snapshot(owner_identity, path_stream.current_lane()),
    );
    let selected_snapshot = path_stream
        .response_output_snapshot(selected_identity, path_stream.current_lane())
        .expect("selected response snapshot");
    let selected_interval =
        reliable_data_retransmission_interval(Some(selected_key.underlay), Some(selected_snapshot));
    assert!(
        selected_interval < owner_interval,
        "fixture requires asymmetric owner and selected-target R",
    );
    let observed_at = Instant::now();
    let outcome = enqueue_live_response_final_tail_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &[],
        4096,
        limits,
        observed_at,
    );
    assert_eq!(outcome.queued, 1);
    let accepted_by = Instant::now();
    let successor_deadline = response_sender
        .live_owner_frontier_floor_deadline()
        .expect("accepted response FIN consumes G");
    assert!(
        successor_deadline >= observed_at + selected_interval
            && successor_deadline <= accepted_by + selected_interval
            && successor_deadline < observed_at + owner_interval,
        "response FIN successor G uses selected target R_t rather than owner R",
    );

    record_server_delivery_evidence_with_srtt(&binding, selected_key, 200_000);
    record_server_delivery_evidence_with_srtt(&binding, challenger_key, 1_000);
    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("the exact FIN target remains dispatchable");
    assert_eq!(dispatch.selected_path, Some(selected_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut selected_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ref payload,
            ..
        })) if payload.len() == 4096
    ));
    assert!(
        try_recv_reliable_path_command(&mut challenger_receivers).is_none(),
        "Apply/dispatch cannot reselect the now-better path after FIN bound its Decide target",
    );
}

#[test]
fn response_live_fin_tail_stops_at_an_already_queued_frontier_copy() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(225);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(225),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    record_server_delivery_evidence(&binding, reinjection_key);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let prefix = send_stream
        .prepare_data(Bytes::from(vec![0x51; 64]))
        .expect("prepare acknowledged prefix");
    send_stream
        .commit_prepared_data(&prefix)
        .expect("commit acknowledged prefix");
    let first_tail = send_stream
        .prepare_data(Bytes::from(vec![0x52; 64]))
        .expect("prepare lowest tail frame");
    send_stream
        .commit_prepared_data(&first_tail)
        .expect("commit lowest tail frame");
    let later_tail = send_stream
        .prepare_data(Bytes::from(vec![0x53; 64]))
        .expect("prepare later tail frame");
    send_stream
        .commit_prepared_data(&later_tail)
        .expect("commit later tail frame");
    for frame in [&prefix, &first_tail, &later_tail] {
        binding.record_original_flight(original_key, frame);
    }
    let ack_ranges = [OffsetRange { start: 0, end: 64 }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new(SessionId(225), stream_id);
    response_sender
        .enqueue_critical_reinjection_frame_with_cause(first_tail, RelaySendCause::TailReinjection);
    let queued_before = response_sender.bytes();
    binding.age_original_flights_for_test(Duration::from_secs(1));
    let outcome = enqueue_live_response_final_tail_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &ack_ranges,
        128,
        limits,
        Instant::now(),
    );
    assert_eq!(outcome.queued, 0);
    assert!(outcome.pending);
    assert_eq!(
        response_sender.bytes(),
        queued_before,
        "an occupied lowest frontier must stop the FIN batch; later tail extents cannot consume the live-owner opportunity",
    );
}

#[test]
fn detached_response_owner_uses_its_own_underlay_recovery_default() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(227);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let current_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(227),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        TrafficClass::Latency,
    );
    let (current_commands, _current_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            current_key.underlay,
            current_key.path_id,
            current_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: current_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x61; 64]))
        .expect("prepare owner frame");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner frame");
    binding.record_original_flight(owner_key, &frame);
    binding.detach(owner_key, &owner_commands);

    let unrelated_current = PathSnapshot::new(
        current_key.path_id,
        current_key.underlay,
        20.0,
        500_000_000.0,
    );
    assert_eq!(
        response_live_owner_recovery_interval_for_frame(
            &path_stream,
            &frame,
            Some(unrelated_current),
        ),
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Tcp), None),
        "a missing exact owner observation must retain that owner's transport clock instead of borrowing the current output's snapshot",
    );
}

#[tokio::test]
async fn exact_terminal_tail_reinjection_uses_only_available_path() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(126);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (original_commands, mut original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(126),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x57; 192]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange { start: 0, end: 128 }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(126),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(128);

    let (reinjection_frames, blocked_frontier_offset, same_output_frontier_retransmit) =
        prefix_final_tail_reinjection_frames_with_available_output(
            &path_stream,
            stream_final_offset_tail_reinjection_frames_normalized(
                &send_stream,
                &ack_ranges,
                64,
                true,
                true,
            ),
        );
    assert_eq!(blocked_frontier_offset, None);
    assert!(same_output_frontier_retransmit);
    assert_eq!(reinjection_frames.len(), 1);
    for frame in reinjection_frames {
        response_sender.enqueue_critical_reinjection_frame_with_cause(
            frame,
            RelaySendCause::PathFailureReinjection,
        );
    }

    assert!(
        response_sender.live_owner_frontier_floor_ready(Instant::now()),
        "exact terminal-failure repair remains independent of the live-owner epoch",
    );

    response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
            limits,
            0,
        )
        .expect("final-tail reinjection must use the only available path");

    let command = try_recv_reliable_path_command(&mut original_receivers)
        .expect("expected final-tail reinjection on the available path");
    match command {
        ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) => {
            assert_eq!(offset, 128);
            assert_eq!(payload.len(), 64);
        }
        _ => panic!("expected final-tail reinjected STREAM_DATA"),
    }
}

#[tokio::test]
async fn failed_original_reinjection_without_ack_frontier_starts_at_zero() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(103);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(103),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, mut failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x46; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(103),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 1,
        "failed-original-transmission path reinjection must retransmit from offset zero when no response ACK frontier exists"
    );
    assert!(!outcome.pending);
    response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            0,
        )
        .expect("dispatch failed-original-transmission path reinjection");
    let command =
        try_recv_reliable_path_command(&mut failover_receivers).expect("reinjection frame");
    match command {
        ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) => {
            assert_eq!(offset, 0);
            assert!(!payload.is_empty());
        }
        _ => panic!("expected failed-original-transmission path reinjection STREAM_DATA"),
    }
}

#[test]
fn live_original_tail_without_ack_frontier_does_not_reinjection_on_alternate() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(104);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternative_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(104),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (alternative_commands, _alternative_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternative_key.underlay,
            alternative_key.path_id,
            alternative_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x47; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(104),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "without a complete ACK frontier or failed original-transmission path, live original-transmission path bytes are normal in-flight data and must not be duplicated onto an alternate"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_original_tail_reinjection_is_not_blocked_by_optional_budget() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(101);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(101),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x44; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(101),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    let optional_budget = response_sender.reinjection_extra_budget_remaining(limits);
    assert!(optional_budget > 0);
    response_sender.record_reinjection_for_test(optional_budget);
    assert_eq!(
        response_sender.reinjection_extra_event_budget_remaining(limits),
        0
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert!(
        outcome.queued > 0,
        "failed-original-transmission path tail recovery is correctness reinjection and must not depend on optional duplicate/probe budget"
    );
    assert!(!outcome.pending);
}

#[test]
fn authoritative_gap_retains_one_critical_frontier_quantum_when_optional_budget_is_exhausted() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(102);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(102),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands.clone(),
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    record_server_delivery_evidence(&binding, reinjection_key);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x45; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [
        OffsetRange {
            start: 0,
            end: 1024,
        },
        OffsetRange {
            start: 2048,
            end: 4096,
        },
    ];
    let _ = send_stream.apply_ack(&ack_ranges);
    let ack_frontier = send_stream.data_ack_frontier();
    assert_eq!(ack_frontier, 1024);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(102),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    let optional_budget = response_sender.reinjection_extra_budget_remaining(limits);
    assert!(optional_budget > 0);
    response_sender.record_reinjection_for_test(optional_budget);
    assert_eq!(
        response_sender.reinjection_extra_event_budget_remaining(limits),
        0
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        ack_frontier,
    );

    assert_eq!(
        outcome.queued, 1,
        "stronger authoritative gap evidence must retain the one-interval liveness floor already granted to a contiguous live tail",
    );
    assert!(!outcome.pending);
    assert_eq!(
        response_sender.bytes(),
        1024,
        "exhausted optional credit may authorize only the exact blocking frontier quantum, not a target service window",
    );

    response_sender.clear_queued_work_for_test();
    binding.detach(reinjection_key, &reinjection_commands);
    let replacement_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            replacement_key.underlay,
            replacement_key.path_id,
            replacement_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    record_server_delivery_evidence(&binding, replacement_key);
    let switched_target = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        ack_frontier,
    );
    assert_eq!(
        switched_target.queued, 0,
        "changing the eligible target cannot mint another live-owner opportunity",
    );

    let contiguous_ranges = [OffsetRange {
        start: 0,
        end: ack_frontier,
    }];
    let changed_evidence = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &contiguous_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        ack_frontier,
    );
    assert_eq!(
        changed_evidence.queued, 0,
        "changing the same blocked frontier from gap evidence to live-tail silence cannot mint another opportunity",
    );
}

#[test]
fn persistent_response_ack_gap_commits_frontier_before_filling_service_window() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(103);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(103),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(1);
    let reinjection_commands_for_clock_churn = reinjection_commands.clone();
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    record_server_delivery_evidence_with_srtt(&binding, original_key, 80_000);
    record_server_delivery_evidence_with_srtt(&binding, reinjection_key, 100_000);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let quantum = MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
    for _ in 0..4 {
        let qualification_frame = send_stream
            .prepare_data(Bytes::from(vec![0x50; quantum]))
            .expect("prepare alternate qualification flight");
        send_stream
            .commit_prepared_data(&qualification_frame)
            .expect("commit alternate qualification flight");
        binding.record_original_flight(reinjection_key, &qualification_frame);
    }
    let qualification_ack = [OffsetRange {
        start: 0,
        end: (quantum * 4) as u64,
    }];
    let _ = send_stream.apply_ack(&qualification_ack);
    binding.release_normalized_acked_ranges(&qualification_ack);

    for _ in 0..9 {
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x51; quantum]))
            .expect("prepare sparse ACK-gap flight");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit sparse ACK-gap flight");
        binding.record_original_flight(original_key, &frame);
    }
    binding.age_original_flights_for_test(Duration::from_millis(150));
    let ack_frontier = (quantum * 5) as u64;
    let ack_ranges = [
        OffsetRange {
            start: 0,
            end: ack_frontier,
        },
        OffsetRange {
            start: (quantum * 6) as u64,
            end: (quantum * 7) as u64,
        },
        OffsetRange {
            start: (quantum * 8) as u64,
            end: (quantum * 9) as u64,
        },
        OffsetRange {
            start: (quantum * 10) as u64,
            end: (quantum * 11) as u64,
        },
        OffsetRange {
            start: (quantum * 12) as u64,
            end: (quantum * 13) as u64,
        },
    ];
    let validated_ack = begin_reliable_stream_ack(&send_stream, true, ack_ranges.to_vec())
        .expect("validate sparse ACK-gap snapshot");
    let _ = send_stream.apply_validated_ack(&validated_ack);
    let mut authoritative_ack = AuthoritativeStreamAckSnapshot::default();
    update_reinjection_authoritative_ack_snapshot(&mut authoritative_ack, &validated_ack);
    let modeled_path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let slow_send_path = PathSnapshot::new(
        original_key.path_id,
        original_key.underlay,
        100.0,
        1_500_000.0,
    );
    let scored_frontier_bytes = adaptive_reliable_relay_reinjection_bytes(
        Some(slow_send_path),
        TrafficClass::Throughput,
        limits,
    )
    .min(path_stream.max_frame_payload_bytes);
    assert!(scored_frontier_bytes < quantum);

    let clock_sender = ServerResponseSenderService::new(SessionId(103), stream_id);
    let present = clock_sender
        .ack_gap_reinjection_path_snapshot(
            &path_stream,
            &send_stream,
            &ack_ranges,
            scored_frontier_bytes,
        )
        .expect("exact-M owner timing remains observable with a target");
    assert!(present.target.is_some());
    let mut clock_progress = ReliableAckGapReinjectionProgress::default();
    let clock_observed_at = Instant::now();
    let _ = clock_progress.observe_recovery_timing(
        true,
        &ack_ranges,
        true,
        Some(present.owner_recovery_timing),
        present.target.map(|target| target.completion),
        clock_observed_at,
    );
    let retained_owner_deadline = clock_progress
        .original_owner_recovery_deadline()
        .expect("first exact-M observation installs immutable owner fallback");
    reinjection_commands_for_clock_churn
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id,
                offset: u64::MAX - 1,
                payload: Bytes::from_static(b"full"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill the only exact alternate after timing observation");
    record_server_delivery_evidence_with_srtt(&binding, original_key, 500_000);
    let absent = clock_sender
        .ack_gap_reinjection_path_snapshot(
            &path_stream,
            &send_stream,
            &ack_ranges,
            scored_frontier_bytes,
        )
        .expect("target loss cannot erase exact-M owner timing");
    assert!(absent.target.is_none());
    let _ = clock_progress.observe_recovery_timing(
        true,
        &ack_ranges,
        true,
        Some(absent.owner_recovery_timing),
        None,
        Instant::now(),
    );
    assert_eq!(
        clock_progress.original_owner_recovery_deadline(),
        Some(retained_owner_deadline),
        "target disappearance and worse mutable owner metrics cannot postpone T_f",
    );
    let filler = try_recv_reliable_path_command(&mut reinjection_receivers)
        .expect("release exact alternate after target-loss observation");
    reinjection_receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&filler));
    let reappeared = clock_sender
        .ack_gap_reinjection_path_snapshot(
            &path_stream,
            &send_stream,
            &ack_ranges,
            scored_frontier_bytes,
        )
        .expect("owner timing remains observable when target returns");
    assert!(reappeared.target.is_some());
    let _ = clock_progress.observe_recovery_timing(
        true,
        &ack_ranges,
        true,
        Some(reappeared.owner_recovery_timing),
        reappeared.target.map(|target| target.completion),
        Instant::now(),
    );
    assert_eq!(
        clock_progress.original_owner_recovery_deadline(),
        Some(retained_owner_deadline),
        "target reappearance cannot begin a later owner epoch",
    );
    record_server_delivery_evidence_with_srtt(&binding, original_key, 80_000);

    for zero_authority_limits in [
        MuxLimits::from(ResourceLimits {
            max_repair_bytes: 0,
            ..ResourceLimits::default()
        }),
        MuxLimits::from(ResourceLimits {
            max_path_flight_bytes: 0,
            ..ResourceLimits::default()
        }),
    ] {
        let mut zero_sender = ServerResponseSenderService::new(SessionId(103), stream_id);
        let mut zero_progress = ReliableAckGapReinjectionProgress::default();
        let zero = evaluate_server_data_ack_reinjection(
            &mut zero_sender,
            &path_stream,
            &send_stream,
            &mut zero_progress,
            &authoritative_ack,
            ack_frontier,
            Some(modeled_path),
            TrafficClass::Throughput,
            zero_authority_limits,
            stream_id,
        );
        assert_eq!(zero.frame_count, 0);
        assert_eq!(zero.queued, 0);
        assert_eq!(zero_progress.next_reinjection_deadline(), None);
        assert_eq!(zero_sender.live_owner_frontier_floor_deadline(), None);
    }

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(103),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 100,
        },
    );
    let mut progress = ReliableAckGapReinjectionProgress::default();
    let assignment_at = path_stream
        .data_ack_recovery_candidate(ack_frontier)
        .expect("exact original assignment")
        .sent_at;
    let stale_pre_selection_epoch = assignment_at + Duration::from_millis(100);
    assert!(Instant::now() > stale_pre_selection_epoch);
    let early = evaluate_server_data_ack_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &mut progress,
        &authoritative_ack,
        ack_frontier,
        Some(modeled_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );
    assert_eq!(
        early.queued, 0,
        "a caller epoch sampled before target selection must not authorize loss-boundary repair when the current observation can no longer finish before fallback",
    );

    binding.age_original_flights_for_test(Duration::from_secs(1));

    let mut exhausted_response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(103),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    let startup_credit = exhausted_response_sender.reinjection_extra_event_budget_remaining(limits);
    assert!(startup_credit > 0);
    exhausted_response_sender.record_reinjection_for_test(startup_credit);
    assert_eq!(
        exhausted_response_sender.reinjection_extra_event_budget_remaining(limits),
        0,
    );
    assert_eq!(exhausted_response_sender.bytes(), 0);
    let mut exhausted_progress = ReliableAckGapReinjectionProgress::default();
    let observed_at = Instant::now();
    let expired = path_stream
        .data_ack_recovery_candidate(ack_frontier)
        .expect("aged exact original assignment")
        .sent_at;
    assert!(expired < observed_at);
    assert!(
        exhausted_progress
            .observe_recovery_timing(
                true,
                authoritative_ack.ranges(),
                true,
                Some(ReliableDataAckGapTiming {
                    assignment_at: expired,
                    loss_at: Some(expired),
                    fallback_at: expired,
                }),
                Some(Duration::ZERO),
                observed_at,
            )
            .is_some_and(|deadline| deadline <= observed_at)
    );
    let exhausted = evaluate_server_data_ack_reinjection(
        &mut exhausted_response_sender,
        &path_stream,
        &send_stream,
        &mut exhausted_progress,
        &authoritative_ack,
        ack_frontier,
        Some(slow_send_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );
    assert!(
        exhausted.has_multipath_alternative,
        "fixture must retain a distinct response repair carrier",
    );
    assert!(
        exhausted.has_measured_target,
        "fixture must retain a measurable response repair target",
    );
    assert!(
        exhausted.persistent_ready,
        "pre-armed live-owner ACK-gap recovery must be due",
    );
    assert!(!exhausted.target_service_exhausted);
    assert!(exhausted.frame_count > 0);
    assert!(exhausted.queued > 0);
    assert_eq!(
        exhausted_response_sender.bytes(),
        scored_frontier_bytes,
        "exhausted optional credit may authorize only the exact blocking frontier quantum",
    );
    let response_epoch_deadline = exhausted_response_sender
        .live_owner_frontier_floor_deadline()
        .expect("accepted response repair starts one shared recovery epoch");
    exhausted_response_sender.record_delivered_data(quantum);
    assert_eq!(
        exhausted_response_sender.live_owner_frontier_floor_deadline(),
        Some(response_epoch_deadline),
        "a newly acknowledged sparse suffix may fund optional credit but cannot postpone the blocked lower frontier's epoch",
    );

    let mut partial_response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(103),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    partial_response_sender.record_delivered_data(quantum.saturating_mul(9));
    let partial_credit = quantum.saturating_add(scored_frontier_bytes);
    let funded_credit = partial_response_sender.reinjection_extra_event_budget_remaining(limits);
    assert!(funded_credit > partial_credit);
    partial_response_sender.record_reinjection_for_test(funded_credit - partial_credit);
    assert_eq!(
        partial_response_sender.reinjection_extra_event_budget_remaining(limits),
        partial_credit,
    );
    let partial = evaluate_server_data_ack_reinjection(
        &mut partial_response_sender,
        &path_stream,
        &send_stream,
        &mut exhausted_progress,
        &authoritative_ack,
        ack_frontier,
        Some(slow_send_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );
    assert!(partial.persistent_ready);
    assert!(!partial.target_service_exhausted);
    assert_eq!(
        partial.service_limit, partial_credit,
        "remaining optional credit, not the larger target service window, bounds this live-owner race",
    );
    assert!(partial.queued > 0);
    assert_eq!(
        partial_response_sender.bytes(),
        quantum,
        "Apply is capped by the lowest owner-uniform gap even when cumulative credit is larger",
    );
    assert_eq!(
        partial_response_sender.reinjection_extra_event_budget_remaining(limits),
        partial_credit - quantum,
        "credit that cannot cross the owner-uniform boundary remains for a new decision",
    );
    let serialized_bytes = partial_response_sender.bytes();
    partial_response_sender.clear_queued_work_for_test();
    assert_eq!(partial_response_sender.bytes(), 0);
    let residual_optional = evaluate_server_data_ack_reinjection(
        &mut partial_response_sender,
        &path_stream,
        &send_stream,
        &mut exhausted_progress,
        &authoritative_ack,
        ack_frontier,
        Some(slow_send_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );
    assert!(residual_optional.frame_count > 0);
    assert!(residual_optional.queued > 0);
    assert_eq!(
        partial_response_sender.bytes(),
        partial_credit - quantum,
        "unused C that could not cross U remains valid for the next independently ranked transaction",
    );
    partial_response_sender.clear_queued_work_for_test();
    let exhausted_again = evaluate_server_data_ack_reinjection(
        &mut partial_response_sender,
        &path_stream,
        &send_stream,
        &mut exhausted_progress,
        &authoritative_ack,
        ack_frontier,
        Some(slow_send_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );
    assert_eq!(exhausted_again.frame_count, 0);
    assert_eq!(exhausted_again.queued, 0);
    assert_eq!(partial_response_sender.bytes(), 0);
    assert!(serialized_bytes > 0);

    response_sender.record_delivered_data(quantum.saturating_mul(9));
    assert!(
        response_sender.reinjection_extra_event_budget_remaining(limits) >= quantum * 5,
        "funded live-gap recovery must retain the complete target service-window authority",
    );
    let frontier_frame = send_stream
        .retransmission_frames_for_normalized_ack_gaps(
            authoritative_ack.ranges(),
            scored_frontier_bytes,
        )
        .into_iter()
        .next()
        .expect("frontier repair frame");
    let mut blocked_response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(103),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 100,
        },
    );
    blocked_response_sender.record_delivered_data(quantum.saturating_mul(9));
    blocked_response_sender.enqueue_critical_reinjection_frame_with_cause(
        frontier_frame,
        RelaySendCause::AckGapReinjection,
    );
    let blocked_bytes = blocked_response_sender.bytes();
    let blocked = evaluate_server_data_ack_reinjection(
        &mut blocked_response_sender,
        &path_stream,
        &send_stream,
        &mut progress,
        &authoritative_ack,
        ack_frontier,
        Some(slow_send_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );
    assert!(
        blocked.frame_count >= 2,
        "the target-bound transaction contains only the lowest owner-uniform gap",
    );
    assert_eq!(
        blocked.queued, 0,
        "a blocked frontier quantum must not enqueue later service-window repairs"
    );
    assert_eq!(
        blocked_response_sender.bytes(),
        blocked_bytes,
        "later repair ranges must remain untouched until the frontier commits"
    );

    blocked_response_sender.clear_queued_work_for_test();
    let candidate_frames = stream_ack_gap_reinjection_frames_normalized(
        &send_stream,
        authoritative_ack.ranges(),
        quantum.saturating_mul(5),
        scored_frontier_bytes,
        true,
        true,
        true,
    );
    assert!(candidate_frames.len() >= 3);
    blocked_response_sender.enqueue_critical_reinjection_frame_with_cause(
        candidate_frames[1].clone(),
        RelaySendCause::AckGapReinjection,
    );
    let middle_blocked_bytes = blocked_response_sender.bytes();
    let middle_blocked = evaluate_server_data_ack_reinjection(
        &mut blocked_response_sender,
        &path_stream,
        &send_stream,
        &mut progress,
        &authoritative_ack,
        ack_frontier,
        Some(slow_send_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );
    assert!(
        middle_blocked.frame_count >= 2,
        "a later sparse gap requires a new target decision rather than joining this batch",
    );
    assert_eq!(middle_blocked.queued, 1);
    assert_eq!(
        blocked_response_sender.bytes(),
        middle_blocked_bytes.saturating_add(scored_frontier_bytes),
        "a blocked middle response chunk may retain the committed frontier but must stop before later omitted ranges",
    );

    let outcome = evaluate_server_data_ack_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &mut progress,
        &authoritative_ack,
        ack_frontier,
        Some(slow_send_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );

    assert!(
        outcome.queued >= 2,
        "a proven recovery-copy timeout may fill the owner-uniform extent behind the scored frontier quantum"
    );
    assert_eq!(
        response_sender.bytes(),
        quantum,
        "optional L-M service may fill U but cannot cross into the next sparse gap"
    );
    assert!(response_sender.bytes() > scored_frontier_bytes);
    assert!(outcome.persistent_ready);
    assert!(progress.next_reinjection_deadline().is_some());
    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("frontier repair dispatch");
    assert_eq!(dispatch.selected_path, Some(reinjection_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut reinjection_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset,
            payload,
            ..
        })) if offset == ack_frontier && payload.len() == scored_frontier_bytes
    ));
}

#[test]
fn draining_response_owner_retains_one_distinct_ack_gap_recovery_target() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(104);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        // UDP accepts the local sender sample below as completion evidence.
        // A TCP target would require Product-derived receipt evidence and
        // would therefore contradict this test's measured-target premise.
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(104),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    record_server_delivery_evidence_with_srtt(&binding, reinjection_key, 100_000);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let quantum = MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x52; quantum * 2]))
        .expect("prepare response flight");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit response flight");
    binding.record_original_flight(original_key, &frame);
    binding.age_original_flights_for_test(Duration::from_secs(1));

    let ack_ranges = vec![OffsetRange {
        start: quantum as u64,
        end: (quantum * 2) as u64,
    }];
    let validated_ack = begin_reliable_stream_ack(&send_stream, true, ack_ranges)
        .expect("validate later response range");
    let _ = send_stream.apply_validated_ack(&validated_ack);
    let mut authoritative_ack = AuthoritativeStreamAckSnapshot::default();
    update_reinjection_authoritative_ack_snapshot(&mut authoritative_ack, &validated_ack);

    original_commands.begin_path_drain();
    let target_snapshot = PathSnapshot::new(
        reinjection_key.path_id,
        reinjection_key.underlay,
        100.0,
        100_000_000.0,
    );
    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(104),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    let mut progress = ReliableAckGapReinjectionProgress::default();
    let outcome = evaluate_server_data_ack_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &mut progress,
        &authoritative_ack,
        0,
        Some(target_snapshot),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );

    assert!(
        outcome.has_multipath_alternative,
        "the one healthy output is a distinct structural target after excluding the draining owner",
    );
    assert!(outcome.has_measured_target);
    assert!(outcome.persistent_ready);
    assert!(outcome.queued > 0);
}

#[test]
fn persistent_tail_reinjection_preserves_original_flight_attribution() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(100);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternative_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(100),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (alternative_commands, _alternative_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternative_key.underlay,
            alternative_key.path_id,
            alternative_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let reinjection_debt = reliable_relay_buffer_len(limits).saturating_mul(4);
    let mut remaining = reinjection_debt;
    while remaining > 0 {
        let chunk = remaining.min(limits.max_payload_bytes);
        let frame = send_stream
            .send_data(Bytes::from(vec![0x43; chunk]))
            .expect("seed original-transmission path data");
        binding.record_original_flight(original_key, &frame);
        remaining = remaining.saturating_sub(chunk);
    }
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    assert!(
        send_stream.reinjection_bytes() > reliable_relay_buffer_len(limits),
        "test must cover a retained tail larger than one bounded reinjection event"
    );

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(100),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "a persistent original-transmission stall should reinject the lowest blocked range on an alternate output without changing original-flight attribution"
    );
    assert!(!outcome.pending);
    let first_unacknowledged_byte = Frame::StreamData {
        stream_id,
        offset: 1024,
        payload: Bytes::from_static(&[0]),
    };
    let original_outputs =
        binding.original_flight_outputs_overlapping_frame(&first_unacknowledged_byte);
    assert_eq!(original_outputs.len(), 1);
    assert_eq!(original_outputs[0].0, original_key);
    assert!(
        binding.has_output_incarnation(original_outputs[0].0, original_outputs[0].1),
        "reinjection must not rewrite exact original-output attribution",
    );
}

#[test]
fn final_tail_reinjection_ready_allows_closed_no_ack_frontier_after_deadline() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");
    let now = tokio::time::Instant::now();

    assert!(reliable_final_tail_reinjection_ready(
        true,
        &send_stream,
        &[],
        0,
        now,
        now,
    ));
}

#[test]
fn tcp_multipath_progress_timer_stays_enabled_with_reinjection_alternatives() {
    assert!(reliable_relay_recv_progress_timer_enabled(
        UnderlayProtocol::Udp,
        false,
    ));
    assert!(reliable_relay_recv_progress_timer_enabled(
        UnderlayProtocol::Tcp,
        true,
    ));
    assert!(!reliable_relay_recv_progress_timer_enabled(
        UnderlayProtocol::Tcp,
        false,
    ));
}

#[test]
fn subthreshold_receive_tail_retains_one_existing_ack_deadline() {
    let limits = MuxLimits::default();
    let mut recv_stream = ReliableRecvStream::new(StreamId(25), limits);
    let mut progress = ReliableRecvProgress::default();
    recv_stream
        .receive_data(0, Bytes::from_static(b"first"))
        .expect("first validation input");
    assert!(progress.should_send_ack(&recv_stream, None, TrafficClass::Throughput, limits, true,));
    assert!(!progress.ack_update_pending());

    recv_stream
        .receive_data(5, Bytes::from_static(b"tail"))
        .expect("validation tail");
    assert!(
        !progress.should_send_ack(&recv_stream, None, TrafficClass::Throughput, limits, false,)
    );
    assert!(progress.ack_update_pending());
    assert!(progress.should_send_ack(&recv_stream, None, TrafficClass::Throughput, limits, true,));
    assert!(!progress.ack_update_pending());
}
