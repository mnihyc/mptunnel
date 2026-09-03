use super::*;
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS};
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::model::path::{CarrierPathKey, PathPolicy};
use crate::mux::MuxLimits;
use crate::protocol::{
    CloseReason, OffsetRange, PathMetricDirection, PathUsage, StreamAttachmentPhase,
    StreamDemandHint, StreamReturnPlan,
};
use crate::runtime::path::authority::{
    NativeCarrierRateAuthorityHandle, NativeCarrierSchedulingShapeSnapshot,
};
use crate::runtime::path::commands::{
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::path::{
    CarrierNativeWindowSample, PathProofObservation, ServerCarrierPeer, ServerLocalPathProperties,
    ServerStreamPathAttachment, ServerTargetAdmission,
};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};
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
        rate_valid_for_us: 1_000_000,
        rate_observed: true,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 0,
        delivery_rate_bps: 1,
        pacing_rate_bps: 1,
        pacing_rate_observed: true,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight_observed: true,
        queue_observed: true,
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

fn native_test_authority_and_shape(
    path_instance_id: CarrierPathInstanceId,
    direction: PathMetricDirection,
    activation: u64,
    controller: u64,
    operational_rate_bps: u128,
) -> (
    Arc<NativeCarrierRateAuthorityHandle>,
    NativeCarrierSchedulingShapeSnapshot,
) {
    let scope = CarrierRateAuthorityScope::new(path_instance_id, direction);
    let authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
        scope,
        8_000_000,
        activation,
        controller,
        Some(operational_rate_bps),
    )
    .expect("native test authority");
    let shape = authority
        .refresh_scheduling_shape_for_test(
            scope,
            activation,
            controller,
            Some(operational_rate_bps),
            Duration::from_millis(40),
            Duration::from_millis(4),
            256_000,
            32_000,
            1_400,
            Some(u64::try_from(operational_rate_bps).expect("test pacing rate")),
            false,
        )
        .expect("native test scheduling shape");
    (authority, shape)
}

fn staged_native_shape(
    registry: &ServerReliableStreamRegistry,
    registration: &ServerCarrierPathRegistration,
) -> Option<NativeCarrierSchedulingShapeSnapshot> {
    registry
        .registered_path_instances
        .lock()
        .expect("server active path instance lock")
        .instances
        .get(&(
            registration.session_id(),
            registration.underlay(),
            registration.path_id(),
            registration.path_instance_id(),
        ))
        .and_then(|path| path.apply_authority.snapshot().native_scheduling_shape)
}

fn server_carrier_path_identity(
    registration: &ServerCarrierPathRegistration,
) -> ServerCarrierPathIdentity {
    ServerCarrierPathIdentity {
        session_id: registration.session_id(),
        underlay: registration.underlay(),
        path_id: registration.path_id(),
        path_instance_id: registration.path_instance_id(),
    }
}

fn assert_server_carrier_status_identity(
    status: &ServerCarrierPathStatusSnapshot,
    identity: ServerCarrierPathIdentity,
) {
    assert_eq!(status.session_id, identity.session_id);
    assert_eq!(status.underlay, identity.underlay);
    assert_eq!(status.path_id, identity.path_id);
    assert_eq!(status.path_instance_id, identity.path_instance_id);
}

fn stable_server_carrier_metrics(mut metrics: Option<PathMetrics>) -> Option<PathMetrics> {
    if let Some(metrics) = metrics.as_mut() {
        // Projection residence advances between independent calls. Every
        // other field is stable and must remain identical.
        metrics.metric_age_us = 0;
        metrics.rate_valid_for_us = 0;
    }
    metrics
}

fn assert_stable_server_carrier_status_eq(
    left: &ServerCarrierPathStatusSnapshot,
    right: &ServerCarrierPathStatusSnapshot,
) {
    assert_eq!(left.session_id, right.session_id);
    assert_eq!(left.underlay, right.underlay);
    assert_eq!(left.path_id, right.path_id);
    assert_eq!(left.path_instance_id, right.path_instance_id);
    assert_eq!(left.configured_index, right.configured_index);
    assert_eq!(left.policy, right.policy);
    assert_eq!(left.state, right.state);
    assert_eq!(left.usage, right.usage);
    assert_eq!(
        stable_server_carrier_metrics(left.metrics),
        stable_server_carrier_metrics(right.metrics),
    );
    assert_eq!(
        left.carrier_delivery_rate_sample,
        right.carrier_delivery_rate_sample,
    );
    assert_eq!(left.eligibility_epoch, right.eligibility_epoch);
    assert_eq!(left.native_scheduling_shape, right.native_scheduling_shape,);
    assert_eq!(left.source, right.source);
}

#[test]
fn carrier_path_statuses_preserve_input_order_duplicates_and_exact_misses() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(8));
    let port = registry.path_port();
    let session_id = SessionId(576);
    let first = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let second = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(2),
        ServerLocalPathProperties::default(),
    );
    let first_identity = server_carrier_path_identity(&first);
    let second_identity = server_carrier_path_identity(&second);
    let missing_exact_identity = ServerCarrierPathIdentity {
        path_instance_id: crate::model::path::next_carrier_path_instance_id(),
        ..first_identity
    };

    let statuses = port.carrier_path_statuses(&[
        second_identity,
        missing_exact_identity,
        first_identity,
        second_identity,
    ]);

    assert_eq!(statuses.len(), 4);
    let first_second = statuses[0].expect("first requested exact path");
    assert_server_carrier_status_identity(&first_second, second_identity);
    assert!(
        statuses[1].is_none(),
        "a missing physical instance must not match its live logical path",
    );
    let first_first = statuses[2].expect("third requested exact path");
    assert_server_carrier_status_identity(&first_first, first_identity);
    let duplicate_second = statuses[3].expect("duplicate requested exact path");
    assert_server_carrier_status_identity(&duplicate_second, second_identity);
    assert_stable_server_carrier_status_eq(&first_second, &duplicate_second);
}

#[test]
fn carrier_path_statuses_match_management_stable_projection_fields() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(8));
    let port = registry.path_port();
    let session_id = SessionId(577);
    let path_id = PathId(3);
    let mut initial_metrics = native_quic_test_metrics(path_id);
    initial_metrics.delivery_rate_bps = 11_000_000;
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties {
            config_ordinal: 7,
            policy: PathPolicy {
                backup: true,
                ..PathPolicy::default()
            },
            startup_rate_prior: crate::transport::RateHint::Unknown,
            initial_metrics: Some(initial_metrics),
        },
    );
    let identity = server_carrier_path_identity(&registration);
    let (_authority, shape) = native_test_authority_and_shape(
        registration.path_instance_id(),
        PathMetricDirection::ServerToClient,
        1,
        7,
        80_000_000,
    );
    assert!(port.stage_native_scheduling_shape(&registration, shape));
    port.record_peer_path_usage(&registration, 1, PathUsage::Backup);
    registration.set_state(PeerPathState::Suspect);

    let mut live_metrics = initial_metrics;
    live_metrics.metric_epoch = live_metrics.metric_epoch.wrapping_add(1);
    live_metrics.delivery_rate_bps = 73_000_000;
    let observed_at = Instant::now();
    let delivery_sample = CarrierDeliveryRateSample {
        delivery_rate_bps: 73_000_000,
        pacing_rate_bps: Some(81_000_000),
        sample_count: 9,
        sample_bytes: 64 * 1_024,
        delivery_window_covered: true,
        observed_at,
        expires_at: observed_at + Duration::from_secs(1),
    };
    port.record_local_path_metrics_with_delivery_rate_sample(
        &registration,
        live_metrics,
        false,
        Some(delivery_sample),
    );

    let batch = port
        .carrier_path_statuses(&[identity])
        .into_iter()
        .next()
        .flatten()
        .expect("exact batch status");
    let management = port
        .management_snapshot()
        .paths
        .into_iter()
        .find(|status| {
            status.session_id == identity.session_id
                && status.underlay == identity.underlay
                && status.path_id == identity.path_id
                && status.path_instance_id == identity.path_instance_id
        })
        .expect("same management path status");

    assert_stable_server_carrier_status_eq(&batch, &management);
    assert_eq!(batch.configured_index, 7);
    assert!(batch.policy.backup);
    assert_eq!(batch.state, PeerPathState::Suspect);
    assert_eq!(batch.usage, Some(PathUsage::Backup));
    assert_eq!(batch.source, Some("local_sender"));
    assert_eq!(batch.carrier_delivery_rate_sample, Some(delivery_sample));
    assert_eq!(batch.native_scheduling_shape, Some(shape));
}

#[test]
fn carrier_path_status_epoch_fails_closed_after_state_and_usage_transitions() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let registration = port.register_test_carrier_path(
        SessionId(578),
        UnderlayProtocol::Tcp,
        PathId(4),
        ServerLocalPathProperties::default(),
    );
    let identity = server_carrier_path_identity(&registration);
    let apply_authority = registration.apply_authority();
    let initial = port
        .carrier_path_statuses(&[identity])
        .into_iter()
        .next()
        .flatten()
        .expect("initial exact batch status");
    let initial_epoch = initial
        .eligibility_epoch
        .expect("initial eligibility epoch");
    assert_eq!(
        apply_authority.commit_if_current(initial_epoch, None, |_| ()),
        Some(()),
    );

    registration.set_state(PeerPathState::Suspect);
    assert_eq!(
        apply_authority.commit_if_current(initial_epoch, None, |_| ()),
        None,
        "a pre-state-transition decision cannot commit",
    );
    let after_state = port
        .carrier_path_statuses(&[identity])
        .into_iter()
        .next()
        .flatten()
        .expect("post-state-transition exact batch status");
    let state_epoch = after_state
        .eligibility_epoch
        .expect("post-state-transition eligibility epoch");
    assert_ne!(state_epoch, initial_epoch);
    assert_eq!(after_state.state, PeerPathState::Suspect);
    assert_eq!(
        apply_authority.commit_if_current(state_epoch, None, |_| ()),
        Some(()),
    );

    port.record_peer_path_usage(&registration, 1, PathUsage::Backup);
    assert_eq!(
        apply_authority.commit_if_current(state_epoch, None, |_| ()),
        None,
        "a pre-usage-transition decision cannot commit",
    );
    let after_usage = port
        .carrier_path_statuses(&[identity])
        .into_iter()
        .next()
        .flatten()
        .expect("post-usage-transition exact batch status");
    assert_ne!(after_usage.eligibility_epoch, Some(state_epoch));
    assert_eq!(after_usage.usage, Some(PathUsage::Backup));
}

#[test]
fn server_native_shape_stage_is_exact_and_revision_ordered() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(578);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let other = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(2),
        ServerLocalPathProperties::default(),
    );

    let (authority, older) = native_test_authority_and_shape(
        registration.path_instance_id(),
        PathMetricDirection::ServerToClient,
        1,
        99,
        80_000_000,
    );
    let (_, wrong_direction) = native_test_authority_and_shape(
        registration.path_instance_id(),
        PathMetricDirection::ClientToServer,
        1,
        99,
        80_000_000,
    );
    let (_, wrong_instance) = native_test_authority_and_shape(
        other.path_instance_id(),
        PathMetricDirection::ServerToClient,
        1,
        99,
        80_000_000,
    );

    assert!(!port.stage_native_scheduling_shape(&registration, wrong_direction));
    assert!(!port.stage_native_scheduling_shape(&registration, wrong_instance));
    assert_eq!(staged_native_shape(&registry, &registration), None);
    assert_eq!(staged_native_shape(&registry, &other), None);

    assert!(port.stage_native_scheduling_shape(&registration, older));
    assert_eq!(staged_native_shape(&registry, &registration), Some(older));
    assert_eq!(staged_native_shape(&registry, &other), None);
    assert!(
        !port.stage_native_scheduling_shape(&registration, older),
        "an identical staged shape is not a new registry generation",
    );

    // Controller identities are opaque equality tokens, not ordered epochs.
    // A restored/lower raw token on a newer transport activation is accepted
    // because the central authority's non-reused revision G is newer.
    authority
        .publish_observation_for_test(2, 1, Some(160_000_000))
        .expect("publish restored controller on a newer activation");
    let scope = CarrierRateAuthorityScope::new(
        registration.path_instance_id(),
        PathMetricDirection::ServerToClient,
    );
    let newer = authority
        .refresh_scheduling_shape_for_test(
            scope,
            2,
            1,
            Some(160_000_000),
            Duration::from_millis(30),
            Duration::from_millis(3),
            512_000,
            64_000,
            1_400,
            Some(160_000_000),
            false,
        )
        .expect("newer native scheduling shape");
    assert!(newer.stamp().revision() > older.stamp().revision());
    assert!(port.stage_native_scheduling_shape(&registration, newer));
    assert_eq!(staged_native_shape(&registry, &registration), Some(newer));

    assert!(
        !port.stage_native_scheduling_shape(&registration, older),
        "an older central revision G cannot roll the registry back",
    );
    assert_eq!(staged_native_shape(&registry, &registration), Some(newer));
}

#[test]
fn server_native_shape_attachment_inherits_and_identical_fanout_is_quiet() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(579);
    let stream_id = StreamId(1);
    let path_id = PathId(3);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let (authority, initial) = native_test_authority_and_shape(
        registration.path_instance_id(),
        PathMetricDirection::ServerToClient,
        1,
        7,
        80_000_000,
    );
    assert!(port.stage_native_scheduling_shape(&registration, initial));

    let (commands, _receivers) = reliable_path_command_channels(8);
    let commands = commands.with_native_rate_authority(authority.clone());
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open response stream with staged Native shape")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected new response stream"),
    };
    let ReliablePathStreamOutput::Switchable(binding) = &accepted.stream().output else {
        panic!("expected switchable response binding");
    };
    let inherited = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.path_instance_id == registration.path_instance_id())
        .expect("inherited Native output");
    assert_eq!(inherited.native_authority_stamp, Some(initial.stamp()));
    assert_eq!(
        inherited.observation.snapshot.delivery_rate_bps,
        80_000_000.0
    );
    assert_eq!(
        inherited.observation.snapshot.carrier_delivery_rate_bps,
        Some(80_000_000.0),
    );
    assert_eq!(inherited.observation.snapshot.srtt_ms, 40.0);
    assert_eq!(inherited.observation.snapshot.jitter_ms, 4.0);
    assert_eq!(
        inherited.observation.snapshot.carrier_inflight_limit_bytes,
        256_000,
    );

    let mut updates = binding.subscribe_updates();
    assert!(
        !updates
            .has_changed()
            .expect("initial response update state")
    );
    let inherited_generation = binding.response_model_generation();

    authority
        .publish_observation_for_test(1, 7, Some(160_000_000))
        .expect("publish newer same-activation Native rate");
    let scope = CarrierRateAuthorityScope::new(
        registration.path_instance_id(),
        PathMetricDirection::ServerToClient,
    );
    let updated = authority
        .refresh_scheduling_shape_for_test(
            scope,
            1,
            7,
            Some(160_000_000),
            Duration::from_millis(35),
            Duration::from_millis(5),
            512_000,
            48_000,
            1_400,
            Some(160_000_000),
            false,
        )
        .expect("updated Native scheduling shape");
    assert!(port.stage_native_scheduling_shape(&registration, updated));
    assert_eq!(binding.response_model_generation(), inherited_generation);
    assert!(
        !updates
            .has_changed()
            .expect("staging alone does not wake outputs"),
        "staging is registry-local until the fenced caller fans out",
    );

    port.fanout_native_scheduling_shape(&registration, updated);
    assert_eq!(
        binding.response_model_generation(),
        inherited_generation + 1,
    );
    assert!(updates.has_changed().expect("Native fanout update"));
    let refreshed = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.path_instance_id == registration.path_instance_id())
        .expect("updated Native output");
    assert_eq!(refreshed.native_authority_stamp, Some(updated.stamp()));
    assert_eq!(
        refreshed.observation.snapshot.delivery_rate_bps,
        160_000_000.0
    );

    let _ = updates.borrow_and_update();
    let updated_generation = binding.response_model_generation();
    port.fanout_native_scheduling_shape(&registration, updated);
    assert_eq!(binding.response_model_generation(), updated_generation);
    assert!(
        !updates.has_changed().expect("identical Native fanout"),
        "an identical fanout must neither mutate the output nor wake scheduling",
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestServerPathAuthority {
    state: PeerPathState,
    peer_usage: Option<(u64, PathUsage)>,
    native_capacity_epoch: u64,
    eligibility_epoch: Option<u64>,
    path_proven: bool,
    retirement_started: bool,
}

fn server_path_authority(
    registry: &ServerReliableStreamRegistry,
    registration: &ServerCarrierPathRegistration,
) -> TestServerPathAuthority {
    let paths = registry
        .registered_path_instances
        .lock()
        .expect("server active path instance lock");
    let path = paths
        .instances
        .get(&(
            registration.session_id(),
            registration.underlay(),
            registration.path_id(),
            registration.path_instance_id(),
        ))
        .expect("registered server carrier path");
    TestServerPathAuthority {
        state: path.state,
        peer_usage: path.peer_usage.map(|entry| (entry.sequence, entry.usage)),
        native_capacity_epoch: path.native_capacity_epoch,
        eligibility_epoch: path.apply_authority.snapshot().eligibility_epoch,
        path_proven: path.path_proof.is_some(),
        retirement_started: path.retirement_started,
    }
}

#[test]
fn server_path_authority_separates_native_and_structural_lifetimes() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(580);
    let path_id = PathId(3);
    let registration = port
        .register_carrier_path_with_observed_peer_and_authority(
            session_id,
            UnderlayProtocol::Udp,
            path_id,
            ServerLocalPathProperties::default(),
            PathUsage::Available,
            7,
            PrincipalPermit::for_test("test-peer"),
            ServerCarrierPeer::fixed(SocketAddr::from(([203, 0, 113, 7], 51_000))),
            None,
        )
        .expect("register exact QUIC authority");

    assert_eq!(
        server_path_authority(&registry, &registration),
        TestServerPathAuthority {
            state: PeerPathState::Active,
            peer_usage: Some((0, PathUsage::Available)),
            native_capacity_epoch: 7,
            eligibility_epoch: Some(1),
            path_proven: false,
            retirement_started: false,
        },
        "physical identity, initial peer use, and both epochs must publish atomically",
    );

    port.record_peer_path_usage(&registration, 1, PathUsage::Available);
    port.record_local_path_metrics_with_native_epoch(
        &registration,
        native_quic_test_metrics(path_id),
        false,
        Some(7),
        None,
    );
    port.record_path_proof_success(
        &registration,
        path_proof_observation(1, Duration::from_millis(5)),
    );
    let refreshed = server_path_authority(&registry, &registration);
    assert_eq!(refreshed.native_capacity_epoch, 7);
    assert_eq!(refreshed.eligibility_epoch, Some(1));
    assert_eq!(refreshed.peer_usage, Some((1, PathUsage::Available)));
    assert!(refreshed.path_proven);

    port.record_peer_path_usage(&registration, 2, PathUsage::Backup);
    assert_eq!(
        server_path_authority(&registry, &registration).eligibility_epoch,
        Some(2),
        "only a changed peer-use value is a structural transition",
    );
    registration.set_state(PeerPathState::Suspect);
    assert_eq!(
        server_path_authority(&registry, &registration).eligibility_epoch,
        Some(3),
    );
    registration.set_state(PeerPathState::Suspect);
    assert_eq!(
        server_path_authority(&registry, &registration).eligibility_epoch,
        Some(3),
        "replaying the same state must not invent a lifetime",
    );
    registration.set_state(PeerPathState::Active);
    assert_eq!(
        server_path_authority(&registry, &registration).eligibility_epoch,
        Some(4),
    );
}

#[test]
fn server_quic_native_epoch_rejects_late_metrics_and_survives_missing_rate() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(581);
    let path_id = PathId(4);
    let registration = port
        .register_carrier_path_with_observed_peer_and_authority(
            session_id,
            UnderlayProtocol::Udp,
            path_id,
            ServerLocalPathProperties::default(),
            PathUsage::Available,
            7,
            PrincipalPermit::for_test("test-peer"),
            ServerCarrierPeer::fixed(SocketAddr::from(([203, 0, 113, 8], 51_001))),
            None,
        )
        .expect("register exact QUIC authority");

    let mut current = native_quic_test_metrics(path_id);
    current.srtt_us = 80_000;
    port.record_local_path_metrics_with_native_epoch(&registration, current, false, Some(8), None);
    assert_eq!(
        server_path_authority(&registry, &registration).native_capacity_epoch,
        8,
        "the controller lifetime must publish even without a bandwidth estimate",
    );

    let mut stale = current;
    stale.srtt_us = 900_000;
    port.record_local_path_metrics_with_native_epoch(&registration, stale, false, Some(7), None);
    let stored = registry
        .path_metrics
        .lock()
        .expect("server path metrics lock")
        .get(&(
            session_id,
            UnderlayProtocol::Udp,
            path_id,
            registration.path_instance_id(),
        ))
        .copied()
        .expect("current native metrics");
    assert_eq!(stored.metrics.srtt_us, 80_000);
    assert_eq!(
        server_path_authority(&registry, &registration).native_capacity_epoch,
        8,
        "a delayed old controller sample must not roll authority backward",
    );

    port.record_peer_path_metrics(
        &registration,
        PathMetrics {
            srtt_us: 1,
            ..stale
        },
    );
    assert_eq!(
        server_path_authority(&registry, &registration).native_capacity_epoch,
        8,
        "peer diagnostics never own the local native controller epoch",
    );
}

#[test]
fn server_registry_preserves_native_window_epoch_across_rtt_only_refresh() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(582);
    let path_id = PathId(5);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        path_id,
        ServerLocalPathProperties::default(),
    );
    let instance_key = (
        session_id,
        UnderlayProtocol::Tcp,
        path_id,
        registration.path_instance_id(),
    );
    let mut metrics = PathMetrics {
        underlay: UnderlayProtocol::Tcp,
        ..native_quic_test_metrics(path_id)
    };
    let old_observed_at = Instant::now() - Duration::from_secs(1);
    let old = CarrierNativeWindowSample {
        inflight_limit_bytes: metrics.inflight_limit_bytes,
        observed_at: old_observed_at,
        expires_at: old_observed_at + Duration::from_millis(10),
    };
    port.record_local_path_metrics_with_native_evidence(
        &registration,
        metrics,
        false,
        None,
        Some(old),
        None,
    );
    let first_recorded_at = registry
        .path_metrics
        .lock()
        .expect("server path metrics lock")
        .get(&instance_key)
        .copied()
        .expect("stored TCP native metrics")
        .recorded_at;

    metrics.srtt_us = 1_000_000;
    metrics.rttvar_us = 500_000;
    metrics.metric_epoch = metrics.metric_epoch.wrapping_add(1);
    port.record_local_path_metrics_with_native_evidence(
        &registration,
        metrics,
        false,
        None,
        Some(old),
        None,
    );
    let retained = registry
        .path_metrics
        .lock()
        .expect("server path metrics lock")
        .get(&instance_key)
        .copied()
        .expect("refreshed TCP diagnostics");
    assert!(retained.recorded_at >= first_recorded_at);
    assert_eq!(retained.metrics.srtt_us, 1_000_000);
    assert_eq!(retained.carrier_native_window_sample, Some(old));
    assert!(
        !old.fresh_at(retained.recorded_at),
        "fresh RTT diagnostics cannot resurrect an expired C epoch",
    );

    let refreshed_at = Instant::now();
    let refreshed = CarrierNativeWindowSample::new(
        metrics.inflight_limit_bytes,
        refreshed_at,
        Duration::from_secs(1),
    )
    .expect("genuine native window refresh");
    port.record_local_path_metrics_with_native_evidence(
        &registration,
        metrics,
        false,
        None,
        Some(refreshed),
        None,
    );
    let replaced = registry
        .path_metrics
        .lock()
        .expect("server path metrics lock")
        .get(&instance_key)
        .copied()
        .expect("genuine C refresh stored");
    assert_eq!(replaced.carrier_native_window_sample, Some(refreshed));
    assert!(refreshed.fresh_at(replaced.recorded_at));
}

#[test]
fn server_eligibility_epoch_exhaustion_disables_future_evidence_without_panicking() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(1));
    let registration = registry.path_port().register_test_carrier_path(
        SessionId(582),
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    {
        let mut paths = registry
            .registered_path_instances
            .lock()
            .expect("server active path instance lock");
        paths
            .instances
            .get_mut(&(
                registration.session_id(),
                registration.underlay(),
                registration.path_id(),
                registration.path_instance_id(),
            ))
            .expect("registered server carrier path")
            .apply_authority
            .set_eligibility_epoch_for_test(Some(u64::MAX));
    }
    registration.set_state(PeerPathState::Suspect);
    assert_eq!(
        server_path_authority(&registry, &registration).eligibility_epoch,
        None,
    );
    registration.set_state(PeerPathState::Active);
    assert_eq!(
        server_path_authority(&registry, &registration).eligibility_epoch,
        None,
        "ordinary service stays fail-open while qualification stays disabled",
    );
}

#[test]
fn forwarding_decrements_only_remaining_authority_and_preserves_provenance() {
    let mut metrics = native_quic_test_metrics(PathId(4));
    metrics.metric_age_us = u32::MAX - 1;
    metrics.rate_valid_for_us = 5;
    metrics.pacing_rate_bps = 9;
    metrics.pacing_rate_observed = true;

    let near_expiry = path_metrics_after_residence(metrics, Duration::from_micros(4));
    assert_eq!(near_expiry.metric_age_us, u32::MAX);
    assert_eq!(near_expiry.rate_valid_for_us, 1);
    assert!(near_expiry.rate_observed);
    assert_eq!(near_expiry.pacing_rate_bps, 9);
    assert!(near_expiry.pacing_rate_observed);

    let expired = path_metrics_after_residence(metrics, Duration::from_micros(5));
    assert_eq!(expired.rate_valid_for_us, 0);
    assert!(expired.rate_observed);
    assert_eq!(expired.pacing_rate_bps, 9);
    assert!(expired.pacing_rate_observed);
}

#[test]
fn accepted_stream_keeps_its_authenticated_opening_carrier_across_reattachment() {
    let registry = constrained_registry(2, 2);
    let port = registry.path_port();
    let session_id = SessionId(588);
    let stream_id = StreamId(1);
    let opening_peer = SocketAddr::from(([203, 0, 113, 7], 51_000));
    let later_peer = SocketAddr::from(([198, 51, 100, 9], 52_000));
    let opening = port
        .register_carrier_path_with_observed_peer(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            ServerLocalPathProperties::default(),
            PrincipalPermit::for_test("test-peer"),
            ServerCarrierPeer::fixed(opening_peer),
            Some(Arc::from("opening-tcp")),
        )
        .expect("opening carrier");
    let later = port
        .register_carrier_path_with_observed_peer(
            session_id,
            UnderlayProtocol::Udp,
            PathId(1),
            ServerLocalPathProperties::default(),
            PrincipalPermit::for_test("test-peer"),
            ServerCarrierPeer::fixed(later_peer),
            Some(Arc::from("later-quic")),
        )
        .expect("later carrier");
    let (opening_commands, _opening_receivers) = reliable_path_command_channels(8);
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: opening,
                commands: opening_commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected new response stream"),
    };
    let (later_commands, _later_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                initial_demand: StreamDemandHint::Latency,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: later,
                    commands: later_commands,
                    max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                },
                mux_limits: MuxLimits::default(),
            })
            .expect("attach later carrier"),
        ServerReliableStreamOpen::Existing(_)
    ));

    let ingress = accepted.ingress().expect("opening ingress snapshot");
    assert_eq!(ingress.peer(), opening_peer);
    assert_eq!(ingress.underlay(), UnderlayProtocol::Tcp);
    assert_eq!(ingress.configured_path(), Some("opening-tcp"));
}

#[test]
fn detached_ingress_observer_snapshots_migration_without_retaining_carrier_registration() {
    let registry = constrained_registry(1, 1);
    let port = registry.path_port();
    let session_id = SessionId(589);
    let first_peer = SocketAddr::from(([203, 0, 113, 8], 51_001));
    let migrated_peer = SocketAddr::from(([198, 51, 100, 10], 52_001));
    let current_peer = Arc::new(Mutex::new(first_peer));
    let observed_peer = current_peer.clone();
    let registration = port
        .register_carrier_path_with_observed_peer(
            session_id,
            UnderlayProtocol::Udp,
            PathId(0),
            ServerLocalPathProperties::default(),
            PrincipalPermit::for_test("test-peer"),
            ServerCarrierPeer::observed(move || *observed_peer.lock().expect("observed peer lock")),
            Some(Arc::from("mobile-quic")),
        )
        .expect("QUIC carrier");
    let observer = registration
        .mpp_ingress_observer()
        .expect("ingress observer");
    let opened_before_migration = observer.snapshot();
    drop(registration);
    assert!(
        port.management_snapshot().paths.is_empty(),
        "the observational authority must not retain carrier registration"
    );
    *current_peer.lock().expect("current peer lock") = migrated_peer;
    let opened_after_migration = observer.snapshot();

    assert_eq!(opened_before_migration.peer(), first_peer);
    assert_eq!(opened_after_migration.peer(), migrated_peer);
    assert_eq!(opened_before_migration.peer(), first_peer);
}

#[tokio::test]
async fn one_opening_peer_snapshot_drives_preflight_and_accepted_flow_identity() {
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel();
    let registry = Arc::new(ServerReliableStreamRegistry::with_accept_sender(
        2,
        2,
        accepted_tx,
        MuxLimits::default(),
    ));
    let first_peer = SocketAddr::from(([203, 0, 113, 10], 51_002));
    let migrated_peer = SocketAddr::from(([198, 51, 100, 11], 52_002));
    let observer_reads = Arc::new(AtomicUsize::new(0));
    let reads = observer_reads.clone();
    let admitted_peer = Arc::new(Mutex::new(None));
    let routed_peer = admitted_peer.clone();
    let port =
        registry
            .path_port()
            .with_target_admission(Arc::new(move |_permit, ingress, _target| {
                *routed_peer.lock().expect("admitted peer lock") = Some(ingress.peer());
                Ok(ServerTargetAdmission::Allow)
            }));
    let registration = port
        .register_carrier_path_with_observed_peer(
            SessionId(591),
            UnderlayProtocol::Udp,
            PathId(0),
            ServerLocalPathProperties::default(),
            PrincipalPermit::for_test("test-peer"),
            ServerCarrierPeer::observed(move || {
                if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                    first_peer
                } else {
                    migrated_peer
                }
            }),
            Some(Arc::from("migrating-quic")),
        )
        .expect("migrating QUIC carrier");
    let (commands, _receivers) = reliable_path_command_channels(8);

    assert!(matches!(
        port.open_or_attach(ServerStreamOpenRequest {
            session_id: SessionId(591),
            stream_id: StreamId(1),
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration,
                commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .await
        .expect("open through target preflight"),
        ServerStreamOpenOutcome::New(_)
    ));
    let accepted = accepted_rx.try_recv().expect("accepted reliable stream");
    assert_eq!(
        *admitted_peer.lock().expect("admitted peer lock"),
        Some(first_peer)
    );
    assert_eq!(
        accepted.ingress().expect("accepted opening ingress").peer(),
        first_peer
    );
    assert_eq!(observer_reads.load(Ordering::SeqCst), 1);
}

#[test]
fn carrier_activation_and_retirement_scan_form_one_atomic_owner_transition() {
    let registry = constrained_registry(4, 4);
    let port = registry.path_port();
    let session_id = SessionId(590);
    let initial = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let retirement = port
        .session_retirement(session_id)
        .expect("subscribe active session retirement");
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let hook_entered = entered.clone();
    let hook_release = release.clone();
    registry.set_carrier_activation_after_session_attach_hook(Some(Arc::new(move || {
        hook_entered.wait();
        hook_release.wait();
    })));

    let late_port = port.clone();
    let late = std::thread::spawn(move || {
        late_port.register_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
            PathId(1),
            ServerLocalPathProperties::default(),
            PrincipalPermit::for_test("test-peer"),
        )
    });
    entered.wait();
    let retiring_registry = registry.clone();
    let retiring = std::thread::spawn(move || {
        retiring_registry.retire_session(session_id, CloseReason::PolicyRejected);
    });
    while !retirement.is_retired() {
        std::thread::yield_now();
    }
    release.wait();
    let late = late.join().expect("late carrier activation thread");
    retiring.join().expect("session retirement thread");
    registry.set_carrier_activation_after_session_attach_hook(None);

    assert!(matches!(
        late,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    assert!(
        port.management_snapshot()
            .paths
            .iter()
            .all(|path| path.session_id != session_id),
        "no carrier whose activation overlaps the fence may survive the owner scan",
    );
    drop(initial);
}

#[test]
fn repeated_session_retirement_resweeps_an_exact_late_path_instance() {
    let registry = constrained_registry(4, 4);
    let port = registry.path_port();
    let session_id = SessionId(591);
    let initial = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let consumed_late_reference = registry
        .register_realtime_flow(session_id)
        .expect("reserve the tracker reference represented by the late owner");
    registry.retire_session(session_id, CloseReason::Normal);
    assert!(port.management_snapshot().paths.is_empty());

    let identity = ServerCarrierPathIdentity {
        session_id,
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
        path_instance_id: crate::model::path::next_carrier_path_instance_id(),
    };
    let (retirement_completion, mut retired) = watch::channel(false);
    {
        let mut paths = registry
            .registered_path_instances
            .lock()
            .expect("server active path instance lock");
        paths
            .logical_instances
            .insert(server_logical_path_key(identity), identity.path_instance_id);
        paths.session_path_counts.insert(session_id, 1);
        paths.instances.insert(
            server_physical_path_key(identity),
            ServerRegisteredPath {
                local: ServerLocalPathProperties::default(),
                state: PeerPathState::Active,
                peer_usage: None,
                native_capacity_epoch: 0,
                apply_authority: ServerCarrierPathApplyAuthority::new(),
                path_proof: None,
                retirement_started: false,
                retirement_completion,
            },
        );
    }

    assert_eq!(
        registry.retire_session(session_id, CloseReason::PolicyRejected),
        CloseReason::Normal,
        "repeat sweeps preserve the first terminal reason",
    );

    assert!(*retired.borrow_and_update());
    assert!(port.management_snapshot().paths.is_empty());
    assert_eq!(registry.session_tracker.reference_count(session_id), 0);
    // The repeated exact scan consumed the tracker reference represented by
    // this synthetic late owner, just as dropping a real registration would.
    std::mem::forget(consumed_late_reference);
    drop(initial);
}

#[tokio::test]
async fn terminal_session_fence_rejects_existing_stream_reattach_after_carrier_scan() {
    let registry = constrained_registry(4, 3);
    let port = registry.path_port();
    let session_id = SessionId(599);
    let stream_id = StreamId(1);
    let first = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let live_sibling = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(1),
        ServerLocalPathProperties::default(),
    );
    let late_sibling = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(2),
        ServerLocalPathProperties::default(),
    );
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let mux_limits = MuxLimits::default();
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: first,
                commands: first_commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open initial response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected new response stream"),
    };

    let (live_commands, _live_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: target.clone(),
                initial_demand: StreamDemandHint::Latency,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: live_sibling,
                    commands: live_commands,
                    max_frame_payload_bytes: mux_limits.max_payload_bytes,
                },
                mux_limits,
            })
            .expect("live session accepts an existing-stream sibling attachment"),
        ServerReliableStreamOpen::Existing(_)
    ));

    registry.retire_session(session_id, CloseReason::Normal);
    assert!(
        !port
            .management_snapshot()
            .paths
            .iter()
            .any(|path| { path.path_instance_id == late_sibling.path_instance_id() }),
        "the sibling carrier scan must finish before the late attach attempt",
    );

    let (late_commands, _late_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
                initial_demand: StreamDemandHint::Latency,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: late_sibling,
                    commands: late_commands,
                    max_frame_payload_bytes: mux_limits.max_payload_bytes,
                },
                mux_limits,
            })
            .expect("terminal reattach is rejected without mutating the stream"),
        ServerReliableStreamOpen::Rejected
    ));

    accepted.close().await;
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
async fn repeated_same_key_reconnect_does_not_wait_for_predecessor_cleanup() {
    const FAILED_GENERATIONS: usize = 3;

    let registry = constrained_registry(1, 1);
    let port = registry.path_port();
    let session_id = SessionId(640);
    let stream_id = StreamId(1);
    let path_id = PathId(0);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let mut metrics = native_quic_test_metrics(path_id);
    metrics.delivery_rate_bps = 100;
    let mut current = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
            path_id,
            ServerLocalPathProperties {
                initial_metrics: Some(metrics),
                ..ServerLocalPathProperties::default()
            },
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("initial carrier");
    let (commands, receivers) = reliable_path_command_channels(8);
    let mut command_receivers = vec![receivers];
    let mut accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: current.clone(),
                commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected new response stream"),
    };
    let mut stream = accepted.take_stream();
    let key = CarrierPathKey {
        underlay: current.underlay(),
        path_id,
    };
    let output_incarnation = |stream: &ReliablePathStream| match &stream.output {
        ReliablePathStreamOutput::Switchable(binding) => {
            binding
                .sender_path_targets(TrafficClass::Throughput, 1)
                .first()
                .expect("current response output")
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
            port.try_route_frame(&current, stream_id, frame)
                .expect("route ACK"),
            ServerStreamFrameRoute::Backpressured(_)
        ) {
            backpressured = true;
            break;
        }
    }
    assert!(backpressured, "test must saturate the ordered actor queue");

    let stable_reference_count = registry.session_tracker.reference_count(session_id);
    let mut predecessor_incarnations = Vec::new();
    let mut predecessor_identities = Vec::new();
    let mut predecessor_registrations = Vec::new();
    let mut retirements = Vec::new();
    let mut repeated_first_retirement = None;
    for generation in 0..FAILED_GENERATIONS {
        predecessor_incarnations.push(output_incarnation(&stream));
        let predecessor_identity = ServerCarrierPathIdentity {
            session_id,
            underlay: current.underlay(),
            path_id,
            path_instance_id: current.path_instance_id(),
        };
        predecessor_identities.push(predecessor_identity);
        let retirement = if generation == 0 {
            registry.retire_carrier_path(predecessor_identity)
        } else {
            current.begin_retirement()
        };
        if generation == 0 {
            repeated_first_retirement = Some(current.begin_retirement());
            let (old_commands, old_receivers) = reliable_path_command_channels(8);
            command_receivers.push(old_receivers);
            assert!(matches!(
                registry
                    .open_or_attach(ServerStreamOpenRequest {
                        session_id,
                        stream_id,
                        target: target.clone(),
                        initial_demand: StreamDemandHint::Throughput,
                        return_plan: Default::default(),
                        attachment: ServerStreamPathAttachment {
                            path_registration: current.clone(),
                            commands: old_commands,
                            max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                        },
                        mux_limits: MuxLimits::default(),
                    })
                    .expect("retired predecessor open is classified"),
                ServerReliableStreamOpen::Rejected
            ));
        }
        retirements.push(retirement);
        assert!(
            port.management_snapshot().paths.is_empty(),
            "physical predecessor ownership ends before actor cleanup"
        );
        assert_eq!(
            registry.session_tracker.reference_count(session_id) + 1,
            stable_reference_count,
            "the carrier session reference is released synchronously"
        );

        let mut successor_metrics = native_quic_test_metrics(path_id);
        successor_metrics.delivery_rate_bps = 200 + generation as u64;
        let successor = port
            .register_carrier_path(
                session_id,
                UnderlayProtocol::Udp,
                path_id,
                ServerLocalPathProperties {
                    initial_metrics: Some(successor_metrics),
                    ..ServerLocalPathProperties::default()
                },
                PrincipalPermit::for_test("test-peer"),
            )
            .expect("same-key successor is not gated by predecessor cleanup");
        let (successor_commands, successor_receivers) = reliable_path_command_channels(8);
        command_receivers.push(successor_receivers);
        assert!(matches!(
            registry
                .open_or_attach(ServerStreamOpenRequest {
                    session_id,
                    stream_id,
                    target: target.clone(),
                    initial_demand: StreamDemandHint::Throughput,
                    return_plan: Default::default(),
                    attachment: ServerStreamPathAttachment {
                        path_registration: successor.clone(),
                        commands: successor_commands,
                        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                    },
                    mux_limits: MuxLimits::default(),
                })
                .expect("attach same-key successor"),
            ServerReliableStreamOpen::Existing(_)
        ));
        assert_eq!(
            registry.session_tracker.reference_count(session_id),
            stable_reference_count,
            "failed generations cannot accumulate carrier tracker references"
        );
        let paths = registry
            .registered_path_instances
            .lock()
            .expect("server active path instance lock");
        assert_eq!(paths.instances.len(), 1);
        assert_eq!(paths.logical_instances.len(), 1);
        assert_eq!(paths.session_path_counts.get(&session_id), Some(&1));
        drop(paths);
        assert_eq!(
            match &stream.output {
                ReliablePathStreamOutput::Switchable(binding) =>
                    binding.sender_path_targets(TrafficClass::Throughput, 1)[0]
                        .observation
                        .path_instance_id,
                ReliablePathStreamOutput::Fixed(_) => {
                    panic!("expected switchable response output")
                }
            },
            successor.path_instance_id()
        );
        predecessor_registrations.push(current);
        current = successor;
    }

    assert_eq!(
        port.management_snapshot().paths.len(),
        1,
        "only the current physical carrier remains registry-owned"
    );
    for incarnation in &predecessor_incarnations {
        assert!(
            stream.has_output_incarnation(key, *incarnation),
            "actor-backpressured cleanup remains exact and pending"
        );
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while predecessor_incarnations
            .iter()
            .any(|incarnation| stream.has_output_incarnation(key, *incarnation))
        {
            let _ = tokio::time::timeout(Duration::from_millis(10), stream.recv_frame()).await;
        }
    })
    .await
    .expect("all exact predecessor detaches drain once the actor resumes");
    for retirement in retirements {
        tokio::time::timeout(Duration::from_secs(1), retirement.wait())
            .await
            .expect("each predecessor retirement completes after its detach");
    }
    tokio::time::timeout(
        Duration::from_secs(1),
        repeated_first_retirement
            .expect("repeated first retirement")
            .wait(),
    )
    .await
    .expect("repeated retirement shares exact completion");

    for identity in predecessor_identities {
        registry.retire_carrier_path(identity).wait().await;
    }
    let snapshot = port.management_snapshot();
    assert_eq!(snapshot.paths.len(), 1);
    assert_eq!(
        snapshot.paths[0].path_instance_id,
        current.path_instance_id()
    );
    assert_eq!(
        registry.session_tracker.reference_count(session_id),
        stable_reference_count,
        "late predecessor cleanup cannot release the successor reference"
    );
    assert_eq!(
        match &stream.output {
            ReliablePathStreamOutput::Switchable(binding) =>
                binding.sender_path_targets(TrafficClass::Throughput, 1)[0]
                    .observation
                    .path_instance_id,
            ReliablePathStreamOutput::Fixed(_) => panic!("expected switchable response output"),
        },
        current.path_instance_id(),
        "predecessor detach completion cannot remove the successor incarnation"
    );
    drop(predecessor_registrations);
    drop(command_receivers);
    drop(current);
}

#[tokio::test]
async fn carrier_retirement_publishes_detach_to_each_stream_without_cross_stream_head_of_line() {
    let registry = constrained_registry(2, 1);
    let port = registry.path_port();
    let session_id = SessionId(641);
    let registration = port
        .register_carrier_path(
            session_id,
            UnderlayProtocol::Udp,
            PathId(0),
            Default::default(),
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("initial carrier");
    let key = CarrierPathKey {
        underlay: registration.underlay(),
        path_id: registration.path_id(),
    };
    let mut accepted_streams = Vec::new();
    let mut streams = Vec::new();
    let mut command_receivers = Vec::new();
    for stream_id in [StreamId(1), StreamId(2)] {
        let (commands, receivers) = reliable_path_command_channels(8);
        let mut accepted = match registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                initial_demand: StreamDemandHint::Throughput,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: registration.clone(),
                    commands,
                    max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                },
                mux_limits: MuxLimits::default(),
            })
            .expect("open response stream")
        {
            ServerReliableStreamOpen::New(accepted, _) => accepted,
            _ => panic!("expected new response stream"),
        };
        let stream = accepted.take_stream();
        accepted_streams.push(accepted);
        streams.push((stream_id, stream));
        command_receivers.push(receivers);
    }

    // The retirement scan and this snapshot traverse the same immutable map.
    // Saturating both queues makes the first entry a deliberately dormant
    // stream and the second entry an independently runnable stream.
    let scan_order = registry
        .streams
        .lock()
        .expect("server reliable stream registry lock")
        .keys()
        .filter_map(|(registered_session, stream_id)| {
            (*registered_session == session_id).then_some(*stream_id)
        })
        .collect::<Vec<_>>();
    assert_eq!(scan_order.len(), 2);
    for (stream_id, _) in &streams {
        let mut filled = 0u64;
        loop {
            let frame = Frame::StreamData {
                stream_id: *stream_id,
                offset: filled,
                payload: bytes::Bytes::from_static(b"x"),
            };
            match port
                .try_route_frame(&registration, *stream_id, frame)
                .expect("route request data")
            {
                ServerStreamFrameRoute::Routed => filled += 1,
                ServerStreamFrameRoute::Backpressured(_) => break,
            }
        }
        assert!(filled > 0, "test must saturate each bounded actor queue");
    }

    let output_incarnation = |stream: &ReliablePathStream| match &stream.output {
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
    let dormant_id = scan_order[0];
    let runnable_id = scan_order[1];
    let dormant_incarnation = output_incarnation(
        &streams
            .iter()
            .find(|(stream_id, _)| *stream_id == dormant_id)
            .expect("dormant stream")
            .1,
    );
    let runnable_incarnation = output_incarnation(
        &streams
            .iter()
            .find(|(stream_id, _)| *stream_id == runnable_id)
            .expect("runnable stream")
            .1,
    );

    let retirement = registration.begin_retirement();
    tokio::task::yield_now().await;
    let runnable = &mut streams
        .iter_mut()
        .find(|(stream_id, _)| *stream_id == runnable_id)
        .expect("runnable stream")
        .1;
    tokio::time::timeout(Duration::from_secs(1), async {
        while runnable.has_output_incarnation(key, runnable_incarnation) {
            // PathDetached is applied internally and then recv_frame waits for
            // the next Product frame, so short cancellation exposes the state
            // transition without publishing an artificial successor frame.
            let _ = tokio::time::timeout(Duration::from_millis(10), runnable.recv_frame()).await;
        }
    })
    .await
    .expect("a dormant stream must not block detach publication to a runnable sibling");

    let dormant = &mut streams
        .iter_mut()
        .find(|(stream_id, _)| *stream_id == dormant_id)
        .expect("dormant stream")
        .1;
    assert!(
        dormant.has_output_incarnation(key, dormant_incarnation),
        "aggregate retirement still waits for every stream-local detach"
    );
    let mut retirement = Box::pin(retirement.wait());
    tokio::select! {
        biased;
        () = retirement.as_mut() => {
            panic!("aggregate retirement completed before the dormant stream applied detach");
        }
        _ = std::future::ready(()) => {}
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while dormant.has_output_incarnation(key, dormant_incarnation) {
            let _ = tokio::time::timeout(Duration::from_millis(10), dormant.recv_frame()).await;
        }
    })
    .await
    .expect("dormant stream eventually applies its ordered detach");
    tokio::time::timeout(Duration::from_secs(1), retirement)
        .await
        .expect("carrier retirement joins every stream-local detach");
    drop(command_receivers);
    drop(accepted_streams);
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
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration,
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected new response stream"),
    };

    assert_eq!(
        accepted.stream().max_offset,
        0,
        "OPEN_STREAM alone must not manufacture reverse-direction send credit"
    );
    accepted
        .admit_opening_path()
        .expect("publish opening admission and validation");
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(crate::runtime::path::commands::ReliablePathCommand::SendFrame(
            Frame::StreamMaxData {
                stream_id: accepted_stream_id,
                max_offset: 0,
            }
        )) if accepted_stream_id == stream_id
    ));
    accepted
        .publish_opening_path_validation()
        .await
        .expect("publish opening validation after admission");
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

#[tokio::test]
async fn new_stream_publishes_zero_admission_before_target_owner_runs() {
    let mux_limits = MuxLimits::default();
    let (registry, mut accepted_rx) =
        ServerReliableStreamRegistry::new_accepting(mux_limits.max_streams);
    let port = registry.path_port();
    let session_id = SessionId(701);
    let stream_id = StreamId(9);
    let registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    port.record_path_proof_success(
        &registration,
        PathProofObservation {
            proof_id: 1,
            elapsed: Duration::from_millis(1),
            sent_at: Instant::now() - Duration::from_millis(1),
        },
    );
    let (commands, mut receivers) = reliable_path_command_channels(8);

    assert!(matches!(
        port.open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .await
        .expect("admit new logical stream"),
        ServerStreamOpenOutcome::New(TrafficClass::Latency)
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
            stream_id: accepted_stream_id,
            max_offset: 0,
        })) if accepted_stream_id == stream_id
    ));
    let accepted = accepted_rx
        .recv()
        .await
        .expect("one target-establishment owner");
    assert_eq!(accepted.stream().stream_id, stream_id);
    assert_eq!(
        accepted.stream().capacity_notifies().len(),
        1,
        "opening output remains live before reattachment",
    );
    assert!(
        accepted_rx.try_recv().is_err(),
        "carrier admission must create exactly one target-establishment owner",
    );

    let alternate = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(1),
        ServerLocalPathProperties {
            config_ordinal: 1,
            ..ServerLocalPathProperties::default()
        },
    );
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        port.open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: alternate.clone(),
                commands: alternate_commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .await
        .expect("reattach the same logical stream"),
        ServerStreamOpenOutcome::Existing(TrafficClass::Latency)
    ));
    assert!(
        accepted_rx.try_recv().is_err(),
        "reattachment must not create a second target-establishment owner",
    );
    assert_eq!(
        accepted.stream().capacity_notifies().len(),
        2,
        "reattachment adds a live output without replacing the opening output",
    );

    accepted.stream().publish_max_data(4096);
    for (label, output) in [
        ("opening", &mut receivers),
        ("alternate", &mut alternate_receivers),
    ] {
        let mut established = false;
        for _ in 0..2 {
            match try_recv_reliable_path_priority_command(output) {
                Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
                    stream_id: accepted_stream_id,
                    max_offset: 4096,
                })) if accepted_stream_id == stream_id => {
                    established = true;
                    break;
                }
                Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. })) => {}
                Some(_) => panic!("unexpected {label} target-establishment command variant"),
                None => panic!("{label} target-establishment command was not queued"),
            }
        }
        assert!(
            established,
            "{label} live attachment receives nonzero credit"
        );
    }

    let late = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(2),
        ServerLocalPathProperties {
            config_ordinal: 2,
            ..ServerLocalPathProperties::default()
        },
    );
    let (late_commands, mut late_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        port.open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: late.clone(),
                commands: late_commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .await
        .expect("attach after target establishment"),
        ServerStreamOpenOutcome::Existing(TrafficClass::Latency)
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut late_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
            stream_id: accepted_stream_id,
            max_offset: 4096,
        })) if accepted_stream_id == stream_id
    ));
    assert!(
        accepted_rx.try_recv().is_err(),
        "late attachment must reuse the established target owner",
    );

    accepted.close().await;
    assert_eq!(
        registry.management_snapshot().active_streams,
        0,
        "closing the sole target owner releases the logical stream registry entry",
    );
}

#[tokio::test]
async fn capacity_one_path_proof_backpressure_preserves_admitted_target_owner() {
    for (ordinal, underlay) in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp]
        .into_iter()
        .enumerate()
    {
        let mux_limits = MuxLimits::default();
        let (registry, mut accepted_rx) =
            ServerReliableStreamRegistry::new_accepting(mux_limits.max_streams);
        let port = registry.path_port();
        let session_id = SessionId(710 + ordinal as u64);
        let stream_id = StreamId(20 + ordinal as u64);
        let registration = port.register_test_carrier_path(
            session_id,
            underlay,
            PathId(0),
            ServerLocalPathProperties::default(),
        );
        let (commands, mut receivers) = reliable_path_command_channels(1);

        assert!(matches!(
            port.open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                initial_demand: StreamDemandHint::Latency,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: registration.clone(),
                    commands,
                    max_frame_payload_bytes: mux_limits.max_payload_bytes,
                },
                mux_limits,
            })
            .await
            .expect("capacity-one carrier admission remains nonterminal"),
            ServerStreamOpenOutcome::New(TrafficClass::Latency)
        ));
        let accepted = accepted_rx
            .recv()
            .await
            .expect("exactly one target-establishment owner");
        assert_eq!(accepted.stream().stream_id, stream_id);
        assert!(accepted_rx.try_recv().is_err());

        let mut validation = Box::pin(accepted.publish_opening_path_validation());
        assert!(matches!(
            futures::poll!(validation.as_mut()),
            std::task::Poll::Pending
        ));
        assert_eq!(
            registry.management_snapshot().active_streams,
            1,
            "proof backpressure cannot retire the admitted logical owner",
        );
        let admission = try_recv_reliable_path_priority_command(&mut receivers)
            .expect("zero-credit admission remains queued");
        let pending_bytes = reliable_path_command_pending_bytes(&admission);
        assert!(matches!(
            admission,
            ReliablePathCommand::SendFrame(Frame::StreamMaxData {
                stream_id: admitted_stream_id,
                max_offset: 0,
            }) if admitted_stream_id == stream_id
        ));
        receivers.release_pending_command_bytes(pending_bytes);

        validation
            .await
            .expect("proof publication resumes after zero admission drains");
        let proof = try_recv_reliable_path_priority_command(&mut receivers)
            .expect("opening proof follows zero admission");
        let pending_bytes = reliable_path_command_pending_bytes(&proof);
        assert!(matches!(
            proof,
            ReliablePathCommand::SendFrame(Frame::PathProofData {
                path_id: PathId(0),
                payload,
                ..
            }) if !payload.is_empty()
        ));
        receivers.release_pending_command_bytes(pending_bytes);
        accepted.close().await;
    }
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
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
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
            startup_rate_prior: crate::transport::RateHint::Unknown,
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
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
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
                initial_demand: StreamDemandHint::Throughput,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: registration.clone(),
                    commands: replacement_commands,
                    max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                },
                mux_limits: MuxLimits::default(),
            },)
            .expect("replace closed response output"),
        ServerReliableStreamOpen::Existing(_)
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
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: first_registration.clone(),
                commands: first_commands,
                max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
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
                initial_demand: StreamDemandHint::Throughput,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: replacement_registration.clone(),
                    commands: replacement_commands,
                    max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                },
                mux_limits: MuxLimits::default(),
            })
            .expect("attach replacement response path"),
        ServerReliableStreamOpen::Existing(_)
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
    first_metrics.bytes_in_flight_observed = false;
    first_metrics.queue_observed = true;
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
            startup_rate_prior: crate::transport::RateHint::Unknown,
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
            startup_rate_prior: crate::transport::RateHint::Unknown,
            initial_metrics: Some(second_metrics),
        },
    );
    port.record_local_path_metrics(&first, first_metrics, false);

    let stale_identity = ServerCarrierPathIdentity {
        session_id: first_session,
        underlay: UnderlayProtocol::Udp,
        path_id,
        path_instance_id: crate::model::path::next_carrier_path_instance_id(),
    };
    let (stale_retirement, _) = watch::channel(false);
    registry
        .registered_path_instances
        .lock()
        .expect("server active path instance lock")
        .instances
        .insert(
            server_physical_path_key(stale_identity),
            ServerRegisteredPath {
                local: ServerLocalPathProperties::default(),
                state: PeerPathState::Draining,
                peer_usage: None,
                native_capacity_epoch: 0,
                apply_authority: ServerCarrierPathApplyAuthority::new(),
                path_proof: None,
                retirement_started: true,
                retirement_completion: stale_retirement,
            },
        );
    let mut stale_metrics = first_metrics;
    stale_metrics.metric_epoch = stale_metrics.metric_epoch.wrapping_add(1);
    stale_metrics.delivery_rate_bps = 999;
    registry.record_local_path_metrics(stale_identity, stale_metrics, false);

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
        .find(|path| {
            path.session_id == first_session && path.path_instance_id == first.path_instance_id()
        })
        .expect("managed path");
    assert_eq!(managed.state, PeerPathState::Draining);
    assert_eq!(managed.configured_index, 0);
    assert!(managed.policy.backup);
    assert_eq!(managed.usage, None);
    assert_eq!(managed.source, Some("local_sender"));
    let managed_metrics = managed.metrics.expect("metrics");
    assert_eq!(managed_metrics.delivery_rate_bps, 111);
    assert!(!managed_metrics.bytes_in_flight_observed);
    assert!(managed_metrics.queue_observed);
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
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
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
async fn exact_requalification_ack_queue_backpressure_is_stream_owned_for_tcp_and_quic() {
    for (ordinal, underlay) in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp]
        .into_iter()
        .enumerate()
    {
        let registry = Arc::new(ServerReliableStreamRegistry::new(4));
        let port = registry.path_port();
        let session_id = SessionId(910 + ordinal as u64);
        let stream_id = StreamId(40 + ordinal as u64);
        let registration = port.register_test_carrier_path(
            session_id,
            underlay,
            PathId(0),
            ServerLocalPathProperties::default(),
        );
        let (commands, mut receivers) = reliable_path_command_channels(1);
        let mut accepted = match registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                initial_demand: StreamDemandHint::Throughput,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: registration.clone(),
                    commands: commands.clone(),
                    max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                },
                mux_limits: MuxLimits::default(),
            })
            .expect("open response stream")
        {
            ServerReliableStreamOpen::New(accepted, _) => accepted,
            _ => panic!("expected new response stream"),
        };
        let mut product_stream = accepted.take_stream();
        commands
            .try_enqueue_admitted_frame(
                Frame::Ping {
                    nonce: ordinal as u64,
                },
                TrafficClass::Control,
            )
            .expect("fill exact control queue");
        let probe = Frame::StreamRequalifyData {
            stream_id,
            probe_id: 71,
            offset: 4096,
            payload: bytes::Bytes::from(vec![0x5a; 256]),
        };
        assert!(matches!(
            port.try_route_frame(&registration, stream_id, probe)
                .expect("try exact probe route"),
            ServerStreamFrameRoute::Routed
        ));
        let binding = match &product_stream.output {
            ReliablePathStreamOutput::Switchable(binding) => binding.clone(),
            ReliablePathStreamOutput::Fixed(_) => panic!("expected switchable response output"),
        };
        assert!(
            binding.has_pending_request_requalification_ack(),
            "a full return queue retains the exact ACK above the ingress carrier"
        );

        let healthy = Frame::StreamData {
            stream_id,
            offset: 0,
            payload: bytes::Bytes::from_static(b"healthy"),
        };
        assert!(matches!(
            port.try_route_frame(&registration, stream_id, healthy.clone()),
            Ok(ServerStreamFrameRoute::Routed)
        ));
        assert_eq!(
            product_stream
                .recv_frame()
                .await
                .expect("healthy Product frame"),
            healthy,
            "retaining an exact ACK must not block Product actor progress"
        );

        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::Ping { .. }))
        ));
        assert!(
            binding
                .retry_pending_request_requalification_ack()
                .expect("retry stream-owned exact ACK")
        );
        assert!(!binding.has_pending_request_requalification_ack());
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamRequalifyAck {
                stream_id: ack_stream,
                probe_id: 71,
                offset: 4096,
                payload_bytes: 256,
            })) if ack_stream == stream_id
        ));
    }
}

#[test]
fn request_requalification_ack_can_return_on_a_healthy_same_session_sibling() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(912);
    let stream_id = StreamId(42);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let mux_limits = MuxLimits::default();
    let carrying = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let sibling = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(1),
        ServerLocalPathProperties {
            config_ordinal: 1,
            ..ServerLocalPathProperties::default()
        },
    );
    let (carrying_commands, mut carrying_receivers) = reliable_path_command_channels(1);
    let _accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: carrying.clone(),
                commands: carrying_commands.clone(),
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected new response stream"),
    };

    let (sibling_commands, mut sibling_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
                initial_demand: StreamDemandHint::Throughput,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: sibling.clone(),
                    commands: sibling_commands,
                    max_frame_payload_bytes: mux_limits.max_payload_bytes,
                },
                mux_limits,
            })
            .expect("attach authenticated same-session sibling"),
        ServerReliableStreamOpen::Existing(_)
    ));

    // Attachment establishment may replay retained control state. Remove that
    // unrelated work so only the carrying queue is full in this RED.
    while try_recv_reliable_path_priority_command(&mut carrying_receivers).is_some() {}
    while try_recv_reliable_path_priority_command(&mut sibling_receivers).is_some() {}

    carrying_commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 912 }, TrafficClass::Control)
        .expect("fill carrying attachment control queue");
    let probe = Frame::StreamRequalifyData {
        stream_id,
        probe_id: 73,
        offset: 8192,
        payload: bytes::Bytes::from(vec![0x5b; 512]),
    };
    match port.try_route_frame(&carrying, stream_id, probe) {
        Ok(ServerStreamFrameRoute::Routed) => {}
        Ok(ServerStreamFrameRoute::Backpressured(_)) => {
            panic!("healthy sibling must carry the ACK, but routing remained backpressured")
        }
        Err(error) => panic!("healthy sibling ACK routing failed: {error}"),
    }
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut carrying_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 912 }))
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut sibling_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamRequalifyAck {
            stream_id: ack_stream,
            probe_id: 73,
            offset: 8192,
            payload_bytes: 512,
        })) if ack_stream == stream_id
    ));
}

#[test]
fn attachment_identity_is_immutable_and_cannot_overwrite_live_response_lane() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(16));
    let port = registry.path_port();
    let session_id = SessionId(704);
    let stream_id = StreamId(12);
    let limits = MuxLimits::default();
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let first_registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        Default::default(),
    );
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: first_registration,
                commands: first_commands,
                max_frame_payload_bytes: limits.max_payload_bytes,
            },
            mux_limits: limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, TrafficClass::Latency) => accepted,
        _ => panic!("expected new latency response stream"),
    };
    let ReliablePathStreamOutput::Switchable(binding) = &accepted.stream().output else {
        panic!("expected switchable response binding");
    };
    let binding = binding.clone();

    binding.set_lane(TrafficClass::Throughput);
    let second_registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(1),
        Default::default(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: target.clone(),
                initial_demand: StreamDemandHint::Latency,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: second_registration,
                    commands: second_commands,
                    max_frame_payload_bytes: limits.max_payload_bytes,
                },
                mux_limits: limits,
            })
            .expect("attach matching response output"),
        ServerReliableStreamOpen::Existing(TrafficClass::Throughput)
    ));
    assert_eq!(binding.lane(), TrafficClass::Throughput);

    let accepted_generation = binding.output_membership_generation();
    let mismatched_registration = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(2),
        Default::default(),
    );
    let (mismatched_commands, _mismatched_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
                initial_demand: StreamDemandHint::Throughput,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: mismatched_registration,
                    commands: mismatched_commands,
                    max_frame_payload_bytes: limits.max_payload_bytes,
                },
                mux_limits: limits,
            })
            .expect("reject mismatched immutable demand"),
        ServerReliableStreamOpen::Rejected
    ));
    assert_eq!(
        binding.output_membership_generation(),
        accepted_generation,
        "a rejected attachment must not change membership"
    );
    assert_eq!(
        binding.lane(),
        TrafficClass::Throughput,
        "a later wire hint must not overwrite sender-local response demand"
    );
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
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: tcp.clone(),
                commands: tcp_commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
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
                initial_demand: StreamDemandHint::Throughput,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: udp.clone(),
                    commands: udp_commands,
                    max_frame_payload_bytes: mux_limits.max_payload_bytes,
                },
                mux_limits,
            })
            .expect("attach QUIC response output"),
        ServerReliableStreamOpen::Existing(_)
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
            initial_demand: StreamDemandHint::Throughput,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
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
async fn sibling_return_plan_final_withdraws_omitted_output_at_ordered_detach_boundary() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(4));
    let port = registry.path_port();
    let session_id = SessionId(915);
    let stream_id = StreamId(45);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let mux_limits = MuxLimits::default();
    let opening = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties::default(),
    );
    let sibling = port.register_test_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        PathId(1),
        ServerLocalPathProperties {
            config_ordinal: 1,
            ..ServerLocalPathProperties::default()
        },
    );
    let return_plan = |phase, candidate_ordinal| StreamReturnPlan {
        trigger_bytes: 58_400,
        candidate_total: 2,
        candidate_tier: PathUsage::Available,
        phase,
        candidate_ordinal,
    };
    let (opening_commands, _opening_receivers) = reliable_path_command_channels(8);
    let mut accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            return_plan: return_plan(StreamAttachmentPhase::Startup, 0),
            attachment: ServerStreamPathAttachment {
                path_registration: opening.clone(),
                commands: opening_commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream with frozen return plan")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected new response stream"),
    };
    let (sibling_commands, _sibling_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
                initial_demand: StreamDemandHint::Throughput,
                return_plan: return_plan(StreamAttachmentPhase::Startup, 1),
                attachment: ServerStreamPathAttachment {
                    path_registration: sibling.clone(),
                    commands: sibling_commands,
                    max_frame_payload_bytes: mux_limits.max_payload_bytes,
                },
                mux_limits,
            })
            .expect("enroll authenticated sibling startup output"),
        ServerReliableStreamOpen::Existing(_)
    ));

    let mut stream = accepted.take_stream();
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable response binding");
    };
    let binding = binding.clone();
    let opening_key = CarrierPathKey {
        underlay: opening.underlay(),
        path_id: opening.path_id(),
    };
    let opening_target = binding
        .sender_path_targets(TrafficClass::Throughput, 58_400)
        .into_iter()
        .find(|candidate| candidate.observation.key == opening_key)
        .expect("opening output before FINAL");
    let sibling_key = CarrierPathKey {
        underlay: sibling.underlay(),
        path_id: sibling.path_id(),
    };
    let sibling_target = binding
        .sender_path_targets(TrafficClass::Throughput, 58_400)
        .into_iter()
        .find(|candidate| candidate.observation.key == sibling_key)
        .expect("sibling output before FINAL");
    binding.record_original_flight(
        opening_key,
        &Frame::StreamData {
            stream_id,
            offset: 0,
            payload: bytes::Bytes::from(vec![0x5a; 58_400]),
        },
    );

    let final_frame = Frame::StreamReturnPlanFinal {
        stream_id,
        retained_ordinals: vec![1],
    };
    assert!(matches!(
        port.try_route_frame(&sibling, stream_id, final_frame.clone()),
        Ok(ServerStreamFrameRoute::Routed)
    ));
    assert!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .into_iter()
            .all(|candidate| candidate.observation.key != opening_key),
        "FINAL must withdraw the omitted exact startup output immediately",
    );
    assert!(binding.has_output_incarnation(opening_key, opening_target.observation.incarnation,));
    assert!(
        binding.uncovered_failed_original_ranges().is_empty(),
        "the omitted output's flight remains owned until its ordered boundary",
    );

    let sentinel = Frame::StreamMaxData {
        stream_id,
        max_offset: 65_536,
    };
    port.route_frame(&sibling, stream_id, sentinel.clone())
        .await
        .expect("queue a product frame after the omitted-output boundary");
    assert_eq!(
        stream
            .recv_frame()
            .await
            .expect("consume omitted-output boundary"),
        sentinel,
    );
    assert!(!binding.has_output_incarnation(opening_key, opening_target.observation.incarnation,));
    assert_eq!(
        binding.uncovered_failed_original_ranges(),
        vec![OffsetRange {
            start: 0,
            end: 58_400,
        }],
        "consuming the omitted output's boundary exposes its exact recovery debt",
    );

    port.detach_path(&sibling, stream_id)
        .expect("detach the sibling that carried FINAL");
    let after_sibling_detach = Frame::StreamMaxData {
        stream_id,
        max_offset: 131_072,
    };
    port.route_frame(&sibling, stream_id, after_sibling_detach.clone())
        .await
        .expect("queue a product frame after the carrying attachment boundary");
    assert_eq!(
        stream
            .recv_frame()
            .await
            .expect("consume carrying attachment boundary"),
        after_sibling_detach,
    );
    assert!(!binding.has_output_incarnation(sibling_key, sibling_target.observation.incarnation,));
    assert!(matches!(
        port.try_route_frame(&sibling, stream_id, final_frame),
        Ok(ServerStreamFrameRoute::Routed)
    ));
    assert!(
        port.try_route_frame(
            &sibling,
            stream_id,
            Frame::StreamReturnPlanFinal {
                stream_id,
                retained_ordinals: vec![0],
            },
        )
        .is_err(),
        "an unequal delayed FINAL is a protocol error",
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
            initial_demand: StreamDemandHint::Latency,
            return_plan: Default::default(),
            attachment: ServerStreamPathAttachment {
                path_registration: registration.clone(),
                commands,
                max_frame_payload_bytes: mux_limits.max_payload_bytes,
            },
            mux_limits,
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
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
