use super::*;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::path::PathPolicy;
use crate::mux::MuxLimits;
use crate::protocol::{PathMetricDirection, PathUsage};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::path::{ServerLocalPathProperties, ServerStreamPathAttachment};
use crate::runtime::stream::response::{ServerPathMetricsEntry, ServerPathMetricsSource};
use std::net::SocketAddr;
use std::time::Duration;

fn receipt_test_metrics(path_id: PathId) -> PathMetrics {
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
        inflight_limit_bytes: 100,
        inflight_hi_bytes: 100,
        confidence_ppm: 0,
        app_limited: true,
        has_ack_derived_data_sample: false,
        data_sample_count: 0,
        data_sample_bytes: 0,
    }
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
    let registration = port.register_carrier_path(
        session_id,
        UnderlayProtocol::Udp,
        path_id,
        ServerLocalPathProperties {
            config_ordinal: 4,
            policy: local_policy,
            initial_metrics: None,
        },
    );
    let path_instance_id = registration.path_instance_id();
    port.record_peer_path_usage(&registration, 7, PathUsage::Backup);
    let metrics = receipt_test_metrics(path_id);
    let accepted_at = Instant::now();
    let candidate = QuicCapacityProofCandidate {
        token: 41,
        train_bytes: 100,
        sample_floor_bytes: 100,
        accounting_slack_bytes: 12,
        warmup_bytes: 0,
        required_proof_bytes: 88,
        written_bytes: 100,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: 100,
        proof_elapsed: Duration::from_millis(10),
        rate_bps: 80_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
        proof_validity: Duration::from_secs(1),
    };
    registry
        .path_metrics
        .lock()
        .expect("test path metrics lock")
        .insert(
            (session_id, UnderlayProtocol::Udp, path_id, path_instance_id),
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: Some(candidate),
            },
        );

    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, first_receivers) = reliable_path_command_channels(8);
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
        .expect("inherited receipt target");
    assert!(inherited.observation.has_bulk_rate_evidence);
    assert_eq!(inherited.observation.snapshot.confidence, 1.0);
    assert_eq!(
        inherited.observation.snapshot.peer_usage,
        Some(PathUsage::Backup)
    );
    assert_eq!(
        inherited.observation.snapshot.policy, local_policy,
        "response scheduling must inherit the accepting listener's local policy",
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
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
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
    let replacement = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key.path_id == path_id)
        .expect("replacement receipt target");
    assert!(replacement.observation.has_bulk_rate_evidence);
    assert_eq!(replacement.observation.snapshot.confidence, 1.0);
    assert_eq!(
        replacement.observation.snapshot.peer_usage,
        Some(PathUsage::Backup)
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
fn peer_status_snapshot_is_session_scoped_and_tracks_registration_lifetime() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(8));
    let port = registry.path_port();
    let first_session = SessionId(801);
    let second_session = SessionId(802);
    let path_id = PathId(4);
    let mut first_metrics = receipt_test_metrics(path_id);
    first_metrics.delivery_rate_bps = 111;
    let mut second_metrics = receipt_test_metrics(path_id);
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
    port.record_local_path_metrics(&first, first_metrics);

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
