use super::super::ResponseStreamBinding;
use super::super::evidence::ServerPathMetricsSource;
use super::super::next_server_carrier_path_instance_id;
use super::super::session::ServerPathLaneTracker;
use super::super::test_support::{mark_test_quic_output_carrier_bulk_proven, output_entry_for_key};
use super::super::topology::{
    RESPONSE_OWNER_MIXED_SEEN, RESPONSE_OWNER_TCP_SEEN, ResponseStreamAttachOutcome,
    ResponseStreamOutputEntry,
};
use super::{server_bulk_output_eta_ms, server_bulk_output_snapshot};
use crate::model::admission::{
    ReliableSourceServiceStagingContext, ReliableSourceStagingContext,
    bulk_service_feed_reservoir_payload_bytes, reliable_relay_source_staging_owner_tail_headroom,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES, reliable_bulk_carrier_feed_quantum_bytes,
    reliable_relay_buffer_len, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{
    PathId, PathMetricDirection, PathMetrics, SessionId, StreamOpenRole, UnderlayProtocol,
};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::{default_path_rate_bps, default_path_srtt_ms, metric_epoch_now};
use crate::scheduler::{FlowLane, PathSnapshot};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

#[test]
fn tcp_response_snapshot_persistent_delivery_samples_override_default_prior() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let prior_rate = default_path_rate_bps(UnderlayProtocol::Tcp);
    let entry = ResponseStreamOutputEntry {
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
        owner_data_in_flight_bytes: 0,
        bytes_in_flight: 0,
        product_queue_bytes: 0,
        product_progress_rate_bps: Some(prior_rate / 10.0),
        delivery_rate_bps: Some(prior_rate / 10.0),
        tcp_ack_clock_rate_bps: None,
        tcp_product_rate_evidence: None,
        tcp_capacity_prior: None,
        srtt_ms: Some(default_path_srtt_ms(UnderlayProtocol::Tcp)),
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        owner_data_acked_bytes: reliable_subflow_startup_sample_limit_bytes(MuxLimits::default()),
        local_path_metrics: None,
        peer_path_metrics: None,
    };

    let lane_tracker = ServerPathLaneTracker::default();
    let snapshot = server_bulk_output_snapshot(
        &entry,
        SessionId(77),
        FlowLane::Throughput,
        &lane_tracker,
        MuxLimits::default(),
        Instant::now(),
    );

    assert_eq!(snapshot.delivery_rate_bps, prior_rate / 10.0);
}

#[test]
fn response_eta_uses_delivered_rate_not_inflated_quic_pacing_rate() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let mut baseline = PathSnapshot::new(key.path_id, key.underlay, 100.0, 50_000_000.0);
    baseline.pacing_rate_bps = 50_000_000.0;
    baseline.confidence = 1.0;

    let mut inflated_pacing = baseline;
    inflated_pacing.pacing_rate_bps = 5_000_000_000.0;

    let payload_bytes = 64 * 1024;
    let baseline_eta = server_bulk_output_eta_ms(
        key,
        baseline,
        Some(key),
        FlowLane::Throughput,
        payload_bytes,
        MuxLimits::default(),
    );
    let inflated_eta = server_bulk_output_eta_ms(
        key,
        inflated_pacing,
        Some(key),
        FlowLane::Throughput,
        payload_bytes,
        MuxLimits::default(),
    );

    assert!(
        (baseline_eta - inflated_eta).abs() < 0.001,
        "QUIC pacing is carrier send permission, not delivered product throughput"
    );
}

#[test]
fn response_subflow_eta_uses_owner_quantum_not_service_horizon() {
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let subflow = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let mut snapshot = PathSnapshot::new(subflow.path_id, subflow.underlay, 80.0, 40_000_000.0);
    snapshot.confidence = 1.0;

    let eta_ms = server_bulk_output_eta_ms(
        subflow,
        snapshot,
        Some(service),
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
    );

    assert!(
        eta_ms < 100.0,
        "Subflow ETA must model the next assigned owner range, not a full Service horizon; got {eta_ms:.3}ms"
    );
}

#[test]
fn independent_source_staging_requires_live_mixed_owner_underlays() {
    let (active_commands, active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        UnderlayProtocol::Tcp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
    );
    let mut receivers = vec![active_receivers];
    assert!(!binding.has_live_mixed_owner_underlays());
    assert!(
        !binding
            .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
            .independent_source_staging
    );
    assert_eq!(
        binding.owner_underlay_history.load(Ordering::Acquire),
        RESPONSE_OWNER_TCP_SEEN
    );

    for (path_id, underlay, role, expected, expected_history) in [
        (
            1,
            UnderlayProtocol::Tcp,
            StreamOpenRole::Validation,
            false,
            RESPONSE_OWNER_TCP_SEEN,
        ),
        (
            2,
            UnderlayProtocol::Udp,
            StreamOpenRole::Repair,
            false,
            RESPONSE_OWNER_TCP_SEEN,
        ),
        (
            3,
            UnderlayProtocol::Udp,
            StreamOpenRole::Validation,
            true,
            RESPONSE_OWNER_MIXED_SEEN,
        ),
    ] {
        let (commands, output_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                underlay,
                PathId(path_id),
                commands,
                FlowLane::Throughput,
                role,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached,
        );
        assert_eq!(
            binding.has_live_mixed_owner_underlays(),
            expected,
            "only a live owner-capable cross-underlay output enables independent raw staging",
        );
        assert_eq!(
            binding
                .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
                .independent_source_staging,
            expected,
            "the composite relay snapshot must use the same live-family policy",
        );
        assert_eq!(
            binding.owner_underlay_history.load(Ordering::Acquire),
            expected_history,
            "Repair-only attachments must retain the single-family fast path",
        );
        receivers.push(output_receivers);
    }
}

#[test]
fn response_relay_read_snapshot_keeps_source_evidence_on_the_ordered_service() {
    let session_id = SessionId(42);
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    let alternate_commands_for_detach = alternate_commands.clone();
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let (latency_commands, _latency_receivers) = reliable_path_command_channels(8);
    let _alternate_latency_flow = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        alternate.underlay,
        alternate.path_id,
        latency_commands,
        FlowLane::Latency,
        MuxLimits::default(),
        lane_tracker,
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let service_entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == service)
            .expect("ordered Service output");
        service_entry.delivery_rate_bps = Some(1_000_000.0);
        service_entry.srtt_ms = Some(500.0);
        service_entry.delivery_samples = 1;
    }
    binding.update_path_metrics(
        alternate,
        PathMetrics {
            path_id: alternate.path_id,
            underlay: alternate.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 5_000,
            srtt_us: 5_000,
            rttvar_us: 500,
            jitter_us: 500,
            delivery_rate_bps: 1_000_000_000,
            pacing_rate_bps: 1_000_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
            inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
        },
        ServerPathMetricsSource::LocalSender,
    );

    let before = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
    assert!(before.send_path.is_some_and(|path| {
        path.id == alternate.path_id && path.underlay == alternate.underlay
    }));
    assert_eq!(
        before
            .send_path
            .expect("faster alternate send path")
            .active_latency_sensitive_flows,
        1
    );
    let source = before
        .source_service
        .expect("live ordered Service snapshot");
    assert_eq!(source.key, service);
    assert!(!source.has_bulk_rate_evidence);
    assert!(before.independent_source_staging);

    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let service_entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == service)
            .expect("ordered Service output");
        service_entry.product_progress_rate_bps = Some(1_000_000.0);
        service_entry.owner_data_acked_bytes =
            reliable_subflow_startup_sample_limit_bytes(binding.mux_limits());
    }
    let after = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
    let source = after.source_service.expect("live ordered Service snapshot");
    assert!(source.has_bulk_rate_evidence);
    assert_eq!(source.active_latency_sensitive_flows, 0);
    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: after.independent_source_staging,
                service: Some(ReliableSourceServiceStagingContext {
                    allows_product_envelope: true,
                    has_latency_pressure: source.active_latency_sensitive_flows > 0,
                    has_feed_evidence: source.has_service_feed_evidence,
                }),
            },
            FlowLane::Throughput,
            0,
            0,
            reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()),
            MuxLimits::default(),
        ),
        bulk_service_feed_reservoir_payload_bytes(
            reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()),
            MuxLimits::default(),
        ),
        "alternate-path latency pressure must not narrow exact-Service source staging"
    );
    let service_target = binding
        .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .find(|target| target.key == service)
        .expect("ordered Service sender target");
    assert_eq!(
        source.has_bulk_rate_evidence, service_target.has_bulk_rate_evidence,
        "source staging and sender admission must consume the same Service proof"
    );

    binding.detach(alternate, &alternate_commands_for_detach);
    assert!(
        !binding
            .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
            .independent_source_staging,
        "mixed-family source staging must end when the alternate family detaches"
    );
}

#[test]
fn udp_product_progress_matures_only_current_service_feed() {
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let service_entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == service)
            .expect("ordered Service output");
        service_entry.product_progress_rate_bps = Some(100_000_000.0);
        service_entry.delivery_rate_bps = Some(100_000_000.0);
        service_entry.srtt_ms = Some(20.0);
        service_entry.delivery_samples = u32::MAX;
        service_entry.owner_data_acked_bytes = u64::MAX;
    }

    let read = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
    let source = read.source_service.expect("live ordered Service snapshot");
    assert_eq!(source.key, service);
    let send_path = read.send_path.expect("single live Service send snapshot");
    assert_eq!(send_path.confidence, 1.0);
    assert!(send_path.product_progress_rate_bps.is_some());
    assert!(
        source.has_service_feed_evidence,
        "substantial uniquely owned product ACKs may release current-Service staging"
    );
    assert!(
        !source.has_bulk_rate_evidence,
        "product ACK timing must not mint optional QUIC placement authority"
    );
}

#[test]
fn udp_app_limited_carrier_progress_feeds_only_the_current_service() {
    let mux_limits = MuxLimits::default();
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
    );
    let mut entry = output_entry_for_key(&binding, service);
    mark_test_quic_output_carrier_bulk_proven(&mut entry, mux_limits);
    let metrics = PathMetrics {
        app_limited: true,
        ..entry
            .local_path_metrics
            .expect("test QUIC sender metrics")
            .metrics
    };
    binding.update_path_metrics(service, metrics, ServerPathMetricsSource::LocalSender);

    let read = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
    let source = read.source_service.expect("current response Service");
    assert!(source.has_service_feed_evidence);
    assert!(
        !source.has_bulk_rate_evidence,
        "an app-limited sample must not authorize optional placement"
    );

    let target = binding
        .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .find(|target| target.key == service)
        .expect("current Service sender target");
    assert!(target.is_active);
    assert!(target.has_service_feed_evidence);
    assert!(!target.has_bulk_rate_evidence);

    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(mux_limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let mut alternate_entry = binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries
        .iter()
        .find(|entry| entry.key == alternate)
        .expect("Validation output")
        .clone();
    mark_test_quic_output_carrier_bulk_proven(&mut alternate_entry, mux_limits);
    let alternate_metrics = PathMetrics {
        app_limited: true,
        ..alternate_entry
            .local_path_metrics
            .expect("alternate QUIC sender metrics")
            .metrics
    };
    binding.update_path_metrics(
        alternate,
        alternate_metrics,
        ServerPathMetricsSource::LocalSender,
    );
    let alternate_target = binding
        .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .find(|target| target.key == alternate)
        .expect("Validation sender target");
    assert!(!alternate_target.is_active);
    assert!(!alternate_target.has_service_feed_evidence);
    assert!(!alternate_target.has_bulk_rate_evidence);
}
