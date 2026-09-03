use super::super::ResponseStreamBinding;
use super::super::ack_clock::{
    ResponseAckClockRateEvidence, apply_response_ack_clock_release_samples,
};
use super::super::attachment::{
    ResponseProductRateEpoch, ResponseStreamOutputEntry, ResponseStreamOutputs,
};
use super::super::evidence::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_output_has_bulk_rate_evidence,
    server_output_has_bulk_rate_evidence_at, server_output_product_rate_epoch_has_bulk_evidence_at,
};
use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{
    qualify_product_assignment, stream_data_frame, stream_data_frame_at,
};
use super::{
    confidence_sample_denominator, server_bulk_output_snapshot, server_bulk_output_snapshot_at,
    server_output_confidence, server_output_confidence_at,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    reliable_bulk_carrier_feed_quantum_bytes, reliable_bulk_product_windows,
    reliable_bulk_unproven_exploration_limit_bytes, reliable_path_startup_sample_limit_bytes,
    reliable_product_feedback_window_bytes,
};
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, PathPolicy};
use crate::model::requalification::StreamPathQualification;
use crate::mux::MuxLimits;
use crate::protocol::{
    PathId, PathMetricDirection, PathMetrics, PathUsage, SessionId, UnderlayProtocol,
};
use crate::runtime::path::authority::NativeCarrierRateAuthorityHandle;
use crate::runtime::path::commands::{
    ReliablePathCommandSender, reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command,
};
use crate::runtime::path::{
    CarrierDeliveryRateSample, CarrierNativeWindowSample, PathProofObservation,
};
use crate::scheduler::{PathRateScope, TrafficClass};
use std::time::{Duration, Instant};

fn output_entry(
    key: CarrierPathKey,
    commands: ReliablePathCommandSender,
) -> ResponseStreamOutputEntry {
    let load_registration = commands.register_inactive_flow(TrafficClass::Throughput);
    ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        local_policy: PathPolicy::default(),
        incarnation: 1,
        commands,
        load_registration,
        original_data_in_flight_bytes: 0,
        qualification: StreamPathQualification::Qualified,
        product_qualification: Default::default(),
        bytes_in_flight: 0,
        product_rate_epoch: None,
        tcp_product_rate_evidence: None,
        srtt_ms: None,
        delivery_samples: 0,
        original_data_acked_bytes: 0,
        published_max_data_offset: 0,
        ack_publication: Default::default(),
        local_path_metrics: None,
        peer_path_metrics: None,
        native_scheduling_shape: None,
        peer_usage: None,
        peer_usage_sequence: None,
        path_proof: None,
    }
}

fn install_product_rate(entry: &mut ResponseStreamOutputEntry, rate_bps: f64) {
    let sample_floor = reliable_path_startup_sample_limit_bytes(MuxLimits::default());
    entry.product_rate_epoch = ResponseProductRateEpoch::new(
        rate_bps,
        1,
        sample_floor,
        Instant::now(),
        Duration::from_secs(60),
    );
    entry.original_data_acked_bytes = sample_floor;
}

fn install_raw_product_point_rate(entry: &mut ResponseStreamOutputEntry, rate_bps: f64) {
    entry.product_rate_epoch = ResponseProductRateEpoch::new(
        rate_bps,
        1,
        reliable_path_startup_sample_limit_bytes(MuxLimits::default()),
        Instant::now(),
        Duration::from_secs(60),
    );
}

#[test]
fn response_product_assignment_qualification_does_not_require_a_rate_epoch() {
    let mux_limits = MuxLimits::default();
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(16),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(41),
        key.underlay,
        key.path_id,
        commands,
        TrafficClass::Throughput,
        mux_limits,
    );
    {
        let mut outputs = binding.outputs.lock().expect("response outputs");
        let entry = outputs.entries.first_mut().expect("initial output");
        entry.original_data_acked_bytes = reliable_path_startup_sample_limit_bytes(mux_limits);
        entry.product_rate_epoch = None;
    }

    assert!(
        !binding
            .sender_path_targets(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
            .into_iter()
            .next()
            .expect("live response target")
            .observation
            .product_assignment_qualified,
        "uncapped ACK diagnostics cannot substitute for exact tagged qualification",
    );
    {
        let mut outputs = binding.outputs.lock().expect("response outputs");
        qualify_product_assignment(
            outputs.entries.first_mut().expect("initial output"),
            mux_limits,
        );
    }

    let target = binding
        .sender_path_targets(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .next()
        .expect("live response target");
    assert!(
        target.observation.product_assignment_qualified,
        "exact Product-volume qualification must not depend on numeric rate validity",
    );
    assert_eq!(
        target.observation.snapshot.delivery_rate_bps,
        crate::runtime::path::model::default_path_rate_bps(),
    );
    assert_eq!(target.observation.snapshot.product_progress_rate_bps, None);
}

#[test]
fn product_epoch_snapshot_and_bulk_authority_share_the_exact_deadline() {
    let mux_limits = MuxLimits::default();
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(17),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let observed_at = Instant::now() - Duration::from_millis(20);
    let expires_at = observed_at + Duration::from_millis(100);
    entry.product_rate_epoch = Some(ResponseProductRateEpoch {
        rate_bps: 90_000_000.0,
        sample_count: 1,
        sample_bytes: reliable_path_startup_sample_limit_bytes(mux_limits),
        observed_at,
        expires_at,
    });
    entry.original_data_acked_bytes = reliable_path_startup_sample_limit_bytes(mux_limits);

    let fresh_at = expires_at - Duration::from_nanos(1);
    let fresh =
        server_bulk_output_snapshot_at(&entry, 0, TrafficClass::Throughput, mux_limits, fresh_at);
    assert_eq!(fresh.delivery_rate_bps, 90_000_000.0);
    assert_eq!(fresh.product_progress_rate_bps, Some(90_000_000.0));
    assert!(server_output_has_bulk_rate_evidence_at(
        &entry, mux_limits, fresh_at,
    ));
    let product_limit = reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;
    assert_eq!(
        crate::model::admission::bulk_original_data_assignment_authority(
            fresh,
            PATH_OPEN_SCORE_BYTES,
            mux_limits,
            crate::model::admission::BulkCandidatePosition::AdditionalPath,
            server_output_product_rate_epoch_has_bulk_evidence_at(&entry, mux_limits, fresh_at),
        )
        .assignment_limit_bytes,
        product_limit,
        "the exact Product startup floor, not generic confidence, qualifies P",
    );

    let expired =
        server_bulk_output_snapshot_at(&entry, 0, TrafficClass::Throughput, mux_limits, expires_at);
    assert_eq!(
        expired.delivery_rate_bps,
        crate::runtime::path::model::default_path_rate_bps()
    );
    assert_eq!(expired.product_progress_rate_bps, None);
    assert!(!server_output_has_bulk_rate_evidence_at(
        &entry, mux_limits, expires_at,
    ));
    assert!(!server_output_product_rate_epoch_has_bulk_evidence_at(
        &entry, mux_limits, expires_at,
    ));
}

#[test]
fn fresh_product_point_rate_requires_epoch_and_lifetime_floors_for_completion_authority() {
    let mux_limits = MuxLimits::default();
    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    let fallback_rate = crate::runtime::path::model::default_path_rate_bps();
    let raw_rate = 900_000_000.0;

    for (ordinal, underlay) in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp]
        .into_iter()
        .enumerate()
    {
        let key = CarrierPathKey {
            underlay,
            path_id: PathId(40 + ordinal as u16),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        let mut entry = output_entry(key, commands);
        let observed_at = Instant::now();
        let expires_at = observed_at + Duration::from_secs(1);

        entry.product_rate_epoch = Some(ResponseProductRateEpoch {
            rate_bps: raw_rate,
            sample_count: 1,
            sample_bytes: sample_floor - 1,
            observed_at,
            expires_at,
        });
        entry.original_data_acked_bytes = sample_floor;
        let partial_epoch = server_bulk_output_snapshot_at(
            &entry,
            0,
            TrafficClass::Throughput,
            mux_limits,
            observed_at,
        );
        assert_eq!(
            partial_epoch.delivery_rate_bps, fallback_rate,
            "{underlay:?}"
        );
        assert_eq!(partial_epoch.rate_scope, PathRateScope::PathCapacity);
        assert_eq!(partial_epoch.product_progress_rate_bps, None);
        assert!(!partial_epoch.has_durable_product_progress);

        entry.product_rate_epoch = Some(ResponseProductRateEpoch {
            rate_bps: raw_rate,
            sample_count: 1,
            sample_bytes: sample_floor,
            observed_at,
            expires_at,
        });
        entry.original_data_acked_bytes = sample_floor - 1;
        let partial_lifetime = server_bulk_output_snapshot_at(
            &entry,
            0,
            TrafficClass::Throughput,
            mux_limits,
            observed_at,
        );
        assert_eq!(
            partial_lifetime.delivery_rate_bps, fallback_rate,
            "{underlay:?}"
        );
        assert_eq!(partial_lifetime.rate_scope, PathRateScope::PathCapacity);
        assert_eq!(partial_lifetime.product_progress_rate_bps, None);
        assert!(!partial_lifetime.has_durable_product_progress);

        entry.original_data_acked_bytes = sample_floor;
        let qualified = server_bulk_output_snapshot_at(
            &entry,
            0,
            TrafficClass::Throughput,
            mux_limits,
            observed_at,
        );
        assert_eq!(qualified.delivery_rate_bps, raw_rate, "{underlay:?}");
        assert_eq!(qualified.rate_scope, PathRateScope::PerFlowGoodput);
        assert_eq!(qualified.product_progress_rate_bps, Some(raw_rate));
        assert!(qualified.has_durable_product_progress);

        let low_product_rate = fallback_rate / 2.0;
        entry.product_rate_epoch = Some(ResponseProductRateEpoch {
            rate_bps: low_product_rate,
            sample_count: 1,
            sample_bytes: sample_floor,
            observed_at,
            expires_at,
        });
        let lower_bound = server_bulk_output_snapshot_at(
            &entry,
            0,
            TrafficClass::Throughput,
            mux_limits,
            observed_at,
        );
        assert_eq!(
            lower_bound.delivery_rate_bps, fallback_rate,
            "{underlay:?}: a Product lower bound cannot downshift the configured baseline",
        );
        assert_eq!(lower_bound.rate_scope, PathRateScope::PathCapacity);
        assert_eq!(
            lower_bound.product_progress_rate_bps,
            Some(low_product_rate)
        );
        assert!(lower_bound.has_durable_product_progress);
    }
}

fn path_metrics(
    key: CarrierPathKey,
    source: ServerPathMetricsSource,
    srtt_us: u32,
    delivery_rate_bps: u64,
    pacing_rate_bps: u64,
) -> ServerPathMetricsEntry {
    let recorded_at = Instant::now();
    let metrics = PathMetrics {
        path_id: key.path_id,
        underlay: key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: 1,
        metric_age_us: 0,
        rate_valid_for_us: 1_000_000,
        rate_observed: true,
        srtt_us,
        rttvar_us: srtt_us / 4,
        jitter_us: srtt_us / 10,
        delivery_rate_bps,
        pacing_rate_bps,
        pacing_rate_observed: true,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight_observed: true,
        queue_observed: true,
        bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
        queue_bytes: 0,
        inflight_limit_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
        inflight_hi_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        data_sample_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
    };
    let mut entry = ServerPathMetricsEntry {
        metrics,
        source,
        native_drain_observed: false,
        carrier_native_window_sample: None,
        carrier_delivery_rate_sample: None,
        recorded_at,
    };
    refresh_native_window_sample(&mut entry);
    entry
}

fn refresh_native_window_sample(entry: &mut ServerPathMetricsEntry) {
    entry.carrier_native_window_sample = (entry.source == ServerPathMetricsSource::LocalSender)
        .then(|| CarrierNativeWindowSample::from_path_metrics_at(entry.metrics, entry.recorded_at))
        .flatten();
}

#[test]
fn snapshot_projects_exact_command_product_queue_and_data_flight_bytes() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (commands, mut receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        key.underlay,
        key.path_id,
        commands.clone(),
        TrafficClass::Throughput,
    );
    let frame = stream_data_frame_at(0, 4_096);
    binding.record_original_flight(key, &frame);
    commands
        .try_enqueue_stream_ordered_frame(frame, TrafficClass::Throughput)
        .expect("test carrier queue accepts data");
    binding.set_sender_queue_bytes(2_048);

    let target = binding
        .sender_path_targets(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .next()
        .expect("live response target");
    assert_eq!(target.observation.original_data_in_flight_bytes, 4_096);
    assert_eq!(target.observation.snapshot.queue_bytes, 4_096);
    assert_eq!(target.observation.snapshot.data_level_queue_bytes, 2_048);
    assert_eq!(
        target.observation.snapshot.data_level_bytes_in_flight,
        4_096
    );

    let command = try_recv_reliable_path_command(&mut receivers).expect("dequeue carrier frame");
    let target = binding
        .sender_path_targets(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .next()
        .expect("live response target");
    assert_eq!(target.observation.snapshot.queue_bytes, 4_096);

    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    let target = binding
        .sender_path_targets(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .next()
        .expect("live response target");
    assert_eq!(target.observation.snapshot.queue_bytes, 0);
}

#[test]
fn reinjection_flight_cannot_mint_new_original_data_credit() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let mux_limits = MuxLimits::default();
    let original_flight = reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;
    let reinjection_flight = 64 * 1024_u64;
    let native_window = 1024 * 1024_u64;
    entry.original_data_in_flight_bytes = original_flight;
    entry.bytes_in_flight = original_flight + reinjection_flight;
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        100_000,
        500_000_000,
        500_000_000,
    );
    metrics.metrics.inflight_limit_bytes = native_window;
    metrics.metrics.inflight_hi_bytes = native_window;
    refresh_native_window_sample(&mut metrics);
    entry.local_path_metrics = Some(metrics);

    let snapshot = server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, mux_limits);
    let product_window = reliable_product_feedback_window_bytes(
        Some(snapshot),
        TrafficClass::Throughput,
        mux_limits,
    ) as u64;

    assert_eq!(
        snapshot.data_level_bytes_in_flight, original_flight,
        "the Product debt view contains unique OriginalData only",
    );
    assert_eq!(
        snapshot.data_level_limit_bytes, product_window,
        "the snapshot publishes configured Product authority rather than converting retained repair debt into new authority",
    );
    assert!(
        entry.original_data_in_flight_bytes >= snapshot.data_level_limit_bytes,
        "existing OriginalData debt must block fresh placement until Data ACKs lower it below the current forward ceiling",
    );
}

#[test]
fn newer_switchable_product_rate_changes_ranking_without_rewriting_product_authority() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(29),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let mux_limits = MuxLimits {
        max_repair_bytes: 32 * 1024 * 1024,
        max_reorder_bytes: 32 * 1024 * 1024,
        max_stream_window_bytes: 32 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let observed_at = Instant::now() - Duration::from_millis(20);
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        100_000,
        80_000_000,
        80_000_000,
    );
    metrics.recorded_at = observed_at;
    metrics.metrics.inflight_limit_bytes = 1024 * 1024;
    metrics.metrics.inflight_hi_bytes = 1024 * 1024;
    refresh_native_window_sample(&mut metrics);
    entry.local_path_metrics = Some(metrics);
    entry.original_data_acked_bytes = reliable_path_startup_sample_limit_bytes(mux_limits);
    entry.product_rate_epoch = Some(ResponseProductRateEpoch {
        rate_bps: 500_000_000.0,
        sample_count: 1,
        sample_bytes: entry.original_data_acked_bytes,
        observed_at: observed_at + Duration::from_millis(10),
        expires_at: observed_at + Duration::from_secs(1),
    });

    let snapshot = server_bulk_output_snapshot_at(
        &entry,
        0,
        TrafficClass::Throughput,
        mux_limits,
        observed_at + Duration::from_millis(20),
    );
    let expected = reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;

    assert_eq!(snapshot.carrier_inflight_limit_bytes, 1024 * 1024);
    assert_eq!(snapshot.delivery_rate_bps, 500_000_000.0);
    assert_eq!(snapshot.rate_scope, PathRateScope::PerFlowGoodput);
    assert_eq!(
        snapshot.data_level_limit_bytes, expected,
        "newer exact Product service changes completion ranking while C remains diagnostic and P remains configured",
    );
    let outputs = ResponseStreamOutputs {
        next_output_incarnation: Some(2),
        detaching: Vec::new(),
        entries: vec![entry],
        original_data_in_flight_bytes: 0,
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
        next_requalification_probe_id: Some(1),
        next_requalification_candidate_index: 0,
    };
    assert_eq!(
        outputs
            .source_admission(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES, mux_limits)
            .window_bytes as u64,
        expected,
        "switchable response source staging uses the same configured Product authority",
    );
}

#[test]
fn expired_switchable_native_shape_cannot_block_no_c_product_portability() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(36),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let mux_limits = MuxLimits::default();
    let observed_at = Instant::now();
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        100_000,
        20_000_000,
        20_000_000,
    );
    metrics.recorded_at = observed_at;
    metrics.metrics.rate_valid_for_us = 10_000;
    metrics.metrics.inflight_limit_bytes = 1024 * 1024;
    metrics.metrics.inflight_hi_bytes = 1024 * 1024;
    metrics.carrier_native_window_sample = Some(CarrierNativeWindowSample {
        inflight_limit_bytes: 1024 * 1024,
        observed_at,
        expires_at: observed_at + Duration::from_millis(10),
    });
    entry.local_path_metrics = Some(metrics);
    entry.original_data_acked_bytes = reliable_path_startup_sample_limit_bytes(mux_limits);
    entry.product_rate_epoch = Some(ResponseProductRateEpoch {
        rate_bps: 1_000_000.0,
        sample_count: 1,
        sample_bytes: entry.original_data_acked_bytes,
        observed_at: observed_at + Duration::from_millis(15),
        expires_at: observed_at + Duration::from_secs(1),
    });

    let snapshot = server_bulk_output_snapshot_at(
        &entry,
        0,
        TrafficClass::Throughput,
        mux_limits,
        observed_at + Duration::from_millis(20),
    );

    assert_eq!(snapshot.carrier_inflight_limit_bytes, 0);
    assert_eq!(snapshot.carrier_delivery_rate_bps, None);
    assert_eq!(
        snapshot.data_level_limit_bytes, mux_limits.max_path_flight_bytes as u64,
        "durable exact Product progress without fresh C restores the configured writer-bounded Product cap instead of self-clocking through stale native geometry",
    );
    assert_eq!(
        entry
            .local_path_metrics
            .expect("stale metrics remain diagnostic")
            .metrics
            .inflight_limit_bytes,
        1024 * 1024,
    );
}

#[test]
fn snapshot_projects_only_flows_sharing_the_ordered_writer_queue() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (shared_commands, _shared_receivers) = reliable_path_command_channels(8);
    let bulk = ResponseStreamBinding::new(
        SessionId(1),
        key.underlay,
        key.path_id,
        shared_commands.clone(),
        TrafficClass::Throughput,
    );
    let latency = ResponseStreamBinding::new(
        SessionId(2),
        key.underlay,
        key.path_id,
        shared_commands,
        TrafficClass::Latency,
    );

    let idle_shared = bulk
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("shared TCP writer snapshot");
    assert_eq!(idle_shared.active_flows, 1);
    assert_eq!(idle_shared.active_latency_sensitive_flows, 0);

    latency.record_original_flight(key, &stream_data_frame_at(0, 4_096));
    let shared = bulk
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("shared TCP writer snapshot with latency demand");
    assert_eq!(shared.active_flows, 2);
    assert_eq!(shared.active_latency_sensitive_flows, 1);

    let (independent_commands, _independent_receivers) = reliable_path_command_channels(8);
    let independent_latency = ResponseStreamBinding::new(
        SessionId(3),
        UnderlayProtocol::Udp,
        PathId(1),
        independent_commands,
        TrafficClass::Latency,
    );
    let still_shared = bulk
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("TCP writer snapshot after independent QUIC stream");
    assert_eq!(still_shared.active_flows, 2);
    assert_eq!(still_shared.active_latency_sensitive_flows, 1);

    drop(independent_latency);
    drop(latency);
    let bulk_only = bulk
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("bulk-only TCP writer snapshot");
    assert_eq!(bulk_only.active_flows, 1);
    assert_eq!(bulk_only.active_latency_sensitive_flows, 0);
}

#[test]
fn idle_response_attachments_do_not_divide_current_bulk_capacity() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (shared_commands, _shared_receivers) = reliable_path_command_channels(128);
    let current = ResponseStreamBinding::new(
        SessionId(101),
        key.underlay,
        key.path_id,
        shared_commands.clone(),
        TrafficClass::Throughput,
    );
    let baseline = current
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("current TCP writer snapshot before idle attachments");
    let mut idle = Vec::new();
    for session_id in 1..=100 {
        idle.push(ResponseStreamBinding::new(
            SessionId(session_id),
            key.underlay,
            key.path_id,
            shared_commands.clone(),
            TrafficClass::Throughput,
        ));
    }

    let snapshot = current
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("current TCP writer snapshot");
    assert_eq!(
        snapshot.active_flows, 1,
        "the evaluated flow counts once, but idle attachments are not bulk demand"
    );
    assert_eq!(snapshot.active_latency_sensitive_flows, 0);
    assert_eq!(
        crate::scheduler::score_path(baseline, TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES,)
            .expect("baseline path score")
            .eta_ms,
        crate::scheduler::score_path(snapshot, TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES,)
            .expect("path score with idle attachments")
            .eta_ms,
        "idle attachment count alone cannot change multipath ranking"
    );
    assert_eq!(shared_commands.active_flow_counts(), (0, 0));
    drop(idle);
}

#[test]
fn original_flight_demand_divides_capacity_until_its_unique_ack() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (shared_commands, _shared_receivers) = reliable_path_command_channels(8);
    let first = ResponseStreamBinding::new(
        SessionId(1),
        key.underlay,
        key.path_id,
        shared_commands.clone(),
        TrafficClass::Throughput,
    );
    let second = ResponseStreamBinding::new(
        SessionId(2),
        key.underlay,
        key.path_id,
        shared_commands.clone(),
        TrafficClass::Throughput,
    );
    assert_eq!(shared_commands.active_flow_counts(), (0, 0));

    first.record_original_flight(key, &stream_data_frame_at(0, 8_192));
    first.record_reinjected_flight(key, &stream_data_frame_at(0, 8_192));
    second.record_original_flight(key, &stream_data_frame_at(0, 4_096));
    assert_eq!(
        shared_commands.active_flow_counts(),
        (2, 0),
        "only two unique-original owners publish bulk demand; the repair copy does not"
    );
    let shared = first
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("two-flow TCP writer snapshot");
    assert_eq!(shared.active_flows, 2);

    first.release_normalized_acked_ranges(&[crate::protocol::OffsetRange {
        start: 0,
        end: 4_096,
    }]);
    assert_eq!(
        shared_commands.active_flow_counts(),
        (2, 0),
        "a partial ACK retains demand while unique OriginalData remains in flight"
    );
    first.release_normalized_acked_ranges(&[crate::protocol::OffsetRange {
        start: 4_096,
        end: 8_192,
    }]);
    assert_eq!(
        shared_commands.active_flow_counts(),
        (1, 0),
        "the final unique OriginalData ACK withdraws only that flow's demand"
    );
    second.release_normalized_acked_ranges(&[crate::protocol::OffsetRange {
        start: 0,
        end: 4_096,
    }]);
    assert_eq!(shared_commands.active_flow_counts(), (0, 0));

    drop(first);
    drop(second);
    assert_eq!(
        shared_commands.active_flow_counts(),
        (0, 0),
        "dropping inactive registrations cannot underflow the queue count"
    );
}

#[test]
fn tcp_qualified_native_capacity_precedes_product_goodput() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    install_raw_product_point_rate(&mut entry, 80_000_000.0);
    let mut local_metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        12_000,
        6_000_000,
        7_000_000,
    );
    local_metrics.metrics.app_limited = true;
    local_metrics.metrics.has_ack_derived_data_sample = false;
    local_metrics.metrics.data_sample_count = 0;
    local_metrics.metrics.data_sample_bytes = 0;
    local_metrics.carrier_delivery_rate_sample = Some(CarrierDeliveryRateSample {
        delivery_rate_bps: 500_000_000,
        pacing_rate_bps: Some(600_000_000),
        sample_count: 8,
        sample_bytes: 512 * 1024,
        delivery_window_covered: true,
        observed_at: Instant::now(),
        expires_at: Instant::now() + Duration::from_secs(1),
    });
    entry.local_path_metrics = Some(local_metrics);

    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(snapshot.delivery_rate_bps, 500_000_000.0);
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(snapshot.carrier_delivery_rate_bps, Some(500_000_000.0));
    assert!(!snapshot.has_durable_product_progress);
    assert_eq!(snapshot.pacing_rate_bps, 600_000_000.0);
    assert!(
        snapshot.app_limited,
        "retained qualified delivery evidence must not overwrite current carrier state",
    );
    assert_eq!(snapshot.bytes_in_flight, PATH_OPEN_SCORE_BYTES as u64);
}

#[test]
fn expired_product_rate_retains_durable_product_progress() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(52),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let mux_limits = MuxLimits::default();
    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    let observed_at = Instant::now();
    let expires_at = observed_at + Duration::from_secs(1);
    entry.product_rate_epoch = Some(ResponseProductRateEpoch {
        rate_bps: 10_000_000.0,
        sample_count: 1,
        sample_bytes: sample_floor,
        observed_at,
        expires_at,
    });
    qualify_product_assignment(&mut entry, mux_limits);

    let expired =
        server_bulk_output_snapshot_at(&entry, 0, TrafficClass::Throughput, mux_limits, expires_at);
    assert_eq!(expired.product_progress_rate_bps, None);
    assert!(
        expired.has_durable_product_progress,
        "numeric freshness expiry cannot erase exact historical Product progress",
    );
}

#[test]
fn udp_native_snapshot_fails_closed_without_an_exact_carrier_direction_shape() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(48),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let expected_scope =
        CarrierRateAuthorityScope::new(entry.path_instance_id, PathMetricDirection::ServerToClient);
    let authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
        expected_scope,
        25_000_000,
        5,
        73,
        Some(200_000_000),
    )
    .expect("valid output authority");
    entry.commands = entry.commands.clone().with_native_rate_authority(authority);

    let missing =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(missing.state, crate::scheduler::PathState::Failed);
    assert_eq!(missing.carrier_delivery_rate_bps, None);
    assert_eq!(missing.confidence, 0.0);

    let wrong_instance_scope = CarrierRateAuthorityScope::new(
        CarrierPathInstanceId::from_raw(entry.path_instance_id.as_u64() + 1),
        PathMetricDirection::ServerToClient,
    );
    let wrong_instance_authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
        wrong_instance_scope,
        25_000_000,
        5,
        73,
        Some(400_000_000),
    )
    .expect("foreign-instance authority fixture");
    entry.native_scheduling_shape = Some(
        wrong_instance_authority
            .refresh_scheduling_shape_for_test(
                wrong_instance_scope,
                5,
                73,
                Some(400_000_000),
                Duration::from_millis(40),
                Duration::from_millis(4),
                4_000_000,
                400_000,
                1_400,
                Some(500_000_000),
                false,
            )
            .expect("foreign-instance shape fixture"),
    );
    let wrong_instance =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(
        wrong_instance.state,
        crate::scheduler::PathState::Failed,
        "a stamped shape from another physical carrier cannot schedule this output",
    );
    assert_eq!(wrong_instance.carrier_delivery_rate_bps, None);

    let wrong_direction_scope =
        CarrierRateAuthorityScope::new(entry.path_instance_id, PathMetricDirection::ClientToServer);
    let wrong_direction_authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
        wrong_direction_scope,
        25_000_000,
        5,
        73,
        Some(600_000_000),
    )
    .expect("opposite-direction authority fixture");
    entry.native_scheduling_shape = Some(
        wrong_direction_authority
            .refresh_scheduling_shape_for_test(
                wrong_direction_scope,
                5,
                73,
                Some(600_000_000),
                Duration::from_millis(30),
                Duration::from_millis(3),
                6_000_000,
                600_000,
                1_420,
                Some(700_000_000),
                false,
            )
            .expect("opposite-direction shape fixture"),
    );
    let wrong_direction =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(
        wrong_direction.state,
        crate::scheduler::PathState::Failed,
        "request-direction native evidence cannot schedule a response output",
    );
    assert_eq!(wrong_direction.carrier_delivery_rate_bps, None);
}

#[test]
fn expired_tcp_sample_cannot_reenter_rate_pacing_confidence_or_absent_native_debt() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let now = Instant::now();
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        2_000_000,
        900_000_000,
        950_000_000,
    );
    // A later app-limited shape refresh may keep the registry record fresh and
    // retain old numeric fields, but the frozen ACK epoch remains authoritative.
    metrics.metrics.app_limited = true;
    metrics.metrics.bytes_in_flight_observed = false;
    metrics.metrics.queue_observed = false;
    metrics.metrics.bytes_in_flight = 8 * 1024 * 1024;
    metrics.metrics.queue_bytes = 4 * 1024 * 1024;
    metrics.carrier_delivery_rate_sample = Some(CarrierDeliveryRateSample {
        delivery_rate_bps: 800_000_000,
        pacing_rate_bps: Some(850_000_000),
        sample_count: 8,
        sample_bytes: 512 * 1024,
        delivery_window_covered: true,
        observed_at: now - Duration::from_secs(2),
        expires_at: now - Duration::from_secs(1),
    });
    entry.local_path_metrics = Some(metrics);

    assert!(!server_output_has_bulk_rate_evidence(
        &entry,
        MuxLimits::default()
    ));
    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    let default_rate = crate::runtime::path::model::default_path_rate_bps();
    assert_eq!(snapshot.delivery_rate_bps, default_rate);
    assert_eq!(snapshot.pacing_rate_bps, default_rate);
    assert_eq!(snapshot.carrier_delivery_rate_bps, None);
    assert_eq!(snapshot.bytes_in_flight, 0);
    assert_eq!(snapshot.queue_bytes, 0);
    assert_eq!(
        snapshot.carrier_inflight_limit_bytes,
        (PATH_OPEN_SCORE_BYTES * 4) as u64,
        "a current independently observed carrier window survives unavailable exact flight"
    );
    assert_eq!(server_output_confidence(&entry), 0.0);
}

#[test]
fn fresh_post_expiry_tcp_epoch_rebuilds_confidence_from_its_own_acks() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let now = Instant::now();
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        20_000,
        900_000_000,
        1_000_000_000,
    );
    metrics.metrics.has_ack_derived_data_sample = false;
    metrics.metrics.confidence_ppm = 1_000_000;
    metrics.carrier_delivery_rate_sample = Some(CarrierDeliveryRateSample {
        delivery_rate_bps: 40_000_000,
        pacing_rate_bps: Some(50_000_000),
        sample_count: 1,
        sample_bytes: 512 * 1024,
        delivery_window_covered: true,
        observed_at: now,
        expires_at: now + Duration::from_secs(1),
    });
    entry.local_path_metrics = Some(metrics);

    let one_ack_confidence = server_output_confidence_at(&entry, now);
    assert_eq!(
        one_ack_confidence,
        1.0 / confidence_sample_denominator(),
        "cumulative socket-epoch PathMetrics confidence cannot restore a fresh sidecar epoch to 1.0"
    );
    entry
        .local_path_metrics
        .as_mut()
        .expect("local metrics")
        .carrier_delivery_rate_sample
        .as_mut()
        .expect("fresh sidecar")
        .sample_count = 2;
    assert_eq!(
        server_output_confidence_at(&entry, now),
        2.0 / confidence_sample_denominator(),
        "confidence grows only with ACKs in the new frozen sidecar epoch"
    );
    assert_eq!(
        server_output_confidence_at(&entry, now + Duration::from_secs(1)),
        0.0,
        "the exact frozen deadline revokes sidecar confidence"
    );
}

#[test]
fn tcp_recent_product_goodput_raises_native_rate_as_per_flow_completion_evidence() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let sampled_at = Instant::now();
    let started_at = sampled_at - Duration::from_millis(100);
    let mut evidence = ResponseAckClockRateEvidence::new(started_at);
    let _ = evidence.observe(64 * 1024, started_at, started_at, started_at);
    let _ = evidence.observe(8 * 1024 * 1024, started_at, started_at, sampled_at);
    let product_rate = evidence
        .goodput_sample()
        .expect("qualified product ACK sample")
        .rate_bps();
    install_product_rate(&mut entry, product_rate);
    entry.tcp_product_rate_evidence = Some(evidence);
    entry.original_data_acked_bytes =
        reliable_path_startup_sample_limit_bytes(MuxLimits::default());
    entry.local_path_metrics = Some(path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        12_000,
        500_000_000,
        600_000_000,
    ));

    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(snapshot.delivery_rate_bps, product_rate);
    assert_eq!(snapshot.rate_scope, PathRateScope::PerFlowGoodput);
    assert_eq!(snapshot.carrier_delivery_rate_bps, Some(500_000_000.0));
}

#[test]
fn quic_completion_rate_is_max_of_native_and_mature_product() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    install_product_rate(&mut entry, 80_000_000.0);
    entry.local_path_metrics = Some(path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        14_000,
        500_000_000,
        600_000_000,
    ));

    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(snapshot.delivery_rate_bps, 500_000_000.0);
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(snapshot.pacing_rate_bps, 600_000_000.0);
    assert_eq!(
        snapshot.data_level_limit_bytes,
        reliable_bulk_product_windows(MuxLimits::default()).per_output_product_limit_bytes,
        "native QUIC capacity ranks completion but never rewrites configured Product authority",
    );

    install_product_rate(&mut entry, 800_000_000.0);
    let raised =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(raised.delivery_rate_bps, 800_000_000.0);
    assert_eq!(raised.rate_scope, PathRateScope::PerFlowGoodput);
    assert_eq!(raised.carrier_delivery_rate_bps, Some(500_000_000.0));
}

#[test]
fn unqualified_tcp_native_pacing_never_becomes_completion_rate() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let now = Instant::now();
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        80_000,
        5_000_000,
        280_458_000,
    );
    metrics.metrics.inflight_limit_bytes = 2_012_844;
    metrics.metrics.inflight_hi_bytes = 2_012_844;
    metrics.metrics.has_ack_derived_data_sample = false;
    metrics.metrics.data_sample_count = 0;
    metrics.metrics.data_sample_bytes = 0;
    metrics.carrier_delivery_rate_sample = Some(CarrierDeliveryRateSample {
        delivery_rate_bps: 5_000_000,
        pacing_rate_bps: Some(280_458_000),
        sample_count: 1,
        sample_bytes: PATH_OPEN_SCORE_BYTES as u64,
        delivery_window_covered: false,
        observed_at: now,
        expires_at: now + Duration::from_secs(1),
    });
    refresh_native_window_sample(&mut metrics);
    entry.local_path_metrics = Some(metrics);

    assert!(!server_output_has_bulk_rate_evidence(
        &entry,
        MuxLimits::default()
    ));
    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(
        snapshot.delivery_rate_bps,
        crate::runtime::path::model::default_path_rate_bps(),
        "unqualified native pacing is send intent, not achieved completion service",
    );
    assert_eq!(snapshot.pacing_rate_bps, 280_458_000.0);
    assert_eq!(snapshot.carrier_delivery_rate_bps, None);
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
}

#[test]
fn unqualified_sidecar_without_same_epoch_pacing_grants_no_startup_rate() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    install_product_rate(&mut entry, 80_000_000.0);
    let now = Instant::now();
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        180_000,
        900_000_000,
        950_000_000,
    );
    metrics.carrier_delivery_rate_sample = Some(CarrierDeliveryRateSample {
        delivery_rate_bps: 700_000_000,
        pacing_rate_bps: None,
        sample_count: 1,
        sample_bytes: PATH_OPEN_SCORE_BYTES as u64,
        delivery_window_covered: false,
        observed_at: now,
        expires_at: now + Duration::from_secs(1),
    });
    entry.local_path_metrics = Some(metrics);

    assert!(!server_output_has_bulk_rate_evidence(
        &entry,
        MuxLimits::default()
    ));
    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(snapshot.delivery_rate_bps, 80_000_000.0);
    assert_eq!(snapshot.pacing_rate_bps, 80_000_000.0);
    assert_eq!(snapshot.rate_scope, PathRateScope::PerFlowGoodput);
    assert_eq!(snapshot.carrier_delivery_rate_bps, None);
}

#[test]
fn quic_app_limited_snapshot_retains_native_exploration_without_rewriting_product_authority() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        425_000,
        25_000_000,
        175_000_000,
    );
    metrics.metrics.inflight_limit_bytes = 32 * 1024 * 1024;
    metrics.metrics.inflight_hi_bytes = metrics.metrics.inflight_limit_bytes;
    metrics.metrics.app_limited = true;
    refresh_native_window_sample(&mut metrics);
    entry.local_path_metrics = Some(metrics);

    let mux_limits = MuxLimits::default();
    let snapshot = server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, mux_limits);
    assert_eq!(
        snapshot.carrier_inflight_limit_bytes,
        32 * 1024 * 1024,
        "an app-limited delivery sample must not erase Quinn's fresh exact native window",
    );
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(snapshot, mux_limits),
        32 * 1024 * 1024
            + u64::try_from(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
                .expect("feed quantum fits u64"),
        "fresh C continues to bound unqualified additional-output exploration",
    );
    assert_eq!(
        snapshot.data_level_limit_bytes,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes,
        "app-limited C/R cannot rewrite configured Product authority",
    );
}

#[test]
fn sole_quic_output_retains_native_exploration_after_rate_expiry_without_shrinking_product_authority()
 {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(22),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    install_product_rate(&mut entry, 2_000_000.0);
    let now = Instant::now();
    let live_cwnd = 4 * 1024 * 1024_u64;
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        100_000,
        100_000_000,
        150_000_000,
    );
    metrics.metrics.rate_valid_for_us = 1_000_000;
    metrics.metrics.inflight_limit_bytes = live_cwnd;
    metrics.metrics.inflight_hi_bytes = live_cwnd;
    metrics.carrier_delivery_rate_sample = Some(CarrierDeliveryRateSample {
        delivery_rate_bps: 100_000_000,
        pacing_rate_bps: Some(150_000_000),
        sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        sample_bytes: 256 * 1024,
        delivery_window_covered: true,
        observed_at: now - Duration::from_secs(1),
        expires_at: now - Duration::from_nanos(1),
    });
    refresh_native_window_sample(&mut metrics);
    entry.local_path_metrics = Some(metrics);
    let mux_limits = MuxLimits::default();
    let outputs = ResponseStreamOutputs {
        next_output_incarnation: Some(2),
        detaching: Vec::new(),
        entries: vec![entry],
        original_data_in_flight_bytes: 0,
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
        next_requalification_probe_id: Some(1),
        next_requalification_candidate_index: 0,
    };

    let admission =
        outputs.source_admission(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES, mux_limits);
    let snapshot = admission.selected_path.expect("sole live QUIC output");
    let expected_native_feed = usize::try_from(live_cwnd)
        .expect("test cwnd fits usize")
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .min(mux_limits.max_path_flight_bytes);
    assert_eq!(snapshot.carrier_delivery_rate_bps, None);
    assert_eq!(snapshot.delivery_rate_bps, 2_000_000.0);
    assert_eq!(snapshot.carrier_inflight_limit_bytes, live_cwnd);
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(snapshot, mux_limits),
        expected_native_feed as u64,
        "fresh C remains the additional-output exploration authority after native R expires",
    );
    let product_windows = reliable_bulk_product_windows(mux_limits);
    assert_eq!(
        admission.window_bytes as u64,
        product_windows.stream_resource_limit_bytes,
    );
    assert_eq!(
        snapshot.data_level_limit_bytes,
        product_windows.per_output_product_limit_bytes,
    );
}

#[test]
fn partial_udp_product_epoch_survives_for_diagnostics_without_becoming_completion_authority() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(23),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let mux_limits = MuxLimits::default();
    let now = Instant::now();
    let base = now - Duration::from_millis(500);
    let native_expires_at = base + Duration::from_millis(250);
    let live_cwnd = 4 * 1024 * 1024_u64;
    let native_rate_bps = 100_000_000_u64;
    let product_rate_bps = 2_000_000_u64;
    let product_ack_bytes = product_rate_bps / 8 / 10;
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        100_000,
        native_rate_bps,
        150_000_000,
    );
    metrics.recorded_at = base;
    metrics.metrics.rate_valid_for_us = 1_000_000;
    metrics.metrics.inflight_limit_bytes = live_cwnd;
    metrics.metrics.inflight_hi_bytes = live_cwnd;
    metrics.carrier_delivery_rate_sample = Some(CarrierDeliveryRateSample {
        delivery_rate_bps: native_rate_bps,
        pacing_rate_bps: Some(150_000_000),
        sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        sample_bytes: 256 * 1024,
        delivery_window_covered: true,
        observed_at: base,
        expires_at: native_expires_at,
    });
    refresh_native_window_sample(&mut metrics);
    entry.local_path_metrics = Some(metrics);
    let incarnation = entry.incarnation;
    let mut outputs = ResponseStreamOutputs {
        next_output_incarnation: Some(2),
        detaching: Vec::new(),
        entries: vec![entry],
        original_data_in_flight_bytes: 0,
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
        next_requalification_probe_id: Some(1),
        next_requalification_candidate_index: 0,
    };

    for offset_ms in [100_u64, 200, 300, 400] {
        let acked_at = base + Duration::from_millis(offset_ms);
        let first_sent_at = acked_at - Duration::from_millis(100);
        apply_response_ack_clock_release_samples(
            &mut outputs,
            std::collections::HashMap::from([(
                (key, incarnation),
                (
                    product_ack_bytes,
                    product_ack_bytes,
                    first_sent_at,
                    first_sent_at,
                ),
            )]),
            acked_at,
        );
    }

    let before_expiry = server_bulk_output_snapshot_at(
        &outputs.entries[0],
        0,
        TrafficClass::Throughput,
        mux_limits,
        native_expires_at - Duration::from_nanos(1),
    );
    assert_eq!(before_expiry.delivery_rate_bps, native_rate_bps as f64);

    let product_epoch = outputs.entries[0]
        .product_rate_epoch
        .expect("continuous UDP Product ACK epoch");
    assert_eq!(product_epoch.sample_count, 4);
    assert_eq!(product_epoch.sample_bytes, product_ack_bytes * 4);
    assert_eq!(product_epoch.rate_bps, product_rate_bps as f64);
    assert!(product_epoch.fresh_rate_at(now).is_some());

    let admission =
        outputs.source_admission(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES, mux_limits);
    let after_expiry = admission.selected_path.expect("sole live QUIC output");
    let expected_native_feed = usize::try_from(live_cwnd)
        .expect("test cwnd fits usize")
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .min(mux_limits.max_path_flight_bytes);
    assert_eq!(after_expiry.carrier_delivery_rate_bps, None);
    assert_eq!(
        after_expiry.delivery_rate_bps,
        crate::runtime::path::model::default_path_rate_bps(),
        "the retained partial Product point must not become ECF completion service",
    );
    assert_eq!(after_expiry.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(after_expiry.product_progress_rate_bps, None);
    assert!(!after_expiry.has_durable_product_progress);
    assert_eq!(after_expiry.carrier_inflight_limit_bytes, live_cwnd);
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(after_expiry, mux_limits),
        expected_native_feed as u64,
        "fresh C remains the additional-output exploration authority while Product ACKs refresh R",
    );
    let product_windows = reliable_bulk_product_windows(mux_limits);
    assert_eq!(
        admission.window_bytes as u64,
        product_windows.stream_resource_limit_bytes,
    );
    assert_eq!(
        after_expiry.data_level_limit_bytes,
        product_windows.per_output_product_limit_bytes,
    );
}

#[test]
fn peer_hint_is_used_only_until_local_evidence_arrives() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(3),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    entry.peer_path_metrics = Some(path_metrics(
        key,
        ServerPathMetricsSource::PeerHint,
        9_000,
        330_000_000,
        440_000_000,
    ));

    let hinted =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(hinted.srtt_ms, 9.0);
    assert_eq!(hinted.delivery_rate_bps, 330_000_000.0);

    entry
        .peer_path_metrics
        .as_mut()
        .expect("peer metrics")
        .recorded_at = Instant::now() - Duration::from_secs(2);
    let stale_hint =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(
        stale_hint.delivery_rate_bps,
        crate::runtime::path::model::default_path_rate_bps(),
        "expired peer advisory rate has no placement authority",
    );

    entry.local_path_metrics = Some(path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        35_000,
        110_000_000,
        120_000_000,
    ));
    let local =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(local.srtt_ms, 35.0);
    assert_eq!(local.delivery_rate_bps, 110_000_000.0);

    entry.local_path_metrics = None;
    entry.delivery_samples = 1;
    install_product_rate(&mut entry, 77_000_000.0);
    entry.srtt_ms = Some(55.0);
    let learned =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(learned.srtt_ms, 55.0);
    assert_eq!(learned.delivery_rate_bps, 77_000_000.0);
    assert_eq!(learned.rate_scope, PathRateScope::PerFlowGoodput);
}

#[test]
fn opposite_direction_peer_hint_is_not_response_rate_authority() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(8),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let mut request_direction_hint = path_metrics(
        key,
        ServerPathMetricsSource::PeerHint,
        9_000,
        330_000_000,
        440_000_000,
    );
    request_direction_hint.metrics.direction = PathMetricDirection::ClientToServer;
    entry.peer_path_metrics = Some(request_direction_hint);

    let response =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());

    assert_eq!(
        response.delivery_rate_bps,
        crate::runtime::path::model::default_path_rate_bps(),
        "client-to-server peer evidence cannot authorize server-to-client response completion",
    );
}

#[test]
fn path_proof_supplies_only_fallback_rtt_until_newer_transport_evidence() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(4),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    let now = Instant::now();
    let sent_at = now
        .checked_sub(Duration::from_millis(30))
        .expect("test instant subtraction");
    let mut native = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        35_000,
        500_000_000,
        600_000_000,
    );
    native.recorded_at = sent_at;
    entry.local_path_metrics = Some(native);
    entry.path_proof = Some(PathProofObservation {
        proof_id: 7,
        elapsed: Duration::from_millis(20),
        sent_at,
    });

    let proof_rtt =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(proof_rtt.srtt_ms, 20.0);
    assert_eq!(proof_rtt.delivery_rate_bps, 500_000_000.0);
    assert_eq!(proof_rtt.pacing_rate_bps, 600_000_000.0);
    assert_eq!(proof_rtt.bytes_in_flight, PATH_OPEN_SCORE_BYTES as u64);
    assert_eq!(
        proof_rtt.carrier_inflight_limit_bytes,
        (PATH_OPEN_SCORE_BYTES * 4) as u64,
        "validation must not replace native congestion state",
    );

    entry
        .local_path_metrics
        .as_mut()
        .expect("local transport evidence")
        .recorded_at = now;
    let newer_native =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(newer_native.srtt_ms, 35.0);
    assert_eq!(newer_native.delivery_rate_bps, 500_000_000.0);
}

#[test]
fn best_live_path_uses_completion_score_including_command_queue() {
    let queued_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(4),
    };
    let clear_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(5),
    };
    let (queued_commands, _queued_receivers) = reliable_path_command_channels(8);
    let (clear_commands, _clear_receivers) = reliable_path_command_channels(8);
    let mut queued = output_entry(queued_key, queued_commands.clone());
    install_product_rate(&mut queued, 400_000_000.0);
    queued.srtt_ms = Some(5.0);
    queued.delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
    queued_commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame(2 * 1024 * 1024),
            TrafficClass::Throughput,
        )
        .expect("test carrier queue accepts data");
    let mut clear = output_entry(clear_key, clear_commands);
    install_product_rate(&mut clear, 100_000_000.0);
    clear.srtt_ms = Some(25.0);
    clear.delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
    let outputs = ResponseStreamOutputs {
        next_output_incarnation: Some(2),
        detaching: Vec::new(),
        entries: vec![queued, clear],
        original_data_in_flight_bytes: 0,
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
        next_requalification_probe_id: Some(1),
        next_requalification_candidate_index: 0,
    };

    let best = outputs
        .best_live_path_snapshot(
            TrafficClass::Throughput,
            PATH_OPEN_SCORE_BYTES,
            MuxLimits::default(),
        )
        .expect("one path is schedulable");
    assert_eq!(best.id, clear_key.path_id);
}

#[test]
fn best_live_path_uses_peer_available_before_faster_backup() {
    let available_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(9),
    };
    let backup_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(10),
    };
    let (available_commands, _available_receivers) = reliable_path_command_channels(8);
    let (backup_commands, _backup_receivers) = reliable_path_command_channels(8);
    let mut available = output_entry(available_key, available_commands);
    install_product_rate(&mut available, 10_000_000.0);
    available.srtt_ms = Some(100.0);
    let mut backup = output_entry(backup_key, backup_commands);
    install_product_rate(&mut backup, 1_000_000_000.0);
    backup.srtt_ms = Some(1.0);
    backup.peer_usage = Some(PathUsage::Backup);
    let outputs = ResponseStreamOutputs {
        next_output_incarnation: Some(2),
        detaching: Vec::new(),
        entries: vec![backup, available],
        original_data_in_flight_bytes: 0,
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
        next_requalification_probe_id: Some(1),
        next_requalification_candidate_index: 0,
    };

    let best = outputs
        .best_live_path_snapshot(
            TrafficClass::Throughput,
            PATH_OPEN_SCORE_BYTES,
            MuxLimits::default(),
        )
        .expect("one available path");
    assert_eq!(best.id, available_key.path_id);
}

#[test]
fn response_source_admission_sums_only_the_exact_preferred_output_tier() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 2 * 1024 * 1024,
        max_repair_bytes: 8 * 1024 * 1024,
        max_reorder_bytes: 8 * 1024 * 1024,
        max_stream_window_bytes: 8 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut entries = Vec::new();
    let mut receivers = Vec::new();
    for path_id in 30..35 {
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(path_id),
        };
        let (commands, output_receivers) = reliable_path_command_channels(8);
        let mut entry = output_entry(key, commands);
        install_product_rate(&mut entry, 500_000_000.0);
        qualify_product_assignment(&mut entry, mux_limits);
        entries.push(entry);
        receivers.push(output_receivers);
    }
    entries[2].local_policy.backup = true;
    entries[3].qualification = StreamPathQualification::Stale {
        retry_at: Instant::now(),
    };
    entries[4].commands.begin_path_drain();

    let first_window = reliable_product_feedback_window_bytes(
        Some(server_bulk_output_snapshot(
            &entries[0],
            0,
            TrafficClass::Throughput,
            mux_limits,
        )),
        TrafficClass::Throughput,
        mux_limits,
    );
    let second_window = reliable_product_feedback_window_bytes(
        Some(server_bulk_output_snapshot(
            &entries[1],
            0,
            TrafficClass::Throughput,
            mux_limits,
        )),
        TrafficClass::Throughput,
        mux_limits,
    );
    let outputs = ResponseStreamOutputs {
        next_output_incarnation: Some(2),
        detaching: Vec::new(),
        entries,
        original_data_in_flight_bytes: 0,
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
        next_requalification_probe_id: Some(1),
        next_requalification_candidate_index: 0,
    };
    let admission =
        outputs.source_admission(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES, mux_limits);

    assert_eq!(
        admission.window_bytes,
        first_window.saturating_add(second_window),
        "fresh regular outputs aggregate; backup, stale, and draining outputs cannot enlarge the chosen tier",
    );
    assert!(admission.window_bytes > first_window);
    drop(receivers);
}

#[test]
fn confidence_and_durable_progress_use_explicit_sample_thresholds() {
    let mux_limits = MuxLimits::default();
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(6),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    install_product_rate(&mut entry, 100_000_000.0);
    entry.delivery_samples = (RELIABLE_INITIAL_WINDOW_PACKETS as u32).saturating_sub(1);
    assert!(server_output_confidence(&entry) < 1.0);
    entry.delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
    assert_eq!(server_output_confidence(&entry), 1.0);

    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    entry.original_data_acked_bytes = sample_floor - 1;
    let immature = server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, mux_limits);
    assert!(!immature.has_durable_product_progress);
    assert!(!server_output_has_bulk_rate_evidence(&entry, mux_limits));

    entry.original_data_acked_bytes = sample_floor;
    let mature = server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, mux_limits);
    assert!(mature.has_durable_product_progress);
    assert!(server_output_has_bulk_rate_evidence(&entry, mux_limits));
}

#[test]
fn closed_and_draining_outputs_are_excluded_from_new_product_selection() {
    let closed_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(7),
    };
    let live_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(8),
    };
    let draining_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(11),
    };
    let (closed_commands, closed_receivers) = reliable_path_command_channels(8);
    let (live_commands, _live_receivers) = reliable_path_command_channels(8);
    let (draining_commands, _draining_receivers) = reliable_path_command_channels(8);
    let mut closed = output_entry(closed_key, closed_commands);
    install_product_rate(&mut closed, 1_000_000_000.0);
    closed.srtt_ms = Some(1.0);
    let mut live = output_entry(live_key, live_commands);
    install_product_rate(&mut live, 10_000_000.0);
    live.srtt_ms = Some(100.0);
    let mut draining = output_entry(draining_key, draining_commands.clone());
    install_product_rate(&mut draining, 2_000_000_000.0);
    draining.srtt_ms = Some(1.0);
    draining_commands.begin_path_drain();
    drop(closed_receivers);
    let outputs = ResponseStreamOutputs {
        next_output_incarnation: Some(2),
        detaching: Vec::new(),
        entries: vec![closed, draining, live],
        original_data_in_flight_bytes: 0,
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
        next_requalification_probe_id: Some(1),
        next_requalification_candidate_index: 0,
    };

    assert!(
        outputs
            .snapshot_for_key(closed_key, TrafficClass::Throughput, MuxLimits::default())
            .is_none()
    );
    let best = outputs
        .best_live_path_snapshot(
            TrafficClass::Throughput,
            PATH_OPEN_SCORE_BYTES,
            MuxLimits::default(),
        )
        .expect("remaining live path");
    assert_eq!(best.id, live_key.path_id);
    let source = outputs.source_admission(
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        MuxLimits::default(),
    );
    assert_eq!(
        source.selected_path.map(|path| path.id),
        Some(live_key.path_id)
    );
}
