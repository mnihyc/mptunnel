use super::super::ResponseStreamBinding;
use super::super::ack_clock::ResponseAckClockRateEvidence;
use super::super::attachment::{ResponseStreamOutputEntry, ResponseStreamOutputs};
use super::super::evidence::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_output_has_bulk_rate_evidence,
};
use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{stream_data_frame, stream_data_frame_at};
use super::{server_bulk_output_snapshot, server_output_confidence};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    reliable_bulk_carrier_feed_quantum_bytes, reliable_path_startup_sample_limit_bytes,
};
use crate::model::path::{CarrierPathKey, PathPolicy};
use crate::mux::MuxLimits;
use crate::protocol::{
    PathId, PathMetricDirection, PathMetrics, PathUsage, SessionId, UnderlayProtocol,
};
use crate::runtime::path::commands::{
    ReliablePathCommandSender, reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command,
};
use crate::runtime::path::{CarrierDeliveryRateSample, PathProofObservation};
use crate::scheduler::{PathRateScope, TrafficClass};
use std::time::{Duration, Instant};

fn output_entry(
    key: CarrierPathKey,
    commands: ReliablePathCommandSender,
) -> ResponseStreamOutputEntry {
    let load_registration = commands.register_flow(TrafficClass::Throughput);
    ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        local_policy: PathPolicy::default(),
        incarnation: 1,
        commands,
        load_registration,
        original_data_in_flight_bytes: 0,
        stale_for_original_data: false,
        bytes_in_flight: 0,
        product_progress_rate_bps: None,
        delivery_rate_bps: None,
        tcp_ack_clock_rate_bps: None,
        tcp_product_rate_evidence: None,
        srtt_ms: None,
        delivery_samples: 0,
        original_data_acked_bytes: 0,
        published_max_data_offset: 0,
        ack_publication: Default::default(),
        local_path_metrics: None,
        peer_path_metrics: None,
        peer_usage: None,
        peer_usage_sequence: None,
        path_proof: None,
    }
}

fn path_metrics(
    key: CarrierPathKey,
    source: ServerPathMetricsSource,
    srtt_us: u32,
    delivery_rate_bps: u64,
    pacing_rate_bps: u64,
) -> ServerPathMetricsEntry {
    ServerPathMetricsEntry {
        source,
        native_drain_observed: false,
        carrier_delivery_rate_sample: None,
        recorded_at: Instant::now(),
        metrics: PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: 1,
            metric_age_us: 0,
            srtt_us,
            rttvar_us: srtt_us / 4,
            jitter_us: srtt_us / 10,
            delivery_rate_bps,
            pacing_rate_bps,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
            queue_bytes: 0,
            inflight_limit_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
            inflight_hi_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: (PATH_OPEN_SCORE_BYTES * 4) as u64,
        },
    }
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

    let shared = bulk
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("shared TCP writer snapshot");
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
fn tcp_qualified_native_capacity_precedes_product_goodput() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    entry.product_progress_rate_bps = Some(80_000_000.0);
    entry.delivery_rate_bps = Some(70_000_000.0);
    let mut local_metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        12_000,
        6_000_000,
        600_000_000,
    );
    local_metrics.metrics.app_limited = true;
    local_metrics.metrics.has_ack_derived_data_sample = false;
    local_metrics.metrics.data_sample_count = 0;
    local_metrics.metrics.data_sample_bytes = 0;
    local_metrics.carrier_delivery_rate_sample = Some(CarrierDeliveryRateSample {
        delivery_rate_bps: 500_000_000,
        sample_count: 8,
        sample_bytes: 512 * 1024,
        delivery_window_covered: true,
    });
    entry.local_path_metrics = Some(local_metrics);

    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(snapshot.delivery_rate_bps, 500_000_000.0);
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(snapshot.carrier_delivery_rate_bps, Some(500_000_000.0));
    assert_eq!(snapshot.pacing_rate_bps, 600_000_000.0);
    assert_eq!(snapshot.bytes_in_flight, PATH_OPEN_SCORE_BYTES as u64);
}

#[test]
fn tcp_recent_product_goodput_is_a_native_capacity_floor() {
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
    entry.product_progress_rate_bps = Some(product_rate);
    entry.delivery_rate_bps = Some(product_rate);
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
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_eq!(snapshot.carrier_delivery_rate_bps, Some(500_000_000.0));
}

#[test]
fn quic_native_path_capacity_precedes_product_goodput() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    entry.product_progress_rate_bps = Some(80_000_000.0);
    entry.delivery_rate_bps = Some(70_000_000.0);
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
    assert_eq!(snapshot.data_level_limit_bytes, 1_750_000);
}

#[test]
fn unqualified_native_sample_uses_pacing_only_for_startup_ranking() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    entry.product_progress_rate_bps = Some(80_000_000.0);
    let mut metrics = path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        180_000,
        5_548_000,
        280_458_000,
    );
    metrics.metrics.inflight_limit_bytes = 2_012_844;
    metrics.metrics.inflight_hi_bytes = 2_012_844;
    metrics.metrics.data_sample_bytes = 1_220_596;
    entry.local_path_metrics = Some(metrics);

    assert!(!server_output_has_bulk_rate_evidence(
        &entry,
        MuxLimits::default()
    ));
    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(snapshot.delivery_rate_bps, 280_458_000.0);
    assert_eq!(snapshot.rate_scope, PathRateScope::PathCapacity);
}

#[test]
fn quic_app_limited_snapshot_retains_native_feed_window() {
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
    entry.local_path_metrics = Some(metrics);

    let mux_limits = MuxLimits::default();
    let snapshot = server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, mux_limits);
    assert_eq!(
        snapshot.data_level_limit_bytes,
        32 * 1024 * 1024
            + u64::try_from(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
                .expect("feed quantum fits u64"),
        "an app-limited QUIC delivery sample must not clamp Quinn's native window",
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
    entry.product_progress_rate_bps = Some(77_000_000.0);
    entry.srtt_ms = Some(55.0);
    let learned =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(learned.srtt_ms, 55.0);
    assert_eq!(learned.delivery_rate_bps, 77_000_000.0);
    assert_eq!(learned.rate_scope, PathRateScope::PerFlowGoodput);
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
    queued.product_progress_rate_bps = Some(400_000_000.0);
    queued.srtt_ms = Some(5.0);
    queued.delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
    queued_commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame(2 * 1024 * 1024),
            TrafficClass::Throughput,
        )
        .expect("test carrier queue accepts data");
    let mut clear = output_entry(clear_key, clear_commands);
    clear.product_progress_rate_bps = Some(100_000_000.0);
    clear.srtt_ms = Some(25.0);
    clear.delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
    let outputs = ResponseStreamOutputs {
        detaching: Vec::new(),
        entries: vec![queued, clear],
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
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
    available.product_progress_rate_bps = Some(10_000_000.0);
    available.srtt_ms = Some(100.0);
    let mut backup = output_entry(backup_key, backup_commands);
    backup.product_progress_rate_bps = Some(1_000_000_000.0);
    backup.srtt_ms = Some(1.0);
    backup.peer_usage = Some(PathUsage::Backup);
    let outputs = ResponseStreamOutputs {
        detaching: Vec::new(),
        entries: vec![backup, available],
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
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
fn confidence_and_durable_progress_use_explicit_sample_thresholds() {
    let mux_limits = MuxLimits::default();
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(6),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    entry.product_progress_rate_bps = Some(100_000_000.0);
    entry.delivery_samples = (RELIABLE_INITIAL_WINDOW_PACKETS as u32).saturating_sub(1);
    assert!(server_output_confidence(&entry) < 1.0);
    entry.delivery_samples = RELIABLE_INITIAL_WINDOW_PACKETS as u32;
    assert_eq!(server_output_confidence(&entry), 1.0);

    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    assert!(sample_floor > accounting_slack);
    entry.original_data_acked_bytes = sample_floor - accounting_slack - 1;
    let immature = server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, mux_limits);
    assert!(!immature.has_durable_product_progress);
    assert!(!server_output_has_bulk_rate_evidence(&entry, mux_limits));

    entry.original_data_acked_bytes = sample_floor - accounting_slack;
    let mature = server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, mux_limits);
    assert!(mature.has_durable_product_progress);
    assert!(server_output_has_bulk_rate_evidence(&entry, mux_limits));
}

#[test]
fn closed_outputs_are_excluded_from_key_and_best_path_snapshots() {
    let closed_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(7),
    };
    let live_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(8),
    };
    let (closed_commands, closed_receivers) = reliable_path_command_channels(8);
    let (live_commands, _live_receivers) = reliable_path_command_channels(8);
    let mut closed = output_entry(closed_key, closed_commands);
    closed.product_progress_rate_bps = Some(1_000_000_000.0);
    closed.srtt_ms = Some(1.0);
    let mut live = output_entry(live_key, live_commands);
    live.product_progress_rate_bps = Some(10_000_000.0);
    live.srtt_ms = Some(100.0);
    drop(closed_receivers);
    let outputs = ResponseStreamOutputs {
        detaching: Vec::new(),
        entries: vec![closed, live],
        data_level_queue_bytes: 0,
        desired_max_data_offset: 0,
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
}
