use super::super::ResponseStreamBinding;
use super::super::attachment::{ResponseStreamOutputEntry, ResponseStreamOutputs};
use super::super::evidence::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_output_has_bulk_rate_evidence,
};
use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{stream_data_frame, stream_data_frame_at};
use super::{server_bulk_output_snapshot, server_output_confidence};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    reliable_path_startup_sample_limit_bytes,
};
use crate::model::path::{CarrierPathKey, PathPolicy};
use crate::mux::MuxLimits;
use crate::protocol::{
    PathId, PathMetricDirection, PathMetrics, PathUsage, SessionId, UnderlayProtocol,
};
use crate::runtime::path::commands::{ReliablePathCommandSender, reliable_path_command_channels};
use crate::scheduler::{PathRateScope, TrafficClass};
use std::time::Instant;

fn output_entry(
    key: CarrierPathKey,
    commands: ReliablePathCommandSender,
) -> ResponseStreamOutputEntry {
    ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        local_policy: PathPolicy::default(),
        incarnation: 1,
        commands,
        original_data_in_flight_bytes: 0,
        bytes_in_flight: 0,
        product_progress_rate_bps: None,
        delivery_rate_bps: None,
        tcp_ack_clock_rate_bps: None,
        tcp_product_rate_evidence: None,
        srtt_ms: None,
        delivery_samples: 0,
        original_data_acked_bytes: 0,
        local_path_metrics: None,
        peer_path_metrics: None,
        peer_usage: None,
        peer_usage_sequence: None,
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
        recorded_at: Instant::now(),
        capacity_proof: None,
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
    let (commands, _receivers) = reliable_path_command_channels(8);
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
}

#[test]
fn tcp_data_ack_goodput_precedes_native_path_capacity() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut entry = output_entry(key, commands);
    entry.product_progress_rate_bps = Some(80_000_000.0);
    entry.delivery_rate_bps = Some(70_000_000.0);
    entry.local_path_metrics = Some(path_metrics(
        key,
        ServerPathMetricsSource::LocalSender,
        12_000,
        500_000_000,
        600_000_000,
    ));

    let snapshot =
        server_bulk_output_snapshot(&entry, 0, TrafficClass::Throughput, MuxLimits::default());
    assert_eq!(snapshot.delivery_rate_bps, 80_000_000.0);
    assert_eq!(snapshot.rate_scope, PathRateScope::PerFlowGoodput);
    assert_eq!(snapshot.pacing_rate_bps, 600_000_000.0);
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
        entries: vec![queued, clear],
        data_level_queue_bytes: 0,
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
        entries: vec![backup, available],
        data_level_queue_bytes: 0,
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
        entries: vec![closed, live],
        data_level_queue_bytes: 0,
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
