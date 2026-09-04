use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::{MAX_RELIABLE_SERVICE_QUANTUM_BYTES, reliable_bulk_product_windows};
use crate::model::path::next_carrier_path_instance_id;
use crate::protocol::PathUsage;
use crate::runtime::path::{PacketPathAttachment, PacketPathSelectionInput};
use crate::transport::PathSpec;
use std::time::{Duration, Instant};

fn packet_path_context(paths: &[&str]) -> ClientPathContext {
    let paths = paths
        .iter()
        .map(|path| path.parse::<PathSpec>().expect("packet test path"))
        .collect();
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("packet test secret"),
    );
    ClientPathContext::new(paths, security, ResourceLimits::default()).expect("packet path context")
}

fn install_packet_attachment(
    context: &ClientPathContext,
    key: RelayPathKey,
    usage: PathUsage,
) -> PacketPathAttachment {
    let path_instance_id = next_carrier_path_instance_id();
    let mut health = context.state.health().lock().expect("path health");
    let record = health.path_record_mut(key).expect("packet path record");
    match key.underlay {
        UnderlayProtocol::Tcp => record.install_tcp_peer_usage(
            PathId(u16::try_from(key.index).expect("test path ID")),
            path_instance_id,
            0,
            usage,
        ),
        UnderlayProtocol::Udp => record.install_peer_usage(path_instance_id, 0, usage),
    }
    PacketPathAttachment {
        key,
        path_instance_id,
    }
}

fn seed_native_packet_evidence(
    context: &ClientPathContext,
    attachment: PacketPathAttachment,
    rate_bps: u64,
    srtt_ms: f64,
) -> crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot {
    let mut health = context.state.health().lock().expect("path health");
    let record = health
        .path_record_mut(attachment.key)
        .expect("packet path record");
    record.carrier_srtt_ms = Some(srtt_ms);
    record.carrier_rttvar_ms = Some(srtt_ms / 8.0);
    record.carrier_delivery_rate_bps = Some(rate_bps as f64);
    record.carrier_pacing_rate_bps = Some(rate_bps as f64);
    record.carrier_delivery_samples = 10;
    record.carrier_delivery_sample_bytes = MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64;
    record.carrier_delivery_window_covered = true;
    let now = Instant::now();
    record.carrier_last_delivery_at = Some(now);
    record.carrier_bulk_proof_expires_at = Some(now + Duration::from_secs(60));
    record.carrier_app_limited = false;
    record.carrier_ack_derived_data_seen = true;
    drop(health);

    let scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
        attachment.path_instance_id,
        PathMetricDirection::ClientToServer,
    );
    let authority =
        crate::runtime::path::authority::NativeCarrierRateAuthorityHandle::from_observation_for_test(
            scope,
            rate_bps,
            1,
            attachment.path_instance_id.as_u64(),
            Some(u128::from(rate_bps)),
        )
        .expect("packet-test native authority");
    authority
        .refresh_scheduling_shape_for_test(
            scope,
            1,
            attachment.path_instance_id.as_u64(),
            Some(u128::from(rate_bps)),
            Duration::from_secs_f64(srtt_ms / 1_000.0),
            Duration::from_secs_f64(srtt_ms / 8_000.0),
            512 * 1_024,
            0,
            1_400,
            Some(rate_bps),
            false,
        )
        .expect("packet-test coherent native shape")
}

#[test]
fn request_bulk_discovery_publishes_only_the_configured_planning_product_window() {
    let context = packet_path_context(&["tcp://127.0.0.1:12701", "quic://127.0.0.1:12702"]);
    let instances = [
        RelayPathInstance {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            },
            path_instance_id: next_carrier_path_instance_id(),
            attachment_id: 1,
        },
        RelayPathInstance {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            },
            path_instance_id: next_carrier_path_instance_id(),
            attachment_id: 2,
        },
    ];
    for instance in instances {
        context.install_relay_path_instance_for_test(instance);
    }

    let observation = context.observe_reliable_request_paths(
        instances.map(|instance| (instance, None, ReliableRequestNativeShape::NotApplicable)),
        PATH_OPEN_SCORE_BYTES,
        true,
    );
    let planning_product_limit =
        reliable_bulk_product_windows(context.mux_limits).per_output_product_limit_bytes;
    assert!(!observation.bulk_candidates.is_empty());
    assert!(
        [UnderlayProtocol::Tcp, UnderlayProtocol::Udp]
            .into_iter()
            .all(|underlay| observation
                .bulk_candidates
                .iter()
                .any(|candidate| candidate.key.underlay == underlay))
    );
    assert!(observation.bulk_candidates.iter().all(|candidate| {
        candidate.snapshot.data_level_limit_bytes == planning_product_limit
            && candidate.snapshot.data_level_bytes_in_flight == 0
    }));
    assert!(observation.paths.iter().all(|path| {
        path.shared_snapshot
            .is_some_and(|snapshot| snapshot.data_level_limit_bytes == 0)
    }));
}

#[test]
fn physical_replacement_clears_predecessor_load_and_stale_drop_cannot_release_successor() {
    let context = packet_path_context(&["tcp://127.0.0.1:12710"]);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let predecessor = RelayPathInstance {
        key,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 1,
    };
    context.install_relay_path_instance_for_test(predecessor);
    let mut predecessor_lease = context
        .try_reserve_relay_path_load_if_unchanged(predecessor, TrafficClass::Throughput, 0, 0)
        .expect("predecessor path load reservation");
    let successor = RelayPathInstance {
        key,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 2,
    };
    context.install_relay_path_instance_for_test(successor);

    assert!(
        context
            .reliable_path_snapshot_for_instance(predecessor)
            .is_none()
    );
    let successor_snapshot = context
        .reliable_path_snapshot_for_instance(successor)
        .expect("successor exact health");
    assert_eq!(successor_snapshot.active_flows, 0);
    let observation = context.observe_reliable_request_paths(
        [(successor, None, ReliableRequestNativeShape::NotApplicable)],
        PATH_OPEN_SCORE_BYTES,
        false,
    );
    assert_eq!(observation.paths[0].instance, successor);
    assert_eq!(
        observation.paths[0]
            .shared_snapshot
            .expect("exact successor observation")
            .active_flows,
        0,
    );
    assert!(
        !predecessor_lease.bind_to_instance(successor.path_instance_id),
        "an exact predecessor lease cannot rebind to a successor",
    );
    assert_eq!(
        context
            .reliable_path_snapshot_for_instance(successor)
            .expect("failed rebind leaves successor unchanged")
            .active_flows,
        0,
    );

    let successor_lease = context
        .try_reserve_relay_path_load_if_unchanged(successor, TrafficClass::Throughput, 0, 0)
        .expect("successor path load reservation");
    assert_eq!(
        context
            .reliable_path_snapshot_for_instance(successor)
            .expect("successor owns its own load")
            .active_flows,
        1,
    );

    predecessor_lease.set_recorded_lane(TrafficClass::Latency);
    assert_eq!(
        context
            .reliable_path_snapshot_for_instance(successor)
            .expect("successor lane load remains exact")
            .active_latency_sensitive_flows,
        0,
        "a predecessor lane change must not reclassify successor load",
    );

    drop(predecessor_lease);
    assert_eq!(
        context
            .reliable_path_snapshot_for_instance(successor)
            .expect("successor remains current")
            .active_flows,
        1,
        "dropping a predecessor lease must not release successor load",
    );

    drop(successor_lease);
    assert_eq!(
        context
            .reliable_path_snapshot_for_instance(successor)
            .expect("successor remains current")
            .active_flows,
        0,
    );
}

#[test]
fn packet_candidates_require_the_current_instance_and_do_not_consume_product_state() {
    let context = packet_path_context(&["quic://127.0.0.1:12720?initial-rate-mbps=80"]);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let stale = PacketPathAttachment {
        key,
        path_instance_id: next_carrier_path_instance_id(),
    };
    let current = install_packet_attachment(&context, key, PathUsage::Available);
    {
        let mut health = context.state.health().lock().expect("path health");
        let record = health.path_record_mut(key).expect("packet path record");
        record.measured_rate_bps = Some(8_000_000_000.0);
        record.delivery_samples = 100;
        record.product_delivery_rate_bps = Some(9_000_000_000.0);
        record.product_delivery_samples = 100;
        record.product_delivery_sample_bytes =
            (MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64).saturating_mul(2);
        let sample_at = Instant::now();
        record.last_delivery_at = Some(sample_at);
        record.delivery_rate_expires_at = Some(sample_at + Duration::from_secs(1));
        record.product_last_delivery_at = Some(sample_at);
        record.product_delivery_rate_expires_at = Some(sample_at + Duration::from_secs(1));
        record.active_flows = 7;
        record.active_latency_sensitive_flows = 3;
        record.relay_queue_bytes = 64 * 1024;
        record.relay_bytes_in_flight = 128 * 1024;
        record.carrier_queue_bytes = 1_200;
        record.carrier_queue_bytes_observed = true;
        record.carrier_bytes_in_flight = 2_400;
        record.carrier_bytes_in_flight_observed = true;
        record.measured_loss_rate = Some(0.02);
    }

    let candidates = context.ordered_packet_path_candidates(
        &[
            PacketPathSelectionInput {
                attachment: stale,
                active_flows: 0,
                native_scheduling_shape: None,
            },
            PacketPathSelectionInput {
                attachment: current,
                active_flows: 0,
                native_scheduling_shape: None,
            },
        ],
        1_400,
    );
    assert_eq!(candidates.len(), 1);
    let candidate = candidates[0];
    assert_eq!(candidate.attachment, current);
    assert_eq!(candidate.snapshot.delivery_rate_bps, 80_000_000.0);
    assert_eq!(candidate.snapshot.product_progress_rate_bps, None);
    assert!(!candidate.snapshot.has_durable_product_progress);
    assert_eq!(candidate.snapshot.data_level_queue_bytes, 0);
    assert_eq!(candidate.snapshot.data_level_bytes_in_flight, 0);
    assert_eq!(candidate.snapshot.active_flows, 0);
    assert_eq!(candidate.snapshot.active_latency_sensitive_flows, 0);
    assert_eq!(candidate.snapshot.queue_bytes, 1_200);
    assert_eq!(candidate.snapshot.bytes_in_flight, 2_400);
    assert_eq!(candidate.snapshot.loss_rate, 0.02);

    let health = context.state.health().lock().expect("path health");
    let record = health.path_record(key).expect("packet path record");
    assert_eq!(record.active_flows, 7);
    assert_eq!(record.active_latency_sensitive_flows, 3);
    assert_eq!(record.relay_queue_bytes, 64 * 1024);
    assert_eq!(record.relay_bytes_in_flight, 128 * 1024);
    assert_eq!(record.product_delivery_rate_bps, Some(9_000_000_000.0));
}

#[test]
fn packet_candidates_use_native_evidence_with_regular_before_backup() {
    let context = packet_path_context(&[
        "quic://127.0.0.1:12730",
        "quic://127.0.0.1:12731",
        "quic://127.0.0.1:12732?backup=true",
        "quic://127.0.0.1:12733?control-only=true",
    ]);
    let attachments = (0..4)
        .map(|index| {
            install_packet_attachment(
                &context,
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                PathUsage::Available,
            )
        })
        .collect::<Vec<_>>();
    let native_shapes = [
        seed_native_packet_evidence(&context, attachments[0], 50_000_000, 40.0),
        seed_native_packet_evidence(&context, attachments[1], 500_000_000, 10.0),
        seed_native_packet_evidence(&context, attachments[2], 10_000_000_000, 1.0),
        seed_native_packet_evidence(&context, attachments[3], 10_000_000_000, 1.0),
    ];

    let inputs = attachments
        .iter()
        .copied()
        .zip(native_shapes)
        .map(
            |(attachment, native_scheduling_shape)| PacketPathSelectionInput {
                attachment,
                active_flows: 0,
                native_scheduling_shape: Some(native_scheduling_shape),
            },
        )
        .collect::<Vec<_>>();
    let candidates = context.ordered_packet_path_candidates(&inputs, 1_400);
    assert_eq!(
        candidates.len(),
        3,
        "control-only paths are not packet outputs"
    );
    assert_eq!(candidates[0].attachment, attachments[1]);
    assert_eq!(
        candidates[0].native_authority_stamp,
        Some(native_shapes[1].stamp()),
        "packet candidate retains the exact attachment-owned authority token",
    );
    assert_eq!(candidates[1].attachment, attachments[0]);
    assert_eq!(
        candidates[2].attachment, attachments[2],
        "backup remains behind every schedulable regular attachment"
    );
    assert_eq!(candidates[0].snapshot.delivery_rate_bps, 500_000_000.0);
    assert!(candidates[0].eta_ms < candidates[1].eta_ms);
    assert!(candidates[2].eta_ms < candidates[0].eta_ms);
}

#[test]
fn packet_candidates_balance_equal_paths_with_packet_plane_load() {
    let context = packet_path_context(&[
        "quic://127.0.0.1:12740?initial-rate-mbps=500",
        "quic://127.0.0.1:12741?initial-rate-mbps=500",
    ]);
    let attachments = (0..2)
        .map(|index| {
            install_packet_attachment(
                &context,
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                },
                PathUsage::Available,
            )
        })
        .collect::<Vec<_>>();
    let candidates = context.ordered_packet_path_candidates(
        &[
            PacketPathSelectionInput {
                attachment: attachments[0],
                active_flows: 8,
                native_scheduling_shape: None,
            },
            PacketPathSelectionInput {
                attachment: attachments[1],
                active_flows: 0,
                native_scheduling_shape: None,
            },
        ],
        1_400,
    );
    assert_eq!(candidates[0].attachment, attachments[1]);
}
