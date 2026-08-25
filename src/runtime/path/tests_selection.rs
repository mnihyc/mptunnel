use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, adaptive_reliable_relay_inflight_bytes,
};
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
    rate_bps: f64,
    srtt_ms: f64,
) {
    let mut health = context.state.health().lock().expect("path health");
    let record = health
        .path_record_mut(attachment.key)
        .expect("packet path record");
    record.carrier_srtt_ms = Some(srtt_ms);
    record.carrier_rttvar_ms = Some(srtt_ms / 8.0);
    record.carrier_delivery_rate_bps = Some(rate_bps);
    record.carrier_pacing_rate_bps = Some(rate_bps);
    record.carrier_delivery_samples = 10;
    record.carrier_delivery_sample_bytes = MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64;
    record.carrier_delivery_window_covered = true;
    record.carrier_bulk_proof_expires_at = Some(Instant::now() + Duration::from_secs(60));
    record.carrier_app_limited = false;
    record.carrier_ack_derived_data_seen = true;
}

#[test]
fn authenticated_output_uses_startup_prior_before_exact_measurement() {
    let path = "quic://127.0.0.1:12700"
        .parse::<PathSpec>()
        .expect("test UDP path");
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("test secret"),
    );
    let context = ClientPathContext::new(vec![path], security, ResourceLimits::default())
        .expect("test path context");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 1,
    };

    let admission = context.reliable_stream_source_admission(
        [(instance, false)],
        TrafficClass::Latency,
        PATH_OPEN_SCORE_BYTES,
    );
    let snapshot = admission
        .selected_path
        .expect("authenticated output remains available before measurement");

    assert_eq!(snapshot.state, SchedulerPathState::Suspect);
    assert_eq!(snapshot.id, PathId(0));
    assert_eq!(
        admission.window_bytes,
        adaptive_reliable_relay_inflight_bytes(
            Some(snapshot),
            TrafficClass::Latency,
            context.mux_limits,
        )
    );
    assert!(admission.window_bytes > 0);
}

#[test]
fn physical_replacement_preserves_logical_configured_slot_load() {
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
    let lease = context
        .try_reserve_relay_path_load_if_unchanged(key, TrafficClass::Throughput, 0, 0)
        .expect("logical path load reservation");
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
    assert_eq!(successor_snapshot.active_flows, 1);
    let observation =
        context.observe_reliable_request_paths([(successor, None)], PATH_OPEN_SCORE_BYTES, false);
    assert_eq!(observation.paths[0].instance, successor);
    assert_eq!(
        observation.paths[0]
            .shared_snapshot
            .expect("exact successor observation")
            .active_flows,
        1,
    );

    drop(lease);
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
        record.product_delivery_sample_bytes =
            (MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64).saturating_mul(2);
        record.last_delivery_at = Some(Instant::now());
        record.active_flows = 7;
        record.active_latency_sensitive_flows = 3;
        record.relay_queue_bytes = 64 * 1024;
        record.relay_bytes_in_flight = 128 * 1024;
        record.carrier_queue_bytes = 1_200;
        record.carrier_bytes_in_flight = 2_400;
        record.measured_loss_rate = Some(0.02);
    }

    let candidates = context.ordered_packet_path_candidates(
        &[
            PacketPathSelectionInput {
                attachment: stale,
                active_flows: 0,
            },
            PacketPathSelectionInput {
                attachment: current,
                active_flows: 0,
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
    seed_native_packet_evidence(&context, attachments[0], 50_000_000.0, 40.0);
    seed_native_packet_evidence(&context, attachments[1], 500_000_000.0, 10.0);
    seed_native_packet_evidence(&context, attachments[2], 10_000_000_000.0, 1.0);
    seed_native_packet_evidence(&context, attachments[3], 10_000_000_000.0, 1.0);

    let inputs = attachments
        .iter()
        .copied()
        .map(|attachment| PacketPathSelectionInput {
            attachment,
            active_flows: 0,
        })
        .collect::<Vec<_>>();
    let candidates = context.ordered_packet_path_candidates(&inputs, 1_400);
    assert_eq!(
        candidates.len(),
        3,
        "control-only paths are not packet outputs"
    );
    assert_eq!(candidates[0].attachment, attachments[1]);
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
            },
            PacketPathSelectionInput {
                attachment: attachments[1],
                active_flows: 0,
            },
        ],
        1_400,
    );
    assert_eq!(candidates[0].attachment, attachments[1]);
}
