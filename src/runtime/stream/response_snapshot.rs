//! Immutable binding views and scheduler projections over validated evidence.
//! This module reads source models without reserving capacity or mutating scheduling state.

use super::ResponseStreamBinding;
use super::response_admission::{
    server_output_accepts_service_capacity_prior, server_output_has_bulk_rate_evidence_with_limits,
    server_output_has_durable_product_ack_progress, server_output_has_sender_evidence,
    server_output_has_service_feed_evidence_with_limits,
};
use super::response_evidence::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_output_local_path_metrics,
    server_output_quic_capacity_proof_marker, server_path_metrics_bulk_sample_floor_bytes,
    server_path_metrics_estimate_rate_bps, server_path_metrics_has_bulk_rate_evidence,
    server_path_metrics_rate_bps, server_quic_capacity_proof, server_tcp_capacity_proof,
    server_udp_path_metrics_has_durable_rate_estimate,
};
use super::response_session::{
    ResponseSessionSchedulingSnapshot, ServerPathLaneTracker, ServerResponsePathSchedulingSnapshot,
};
use super::response_topology::{
    ResponseSenderPathTarget, ResponseStreamOutputEntry, ResponseStreamOutputs,
    response_live_ordered_data_owner, response_outputs_have_live_mixed_owner_underlays,
};
use crate::model::admission::bulk_service_horizon_payload_bytes;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    product_delivery_samples_override_startup_prior,
};
use crate::model::path::CarrierPathKey;
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::protocol::{SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::model::{
    default_path_rate_bps, default_path_srtt_ms, udp_reliable_stream_loss_repair_penalty_ms,
};
use crate::runtime::relay::io::adaptive_reliable_relay_inflight_bytes;
use crate::runtime::stream::response_placement::ResponseRateScope;
use crate::scheduler::{FlowLane, PathSnapshot};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseSourceServiceSnapshot {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) active_latency_sensitive_flows: u32,
    pub(in crate::runtime) has_service_feed_evidence: bool,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseRelayReadSnapshot {
    pub(in crate::runtime) send_path: Option<PathSnapshot>,
    pub(in crate::runtime) source_service: Option<ResponseSourceServiceSnapshot>,
    pub(in crate::runtime) independent_source_staging: bool,
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn send_path_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.relay_read_snapshot(lane, payload_bytes).send_path
    }

    pub(in crate::runtime) fn relay_read_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> ResponseRelayReadSnapshot {
        let may_have_mixed_owner_underlays = self.may_have_mixed_owner_underlays();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let stored_service_key = *self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        outputs.relay_read_snapshot(
            stored_service_key,
            may_have_mixed_owner_underlays,
            self.session_id,
            &self.lane_tracker,
            lane,
            payload_bytes,
            self.mux_limits,
        )
    }

    pub(in crate::runtime::stream) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .map(|entry| entry.commands.capacity_notify())
            .collect()
    }

    pub(in crate::runtime) fn response_model_generation(&self) -> u64 {
        self.response_model_generation.load(Ordering::Acquire)
    }

    pub(in crate::runtime) fn response_scheduling_snapshot(
        &self,
    ) -> ResponseSessionSchedulingSnapshot {
        self.lane_tracker
            .response_scheduling_snapshot(self.session_id)
    }

    pub(in crate::runtime) fn sender_path_targets(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<ResponseSenderPathTarget> {
        let stored_active_key = self.ordered_data_owner();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let request_active_key = *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock");
        let active_key = response_live_ordered_data_owner(stored_active_key, &outputs.entries);
        let now = Instant::now();
        let response_scheduling = self.lane_tracker.response_path_scheduling_snapshots(
            self.session_id,
            outputs
                .entries
                .iter()
                .map(|entry| (entry.key, entry.path_instance_id)),
        );
        outputs
            .entries
            .iter()
            .zip(response_scheduling)
            .map(|(entry, response_scheduling)| {
                let command_pending_bytes = entry.commands.pending_bytes();
                let calibration_identity = (entry.key, entry.incarnation);
                let calibration = outputs
                    .ack_clock_calibrations
                    .get(&calibration_identity)
                    .copied();
                let response_snapshot = server_bulk_output_snapshot_with_scheduling(
                    entry,
                    lane,
                    self.mux_limits,
                    now,
                    command_pending_bytes,
                    response_scheduling,
                );
                let snapshot = response_snapshot.path;
                let is_active = Some(entry.key) == active_key;
                let has_bulk_rate_evidence =
                    server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits);
                let has_service_feed_evidence = has_bulk_rate_evidence
                    || (is_active
                        && server_output_has_service_feed_evidence_with_limits(
                            entry,
                            self.mux_limits,
                        ));
                let endpoint_only_service_prior_eligible =
                    server_output_accepts_service_capacity_prior(entry);
                #[cfg(feature = "lab-diagnostics")]
                self.lab_response_service_feed_state(
                    entry,
                    snapshot,
                    lane,
                    is_active,
                    has_bulk_rate_evidence,
                    has_service_feed_evidence,
                    command_pending_bytes,
                );
                ResponseSenderPathTarget {
                    #[cfg(feature = "lab-diagnostics")]
                    session_id: self.session_id,
                    #[cfg(feature = "lab-diagnostics")]
                    binding_instance_id: self.binding_instance_id,
                    key: entry.key,
                    path_instance_id: entry.path_instance_id,
                    incarnation: entry.incarnation,
                    commands: entry.commands.clone(),
                    attachment_role: entry.role,
                    snapshot,
                    owner_data_in_flight_bytes: entry.owner_data_in_flight_bytes,
                    command_pending_bytes,
                    eta_ms: server_bulk_output_eta_ms(
                        entry.key,
                        snapshot,
                        active_key,
                        lane,
                        payload_bytes,
                        self.mux_limits,
                    ),
                    is_active,
                    is_request_active: Some(entry.key) == request_active_key,
                    has_sender_evidence: server_output_has_sender_evidence(entry),
                    has_service_feed_evidence,
                    has_bulk_rate_evidence,
                    endpoint_only_service_prior_eligible,
                    quic_capacity_proof: server_output_quic_capacity_proof_marker(entry),
                    quic_capacity_calibration_attempts: response_snapshot
                        .quic_capacity_calibration_attempts,
                    ack_clock_calibration_eligible: calibration.is_some(),
                    ack_clock_calibration_proven: calibration
                        .is_some_and(|calibration| calibration.proven),
                    ack_clock_calibration_spent_bytes: calibration
                        .map_or(0, |calibration| calibration.spent_bytes),
                    ack_clock_calibration_credit_limit_bytes: calibration
                        .map_or(0, |calibration| calibration.credit_limit_bytes),
                    ack_clock_calibration_max_limit_bytes: calibration
                        .map_or(0, |calibration| calibration.max_limit_bytes),
                    ack_clock_calibration_active: outputs.active_ack_clock_calibration
                        == Some(calibration_identity),
                }
            })
            .collect()
    }

    pub(in crate::runtime) fn mux_limits(&self) -> MuxLimits {
        self.mux_limits
    }

    pub(in crate::runtime) fn active_tcp_ack_clock_calibration_remaining_bytes(
        &self,
    ) -> Option<usize> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let identity = outputs.active_ack_clock_calibration?;
        if identity.0.underlay != UnderlayProtocol::Tcp {
            return None;
        }
        let calibration = outputs.ack_clock_calibrations.get(&identity)?;
        if calibration.proven || calibration.retired {
            return None;
        }
        let remaining = calibration
            .credit_limit_bytes
            .saturating_sub(calibration.spent_bytes);
        (remaining > 0).then(|| usize::try_from(remaining).unwrap_or(usize::MAX))
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl ResponseStreamOutputs {
    pub(super) fn snapshot_for_key(
        &self,
        key: CarrierPathKey,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let now = Instant::now();
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| {
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now)
            })
    }

    pub(super) fn read_backpressure_snapshot(
        &self,
        active_key: Option<CarrierPathKey>,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let now = Instant::now();
        if !lane.is_bulk() {
            return self.entries.last().map(|entry| {
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now)
            });
        }
        self.entries
            .iter()
            .filter(|entry| {
                Some(entry.key) == active_key || server_output_has_sender_evidence(entry)
            })
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (eta_ms, snapshot)
            })
            .min_by(|left, right| left.0.total_cmp(&right.0))
            .map(|(_, snapshot)| snapshot)
    }

    pub(super) fn relay_read_snapshot(
        &self,
        stored_service_key: Option<CarrierPathKey>,
        may_have_mixed_owner_underlays: bool,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> ResponseRelayReadSnapshot {
        let service_key = response_live_ordered_data_owner(stored_service_key, &self.entries);
        let send_path = self.read_backpressure_snapshot(
            service_key,
            session_id,
            lane_tracker,
            lane,
            payload_bytes,
            mux_limits,
        );
        let source_service = service_key.and_then(|key| {
            self.entries
                .iter()
                .find(|entry| {
                    entry.key == key
                        && entry.role != StreamOpenRole::Repair
                        && !entry.commands.is_closed()
                })
                .map(|entry| {
                    // Source staging needs exact identity, local pressure, and
                    // proof only. Avoid rebuilding an unused full path model
                    // while the response outputs lock is held.
                    let active_latency_sensitive_flows = send_path
                        .filter(|path| path.id == key.path_id && path.underlay == key.underlay)
                        .map(|path| path.active_latency_sensitive_flows)
                        .unwrap_or_else(|| {
                            lane_tracker
                                .response_service_snapshot(session_id, key)
                                .active_latency_sensitive_flows
                        });
                    ResponseSourceServiceSnapshot {
                        key,
                        active_latency_sensitive_flows,
                        has_service_feed_evidence:
                            server_output_has_service_feed_evidence_with_limits(entry, mux_limits),
                        has_bulk_rate_evidence: server_output_has_bulk_rate_evidence_with_limits(
                            entry, mux_limits,
                        ),
                    }
                })
        });
        ResponseRelayReadSnapshot {
            send_path,
            source_service,
            independent_source_staging: may_have_mixed_owner_underlays
                && response_outputs_have_live_mixed_owner_underlays(&self.entries),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseBulkOutputSnapshot {
    pub(super) path: PathSnapshot,
    pub(super) quic_capacity_calibration_attempts: u8,
}

pub(super) fn server_bulk_output_snapshot(
    entry: &ResponseStreamOutputEntry,
    session_id: SessionId,
    lane: FlowLane,
    lane_tracker: &ServerPathLaneTracker,
    mux_limits: MuxLimits,
    now: Instant,
) -> PathSnapshot {
    server_bulk_output_snapshot_with_command_pending(
        entry,
        session_id,
        lane,
        lane_tracker,
        mux_limits,
        now,
        entry.commands.pending_bytes(),
    )
    .path
}

pub(super) fn server_bulk_output_snapshot_with_command_pending(
    entry: &ResponseStreamOutputEntry,
    session_id: SessionId,
    lane: FlowLane,
    lane_tracker: &ServerPathLaneTracker,
    mux_limits: MuxLimits,
    now: Instant,
    command_pending_bytes: u64,
) -> ResponseBulkOutputSnapshot {
    let response_scheduling = lane_tracker.response_path_scheduling_snapshot(
        session_id,
        entry.key,
        entry.path_instance_id,
    );
    server_bulk_output_snapshot_with_scheduling(
        entry,
        lane,
        mux_limits,
        now,
        command_pending_bytes,
        response_scheduling,
    )
}

/// Combines output-local evidence with one caller-batched scheduling record.
pub(super) fn server_bulk_output_snapshot_with_scheduling(
    entry: &ResponseStreamOutputEntry,
    lane: FlowLane,
    mux_limits: MuxLimits,
    now: Instant,
    command_pending_bytes: u64,
    response_scheduling: ServerResponsePathSchedulingSnapshot,
) -> ResponseBulkOutputSnapshot {
    let local_carrier_metrics = server_output_local_path_metrics(entry);
    let peer_hint_metrics = (entry.delivery_samples == 0)
        .then_some(entry.peer_path_metrics)
        .flatten();
    let liveness_metrics = local_carrier_metrics.or(peer_hint_metrics);
    let bulk_rate_metrics = local_carrier_metrics
        .filter(|path_metrics| server_path_metrics_has_bulk_rate_evidence(*path_metrics));
    let srtt_ms = liveness_metrics.map_or_else(
        || {
            entry
                .srtt_ms
                .unwrap_or_else(|| default_path_srtt_ms(entry.key.underlay))
        },
        |path_metrics| f64::from(path_metrics.metrics.srtt_us.max(1)) / 1000.0,
    );
    let jitter_ms = liveness_metrics.map_or(0.0, |path_metrics| {
        f64::from(path_metrics.metrics.jitter_us) / 1000.0
    });
    let loss_rate = liveness_metrics
        .filter(|path_metrics| path_metrics.metrics.loss_observed)
        .map_or(0.0, |path_metrics| {
            f64::from(path_metrics.metrics.loss_ppm) / 1_000_000.0
        })
        .clamp(0.0, 1.0);
    let peer_hint_rate_bps = peer_hint_metrics
        .filter(|path_metrics| !path_metrics.metrics.app_limited)
        .map(server_path_metrics_rate_bps);
    let product_owner_rate_bps = (entry.key.underlay == UnderlayProtocol::Tcp)
        .then_some(entry.product_progress_rate_bps)
        .flatten()
        .filter(|_| entry.delivery_samples > 0);
    // QUIC keeps its carrier bandwidth estimate after placement proof expires.
    // Use that estimate for pacing/ETA, while `bulk_rate_metrics` remains the
    // separate authority that may admit or move a whole product flow.
    let udp_carrier_estimate_bps = if entry.key.underlay == UnderlayProtocol::Udp {
        local_carrier_metrics
            .filter(|path_metrics| server_udp_path_metrics_has_durable_rate_estimate(*path_metrics))
            .map(server_path_metrics_estimate_rate_bps)
    } else {
        None
    };
    let model_rate_bps = bulk_rate_metrics
        .map(server_path_metrics_rate_bps)
        .or(udp_carrier_estimate_bps);
    let (prior_rate_bps, prior_rate_scope) = if let Some(rate_bps) = model_rate_bps {
        (rate_bps, ResponseRateScope::PathCapacity)
    } else if let Some(rate_bps) = peer_hint_rate_bps {
        (rate_bps, ResponseRateScope::PathCapacity)
    } else if let Some(rate_bps) = product_owner_rate_bps {
        (rate_bps, ResponseRateScope::PerFlowGoodput)
    } else {
        (
            default_path_rate_bps(entry.key.underlay),
            ResponseRateScope::PathCapacity,
        )
    };
    let (rate_bps, rate_scope) = match (
        entry.key.underlay,
        bulk_rate_metrics,
        entry.delivery_rate_bps,
        product_owner_rate_bps,
    ) {
        (_, Some(path_metrics), _, _) => (
            server_path_metrics_rate_bps(path_metrics),
            ResponseRateScope::PathCapacity,
        ),
        (UnderlayProtocol::Udp, None, _, _) => (prior_rate_bps, prior_rate_scope),
        (UnderlayProtocol::Tcp, None, _, _) if entry.tcp_capacity_prior.is_some() => (
            entry
                .tcp_capacity_prior
                .expect("guarded TCP capacity prior")
                .rate_bps,
            ResponseRateScope::PathCapacity,
        ),
        (UnderlayProtocol::Tcp, None, Some(rate), _)
            if !product_delivery_samples_override_startup_prior(entry.delivery_samples) =>
        {
            if rate >= prior_rate_bps {
                (rate, ResponseRateScope::PerFlowGoodput)
            } else {
                (prior_rate_bps, prior_rate_scope)
            }
        }
        (UnderlayProtocol::Tcp, None, Some(rate), _) => (rate, ResponseRateScope::PerFlowGoodput),
        (_, None, None, Some(rate)) => (rate, ResponseRateScope::PerFlowGoodput),
        (_, None, None, None) => (prior_rate_bps, prior_rate_scope),
    };
    let rate_bps = rate_bps.max(1.0);
    let mut snapshot = PathSnapshot::new(entry.key.path_id, entry.key.underlay, srtt_ms, rate_bps);
    snapshot.rate_scope = rate_scope;
    if let Some(path_metrics) = liveness_metrics {
        snapshot.min_rtt_ms = f64::from(path_metrics.metrics.min_rtt_us.max(1)) / 1000.0;
    }
    snapshot.product_progress_rate_bps = entry.product_progress_rate_bps;
    snapshot.has_durable_product_progress =
        server_output_has_durable_product_ack_progress(entry, mux_limits);
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    if let Some(path_metrics) = local_carrier_metrics {
        snapshot.pacing_rate_bps =
            (path_metrics.metrics.pacing_rate_bps.max(1) as f64).max(snapshot.delivery_rate_bps);
    }
    if let Some(path_metrics) = liveness_metrics {
        snapshot.app_limited = path_metrics.metrics.app_limited;
    }
    let metric_queue_bytes =
        local_carrier_metrics.map_or(0, |path_metrics| path_metrics.metrics.queue_bytes);
    snapshot.queue_bytes = metric_queue_bytes.saturating_add(command_pending_bytes);
    snapshot.product_queue_bytes = entry.product_queue_bytes;
    snapshot.bytes_in_flight = match entry.key.underlay {
        UnderlayProtocol::Udp => {
            local_carrier_metrics.map_or(0, |path_metrics| path_metrics.metrics.bytes_in_flight)
        }
        // TCP does not expose packet-level carrier flight to the product layer.
        // Product stream ranges waiting for STREAM_ACK remain in
        // product_bytes_in_flight below; treating them as carrier flight makes
        // the BBR-style send quantum collapse as soon as the product window is
        // full even when the kernel TCP stream is healthy.
        UnderlayProtocol::Tcp => 0,
    };
    snapshot.product_bytes_in_flight = entry.bytes_in_flight;
    snapshot.inflight_limit_bytes = match entry.key.underlay {
        UnderlayProtocol::Udp => local_carrier_metrics
            .map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes),
        UnderlayProtocol::Tcp => {
            bulk_rate_metrics.map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes)
        }
    };
    snapshot.confidence = server_output_confidence(entry, now);
    // Response pressure follows product Service ownership. Control-plane Active
    // attachment roles intentionally remain unchanged across a whole-flow
    // handoff and therefore cannot describe the carrier doing response work.
    let lane_load = response_scheduling.path_load;
    let session_lane_load = response_scheduling.session_load;
    snapshot.active_flows = lane_load.active_flows;
    snapshot.active_latency_sensitive_flows = lane_load.active_latency_sensitive_flows;
    snapshot.session_active_latency_sensitive_flows =
        session_lane_load.active_latency_sensitive_flows;
    let known_bulk_flows = lane_load
        .active_flows
        .saturating_sub(lane_load.active_latency_sensitive_flows);
    if lane.is_bulk() && lane_load.active_latency_sensitive_flows > 0 && known_bulk_flows > 0 {
        let latency_headroom =
            adaptive_reliable_relay_inflight_bytes(Some(snapshot), FlowLane::Latency, mux_limits)
                as u64;
        let protected_queue =
            latency_headroom.saturating_mul(u64::from(lane_load.active_latency_sensitive_flows));
        snapshot.queue_bytes = snapshot.queue_bytes.saturating_add(protected_queue);
    }
    ResponseBulkOutputSnapshot {
        path: snapshot,
        quic_capacity_calibration_attempts: response_scheduling.quic_capacity_calibration_attempts,
    }
}

pub(in crate::runtime) fn server_bulk_output_eta_ms(
    key: CarrierPathKey,
    snapshot: PathSnapshot,
    active_key: Option<CarrierPathKey>,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> f64 {
    let queued_bits = snapshot
        .queue_bytes
        .saturating_add(snapshot.product_queue_bytes)
        .saturating_add(snapshot.bytes_in_flight)
        .saturating_mul(8) as f64;
    let scoring_payload_bytes =
        if lane.is_bulk() && (active_key.is_none() || Some(key) == active_key) {
            bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
        } else {
            payload_bytes
        };
    let payload_bits = scoring_payload_bytes as f64 * 8.0;
    let mut eta_ms = snapshot.srtt_ms / 2.0;
    let effective_rate_bps = snapshot.delivery_rate_bps.max(1.0);
    eta_ms += (queued_bits + payload_bits) / effective_rate_bps * 1000.0;
    eta_ms += snapshot.jitter_ms;
    eta_ms += response_loss_penalty_ms(snapshot);
    if key.underlay == UnderlayProtocol::Udp && lane.is_bulk() {
        eta_ms += udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes);
    }
    let uncertainty = 1.0 - snapshot.confidence.clamp(0.0, 1.0);
    let pto_ms = transport_pto_from_snapshot(Some(snapshot)).as_secs_f64() * 1000.0;
    eta_ms += uncertainty * pto_ms;
    if Some(key) != active_key {
        eta_ms += uncertainty * pto_ms;
        if snapshot.bytes_in_flight > 0 {
            eta_ms +=
                (snapshot.bytes_in_flight as f64 * 8.0 / effective_rate_bps.max(1.0)) * 1000.0;
        }
    }
    eta_ms
}

fn response_loss_penalty_ms(snapshot: PathSnapshot) -> f64 {
    let loss = snapshot.loss_rate.clamp(0.0, 1.0);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let min_progress = PATH_OPEN_SCORE_BYTES as f64
        / ((snapshot.delivery_rate_bps.max(1.0) / 8.0) * (snapshot.srtt_ms.max(1.0) / 1000.0))
            .max(PATH_OPEN_SCORE_BYTES as f64);
    let expected_repairs = loss / (1.0 - loss).max(min_progress);
    expected_repairs * transport_pto_from_snapshot(Some(snapshot)).as_secs_f64() * 1000.0
}

fn confidence_sample_denominator() -> f64 {
    f64::from(RELIABLE_INITIAL_WINDOW_PACKETS as u32)
}

pub(super) fn server_output_confidence(entry: &ResponseStreamOutputEntry, _now: Instant) -> f64 {
    let delivery_confidence =
        (f64::from(entry.delivery_samples) / confidence_sample_denominator()).clamp(0.0, 1.0);
    let metric_confidence = match server_output_local_path_metrics(entry) {
        Some(
            path_metrics @ ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics,
                ..
            },
        ) if metrics.has_ack_derived_data_sample
            || metrics.confidence_ppm > 0
            || server_quic_capacity_proof(path_metrics).is_some()
            || server_tcp_capacity_proof(path_metrics).is_some() =>
        {
            let capacity_proof = server_quic_capacity_proof(path_metrics);
            if let Some(proof) = capacity_proof {
                // Receipt bytes are exact token evidence. Encoder record count
                // is an integrity check, not a QUIC packet-sample population.
                let receipt_confidence = (proof.received_bytes as f64
                    / proof.sample_floor_bytes.max(1) as f64)
                    .clamp(0.0, 1.0);
                return delivery_confidence.max(receipt_confidence).clamp(0.0, 1.0);
            }
            if let Some(proof) = server_tcp_capacity_proof(path_metrics) {
                let receipt_confidence =
                    (proof.received_bytes as f64 / proof.train_bytes.max(1) as f64).clamp(0.0, 1.0);
                return delivery_confidence.max(receipt_confidence).clamp(0.0, 1.0);
            }
            let source_confidence =
                f64::from(metrics.confidence_ppm).clamp(0.0, 1_000_000.0) / 1_000_000.0;
            let sample_bytes = metrics.data_sample_bytes;
            let sample_count = u64::from(metrics.data_sample_count);
            let sample_floor = server_path_metrics_bulk_sample_floor_bytes(metrics).max(1);
            let byte_confidence = (sample_bytes as f64 / sample_floor as f64).clamp(0.0, 1.0);
            let count_confidence =
                (sample_count as f64 / confidence_sample_denominator()).clamp(0.0, 1.0);
            let sample_confidence = byte_confidence.min(count_confidence);
            if metrics.has_ack_derived_data_sample {
                source_confidence * sample_confidence
            } else {
                source_confidence
            }
        }
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::PeerHint,
            ..
        }) => 0.0,
        _ => 0.0,
    };
    delivery_confidence.max(metric_confidence).clamp(0.0, 1.0)
}

#[cfg(test)]
#[path = "response_snapshot_test.rs"]
mod tests;
