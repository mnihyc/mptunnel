//! Immutable response-path observations for connection scheduling.
//!
//! This module projects carrier metrics and exact Data-ACK flight into a common
//! snapshot. It does not reserve queues, assign persistent path roles, or run a
//! transport congestion controller.

use super::ResponseStreamBinding;
use super::attachment::{
    ResponseSenderPathObservation, ResponseSenderPathTarget, ResponseStreamOutputEntry,
    ResponseStreamOutputs,
};
use super::evidence::{
    server_output_has_bulk_rate_evidence, server_output_has_durable_product_ack_progress,
    server_output_local_path_metrics, server_path_metrics_estimate_rate_bps,
    server_path_metrics_has_bulk_rate_evidence,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS, adaptive_reliable_relay_inflight_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::model::response::ResponsePathObservation;
use crate::mux::MuxLimits;
use crate::protocol::{SessionId, UnderlayProtocol};
use crate::runtime::path::model::{default_path_rate_bps, default_path_srtt_ms};
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::{PathRateScope, PathSnapshot, TrafficClass, path_is_backup, score_path};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;

impl ResponseStreamBinding {
    pub(in crate::runtime) fn send_path_snapshot(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.best_live_path_snapshot(lane, payload_bytes, self.mux_limits)
    }

    pub(in crate::runtime::stream) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .filter(|entry| !entry.commands.is_closed())
            .flat_map(|entry| entry.commands.capacity_notifies())
            .collect()
    }

    pub(in crate::runtime::stream) fn response_recovery_capacity_notifies(
        &self,
    ) -> Vec<Arc<Notify>> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .filter(|entry| {
                !entry.stale_for_original_data
                    && !entry.commands.reinjection_frame_queue_is_closed()
            })
            .map(|entry| entry.commands.capacity_notify())
            .collect()
    }

    pub(in crate::runtime) fn response_model_generation(&self) -> u64 {
        self.response_model_generation.load(Ordering::Acquire)
    }

    pub(in crate::runtime) fn response_output_snapshot(
        &self,
        identity: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .snapshot_for_instance(identity.key, identity.incarnation, lane, self.mux_limits)
    }

    pub(in crate::runtime) fn sender_path_targets(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<ResponseSenderPathTarget> {
        self.sender_path_observation(lane, payload_bytes).targets
    }

    pub(in crate::runtime) fn sender_path_observation(
        &self,
        lane: TrafficClass,
        _payload_bytes: usize,
    ) -> ResponseSenderPathObservation {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let eligibility_changed = outputs.reconcile_stale_output_eligibility();
        let request_feedback_ingress = *self
            .request_feedback_ingress
            .lock()
            .expect("server reliable stream request feedback ingress lock");

        let targets = outputs
            .entries
            .iter()
            .filter(|entry| !entry.commands.is_closed())
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    outputs.data_level_queue_bytes,
                    lane,
                    self.mux_limits,
                );
                ResponseSenderPathTarget {
                    observation: ResponsePathObservation {
                        key: entry.key,
                        path_instance_id: entry.path_instance_id,
                        incarnation: entry.incarnation,
                        snapshot,
                        native_queue_bytes: server_output_native_queue_bytes(entry),
                        native_drain_observed: server_output_local_path_metrics(entry)
                            .is_some_and(|metrics| metrics.native_drain_observed),
                        writer_pending_bytes: entry.commands.writer_pending_bytes(),
                        original_data_in_flight_bytes: entry.original_data_in_flight_bytes,
                        is_request_feedback: request_feedback_ingress.is_some_and(|ingress| {
                            ingress.key == entry.key
                                && ingress.path_instance_id == entry.path_instance_id
                        }),
                        stale_for_original_data: entry.stale_for_original_data,
                        #[cfg(test)]
                        has_path_proof_evidence: entry.path_proof.is_some(),
                        has_bulk_rate_evidence: server_output_has_bulk_rate_evidence(
                            entry,
                            self.mux_limits,
                        ),
                    },
                    command_queue: entry.commands.queue_snapshot(),
                }
            })
            .collect();
        let observation = ResponseSenderPathObservation {
            targets,
            membership_generation: self.output_membership_generation.load(Ordering::Acquire),
            ordinary_eligibility_generation: self.tcp_carrier_ordinary_eligibility_generation(),
        };
        drop(outputs);
        if eligibility_changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
            self.notify_update();
        }
        observation
    }

    pub(in crate::runtime) fn mux_limits(&self) -> MuxLimits {
        self.mux_limits
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.session_id
    }
}

fn server_output_native_queue_bytes(entry: &ResponseStreamOutputEntry) -> u64 {
    server_output_local_path_metrics(entry).map_or(0, |metrics| metrics.metrics.queue_bytes)
}

impl ResponseStreamOutputs {
    pub(super) fn snapshot_for_instance(
        &self,
        key: CarrierPathKey,
        incarnation: u64,
        lane: TrafficClass,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        self.entries
            .iter()
            .find(|entry| {
                entry.key == key && entry.incarnation == incarnation && !entry.commands.is_closed()
            })
            .map(|entry| {
                server_bulk_output_snapshot(entry, self.data_level_queue_bytes, lane, mux_limits)
            })
    }

    #[cfg(test)]
    pub(super) fn snapshot_for_key(
        &self,
        key: CarrierPathKey,
        lane: TrafficClass,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        self.entries
            .iter()
            .find(|entry| entry.key == key && !entry.commands.is_closed())
            .map(|entry| {
                server_bulk_output_snapshot(entry, self.data_level_queue_bytes, lane, mux_limits)
            })
    }

    fn best_live_path_snapshot(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let has_nonstale_output = self
            .entries
            .iter()
            .any(|entry| !entry.commands.is_closed() && !entry.stale_for_original_data);
        let choose = |allow_backup: bool, allow_stale: bool| {
            self.entries
                .iter()
                .filter(|entry| !entry.commands.is_closed())
                .filter(|entry| allow_stale || !entry.stale_for_original_data)
                .filter_map(|entry| {
                    let snapshot = server_bulk_output_snapshot(
                        entry,
                        self.data_level_queue_bytes,
                        lane,
                        mux_limits,
                    );
                    (allow_backup || !path_is_backup(snapshot))
                        .then(|| score_path(snapshot, lane, payload_bytes))
                        .flatten()
                        .map(|score| (score.eta_ms, snapshot))
                })
                .min_by(|left, right| left.0.total_cmp(&right.0))
                .map(|(_, snapshot)| snapshot)
        };
        choose(false, false)
            .or_else(|| choose(true, false))
            .or_else(|| {
                (!has_nonstale_output)
                    .then(|| choose(false, true))
                    .flatten()
            })
            .or_else(|| (!has_nonstale_output).then(|| choose(true, true)).flatten())
    }
}

pub(super) fn server_bulk_output_snapshot(
    entry: &ResponseStreamOutputEntry,
    data_level_queue_bytes: u64,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> PathSnapshot {
    let local_metrics = server_output_local_path_metrics(entry);
    let peer_hint = (entry.delivery_samples == 0)
        .then_some(entry.peer_path_metrics)
        .flatten();
    let liveness_metrics = local_metrics.or(peer_hint);

    let proof_rtt = entry.path_proof.filter(|observation| {
        let proof_acked_at = observation
            .sent_at
            .checked_add(observation.elapsed)
            .unwrap_or(observation.sent_at);
        local_metrics.is_none_or(|metrics| metrics.recorded_at < proof_acked_at)
    });
    let srtt_ms = proof_rtt.map_or_else(
        || {
            liveness_metrics.map_or_else(
                || entry.srtt_ms.unwrap_or_else(default_path_srtt_ms),
                |metrics| f64::from(metrics.metrics.srtt_us.max(1)) / 1000.0,
            )
        },
        |observation| {
            observation
                .elapsed
                .max(Duration::from_micros(1))
                .as_secs_f64()
                * 1000.0
        },
    );
    let jitter_ms =
        liveness_metrics.map_or(0.0, |metrics| f64::from(metrics.metrics.jitter_us) / 1000.0);
    let loss_rate = liveness_metrics
        .filter(|metrics| metrics.metrics.loss_observed)
        .map_or(0.0, |metrics| {
            f64::from(metrics.metrics.loss_ppm) / 1_000_000.0
        })
        .clamp(0.0, 1.0);

    let product_rate = entry
        .product_progress_rate_bps
        .or(entry.delivery_rate_bps)
        .filter(|rate| rate.is_finite() && *rate > 0.0);
    // Startup configuration is a prior, not measured carrier capacity. Only
    // ACK-derived evidence may turn local metrics into a scheduling rate.
    let native_rate = local_metrics
        .filter(|metrics| server_path_metrics_has_bulk_rate_evidence(*metrics))
        .map(server_path_metrics_estimate_rate_bps);
    let native_startup_rate = local_metrics
        .filter(|metrics| !server_path_metrics_has_bulk_rate_evidence(*metrics))
        .filter(|metrics| {
            metrics
                .metrics
                .inflight_limit_bytes
                .max(metrics.metrics.inflight_hi_bytes)
                > 0
        })
        .map(|metrics| {
            metrics
                .metrics
                .pacing_rate_bps
                .max(metrics.metrics.delivery_rate_bps)
                .max(1) as f64
        });
    let peer_rate = peer_hint
        .filter(|metrics| !metrics.metrics.app_limited)
        .map(server_path_metrics_estimate_rate_bps);
    let (rate_bps, rate_scope) = match entry.key.underlay {
        UnderlayProtocol::Tcp if product_rate.is_some() => (
            product_rate.expect("guarded product rate"),
            PathRateScope::PerFlowGoodput,
        ),
        _ if native_rate.is_some() => (
            native_rate.expect("guarded native rate"),
            PathRateScope::PathCapacity,
        ),
        // During transport Startup, pacing ranks paths with available native
        // congestion credit but never grants completion-time authority. A
        // window-covering ACK sample replaces it with delivery rate above.
        _ if native_startup_rate.is_some() => (
            native_startup_rate.expect("guarded native startup rate"),
            PathRateScope::PathCapacity,
        ),
        _ if peer_rate.is_some() => (
            peer_rate.expect("guarded peer rate"),
            PathRateScope::PathCapacity,
        ),
        _ if product_rate.is_some() => (
            product_rate.expect("guarded product rate"),
            PathRateScope::PerFlowGoodput,
        ),
        _ => (default_path_rate_bps(), PathRateScope::PathCapacity),
    };

    let mut snapshot = PathSnapshot::new(
        entry.key.path_id,
        entry.key.underlay,
        srtt_ms,
        rate_bps.max(1.0),
    );
    snapshot.rate_scope = rate_scope;
    snapshot.carrier_delivery_rate_bps = native_rate;
    snapshot.policy = entry.local_policy;
    snapshot.peer_usage = entry.peer_usage;
    let (active_flows, active_latency_sensitive_flows) = entry.commands.active_flow_counts();
    snapshot.active_flows = active_flows;
    snapshot.active_latency_sensitive_flows = active_latency_sensitive_flows;
    snapshot.product_progress_rate_bps = entry.product_progress_rate_bps;
    snapshot.has_durable_product_progress =
        server_output_has_durable_product_ack_progress(entry, mux_limits);
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    if let Some(metrics) = local_metrics {
        snapshot.pacing_rate_bps =
            (metrics.metrics.pacing_rate_bps.max(1) as f64).max(snapshot.delivery_rate_bps);
        snapshot.app_limited = metrics.metrics.app_limited;
        snapshot.queue_bytes = metrics.metrics.queue_bytes;
        // Native flight ranks carrier completion; exact queue/send-credit checks
        // remain the separate admission authority at command publication.
        snapshot.bytes_in_flight = metrics.metrics.bytes_in_flight;
        snapshot.carrier_inflight_limit_bytes = metrics.metrics.inflight_limit_bytes;
    }
    snapshot.queue_bytes = snapshot
        .queue_bytes
        .saturating_add(entry.commands.pending_bytes());
    snapshot.data_level_queue_bytes = data_level_queue_bytes;
    snapshot.data_level_bytes_in_flight = entry.bytes_in_flight;
    snapshot.confidence = server_output_confidence(entry);
    snapshot.data_level_limit_bytes = u64::try_from(adaptive_reliable_relay_inflight_bytes(
        Some(snapshot),
        lane,
        mux_limits,
    ))
    .unwrap_or(u64::MAX);
    snapshot
}

fn confidence_sample_denominator() -> f64 {
    f64::from(RELIABLE_INITIAL_WINDOW_PACKETS as u32)
}

pub(super) fn server_output_confidence(entry: &ResponseStreamOutputEntry) -> f64 {
    let delivery_confidence =
        (f64::from(entry.delivery_samples) / confidence_sample_denominator()).clamp(0.0, 1.0);
    let Some(metrics) = server_output_local_path_metrics(entry) else {
        return delivery_confidence;
    };

    let source_confidence =
        f64::from(metrics.metrics.confidence_ppm).clamp(0.0, 1_000_000.0) / 1_000_000.0;
    if !metrics.metrics.has_ack_derived_data_sample {
        return delivery_confidence.max(source_confidence).clamp(0.0, 1.0);
    }
    let sample_floor = PATH_OPEN_SCORE_BYTES.max(1) as f64;
    let byte_confidence = (metrics.metrics.data_sample_bytes as f64 / sample_floor).clamp(0.0, 1.0);
    let count_confidence = (f64::from(metrics.metrics.data_sample_count)
        / confidence_sample_denominator())
    .clamp(0.0, 1.0);
    delivery_confidence
        .max(source_confidence * byte_confidence.min(count_confidence))
        .clamp(0.0, 1.0)
}

#[cfg(test)]
#[path = "tests_snapshot.rs"]
mod tests;
