//! Immutable response-path observations for connection scheduling.
//!
//! This module projects carrier metrics and exact Data-ACK flight into a common
//! snapshot. It does not reserve queues, assign persistent path roles, or run a
//! transport congestion controller.

use super::ResponseStreamBinding;
use super::attachment::{
    ResponseSenderPathTarget, ResponseStreamOutputEntry, ResponseStreamOutputs,
};
use super::evidence::{
    ServerPathMetricsEntry, server_output_has_bulk_rate_evidence_at,
    server_output_local_path_metrics, server_output_peer_path_metrics,
    server_output_product_assignment_qualified,
    server_output_product_rate_epoch_has_bulk_evidence_at, server_path_metrics_estimate_rate_bps,
    server_path_metrics_has_bulk_rate_evidence_at,
    server_path_metrics_has_qualified_delivery_history, server_path_metrics_native_window_sample,
    server_path_metrics_rate_evidence_is_fresh_at, server_path_metrics_snapshot_is_fresh_at,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS, ReliableOriginalDataOutput,
    ReliableStreamSourceAdmission, reliable_product_feedback_window_bytes,
    reliable_stream_source_admission, reliable_stream_source_path,
};
use crate::model::carrier_rate_authority::CarrierRateAuthorityBasis;
use crate::model::path::CarrierPathKey;
use crate::model::response::ResponsePathObservation;
use crate::model::service_rate::{DirectionalServiceRate, DirectionalServiceRateScope};
use crate::mux::MuxLimits;
use crate::protocol::SessionId;
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::model::{default_path_srtt_ms, startup_rate_prediction_bps};
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::{PathRateScope, PathSnapshot, TrafficClass};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

impl ResponseStreamBinding {
    pub(in crate::runtime) fn send_path_snapshot_and_source_window(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> (Option<PathSnapshot>, usize) {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let admission = outputs.source_admission(lane, payload_bytes, self.mux_limits);
        (admission.selected_path, admission.window_bytes)
    }

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
                !entry.qualification.stale_for_original_data()
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
        _payload_bytes: usize,
    ) -> Vec<ResponseSenderPathTarget> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let request_feedback_ingress = *self
            .request_feedback_ingress
            .lock()
            .expect("server reliable stream request feedback ingress lock");
        let now = Instant::now();

        let targets =
            outputs
                .entries
                .iter()
                .filter(|entry| !entry.commands.is_closed())
                .map(|entry| {
                    let snapshot = server_bulk_output_snapshot_at(
                        entry,
                        outputs.data_level_queue_bytes,
                        lane,
                        self.mux_limits,
                        now,
                    );
                    ResponseSenderPathTarget {
                        native_authority_stamp: entry
                            .native_scheduling_shape
                            .map(|shape| shape.stamp()),
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
                            stale_for_original_data: entry.qualification.stale_for_original_data(),
                            #[cfg(test)]
                            has_path_proof_evidence: entry.path_proof.is_some(),
                            product_assignment_qualified:
                                server_output_product_assignment_qualified(entry, self.mux_limits),
                            has_bulk_rate_evidence: server_output_has_bulk_rate_evidence_at(
                                entry,
                                self.mux_limits,
                                now,
                            ),
                        },
                        product_admission_active: entry.commands.product_admission_active(),
                        command_queue: entry.commands.queue_snapshot(),
                    }
                })
                .collect();
        drop(outputs);
        targets
    }

    pub(in crate::runtime) fn mux_limits(&self) -> MuxLimits {
        self.mux_limits
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.session_id
    }
}

fn server_output_native_queue_bytes(entry: &ResponseStreamOutputEntry) -> u64 {
    if entry.key.underlay == crate::protocol::UnderlayProtocol::Udp
        && entry.commands.native_rate_authority().is_some()
    {
        return 0;
    }
    server_output_local_path_metrics(entry)
        .filter(|metrics| metrics.metrics.queue_observed)
        .map_or(0, |metrics| metrics.metrics.queue_bytes)
}

fn server_native_shape_is_fresh_at(metrics: ServerPathMetricsEntry, now: Instant) -> bool {
    server_path_metrics_native_window_sample(metrics).is_some_and(|sample| sample.fresh_at(now))
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

    fn source_admission(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> ReliableStreamSourceAdmission {
        let now = Instant::now();
        reliable_stream_source_admission(
            self.entries
                .iter()
                .filter(|entry| entry.commands.product_admission_active())
                .map(|entry| ReliableOriginalDataOutput {
                    snapshot: server_bulk_output_snapshot_at(
                        entry,
                        self.data_level_queue_bytes,
                        lane,
                        mux_limits,
                        now,
                    ),
                    stale: entry.qualification.stale_for_original_data(),
                }),
            lane,
            payload_bytes,
            mux_limits,
        )
    }

    fn best_live_path_snapshot(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let now = Instant::now();
        reliable_stream_source_path(
            self.entries
                .iter()
                .filter(|entry| entry.commands.product_admission_active())
                .map(|entry| ReliableOriginalDataOutput {
                    snapshot: server_bulk_output_snapshot_at(
                        entry,
                        self.data_level_queue_bytes,
                        lane,
                        mux_limits,
                        now,
                    ),
                    stale: entry.qualification.stale_for_original_data(),
                }),
            lane,
            payload_bytes,
            mux_limits,
        )
    }
}

pub(super) fn server_bulk_output_snapshot(
    entry: &ResponseStreamOutputEntry,
    data_level_queue_bytes: u64,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> PathSnapshot {
    server_bulk_output_snapshot_at(
        entry,
        data_level_queue_bytes,
        lane,
        mux_limits,
        Instant::now(),
    )
}

/// Structural Product eligibility shared by response placement, stale-owner
/// withdrawal, and recovery. Queue fullness is deliberately excluded: it is a
/// transient capacity condition, whereas drain, health, and lane policy decide
/// whether an output can be the surviving payload owner at all.
pub(super) fn server_output_payload_schedulable(
    entry: &ResponseStreamOutputEntry,
    data_level_queue_bytes: u64,
    lane: TrafficClass,
    mux_limits: MuxLimits,
) -> bool {
    entry.commands.product_admission_active()
        && crate::scheduler::path_is_schedulable(
            server_bulk_output_snapshot_at(
                entry,
                data_level_queue_bytes,
                lane,
                mux_limits,
                Instant::now(),
            ),
            lane,
        )
}

pub(super) fn server_bulk_output_snapshot_at(
    entry: &ResponseStreamOutputEntry,
    data_level_queue_bytes: u64,
    lane: TrafficClass,
    mux_limits: MuxLimits,
    now: Instant,
) -> PathSnapshot {
    if entry.key.underlay == crate::protocol::UnderlayProtocol::Udp
        && entry.commands.native_rate_authority().is_some()
    {
        return server_native_bulk_output_snapshot_at(
            entry,
            data_level_queue_bytes,
            lane,
            mux_limits,
            entry.native_scheduling_shape,
        );
    }

    let local_metrics = server_output_local_path_metrics(entry);
    let peer_hint = (entry.delivery_samples == 0)
        .then(|| server_output_peer_path_metrics(entry))
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

    // The ACK-clock epoch retains its raw point estimate for diagnostics and
    // subsequent batch smoothing. Completion ranking requires both the
    // current epoch's byte floor and the output's lifetime Product-ACK floor;
    // freshness alone cannot turn a partial point into ECF authority.
    let raw_product_point_rate = entry
        .product_rate_epoch
        .and_then(|epoch| epoch.fresh_rate_at(now));
    let qualified_product_completion_rate =
        server_output_product_rate_epoch_has_bulk_evidence_at(entry, mux_limits, now)
            .then_some(raw_product_point_rate)
            .flatten();
    // Startup configuration is a prior, not measured carrier capacity. Only
    // ACK-derived evidence may turn local metrics into a scheduling rate.
    let diagnostic_local_rate = local_metrics
        .filter(|metrics| server_path_metrics_has_bulk_rate_evidence_at(*metrics, now))
        .map(server_path_metrics_estimate_rate_bps);
    // TCP has no named NativeMode adapter. Its kernel telemetry remains
    // writer-shape diagnostics. Legacy non-Native UDP retains its qualified
    // local transport sample as the scalar compatibility baseline.
    let legacy_local_rate = (entry.key.underlay != crate::protocol::UnderlayProtocol::Tcp)
        .then_some(diagnostic_local_rate)
        .flatten();
    // A partial native ACK epoch may expose current send intent before it has
    // covered the frozen delivery window. Preserve that value as pacing shape,
    // but never project it into achieved completion service below.
    let native_startup_pacing_rate = local_metrics
        .filter(|metrics| !server_path_metrics_has_bulk_rate_evidence_at(*metrics, now))
        .filter(|metrics| {
            !server_path_metrics_has_qualified_delivery_history(*metrics)
                || server_path_metrics_rate_evidence_is_fresh_at(*metrics, now)
        })
        .filter(|metrics| {
            metrics
                .metrics
                .inflight_limit_bytes
                .max(metrics.metrics.inflight_hi_bytes)
                > 0
        })
        .and_then(|metrics| {
            metrics.carrier_delivery_rate_sample.map_or_else(
                || {
                    metrics
                        .metrics
                        .pacing_rate_observed
                        .then_some(metrics.metrics.pacing_rate_bps.max(1) as f64)
                },
                |sample| {
                    sample
                        .pacing_rate_bps
                        .map(|pacing_rate_bps| pacing_rate_bps.max(1) as f64)
                },
            )
        });
    let native_pacing_rate = diagnostic_local_rate
        .and_then(|_| {
            local_metrics.and_then(|metrics| {
                metrics.carrier_delivery_rate_sample.map_or_else(
                    || {
                        Some(if metrics.metrics.pacing_rate_observed {
                            metrics.metrics.pacing_rate_bps
                        } else {
                            metrics.metrics.delivery_rate_bps
                        })
                    },
                    |sample| Some(sample.pacing_rate_bps.unwrap_or(sample.delivery_rate_bps)),
                )
            })
        })
        .map(|rate| rate.max(1) as f64)
        .or(native_startup_pacing_rate);
    let legacy_peer_rate = (entry.key.underlay != crate::protocol::UnderlayProtocol::Tcp)
        .then(|| {
            peer_hint
                .filter(|metrics| server_path_metrics_snapshot_is_fresh_at(*metrics, now))
                .filter(|metrics| !metrics.metrics.app_limited)
                .map(server_path_metrics_estimate_rate_bps)
        })
        .flatten();

    // Preserve the exact endpoint-local startup sidecar independently of the
    // legacy scalar projection below. The typed value is not rewritten by
    // local/peer diagnostics or Product completion.
    let scheduling_service_rate = DirectionalServiceRate::from_startup_hint(
        DirectionalServiceRateScope::new(
            entry.path_instance_id,
            crate::protocol::PathMetricDirection::ServerToClient,
        ),
        entry.startup_rate_prior,
    )
    .ok();
    // The deployed scorer still consumes this scalar. Restore its complete
    // pre-typed source order until scorer migration is atomic: qualified
    // non-Native UDP local evidence, then a fresh non-app-limited peer hint,
    // then startup. TCP deliberately skips both transport sources. Qualified
    // Product completion may raise, never lower, the selected baseline.
    let scalar_baseline_rate = legacy_local_rate
        .or(legacy_peer_rate)
        .unwrap_or_else(|| startup_rate_prediction_bps(entry.startup_rate_prior));
    let (rate_bps, rate_scope) = match qualified_product_completion_rate
        .filter(|product_rate| *product_rate > scalar_baseline_rate)
    {
        Some(product_rate) => (product_rate, PathRateScope::PerFlowGoodput),
        None => (scalar_baseline_rate, PathRateScope::PathCapacity),
    };

    let mut snapshot = PathSnapshot::new(
        entry.key.path_id,
        entry.key.underlay,
        srtt_ms,
        rate_bps.max(1.0),
    );
    snapshot.scheduling_service_rate = scheduling_service_rate;
    snapshot.rate_scope = rate_scope;
    if scheduling_service_rate.is_none() {
        // A zero configured finite rate is invalid at the typed boundary. It
        // must not silently fall back to a different scheduling authority.
        snapshot.state = crate::scheduler::PathState::Failed;
    }
    // Retain qualified local transport rate for diagnostics and writer-shape
    // observability. For TCP it is not the scheduling service-rate basis.
    snapshot.carrier_delivery_rate_bps = diagnostic_local_rate;
    snapshot.policy = entry.local_policy;
    snapshot.peer_usage = entry.peer_usage;
    let (mut active_flows, mut active_latency_sensitive_flows) =
        entry.commands.active_flow_counts();
    // Queue counters describe existing backlogged owners. Ranking this output
    // is a prospective service request, so include this flow exactly once even
    // before its first OriginalData flight is committed.
    if entry.original_data_in_flight_bytes == 0 {
        active_flows = active_flows.saturating_add(1);
        active_latency_sensitive_flows =
            active_latency_sensitive_flows.saturating_add(u32::from(lane.is_latency_sensitive()));
    }
    snapshot.active_flows = active_flows;
    snapshot.active_latency_sensitive_flows = active_latency_sensitive_flows;
    // Product goodput remains independently qualified for legacy scalar
    // completion and Product/reorder policy; it cannot replace the typed
    // service-rate sidecar.
    snapshot.product_progress_rate_bps = qualified_product_completion_rate;
    // Tagged Product qualification is an incarnation-local historical fact.
    // Numeric completion service may expire independently without returning
    // this output to the unproven startup-flight tier.
    snapshot.has_durable_product_progress = entry.product_qualification.qualified();
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    if let Some(metrics) = local_metrics {
        if let Some(pacing_rate_bps) = native_pacing_rate {
            snapshot.pacing_rate_bps = pacing_rate_bps.max(1.0).max(snapshot.delivery_rate_bps);
        }
        // Current carrier state and retained delivery provenance are separate:
        // keeping a qualified rate epoch must not relabel an idle sender busy.
        snapshot.app_limited = metrics.metrics.app_limited;
        if metrics.metrics.queue_observed {
            snapshot.queue_bytes = metrics.metrics.queue_bytes;
        }
        if metrics.metrics.bytes_in_flight_observed {
            // Native flight ranks carrier completion; exact queue/send-credit
            // checks remain the separate admission authority at publication.
            snapshot.bytes_in_flight = metrics.metrics.bytes_in_flight;
        }
        // The carrier window is an independent TCP capability: a platform can
        // expose cwnd/send credit even when it cannot report exact current
        // flight. Do not erase that useful bound with the flight presence bit.
        if metrics.metrics.inflight_limit_bytes > 0 && server_native_shape_is_fresh_at(metrics, now)
        {
            snapshot.carrier_inflight_limit_bytes = metrics.metrics.inflight_limit_bytes;
        }
    }
    snapshot.queue_bytes = snapshot
        .queue_bytes
        .saturating_add(entry.commands.pending_bytes());
    snapshot.data_level_queue_bytes = data_level_queue_bytes;
    snapshot.data_level_bytes_in_flight = entry.original_data_in_flight_bytes;
    snapshot.confidence = server_output_confidence_at(entry, now);
    // This scalar is `P`: total unique Product exposure released only by MPP
    // DataACK. Native TCP/QUIC admission remains writer/backpressure-owned.
    snapshot.data_level_limit_bytes = u64::try_from(reliable_product_feedback_window_bytes(
        Some(snapshot),
        lane,
        mux_limits,
    ))
    .unwrap_or(u64::MAX);
    snapshot
}

/// Exclusive server NativeMode projection.
///
/// The central C0/Bop value and activation-local Quinn shape are one stamped
/// bundle. Peer hints, lineage ACK/loss, and Product rates are deliberately
/// absent. A missing or structurally mismatched bundle is unschedulable for
/// this observation; the final commit independently revalidates the stamp.
pub(super) fn server_native_bulk_output_snapshot_at(
    entry: &ResponseStreamOutputEntry,
    data_level_queue_bytes: u64,
    lane: TrafficClass,
    mux_limits: MuxLimits,
    shape: Option<NativeCarrierSchedulingShapeSnapshot>,
) -> PathSnapshot {
    let shape = shape.filter(|shape| {
        let scope = shape.stamp().scope();
        scope.carrier_instance_id() == entry.path_instance_id
            && scope.direction() == crate::protocol::PathMetricDirection::ServerToClient
    });
    let scheduling_service_rate = shape.map(NativeCarrierSchedulingShapeSnapshot::service_rate);
    let srtt_ms = shape
        .filter(|shape| !shape.srtt().is_zero())
        .map_or_else(default_path_srtt_ms, |shape| {
            shape.srtt().as_secs_f64() * 1_000.0
        });
    // Typed Unlimited remains nonnumeric. The active legacy projection uses
    // its historical ordering sentinel until a complete allocator consumes
    // the typed sidecar atomically.
    let rate_bps = scheduling_service_rate
        .and_then(DirectionalServiceRate::finite_rate_bps)
        .map_or_else(
            || startup_rate_prediction_bps(entry.startup_rate_prior),
            |rate| rate as f64,
        );
    let mut snapshot = PathSnapshot::new(
        entry.key.path_id,
        entry.key.underlay,
        srtt_ms,
        rate_bps.max(1.0),
    );
    snapshot.scheduling_service_rate = scheduling_service_rate;
    snapshot.policy = entry.local_policy;
    snapshot.peer_usage = entry.peer_usage;
    if shape.is_none() {
        snapshot.state = crate::scheduler::PathState::Failed;
    }
    let (mut active_flows, mut active_latency_sensitive_flows) =
        entry.commands.active_flow_counts();
    if entry.original_data_in_flight_bytes == 0 {
        active_flows = active_flows.saturating_add(1);
        active_latency_sensitive_flows =
            active_latency_sensitive_flows.saturating_add(u32::from(lane.is_latency_sensitive()));
    }
    snapshot.active_flows = active_flows;
    snapshot.active_latency_sensitive_flows = active_latency_sensitive_flows;
    snapshot.queue_bytes = entry.commands.pending_bytes();
    snapshot.data_level_queue_bytes = data_level_queue_bytes;
    snapshot.data_level_bytes_in_flight = entry.original_data_in_flight_bytes;
    if let Some(shape) = shape {
        let finite_rate_bps = shape.finite_rate_bps();
        snapshot.carrier_delivery_rate_bps = (shape.basis()
            == CarrierRateAuthorityBasis::NativeOperational)
            .then_some(finite_rate_bps)
            .flatten()
            .map(|rate| rate as f64);
        snapshot.jitter_ms = shape.rttvar().as_secs_f64() * 1_000.0;
        snapshot.pacing_rate_bps = shape
            .pacing_rate_bps()
            .or(finite_rate_bps)
            .map_or(rate_bps, |rate| rate as f64)
            .max(1.0);
        snapshot.bytes_in_flight = shape.bytes_in_flight();
        snapshot.carrier_inflight_limit_bytes = shape
            .congestion_window()
            .max(u64::from(shape.current_mtu()));
        snapshot.app_limited = shape.app_limited();
        snapshot.confidence = if shape.basis() == CarrierRateAuthorityBasis::NativeOperational {
            1.0
        } else {
            1.0 / confidence_sample_denominator()
        };
    } else {
        snapshot.confidence = 0.0;
    }
    snapshot.data_level_limit_bytes = u64::try_from(reliable_product_feedback_window_bytes(
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

#[cfg(test)]
#[cfg(test)]
pub(super) fn server_output_confidence(entry: &ResponseStreamOutputEntry) -> f64 {
    server_output_confidence_at(entry, Instant::now())
}

fn server_output_confidence_at(entry: &ResponseStreamOutputEntry, now: Instant) -> f64 {
    let delivery_confidence =
        (f64::from(entry.delivery_samples) / confidence_sample_denominator()).clamp(0.0, 1.0);
    let Some(metrics) = server_output_local_path_metrics(entry) else {
        return delivery_confidence;
    };
    if let Some(sample) = metrics.carrier_delivery_rate_sample {
        if sample.observed_at > now || now >= sample.expires_at {
            return delivery_confidence;
        }
        let byte_confidence =
            (sample.sample_bytes as f64 / PATH_OPEN_SCORE_BYTES.max(1) as f64).clamp(0.0, 1.0);
        let count_confidence =
            (f64::from(sample.sample_count) / confidence_sample_denominator()).clamp(0.0, 1.0);
        return delivery_confidence
            .max(byte_confidence.min(count_confidence))
            .clamp(0.0, 1.0);
    }
    if server_path_metrics_has_qualified_delivery_history(metrics)
        && !server_path_metrics_rate_evidence_is_fresh_at(metrics, now)
    {
        return delivery_confidence;
    }

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
