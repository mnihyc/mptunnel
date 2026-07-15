//! Carrier evidence validation and mutation for one response binding.
//! Writers preserve attachment identity; admission and scheduling only consume facts.

use super::ResponseStreamBinding;
#[cfg(test)]
use super::ack_clock::ResponseAckClockCalibrationState;
use super::attachment::ResponseStreamOutputEntry;
use super::quic_capacity::{
    valid_quic_capacity_proof_candidate_at, well_formed_quic_capacity_proof_candidate,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
#[cfg(test)]
use crate::model::capacity::reliable_subflow_startup_sample_limit_bytes;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, QuicCapacityProofCandidate, RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::timing::quic_bulk_proof_freshness_horizon;
use crate::protocol::{PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::tcp::capacity::{
    TcpCapacityProofCandidate, valid_tcp_capacity_proof_candidate_at,
};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerPathMetricsSource {
    PeerHint,
    LocalSender,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerPathMetricsEntry {
    pub(in crate::runtime::stream) metrics: PathMetrics,
    pub(in crate::runtime::stream) source: ServerPathMetricsSource,
    // Metric age is measured at the source; residence time closes the gap when
    // the local idle publisher is delayed after this snapshot is installed.
    pub(in crate::runtime::stream) recorded_at: Instant,
    // Only the exact capacity transaction creates this marker. Ordinary metric
    // refreshes may carry it to the fixed deadline but cannot mint a new proof.
    pub(in crate::runtime::stream) capacity_proof: Option<QuicCapacityProofCandidate>,
    // TCP uses an independent receiver receipt plus exact socket telemetry.
    pub(in crate::runtime::stream) tcp_capacity_proof: Option<TcpCapacityProofCandidate>,
}

pub(super) fn server_output_local_path_metrics(
    entry: &ResponseStreamOutputEntry,
) -> Option<ServerPathMetricsEntry> {
    entry.local_path_metrics.filter(|path_metrics| {
        path_metrics.source == ServerPathMetricsSource::LocalSender
            && path_metrics.metrics.direction == PathMetricDirection::ServerToClient
            && path_metrics.metrics.underlay == entry.key.underlay
            && path_metrics.metrics.path_id == entry.key.path_id
    })
}
pub(super) fn server_quic_capacity_proof(
    path_metrics: ServerPathMetricsEntry,
) -> Option<QuicCapacityProofCandidate> {
    let proof = path_metrics.capacity_proof?;
    (path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.underlay == UnderlayProtocol::Udp
        && valid_quic_capacity_proof_candidate_at(proof, Instant::now()))
    .then_some(proof)
}

pub(super) fn server_tcp_capacity_proof(
    path_metrics: ServerPathMetricsEntry,
) -> Option<TcpCapacityProofCandidate> {
    let proof = path_metrics.tcp_capacity_proof?;
    (path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.underlay == UnderlayProtocol::Tcp
        && valid_tcp_capacity_proof_candidate_at(proof, Instant::now()))
    .then_some(proof)
}

pub(super) fn server_path_metrics_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    server_quic_capacity_proof(path_metrics)
        .map(|proof| proof.rate_bps.max(1) as f64)
        .or_else(|| {
            server_tcp_capacity_proof(path_metrics).map(|proof| proof.rate_bps.max(1) as f64)
        })
        .unwrap_or_else(|| path_metrics.metrics.delivery_rate_bps.max(1) as f64)
}

pub(super) fn server_path_metrics_estimate_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    path_metrics
        .capacity_proof
        .filter(|proof| well_formed_quic_capacity_proof_candidate(*proof))
        .map_or_else(
            || path_metrics.metrics.delivery_rate_bps.max(1) as f64,
            |proof| proof.rate_bps.max(1) as f64,
        )
}

pub(super) fn server_path_metrics_bulk_sample_floor_bytes(metrics: PathMetrics) -> u64 {
    let carrier_floor = metrics
        .inflight_hi_bytes
        .max(metrics.inflight_limit_bytes)
        .max(PATH_OPEN_SCORE_BYTES as u64);
    match metrics.underlay {
        UnderlayProtocol::Tcp => carrier_floor,
        UnderlayProtocol::Udp => {
            let minimum_meaningful_sample = (PATH_OPEN_SCORE_BYTES as u64).saturating_mul(4);
            let startup_graduation_sample = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
                .saturating_div(2)
                .max(minimum_meaningful_sample);
            carrier_floor
                .max(minimum_meaningful_sample)
                .min(startup_graduation_sample)
        }
    }
}

pub(super) fn server_udp_path_metrics_has_durable_rate_estimate(
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    if path_metrics.source != ServerPathMetricsSource::LocalSender
        || path_metrics.metrics.underlay != UnderlayProtocol::Udp
    {
        return false;
    }
    if path_metrics
        .capacity_proof
        .is_some_and(well_formed_quic_capacity_proof_candidate)
    {
        return true;
    }
    let sample_floor = server_path_metrics_bulk_sample_floor_bytes(path_metrics.metrics);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    path_metrics.metrics.has_ack_derived_data_sample
        && path_metrics.metrics.data_sample_count > 0
        && path_metrics
            .metrics
            .data_sample_bytes
            .saturating_add(packet_accounting_slack)
            >= sample_floor
}

pub(super) fn server_path_metrics_has_bulk_rate_evidence(
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    if server_quic_capacity_proof(path_metrics).is_some()
        || server_tcp_capacity_proof(path_metrics).is_some()
    {
        return true;
    }
    let sample_floor = server_path_metrics_bulk_sample_floor_bytes(path_metrics.metrics);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    let effective_metric_age = Duration::from_micros(u64::from(path_metrics.metrics.metric_age_us))
        .saturating_add(Instant::now().saturating_duration_since(path_metrics.recorded_at));
    let native_bulk_proof_is_eligible = !path_metrics.metrics.app_limited
        && (path_metrics.metrics.underlay != UnderlayProtocol::Udp
            || effective_metric_age
                < quic_bulk_proof_freshness_horizon(
                    Duration::from_micros(u64::from(path_metrics.metrics.srtt_us.max(1))),
                    Duration::from_micros(u64::from(path_metrics.metrics.rttvar_us)),
                ));
    path_metrics.source == ServerPathMetricsSource::LocalSender
        // Source expiry is authoritative; age is defense in depth if an idle
        // refresh is delayed or reordered before response admission runs.
        && native_bulk_proof_is_eligible
        && path_metrics.metrics.has_ack_derived_data_sample
        && path_metrics.metrics.data_sample_count > 0
        && path_metrics
            .metrics
            .data_sample_bytes
            .saturating_add(packet_accounting_slack)
            >= sample_floor
}

fn server_path_metrics_has_ack_data_evidence(path_metrics: ServerPathMetricsEntry) -> bool {
    path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.has_ack_derived_data_sample
}

pub(super) fn server_path_metrics_has_sender_evidence(
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    path_metrics.source == ServerPathMetricsSource::LocalSender
        && (server_path_metrics_has_bulk_rate_evidence(path_metrics)
            || server_path_metrics_has_ack_data_evidence(path_metrics)
            || path_metrics.metrics.confidence_ppm > 0)
}

pub(super) fn server_output_quic_capacity_proof_marker(
    entry: &ResponseStreamOutputEntry,
) -> Option<QuicCapacityProofCandidate> {
    server_output_local_path_metrics(entry)
        .filter(|path_metrics| {
            path_metrics.source == ServerPathMetricsSource::LocalSender
                && path_metrics.metrics.underlay == UnderlayProtocol::Udp
        })
        .and_then(|path_metrics| path_metrics.capacity_proof)
}

pub(super) fn server_output_fresh_quic_capacity_proof(
    entry: &ResponseStreamOutputEntry,
) -> Option<QuicCapacityProofCandidate> {
    server_output_local_path_metrics(entry).and_then(server_quic_capacity_proof)
}

impl ResponseStreamBinding {
    pub(in crate::runtime::stream) fn set_sender_queue_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let changed = outputs.product_queue_bytes != bytes;
        outputs.product_queue_bytes = bytes;
        if changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(outputs);
        if changed {
            self.notify_update();
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_output_bulk_proven_for_test(&self, key: CarrierPathKey) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("test bulk-proven output");
        entry.product_progress_rate_bps = Some(100_000_000.0);
        entry.delivery_rate_bps = Some(100_000_000.0);
        entry.delivery_samples = 1;
        entry.owner_data_acked_bytes = reliable_subflow_startup_sample_limit_bytes(self.mux_limits);
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn set_output_product_model_for_test(
        &self,
        key: CarrierPathKey,
        rate_bps: f64,
        srtt_ms: f64,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("test modeled output");
        entry.product_progress_rate_bps = Some(rate_bps.max(1.0));
        entry.delivery_rate_bps = Some(rate_bps.max(1.0));
        entry.srtt_ms = Some(srtt_ms.max(1.0));
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_tcp_ack_clock_calibration_for_test(
        &self,
        key: CarrierPathKey,
        spent_bytes: u64,
        credit_limit_bytes: u64,
        max_limit_bytes: u64,
        active: bool,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .expect("test calibration output");
        assert_eq!(entry.key.underlay, UnderlayProtocol::Tcp);
        let identity = (entry.key, entry.incarnation);
        let mut calibration =
            ResponseAckClockCalibrationState::new(credit_limit_bytes, max_limit_bytes);
        calibration.spent_bytes = spent_bytes;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        if active {
            outputs.active_ack_clock_calibration = Some(identity);
        } else if outputs.active_ack_clock_calibration == Some(identity) {
            outputs.active_ack_clock_calibration = None;
        }
    }

    pub(in crate::runtime) fn update_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        self.update_path_metrics_matching(key, Some(path_instance_id), metrics, source);
    }

    pub(in crate::runtime) fn install_quic_capacity_proof_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        metrics: PathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) -> bool {
        self.install_path_metrics_entry_matching(
            key,
            Some(path_instance_id),
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: Some(candidate),
                tcp_capacity_proof: None,
            },
            false,
        )
        .0
    }

    pub(in crate::runtime) fn install_tcp_capacity_proof_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        metrics: PathMetrics,
        candidate: TcpCapacityProofCandidate,
    ) -> bool {
        self.install_path_metrics_entry_matching(
            key,
            Some(path_instance_id),
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                tcp_capacity_proof: Some(candidate),
            },
            false,
        )
        .0
    }

    pub(in crate::runtime) fn install_stored_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        path_metrics: ServerPathMetricsEntry,
    ) {
        self.install_path_metrics_entry_matching(key, Some(path_instance_id), path_metrics, true);
    }

    pub(in crate::runtime) fn notify_installed_path_metrics(&self) {
        self.graduate_completed_response_startup_owner();
        self.notify_update();
    }

    #[cfg(test)]
    pub(in crate::runtime) fn update_path_metrics(
        &self,
        key: CarrierPathKey,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        self.update_path_metrics_matching(key, None, metrics, source);
    }

    fn update_path_metrics_matching(
        &self,
        key: CarrierPathKey,
        path_instance_id: Option<CarrierPathInstanceId>,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        let (_, changed) = self.install_path_metrics_entry_matching(
            key,
            path_instance_id,
            ServerPathMetricsEntry {
                metrics,
                source,
                recorded_at: Instant::now(),
                capacity_proof: None,
                tcp_capacity_proof: None,
            },
            true,
        );
        if changed {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_response_path_metrics_attached",
                format_args!(
                    "session_id={} underlay={:?} path_id={} source={:?} direction={:?} rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence_ppm={} app_limited={} ack_sample={} sample_count={} sample_bytes={}",
                    self.session_id.0,
                    key.underlay,
                    key.path_id.0,
                    source,
                    metrics.direction,
                    metrics.delivery_rate_bps as f64 / 1_000_000.0,
                    metrics.pacing_rate_bps as f64 / 1_000_000.0,
                    metrics.srtt_us as f64 / 1000.0,
                    metrics.confidence_ppm,
                    metrics.app_limited,
                    metrics.has_ack_derived_data_sample,
                    metrics.data_sample_count,
                    metrics.data_sample_bytes,
                ),
            );
        }
    }

    fn install_path_metrics_entry_matching(
        &self,
        key: CarrierPathKey,
        path_instance_id: Option<CarrierPathInstanceId>,
        mut path_metrics: ServerPathMetricsEntry,
        notify: bool,
    ) -> (bool, bool) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let now = Instant::now();
        let source = path_metrics.source;
        let metrics = path_metrics.metrics;
        let explicit_quic_capacity_proof = path_metrics.capacity_proof.is_some();
        let explicit_tcp_capacity_proof = path_metrics.tcp_capacity_proof.is_some();
        let mut matched = false;
        let mut changed = false;
        for entry in &mut outputs.entries {
            if entry.key == key
                && path_instance_id.is_none_or(|instance| entry.path_instance_id == instance)
            {
                matched = true;
                let current = match source {
                    ServerPathMetricsSource::LocalSender => &mut entry.local_path_metrics,
                    ServerPathMetricsSource::PeerHint => &mut entry.peer_path_metrics,
                };
                if !explicit_quic_capacity_proof {
                    path_metrics.capacity_proof = current
                        .and_then(|previous| previous.capacity_proof)
                        .filter(|proof| proof.expires_at > now);
                }
                if !explicit_tcp_capacity_proof {
                    path_metrics.tcp_capacity_proof = current
                        .and_then(|previous| previous.tcp_capacity_proof)
                        .filter(|proof| proof.expires_at > now);
                }
                let scheduling_changed = current.is_none_or(|previous| {
                    previous.source != source
                        || !server_path_metrics_scheduling_equivalent(previous.metrics, metrics)
                        || previous.capacity_proof != path_metrics.capacity_proof
                        || previous.tcp_capacity_proof != path_metrics.tcp_capacity_proof
                });
                *current = Some(path_metrics);
                changed |= scheduling_changed;
            }
        }
        if changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(outputs);
        if changed && notify {
            self.graduate_completed_response_startup_owner();
            self.notify_update();
        }
        (matched, changed)
    }
}

fn server_path_metrics_scheduling_equivalent(
    mut left: PathMetrics,
    mut right: PathMetrics,
) -> bool {
    // Epoch and age refresh evidence lifetime but do not change a ranking or
    // admission input. Suppressing that no-op update avoids waking every bound
    // response stream on each idle QUIC metrics poll.
    left.metric_epoch = 0;
    left.metric_age_us = 0;
    right.metric_epoch = 0;
    right.metric_age_us = 0;
    left == right
}

#[cfg(test)]
#[path = "evidence_test.rs"]
mod tests;

#[cfg(test)]
#[path = "quic_evidence_test.rs"]
mod quic_tests;
