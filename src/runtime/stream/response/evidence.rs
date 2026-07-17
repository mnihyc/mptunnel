//! Carrier evidence validation and mutation for one response binding.
//! Writers preserve attachment identity; admission and scheduling only consume facts.

use super::ResponseStreamBinding;
use super::attachment::ResponseStreamOutputEntry;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
    reliable_path_startup_sample_limit_bytes,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::protocol::{PathMetricDirection, PathMetrics, UnderlayProtocol};
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
pub(super) fn server_path_metrics_estimate_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    path_metrics.metrics.delivery_rate_bps.max(1) as f64
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
            let startup_capacity_admission_sample = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
                .saturating_div(2)
                .max(minimum_meaningful_sample);
            carrier_floor
                .max(minimum_meaningful_sample)
                .min(startup_capacity_admission_sample)
        }
    }
}

pub(super) fn server_path_metrics_has_bulk_rate_evidence(
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    let sample_floor = server_path_metrics_bulk_sample_floor_bytes(path_metrics.metrics);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    let effective_metric_age = Duration::from_micros(u64::from(path_metrics.metrics.metric_age_us))
        .saturating_add(Instant::now().saturating_duration_since(path_metrics.recorded_at));
    // `app_limited` describes the current carrier instant. The sample timestamp
    // and accumulated non-app-limited ACK evidence own the lifetime of the last
    // qualified delivery rate, so a later idle instant must not invalidate it.
    let native_bulk_proof_is_eligible = effective_metric_age
        < transport_rate_sample_freshness_horizon(
            Duration::from_micros(u64::from(path_metrics.metrics.srtt_us.max(1))),
            Duration::from_micros(u64::from(path_metrics.metrics.rttvar_us)),
        );
    path_metrics.source == ServerPathMetricsSource::LocalSender
        // Source expiry is authoritative; age is defense in depth if an idle
        // refresh is delayed or reordered before response scheduling observes it.
        && native_bulk_proof_is_eligible
        && path_metrics.metrics.has_ack_derived_data_sample
        && path_metrics.metrics.data_sample_count > 0
        && path_metrics
            .metrics
            .data_sample_bytes
            .saturating_add(packet_accounting_slack)
            >= sample_floor
}

pub(super) fn server_output_has_durable_product_ack_progress(
    entry: &ResponseStreamOutputEntry,
    mux_limits: crate::mux::MuxLimits,
) -> bool {
    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    entry
        .original_data_acked_bytes
        .saturating_add(accounting_slack)
        >= sample_floor
}

pub(super) fn server_output_has_bulk_rate_evidence(
    entry: &ResponseStreamOutputEntry,
    mux_limits: crate::mux::MuxLimits,
) -> bool {
    let has_local_carrier_sample = server_output_local_path_metrics(entry)
        .is_some_and(server_path_metrics_has_bulk_rate_evidence);
    match entry.key.underlay {
        UnderlayProtocol::Udp => has_local_carrier_sample,
        UnderlayProtocol::Tcp => {
            has_local_carrier_sample
                || (entry.product_progress_rate_bps.is_some()
                    && server_output_has_durable_product_ack_progress(entry, mux_limits))
        }
    }
}

impl ResponseStreamBinding {
    pub(in crate::runtime::stream) fn set_sender_queue_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let changed = outputs.data_level_queue_bytes != bytes;
        outputs.data_level_queue_bytes = bytes;
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

    pub(in crate::runtime) fn update_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        self.update_path_metrics_matching(key, Some(path_instance_id), metrics, source);
    }

    pub(in crate::runtime) fn install_stored_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        path_metrics: ServerPathMetricsEntry,
    ) {
        self.install_path_metrics_entry_matching(key, Some(path_instance_id), path_metrics, true);
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
        path_metrics: ServerPathMetricsEntry,
        notify: bool,
    ) -> (bool, bool) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let source = path_metrics.source;
        let metrics = path_metrics.metrics;
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
                let scheduling_changed = current.is_none_or(|previous| {
                    previous.source != source
                        || !server_path_metrics_scheduling_equivalent(previous.metrics, metrics)
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
    // enqueue input. Suppressing that no-op update avoids waking every bound
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
