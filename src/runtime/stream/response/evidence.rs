//! Carrier evidence validation and mutation for one response binding.
//! Writers preserve attachment identity; admission and scheduling only consume facts.

use super::ResponseStreamBinding;
use super::attachment::ResponseStreamOutputEntry;
#[cfg(all(test, feature = "lab-diagnostics"))]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES,
    reliable_path_startup_sample_limit_bytes,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::protocol::{PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::CarrierDeliveryRateSample;
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
    pub(in crate::runtime::stream) native_drain_observed: bool,
    pub(in crate::runtime::stream) carrier_delivery_rate_sample: Option<CarrierDeliveryRateSample>,
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
    if server_carrier_delivery_rate_sample_has_bulk_evidence(path_metrics)
        && let Some(sample) = path_metrics.carrier_delivery_rate_sample
    {
        return sample.delivery_rate_bps.max(1) as f64;
    }
    path_metrics.metrics.delivery_rate_bps.max(1) as f64
}

pub(super) fn server_path_metrics_bulk_sample_floor_bytes(metrics: PathMetrics) -> u64 {
    // Completion scheduling may trust a rate only after ACK evidence covers
    // the native congestion window. TCP and QUIC acquire that evidence
    // differently, but neither may be frozen by a scheduler-limited sample.
    metrics
        .inflight_hi_bytes
        .max(metrics.inflight_limit_bytes)
        .max(PATH_OPEN_SCORE_BYTES as u64)
}

pub(super) fn server_path_metrics_has_bulk_rate_evidence(
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    if server_carrier_delivery_rate_sample_has_bulk_evidence(path_metrics) {
        return true;
    }
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

fn server_carrier_delivery_rate_sample_has_bulk_evidence(
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    let Some(sample) = path_metrics.carrier_delivery_rate_sample else {
        return false;
    };
    // The native tracker freezes the congestion-window coverage threshold at
    // the start of its delivery epoch. Requiring a later, larger live cwnd here
    // would make successful slow-start evidence revoke itself as cwnd grows.
    let sample_floor =
        (MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64).max(PATH_OPEN_SCORE_BYTES as u64);
    sample.delivery_rate_bps > 0
        && sample.sample_count > 0
        && sample.delivery_window_covered
        && sample.sample_bytes >= sample_floor
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

    #[cfg(test)]
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

    #[cfg(test)]
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
                native_drain_observed: false,
                carrier_delivery_rate_sample: None,
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
        let mut matched = false;
        let mut changed = false;
        for entry in &mut outputs.entries {
            if entry.key == key
                && path_instance_id.is_none_or(|instance| entry.path_instance_id == instance)
            {
                matched = true;
                changed |= install_path_metrics_entry(entry, path_metrics);
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

pub(super) fn install_path_metrics_entry(
    entry: &mut ResponseStreamOutputEntry,
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    let source = path_metrics.source;
    let current = match source {
        ServerPathMetricsSource::LocalSender => &mut entry.local_path_metrics,
        ServerPathMetricsSource::PeerHint => &mut entry.peer_path_metrics,
    };
    let scheduling_changed = current.is_none_or(|previous| {
        previous.source != source
            || previous.native_drain_observed != path_metrics.native_drain_observed
            || previous.carrier_delivery_rate_sample != path_metrics.carrier_delivery_rate_sample
            || !server_path_metrics_scheduling_equivalent(previous.metrics, path_metrics.metrics)
    });
    *current = Some(path_metrics);
    scheduling_changed
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
