//! Carrier evidence validation and mutation for one response binding.
//! Writers preserve attachment identity; admission and scheduling only consume facts.

use super::ResponseStreamBinding;
#[cfg(test)]
use super::attachment::ResponseProductRateEpoch;
use super::attachment::ResponseStreamOutputEntry;
#[cfg(all(test, feature = "lab-diagnostics"))]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES,
    reliable_path_startup_sample_limit_bytes,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
#[cfg(test)]
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::protocol::{PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::{CarrierDeliveryRateSample, CarrierNativeWindowSample};
use std::sync::atomic::Ordering;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

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
    /// Immutable native-window authority from the exact local carrier sample.
    /// `metrics.inflight_limit_bytes` may outlive it as diagnostics only.
    pub(in crate::runtime::stream) carrier_native_window_sample: Option<CarrierNativeWindowSample>,
    pub(in crate::runtime::stream) carrier_delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    // The wire budget is remaining authority at receipt. Residence is measured
    // from this instant and never recomputed from mutable RTT.
    pub(in crate::runtime::stream) recorded_at: Instant,
}

pub(super) fn server_path_metrics_native_window_sample(
    path_metrics: ServerPathMetricsEntry,
) -> Option<CarrierNativeWindowSample> {
    path_metrics.carrier_native_window_sample
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

pub(super) fn server_output_peer_path_metrics(
    entry: &ResponseStreamOutputEntry,
) -> Option<ServerPathMetricsEntry> {
    entry.peer_path_metrics.filter(|path_metrics| {
        path_metrics.source == ServerPathMetricsSource::PeerHint
            && path_metrics.metrics.direction == PathMetricDirection::ServerToClient
            && path_metrics.metrics.underlay == entry.key.underlay
            && path_metrics.metrics.path_id == entry.key.path_id
    })
}

pub(super) fn server_path_metrics_estimate_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    // Eligibility and value are separate facts. The caller evaluates the
    // sample's frozen deadline once; this accessor must not perform a second
    // clock read and fall back to retained PathMetrics at the expiry boundary.
    if let Some(sample) = path_metrics.carrier_delivery_rate_sample {
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

#[cfg(test)]
pub(super) fn server_path_metrics_has_bulk_rate_evidence(
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    server_path_metrics_has_bulk_rate_evidence_at(path_metrics, Instant::now())
}

pub(super) fn server_path_metrics_has_bulk_rate_evidence_at(
    path_metrics: ServerPathMetricsEntry,
    now: Instant,
) -> bool {
    if path_metrics.carrier_delivery_rate_sample.is_some() {
        // The local native-carrier sidecar owns the qualified ACK epoch. Refreshed merged
        // PathMetrics may retain old ACK counters, but cannot extend it.
        return server_carrier_delivery_rate_sample_has_bulk_evidence_at(path_metrics, now);
    }
    let sample_floor = server_path_metrics_bulk_sample_floor_bytes(path_metrics.metrics);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    // `app_limited` describes the current carrier instant. The sample timestamp
    // and accumulated non-app-limited ACK evidence own the lifetime of the last
    // qualified delivery rate, so a later idle instant must not invalidate it.
    path_metrics.source == ServerPathMetricsSource::LocalSender
        // Source expiry is authoritative; age is defense in depth if an idle
        // refresh is delayed or reordered before response scheduling observes it.
        && server_path_metrics_snapshot_is_fresh_at(path_metrics, now)
        && path_metrics.metrics.has_ack_derived_data_sample
        && path_metrics.metrics.data_sample_count > 0
        && path_metrics
            .metrics
            .data_sample_bytes
            .saturating_add(packet_accounting_slack)
            >= sample_floor
}

pub(super) fn server_path_metrics_rate_evidence_is_fresh_at(
    path_metrics: ServerPathMetricsEntry,
    now: Instant,
) -> bool {
    path_metrics.carrier_delivery_rate_sample.map_or_else(
        || server_path_metrics_snapshot_is_fresh_at(path_metrics, now),
        |sample| sample.observed_at <= now && now < sample.expires_at,
    )
}

pub(super) fn server_path_metrics_snapshot_is_fresh_at(
    path_metrics: ServerPathMetricsEntry,
    now: Instant,
) -> bool {
    path_metrics.metrics.rate_observed
        && path_metrics.metrics.rate_valid_for_us > 0
        && now.saturating_duration_since(path_metrics.recorded_at)
            < std::time::Duration::from_micros(path_metrics.metrics.rate_valid_for_us)
}

/// Distinguishes a genuine never-qualified startup prior from retained
/// measured evidence whose placement lifetime has expired.
pub(super) fn server_path_metrics_has_qualified_delivery_history(
    path_metrics: ServerPathMetricsEntry,
) -> bool {
    path_metrics.carrier_delivery_rate_sample.is_some() || path_metrics.metrics.rate_observed
}

fn server_carrier_delivery_rate_sample_has_bulk_evidence_at(
    path_metrics: ServerPathMetricsEntry,
    now: Instant,
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
        && sample.observed_at <= now
        && now < sample.expires_at
}

pub(super) fn server_output_has_durable_product_ack_progress(
    entry: &ResponseStreamOutputEntry,
    mux_limits: crate::mux::MuxLimits,
) -> bool {
    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    entry.original_data_acked_bytes >= sample_floor
}

/// Exact Product-volume authority for additional-output assignment.
///
/// This is an incarnation-local historical fact. Numeric rate publication and
/// its freshness lifetime are separate completion-ranking authorities.
pub(super) fn server_output_product_assignment_qualified(
    entry: &ResponseStreamOutputEntry,
    _mux_limits: crate::mux::MuxLimits,
) -> bool {
    entry.product_qualification.qualified()
}

#[cfg(test)]
pub(super) fn server_output_has_bulk_rate_evidence(
    entry: &ResponseStreamOutputEntry,
    mux_limits: crate::mux::MuxLimits,
) -> bool {
    server_output_has_bulk_rate_evidence_at(entry, mux_limits, Instant::now())
}

pub(super) fn server_output_product_rate_epoch_has_bulk_evidence_at(
    entry: &ResponseStreamOutputEntry,
    mux_limits: crate::mux::MuxLimits,
    now: Instant,
) -> bool {
    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    entry.product_rate_epoch.is_some_and(|epoch| {
        epoch.fresh_rate_at(now).is_some()
            && epoch.sample_count > 0
            && epoch.sample_bytes >= sample_floor
    }) && server_output_has_durable_product_ack_progress(entry, mux_limits)
}

pub(super) fn server_output_has_bulk_rate_evidence_at(
    entry: &ResponseStreamOutputEntry,
    mux_limits: crate::mux::MuxLimits,
    now: Instant,
) -> bool {
    if entry.key.underlay == UnderlayProtocol::Udp
        && entry.commands.native_rate_authority().is_some()
    {
        return entry.native_scheduling_shape.is_some_and(|shape| {
            let scope = shape.stamp().scope();
            scope.carrier_instance_id() == entry.path_instance_id
                && scope.direction() == PathMetricDirection::ServerToClient
                && shape.rate_bps() > 0
        });
    }
    let has_local_carrier_sample = server_output_local_path_metrics(entry)
        .is_some_and(|metrics| server_path_metrics_has_bulk_rate_evidence_at(metrics, now));
    match entry.key.underlay {
        UnderlayProtocol::Udp => has_local_carrier_sample,
        // TCP selects ReceiptMode: kernel ACK telemetry cannot become C.
        UnderlayProtocol::Tcp => {
            server_output_product_rate_epoch_has_bulk_evidence_at(entry, mux_limits, now)
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
        let srtt = Duration::from_secs_f64(srtt_ms.max(1.0) / 1000.0);
        entry.product_rate_epoch = ResponseProductRateEpoch::new(
            rate_bps.max(1.0),
            1,
            reliable_path_startup_sample_limit_bytes(self.mux_limits),
            Instant::now(),
            transport_rate_sample_freshness_horizon(srtt, srtt / 8),
        );
        entry.srtt_ms = Some(srtt_ms.max(1.0));
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    /// Installs fresh historical completion evidence while deliberately
    /// leaving the current Product qualification generation incomplete.
    /// This models a reactivated attachment whose prior service is known but
    /// whose new exact generation still requires acquisition authority.
    #[cfg(test)]
    pub(in crate::runtime) fn set_output_historical_product_model_for_test(
        &self,
        key: CarrierPathKey,
        rate_bps: f64,
        srtt_ms: f64,
    ) {
        self.set_output_product_model_for_test(key, rate_bps, srtt_ms);
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("test modeled output");
        entry.original_data_acked_bytes = entry
            .original_data_acked_bytes
            .max(reliable_path_startup_sample_limit_bytes(self.mux_limits));
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

    pub(in crate::runtime) fn install_native_scheduling_shape_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) -> bool {
        let scope = shape.stamp().scope();
        if key.underlay != UnderlayProtocol::Udp
            || scope.carrier_instance_id() != path_instance_id
            || scope.direction() != PathMetricDirection::ServerToClient
        {
            return false;
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.path_instance_id == path_instance_id)
        else {
            return false;
        };
        if let Some(previous) = entry.native_scheduling_shape {
            let previous_stamp = previous.stamp();
            let replacement_stamp = shape.stamp();
            if replacement_stamp.revision() < previous_stamp.revision()
                || (replacement_stamp.revision() == previous_stamp.revision()
                    && replacement_stamp != previous_stamp)
            {
                return false;
            }
        }
        if entry.native_scheduling_shape == Some(shape) {
            return false;
        }
        entry.native_scheduling_shape = Some(shape);
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        self.notify_update();
        true
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
        let recorded_at = Instant::now();
        let carrier_native_window_sample = (source == ServerPathMetricsSource::LocalSender)
            .then(|| CarrierNativeWindowSample::from_path_metrics_at(metrics, recorded_at))
            .flatten();
        let (_, changed) = self.install_path_metrics_entry_matching(
            key,
            path_instance_id,
            ServerPathMetricsEntry {
                metrics,
                source,
                native_drain_observed: false,
                carrier_native_window_sample,
                carrier_delivery_rate_sample: None,
                recorded_at,
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
    let native_quic_diagnostics_only = entry.key.underlay == UnderlayProtocol::Udp
        && entry.commands.native_rate_authority().is_some();
    let current = match source {
        ServerPathMetricsSource::LocalSender => &mut entry.local_path_metrics,
        ServerPathMetricsSource::PeerHint => &mut entry.peer_path_metrics,
    };
    if native_quic_diagnostics_only {
        // Lineage-scoped ACK/loss/peer telemetry remains inspectable, but it
        // cannot wake or modify the NativeMode scheduling model.
        *current = Some(path_metrics);
        return false;
    }
    let native_window_authority_changed = source == ServerPathMetricsSource::LocalSender
        && current.is_none_or(|previous| {
            let previous = server_path_metrics_native_window_sample(previous);
            let replacement = server_path_metrics_native_window_sample(path_metrics);
            previous.map(|sample| sample.inflight_limit_bytes)
                != replacement.map(|sample| sample.inflight_limit_bytes)
                || (previous.is_none_or(|sample| !sample.fresh_at(path_metrics.recorded_at))
                    && replacement.is_some_and(|sample| sample.fresh_at(path_metrics.recorded_at)))
        });
    let scheduling_changed = native_window_authority_changed
        || current.is_none_or(|previous| {
            previous.source != source
                || previous.native_drain_observed != path_metrics.native_drain_observed
                || previous.carrier_delivery_rate_sample
                    != path_metrics.carrier_delivery_rate_sample
                || !server_path_metrics_scheduling_equivalent(
                    previous.metrics,
                    path_metrics.metrics,
                )
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
    left.rate_valid_for_us = 0;
    right.metric_epoch = 0;
    right.metric_age_us = 0;
    right.rate_valid_for_us = 0;
    left == right
}

#[cfg(test)]
#[path = "tests_evidence.rs"]
mod tests;
