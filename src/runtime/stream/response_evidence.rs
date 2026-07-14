//! Carrier evidence provenance, direction, identity, and freshness checks.
//! Admission and scheduling consume these facts but cannot manufacture them.

use super::response_session::{
    valid_quic_capacity_proof_candidate_at, well_formed_quic_capacity_proof_candidate,
};
use super::response_topology::ResponseStreamOutputEntry;
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES};
use crate::model::timing::quic_bulk_proof_freshness_horizon;
use crate::protocol::{PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
use crate::runtime::path::tcp::capacity::{
    TcpCapacityProofCandidate, valid_tcp_capacity_proof_candidate_at,
};
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

#[cfg(test)]
#[path = "response_evidence_test.rs"]
mod tests;

#[cfg(test)]
#[path = "response_quic_evidence_test.rs"]
mod quic_tests;
