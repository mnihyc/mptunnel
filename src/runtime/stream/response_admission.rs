//! Response sender and Service-feed eligibility derived from validated evidence.
//! This owner does not collect metrics, build snapshots, or mutate topology.

use super::response_evidence::{
    ServerPathMetricsSource, server_output_local_path_metrics,
    server_path_metrics_has_bulk_rate_evidence, server_path_metrics_has_sender_evidence,
    server_udp_path_metrics_has_durable_rate_estimate,
};
use super::response_topology::ResponseStreamOutputEntry;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, product_delivery_samples_override_startup_prior,
    reliable_subflow_startup_sample_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;

pub(in crate::runtime) fn server_output_has_sender_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    entry.owner_data_acked_bytes > 0
        || entry.delivery_samples > 0
        || entry.delivery_rate_bps.is_some()
        || matches!(
            server_output_local_path_metrics(entry),
            Some(path_metrics) if server_path_metrics_has_sender_evidence(path_metrics)
        )
}

/// Endpoint-only TCP has no carrier hint worth preserving. After an exact
/// startup sample, it may temporarily inherit the proven Service opportunity
/// instead of running a second exclusive measurement transport.
pub(in crate::runtime) fn server_output_accepts_service_capacity_prior(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    entry.key.underlay == UnderlayProtocol::Tcp
        && !product_delivery_samples_override_startup_prior(entry.delivery_samples)
        && !server_output_local_path_metrics(entry)
            .is_some_and(server_path_metrics_has_bulk_rate_evidence)
        && entry.peer_path_metrics.is_some_and(|metrics| {
            metrics.source == ServerPathMetricsSource::PeerHint
                && metrics.metrics.app_limited
                && !metrics.metrics.has_ack_derived_data_sample
        })
}

pub(in crate::runtime) fn server_output_has_durable_product_progress(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    entry.product_progress_rate_bps.is_some()
        && server_output_has_durable_product_ack_progress(entry, mux_limits)
}

pub(super) fn server_output_has_durable_product_ack_progress(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    // Exact ownership bytes may be durable even when fragmented callbacks do
    // not produce an individual point-rate sample.
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    entry
        .owner_data_acked_bytes
        .saturating_add(accounting_slack)
        >= sample_floor
}

#[cfg(test)]
pub(in crate::runtime) fn server_output_has_bulk_rate_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    server_output_has_bulk_rate_evidence_with_limits(entry, MuxLimits::default())
}

pub(in crate::runtime) fn server_output_has_bulk_rate_evidence_with_limits(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    let has_local_carrier_bulk = matches!(
        server_output_local_path_metrics(entry),
        Some(path_metrics) if server_path_metrics_has_bulk_rate_evidence(path_metrics)
    );
    match entry.key.underlay {
        UnderlayProtocol::Udp => has_local_carrier_bulk,
        UnderlayProtocol::Tcp => {
            has_local_carrier_bulk || server_output_has_durable_product_progress(entry, mux_limits)
        }
    }
}

pub(in crate::runtime) fn server_output_has_service_feed_evidence_with_limits(
    entry: &ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) -> bool {
    match entry.key.underlay {
        UnderlayProtocol::Udp => {
            server_output_has_durable_product_progress(entry, mux_limits)
                || matches!(
                    server_output_local_path_metrics(entry),
                    Some(path_metrics) if server_udp_path_metrics_has_durable_rate_estimate(path_metrics)
                )
        }
        UnderlayProtocol::Tcp => {
            server_output_has_bulk_rate_evidence_with_limits(entry, mux_limits)
        }
    }
}

#[cfg(test)]
#[path = "response_admission_test.rs"]
mod tests;
