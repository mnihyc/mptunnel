//! Optional observation of response sender decisions after policy selection.
//! Instrumentation may observe snapshots but cannot change admission or scheduling results.

use super::ResponseStreamBinding;
#[cfg(feature = "lab-diagnostics")]
use super::attachment::ResponseStreamOutputEntry;
#[cfg(feature = "lab-diagnostics")]
use super::evidence::ServerPathMetricsSource;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{
    lab_diagnostic, lab_diagnostic_event_enabled, lab_sender_service_decision,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::admission::{
    bulk_active_service_product_envelope_bytes, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::capacity::{
    reliable_bulk_carrier_feed_quantum_bytes, reliable_subflow_startup_sample_limit_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::path::CarrierPathInstanceId;
use crate::model::path::CarrierPathKey;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::{
    reliable_path_frame_pacing_bytes, reliable_stream_frame_accounted_bytes,
};
use crate::protocol::{Frame, SessionId, StreamId};
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use crate::scheduler::FlowLane;
#[cfg(feature = "lab-diagnostics")]
use crate::scheduler::PathSnapshot;
#[cfg(feature = "lab-diagnostics")]
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
#[derive(Clone, Copy)]
pub(super) struct ResponseServiceHandoffDiagnosticState {
    model_generation: u64,
    evaluation_signature: u64,
    capacity_marker_signature: u64,
    emitted_at: Instant,
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct ResponseServiceFeedDiagnosticState {
    path_instance_id: CarrierPathInstanceId,
    attachment_role: StreamOpenRole,
    is_active: bool,
    has_bulk_rate_evidence: bool,
    has_service_feed_evidence: bool,
    owner_progress_bucket: u8,
    product_rate_available: bool,
    carrier_sample_bucket: u8,
    carrier_sample_available: bool,
    carrier_app_limited: bool,
    latency_pressure: bool,
    source_limit_bytes: u64,
    emission_limit_bytes: u64,
}

impl ResponseStreamBinding {
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn should_emit_response_service_handoff_diagnostic(
        &self,
        model_generation: u64,
        evaluation_signature: u64,
        capacity_marker_signature: u64,
        now: Instant,
    ) -> bool {
        const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

        let mut previous = self
            .response_service_handoff_diagnostic
            .lock()
            .expect("response Service handoff diagnostic lock");
        let should_emit = previous.is_none_or(|previous| {
            previous.evaluation_signature != evaluation_signature
                || previous.capacity_marker_signature != capacity_marker_signature
                || (previous.model_generation != model_generation
                    && now.saturating_duration_since(previous.emitted_at) >= REFRESH_INTERVAL)
        });
        if should_emit {
            *previous = Some(ResponseServiceHandoffDiagnosticState {
                model_generation,
                evaluation_signature,
                capacity_marker_signature,
                emitted_at: now,
            });
        }
        should_emit
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(super) fn lab_response_service_feed_state(
        &self,
        entry: &ResponseStreamOutputEntry,
        snapshot: PathSnapshot,
        lane: FlowLane,
        is_active: bool,
        has_bulk_rate_evidence: bool,
        has_service_feed_evidence: bool,
        command_pending_bytes: u64,
    ) {
        if !lab_diagnostic_event_enabled("response_service_feed_state") {
            return;
        }

        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(self.mux_limits);
        let startup_floor = reliable_subflow_startup_sample_limit_bytes(self.mux_limits);
        let service_floor =
            bulk_service_horizon_payload_bytes(payload_bytes, self.mux_limits) as u64;
        let progress_bucket = |bytes: u64| {
            if bytes == 0 {
                0
            } else if bytes < startup_floor {
                1
            } else if bytes < service_floor {
                2
            } else {
                3
            }
        };
        let local_metrics = entry
            .local_path_metrics
            .as_ref()
            .filter(|metrics| metrics.source == ServerPathMetricsSource::LocalSender)
            .map(|metrics| metrics.metrics);
        let carrier_sample_bytes = local_metrics.map_or(0, |metrics| metrics.data_sample_bytes);
        let carrier_sample_count = local_metrics.map_or(0, |metrics| metrics.data_sample_count);
        let carrier_sample_available =
            local_metrics.is_some_and(|metrics| metrics.has_ack_derived_data_sample);
        let carrier_app_limited = local_metrics.is_none_or(|metrics| metrics.app_limited);
        let latency_pressure = snapshot.active_latency_sensitive_flows > 0
            || snapshot.session_active_latency_sensitive_flows > 0;
        let source_limit_bytes = if latency_pressure {
            service_floor
        } else if has_service_feed_evidence {
            bulk_active_service_product_envelope_bytes(snapshot, payload_bytes, self.mux_limits)
        } else {
            bulk_service_feed_reservoir_payload_bytes(payload_bytes, self.mux_limits) as u64
        };
        let emission_limit_bytes = if !has_service_feed_evidence {
            if entry.key.underlay == UnderlayProtocol::Udp {
                bulk_service_feed_reservoir_payload_bytes(payload_bytes, self.mux_limits) as u64
            } else {
                service_floor
            }
        } else if latency_pressure {
            bulk_latency_pressure_service_feed_window_bytes(payload_bytes, self.mux_limits)
        } else {
            bulk_active_service_product_envelope_bytes(snapshot, payload_bytes, self.mux_limits)
        };
        let state = ResponseServiceFeedDiagnosticState {
            path_instance_id: entry.path_instance_id,
            attachment_role: entry.role,
            is_active,
            has_bulk_rate_evidence,
            has_service_feed_evidence,
            owner_progress_bucket: progress_bucket(entry.owner_data_acked_bytes),
            product_rate_available: entry.product_progress_rate_bps.is_some(),
            carrier_sample_bucket: progress_bucket(carrier_sample_bytes),
            carrier_sample_available,
            carrier_app_limited,
            latency_pressure,
            source_limit_bytes,
            emission_limit_bytes,
        };
        let identity = (entry.key, entry.incarnation);
        let mut previous = self
            .response_service_feed_diagnostic
            .lock()
            .expect("response Service-feed diagnostic lock");
        if previous.get(&identity) == Some(&state) {
            return;
        }
        previous.insert(identity, state);
        drop(previous);

        lab_diagnostic(
            "response_service_feed_state",
            format_args!(
                "session_id={} binding_instance_id={} path_underlay={:?} path_id={} path_instance_id={} incarnation={} attachment_role={:?} lane={:?} is_active={} latency_pressure={} owner_data_acked_bytes={} owner_progress_bucket={} product_progress_rate_mbps={:.3} product_rate_available={} carrier_sample_bytes={} carrier_sample_count={} carrier_sample_available={} carrier_app_limited={} startup_floor_bytes={} service_floor_bytes={} bulk_rate_evidence={} service_feed_evidence={} source_limit_bytes={} emission_limit_bytes={} command_pending_bytes={} owner_data_inflight_bytes={} product_inflight_bytes={} product_queue_bytes={} path_queue_bytes={}",
                self.session_id.0,
                self.binding_instance_id,
                entry.key.underlay,
                entry.key.path_id.0,
                entry.path_instance_id.as_u64(),
                entry.incarnation,
                entry.role,
                lane,
                is_active,
                latency_pressure,
                entry.owner_data_acked_bytes,
                state.owner_progress_bucket,
                entry.product_progress_rate_bps.unwrap_or(0.0) / 1_000_000.0,
                state.product_rate_available,
                carrier_sample_bytes,
                carrier_sample_count,
                carrier_sample_available,
                carrier_app_limited,
                startup_floor,
                service_floor,
                has_bulk_rate_evidence,
                has_service_feed_evidence,
                source_limit_bytes,
                emission_limit_bytes,
                command_pending_bytes,
                entry.owner_data_in_flight_bytes,
                snapshot.product_bytes_in_flight,
                snapshot.product_queue_bytes,
                snapshot.queue_bytes,
            ),
        );
    }
}

pub(in crate::runtime) fn record_server_sender_decision(
    session_id: SessionId,
    stream_id: StreamId,
    key: CarrierPathKey,
    frame: &Frame,
    lane: FlowLane,
    reason: &'static str,
    bulk_rate_evidence: Option<bool>,
) {
    #[cfg(feature = "lab-diagnostics")]
    lab_sender_service_decision(
        "server",
        Some(session_id.0),
        stream_id.0,
        reason,
        sender_service_frame_kind(frame),
        reliable_stream_frame_accounted_bytes(frame),
        bulk_rate_evidence,
        format_args!(
            "path_underlay={:?} path_id={} lane={:?} pacing_bytes={}",
            key.underlay,
            key.path_id.0,
            lane,
            reliable_path_frame_pacing_bytes(frame),
        ),
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (
        session_id,
        stream_id,
        key,
        frame,
        lane,
        reason,
        bulk_rate_evidence,
    );
}

#[cfg(feature = "lab-diagnostics")]
pub(super) fn sender_service_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}
