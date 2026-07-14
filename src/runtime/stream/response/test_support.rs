use super::ResponseStreamBinding;
use super::attachment::ResponseStreamOutputEntry;
use super::evidence::{ServerPathMetricsEntry, ServerPathMetricsSource};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, PathRateSample, QuicCapacityProofCandidate,
    RELIABLE_INITIAL_WINDOW_PACKETS, quic_capacity_receipt_rate_bps,
    reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{
    Frame, PathId, PathMetricDirection, PathMetrics, SessionId, StreamFlags, StreamId,
    UnderlayProtocol,
};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::scheduler::FlowLane;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) fn binding_for_underlay(
    underlay: UnderlayProtocol,
) -> (Arc<ResponseStreamBinding>, CarrierPathKey) {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay,
        path_id: PathId(0),
    };
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        underlay,
        key.path_id,
        commands,
        FlowLane::Throughput,
    );
    (binding, key)
}

pub(super) fn stream_data_frame(payload_len: usize) -> Frame {
    stream_data_frame_at(0, payload_len)
}

pub(super) fn stream_data_frame_at(offset: u64, payload_len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; payload_len]),
    }
}

pub(super) fn test_ack_clock_rate_sample(bytes: u64, rate_bps: f64) -> PathRateSample {
    PathRateSample::new(
        bytes,
        Duration::from_secs_f64(bytes as f64 * 8.0 / rate_bps),
    )
    .expect("valid ACK-clock rate sample")
}

pub(super) fn assert_test_rate_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("calibrated rate");
    assert!((actual - expected).abs() / expected.max(1.0) < 1e-6);
}

pub(super) fn output_entry_for_key(
    binding: &ResponseStreamBinding,
    key: CarrierPathKey,
) -> ResponseStreamOutputEntry {
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let mut matching = outputs.entries.iter().filter(|entry| entry.key == key);
    let entry = matching
        .next()
        .expect("test response output key is attached");
    assert!(
        matching.next().is_none(),
        "test response output key must identify exactly one attachment"
    );
    entry.clone()
}

pub(super) fn mark_test_response_output_bulk_proven(
    entry: &mut ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) {
    entry.product_progress_rate_bps = Some(100_000_000.0);
    entry.delivery_rate_bps = Some(100_000_000.0);
    entry.delivery_samples = 1;
    entry.owner_data_acked_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
}

pub(super) fn mark_test_quic_output_carrier_bulk_proven(
    entry: &mut ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) {
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    entry.local_path_metrics = Some(ServerPathMetricsEntry {
        source: ServerPathMetricsSource::LocalSender,
        recorded_at: Instant::now(),
        capacity_proof: None,
        tcp_capacity_proof: None,
        metrics: PathMetrics {
            path_id: entry.key.path_id,
            underlay: UnderlayProtocol::Udp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: 1,
            metric_age_us: 0,
            min_rtt_us: 10_000,
            srtt_us: 12_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 500_000_000,
            pacing_rate_bps: 500_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: sample_bytes,
            inflight_hi_bytes: sample_bytes,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: sample_bytes,
        },
    });
}

pub(super) fn test_quic_capacity_proof(
    mux_limits: MuxLimits,
    token: u64,
    proof_validity: Duration,
) -> QuicCapacityProofCandidate {
    let proof_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(proof_bytes / 8);
    let proof_elapsed = Duration::from_millis(2);
    let accepted_at = Instant::now();
    QuicCapacityProofCandidate {
        token,
        train_bytes: proof_bytes,
        sample_floor_bytes: proof_bytes,
        accounting_slack_bytes,
        warmup_bytes: 0,
        required_proof_bytes: proof_bytes - accounting_slack_bytes,
        written_bytes: proof_bytes,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: proof_bytes,
        proof_elapsed,
        rate_bps: quic_capacity_receipt_rate_bps(proof_bytes, proof_elapsed)
            .expect("test receipt rate"),
        accepted_at,
        expires_at: accepted_at + proof_validity,
        proof_validity,
    }
}

pub(super) fn mark_test_quic_output_receipt_bulk_proven(
    entry: &mut ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
    token: u64,
    proof_validity: Duration,
) -> QuicCapacityProofCandidate {
    mark_test_quic_output_carrier_bulk_proven(entry, mux_limits);
    let proof = test_quic_capacity_proof(mux_limits, token, proof_validity);
    let path_metrics = entry
        .local_path_metrics
        .as_mut()
        .expect("test QUIC metrics");
    // Keep receipt proof as the only bulk authority so expiry is observable.
    path_metrics.metrics.app_limited = true;
    path_metrics.metrics.has_ack_derived_data_sample = false;
    path_metrics.metrics.confidence_ppm = 0;
    path_metrics.metrics.data_sample_count = 0;
    path_metrics.metrics.data_sample_bytes = 0;
    path_metrics.capacity_proof = Some(proof);
    proof
}
