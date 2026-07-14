use super::*;
use crate::model::capacity::relay_lane_startup_chunk_bytes;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_PATH_PROOF_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct PathProofObservation {
    pub(in crate::runtime) proof_id: u64,
    pub(in crate::runtime) bytes: u64,
    pub(in crate::runtime) elapsed: Duration,
    pub(in crate::runtime) sent_at: Instant,
}

#[derive(Default)]
pub(in crate::runtime) struct PathProofTracker {
    pending: HashMap<(PathId, u64), PendingPathProof>,
}

struct PendingPathProof {
    bytes: u32,
    sent_at: Instant,
}

impl PathProofTracker {
    pub(in crate::runtime) fn record_sent_frame(&mut self, frame: &Frame) {
        let Frame::PathProofData {
            path_id,
            proof_id,
            payload,
        } = frame
        else {
            return;
        };
        let bytes = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        self.pending.insert(
            (*path_id, *proof_id),
            PendingPathProof {
                bytes,
                sent_at: Instant::now(),
            },
        );
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "path_proof_tracker",
            format_args!(
                "phase=sent path_id={} proof_id={} payload_bytes={} pending={}",
                path_id.0,
                proof_id,
                bytes,
                self.pending.len(),
            ),
        );
    }

    pub(in crate::runtime) fn acknowledge(
        &mut self,
        path_id: PathId,
        proof_id: u64,
        payload_bytes: u32,
    ) -> Option<PathProofObservation> {
        let Some(pending) = self.pending.remove(&(path_id, proof_id)) else {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_proof_tracker",
                format_args!(
                    "phase=ack_miss path_id={} proof_id={} payload_bytes={} pending={}",
                    path_id.0,
                    proof_id,
                    payload_bytes,
                    self.pending.len(),
                ),
            );
            return None;
        };
        let bytes = pending.bytes.min(payload_bytes);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "path_proof_tracker",
            format_args!(
                "phase=ack path_id={} proof_id={} payload_bytes={} acknowledged_bytes={} elapsed_us={} pending={}",
                path_id.0,
                proof_id,
                payload_bytes,
                bytes,
                pending.sent_at.elapsed().as_micros(),
                self.pending.len(),
            ),
        );
        (bytes > 0).then_some(PathProofObservation {
            proof_id,
            bytes: u64::from(bytes),
            elapsed: pending.sent_at.elapsed(),
            sent_at: pending.sent_at,
        })
    }
}

fn allocated_path_proof_data_frame(path_id: PathId, mux_limits: MuxLimits) -> (u64, Frame) {
    let payload_bytes = path_proof_payload_bytes(mux_limits);
    let proof_id = NEXT_PATH_PROOF_ID.fetch_add(1, Ordering::Relaxed);
    let frame = Frame::PathProofData {
        path_id,
        proof_id,
        payload: Bytes::from(vec![0u8; payload_bytes]),
    };
    (proof_id, frame)
}

pub(in crate::runtime) fn path_proof_ack_frame(
    path_id: PathId,
    proof_id: u64,
    payload_len: usize,
) -> Frame {
    Frame::PathProofAck {
        path_id,
        proof_id,
        payload_bytes: u32::try_from(payload_len).unwrap_or(u32::MAX),
    }
}

pub(in crate::runtime) fn enqueue_path_proof_frame(
    commands: &ReliablePathCommandSender,
    path_id: PathId,
    mux_limits: MuxLimits,
) -> Result<u64, RuntimeError> {
    let (proof_id, frame) = allocated_path_proof_data_frame(path_id, mux_limits);
    commands.try_enqueue_admitted_frame(frame, FlowLane::Control)?;
    Ok(proof_id)
}

pub(in crate::runtime) fn enqueue_stream_ordered_path_proof_frame(
    commands: &ReliablePathCommandSender,
    path_id: PathId,
    mux_limits: MuxLimits,
    lane: FlowLane,
) -> Result<u64, RuntimeError> {
    let (proof_id, frame) = allocated_path_proof_data_frame(path_id, mux_limits);
    commands.try_enqueue_stream_ordered_frame(frame, lane)?;
    Ok(proof_id)
}

pub(in crate::runtime) fn path_proof_metrics(
    path_id: PathId,
    underlay: UnderlayProtocol,
    direction: PathMetricDirection,
    observation: PathProofObservation,
) -> Option<PathMetrics> {
    if observation.bytes == 0 {
        return None;
    }
    let rate_bps = (observation.bytes as f64 * 8.0
        / observation
            .elapsed
            .max(QUIC_TIMER_GRANULARITY)
            .as_secs_f64())
    .max(1.0)
    .round() as u64;
    let srtt_us = duration_to_micros_u32(observation.elapsed);
    Some(PathMetrics {
        path_id,
        underlay,
        direction,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: srtt_us,
        srtt_us,
        rttvar_us: 0,
        jitter_us: 0,
        delivery_rate_bps: rate_bps,
        pacing_rate_bps: rate_bps,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 0,
        inflight_hi_bytes: 0,
        confidence_ppm: ratio_to_ppm(
            observation.bytes as f64 / (PATH_OPEN_SCORE_BYTES.max(1) as f64),
        ),
        app_limited: true,
        has_ack_derived_data_sample: false,
        data_sample_count: 0,
        data_sample_bytes: 0,
    })
}

fn path_proof_payload_bytes(mux_limits: MuxLimits) -> usize {
    PATH_OPEN_SCORE_BYTES
        .min(relay_lane_startup_chunk_bytes(
            FlowLane::Latency,
            mux_limits,
        ))
        .min(mux_limits.max_payload_bytes)
        .max(1)
}

fn duration_to_micros_u32(duration: Duration) -> u32 {
    u32::try_from(duration.as_micros()).unwrap_or(u32::MAX)
}
