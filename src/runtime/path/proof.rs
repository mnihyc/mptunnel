#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_PATH_PROOF_ID: AtomicU64 = AtomicU64::new(1);
const PATH_PROOF_TOKEN_BYTES: usize = 8;

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct PathProofObservation {
    pub(in crate::runtime) proof_id: u64,
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
            elapsed: pending.sent_at.elapsed(),
            sent_at: pending.sent_at,
        })
    }
}

pub(in crate::runtime) fn allocated_path_proof_data_frame(
    path_id: PathId,
    mux_limits: MuxLimits,
) -> (u64, Frame) {
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
    commands.try_enqueue_admitted_frame(frame, TrafficClass::Control)?;
    Ok(proof_id)
}

fn path_proof_payload_bytes(mux_limits: MuxLimits) -> usize {
    // Reachability validation is a challenge/response, not a capacity sample.
    // Eight bytes matches QUIC PATH_CHALLENGE while the authenticated carrier
    // continues to own MTU discovery, congestion control, and loss recovery.
    PATH_PROOF_TOKEN_BYTES
        .min(mux_limits.max_payload_bytes)
        .max(1)
}
