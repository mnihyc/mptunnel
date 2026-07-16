//! Shared response-stream test fixtures.

use super::ResponseStreamBinding;
use super::attachment::ResponseStreamOutputEntry;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, QuicCapacityProofCandidate, quic_capacity_receipt_rate_bps,
    reliable_path_startup_sample_limit_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, reliable_path_command_channels,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) fn binding_for_underlay(
    underlay: UnderlayProtocol,
) -> (
    Arc<ResponseStreamBinding>,
    CarrierPathKey,
    ReliablePathCommandReceivers,
) {
    let (commands, receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay,
        path_id: PathId(0),
    };
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        underlay,
        key.path_id,
        commands,
        TrafficClass::Throughput,
    );
    (binding, key, receivers)
}

pub(super) fn stream_data_frame(payload_len: usize) -> Frame {
    stream_data_frame_at(0, payload_len)
}

pub(super) fn stream_data_frame_at(offset: u64, payload_len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        payload: Bytes::from(vec![0x5a; payload_len]),
    }
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

pub(super) fn test_quic_capacity_proof(
    mux_limits: MuxLimits,
    token: u64,
    proof_validity: Duration,
) -> QuicCapacityProofCandidate {
    let proof_bytes = reliable_path_startup_sample_limit_bytes(mux_limits);
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
