//! Shared request-sender topology fixtures.
//!
//! These helpers construct exact path membership and proof evidence used by
//! the request facade plus both concrete carrier-controller suites.

use crate::config::{ResourceLimits, SecurityConfig, SharedSecret};
use crate::model::capacity::PathRateSample;
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, reliable_relay_buffer_len};
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, StreamId, UnderlayProtocol};
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::PathProofObservation;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::relay::remote::{OpenedRemoteStream, ReliableRelayRemoteSet};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::FlowLane;
use crate::transport::PathSpec;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

fn security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

pub(super) fn poison_client_path_health_for_test(context: &ClientPathContext) {
    let poisoned_context = context.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned_context.health().lock().expect("path health lock");
            panic!("poison path health for a no-lock fast-path assertion");
        })
        .join()
        .is_err()
    );
    assert!(context.health().is_poisoned());
}

pub(super) fn client_test_context() -> ClientPathContext {
    let path = "tcp://127.0.0.1:10251".parse::<PathSpec>().expect("path");
    ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context")
}

pub(super) fn client_test_context_with_paths(paths: &[&str]) -> ClientPathContext {
    ClientPathContext::new(
        paths
            .iter()
            .map(|path| path.parse::<PathSpec>().expect("path"))
            .collect(),
        security(),
        ResourceLimits::default(),
    )
    .expect("context")
}

pub(super) fn opened_test_relay_stream(
    stream_id: StreamId,
    path_index: usize,
    commands: ReliablePathCommandSender,
) -> OpenedRemoteStream {
    opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, path_index, commands)
}

pub(super) fn opened_test_relay_stream_with_underlay(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
    commands: ReliablePathCommandSender,
) -> OpenedRemoteStream {
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    OpenedRemoteStream {
        path_index,
        stream: ReliablePathStream {
            stream_id,
            max_offset: MuxLimits::default().max_stream_window_bytes,
            lane: FlowLane::Throughput,
            underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            output: ReliablePathStreamOutput::fixed(
                underlay,
                PathId(path_index as u16),
                commands,
                MuxLimits::default(),
            ),
            frames: frame_rx,
        },
    }
}

pub(super) fn seed_client_bulk_evidence_for_test(context: &ClientPathContext, key: RelayPathKey) {
    context.mark_relay_path_rate_sample(
        key.underlay,
        key.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20)).expect("bulk rate sample"),
    );
}

pub(super) fn mark_client_validation_proof_fresh_for_test(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    instance: RelayPathInstance,
    elapsed: Duration,
) {
    let (attached_at, proof_id) = remotes
        .paths
        .iter()
        .find(|path| path.instance() == instance)
        .map(|path| {
            (
                path.attached_at,
                path.path_proof_id.expect("queued attachment proof"),
            )
        })
        .expect("attached validation instance");
    context.mark_relay_path_proof_observation(
        instance.key.underlay,
        instance.key.index,
        PathProofObservation {
            proof_id,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed,
            sent_at: Instant::now(),
        },
    );
    assert!(context.relay_path_has_fresh_proof(
        instance.key.underlay,
        instance.key.index,
        proof_id,
        attached_at,
    ));
}

pub(super) fn consume_client_validation_proof_for_test(
    receivers: &mut ReliablePathCommandReceivers,
) {
    assert!(matches!(
        try_recv_reliable_path_priority_command(receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}
