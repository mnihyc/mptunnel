//! Shared request-sender topology fixtures for tests.
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
use crate::runtime::stream::{
    OpenedRemoteStream, ReliablePathStream, ReliablePathStreamOutput, ReliableRelayRemoteSet,
};
use crate::scheduler::TrafficClass;
use crate::transport::PathSpec;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub(super) fn security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
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
    OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: MuxLimits::default().max_stream_window_bytes,
            lane: TrafficClass::Throughput,
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
        path_index,
    )
}

pub(super) fn seed_client_bulk_evidence_for_test(context: &ClientPathContext, key: RelayPathKey) {
    match key.underlay {
        UnderlayProtocol::Tcp => context.mark_tcp_path_open_success(
            key.index,
            Duration::from_millis(20),
            TrafficClass::Throughput,
        ),
        UnderlayProtocol::Udp => {
            context.mark_udp_path_open_success(key.index, Duration::from_millis(20));
        }
    }
    context.mark_relay_path_rate_sample(
        key.underlay,
        key.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20)).expect("bulk rate sample"),
    );
}

pub(super) fn mark_client_path_proof_fresh_for_test(
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
        .expect("attached path-proof instance");
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

pub(super) fn consume_client_path_proof_for_test(receivers: &mut ReliablePathCommandReceivers) {
    assert!(matches!(
        try_recv_reliable_path_priority_command(receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}
