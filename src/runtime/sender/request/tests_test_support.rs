//! Shared request-sender topology fixtures for tests.
//!
//! These helpers construct exact path membership and proof evidence used by
//! the request facade plus both concrete carrier-controller suites.

use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::PathRateSample;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::model::path::{RelayPathInstance, next_carrier_path_instance_id};
use crate::model::service_rate::{DirectionalServiceRate, DirectionalServiceRateScope};
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, PathMetricDirection, StreamId, UnderlayProtocol};
use crate::runtime::path::PathProofObservation;
use crate::runtime::path::authority::NativeCarrierRateAuthorityHandle;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::path::{ClientPathContext, OpenedReliableCarrierStream};
use crate::runtime::stream::{OpenedRemoteStream, ReliableRelayRemoteSet};
use crate::scheduler::{PathSnapshot, TrafficClass};
use crate::transport::{PathSpec, RateHint};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub(super) fn security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
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
    opened_test_relay_stream_with_native_source(
        stream_id,
        underlay,
        path_index,
        commands,
        RateHint::Unknown,
        1,
        None,
    )
    .0
}

pub(super) fn opened_test_relay_stream_with_native_source(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
    commands: ReliablePathCommandSender,
    startup_rate: RateHint,
    native_controller: u64,
    native_operational_rate_bps: Option<u128>,
) -> (
    OpenedRemoteStream,
    Option<std::sync::Arc<NativeCarrierRateAuthorityHandle>>,
) {
    let path_instance_id = next_carrier_path_instance_id();
    let direction = PathMetricDirection::ClientToServer;
    let service_rate = DirectionalServiceRate::from_startup_hint(
        DirectionalServiceRateScope::new(path_instance_id, direction),
        startup_rate,
    )
    .expect("valid test startup service rate");
    let startup_rate_bps = service_rate
        .finite_rate_bps()
        .map_or_else(crate::runtime::path::model::default_path_rate_bps, |rate| {
            rate as f64
        });
    let startup = PathSnapshot::new(
        PathId(path_index as u16),
        underlay,
        crate::runtime::path::model::default_path_srtt_ms(),
        startup_rate_bps,
    )
    .with_scheduling_service_rate(service_rate);
    let (commands, native_authority) = match underlay {
        UnderlayProtocol::Tcp => (commands, None),
        UnderlayProtocol::Udp => {
            let scope = CarrierRateAuthorityScope::new(path_instance_id, direction);
            let authority = NativeCarrierRateAuthorityHandle::from_startup_hint_for_test(
                scope,
                startup_rate,
                1,
                native_controller,
                native_operational_rate_bps,
            )
            .expect("valid exact-instance test QUIC authority");
            (
                commands.with_native_rate_authority(authority.clone()),
                Some(authority),
            )
        }
    };
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    (
        OpenedRemoteStream::from_opened_carrier(
            OpenedReliableCarrierStream {
                stream_id,
                path_instance_id,
                max_offset: MuxLimits::default().max_stream_window_bytes,
                lane: TrafficClass::Throughput,
                underlay,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                portable_startup: startup,
                startup,
                startup_native_window: None,
                startup_metrics: None,
                commands,
                mux_limits: MuxLimits::default(),
                frames: frame_rx,
            },
            path_index,
            0,
        ),
        native_authority,
    )
}

pub(super) fn seed_client_bulk_evidence_for_test(
    context: &ClientPathContext,
    instance: RelayPathInstance,
) {
    context.install_relay_path_instance_for_test(instance);
    let key = instance.key;
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
    context.mark_relay_path_rate_sample_for_test(
        key,
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
        instance.path_instance_id,
        PathProofObservation {
            proof_id,
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
