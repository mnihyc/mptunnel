//! Server-side lifecycle for one carrier-neutral reliable product stream.

use super::ReliablePathStream;
use super::ServerReliableStreamRegistry;
use crate::config::MppPerformanceConfig;
use crate::model::capacity::reliable_stream_initial_advertised_window_bytes;
use crate::mux::MuxLimits;
use crate::outbound;
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::{Frame, ResetReason, SessionId, TargetAddr};
use crate::runtime::RuntimeError;
use crate::runtime::relay::relay_reliable_stream;
use crate::runtime::sender::emit_response_control_frame;
use std::sync::Arc;
use std::time::Duration;

/// Product-stream dependencies that remain valid across carrier changes.
///
/// Keeping this context narrow prevents a long-lived target relay from owning
/// TCP/QUIC listener, authentication, and path-registration state.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ServerStreamContext {
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    outbound_connect_timeout: Duration,
    performance: MppPerformanceConfig,
    mux_limits: MuxLimits,
    reliable_streams: Arc<ServerReliableStreamRegistry>,
}

impl ServerStreamContext {
    pub(in crate::runtime) fn new(
        outbound: OutboundConfig,
        outbound_dns: DnsConfig,
        outbound_connect_timeout: Duration,
        performance: MppPerformanceConfig,
        mux_limits: MuxLimits,
        reliable_streams: Arc<ServerReliableStreamRegistry>,
    ) -> Self {
        Self {
            outbound,
            outbound_dns,
            outbound_connect_timeout,
            performance,
            mux_limits,
            reliable_streams,
        }
    }
}

/// Connects product I/O only after a carrier path has established the binding.
pub(in crate::runtime) async fn run_server_reliable_stream(
    context: ServerStreamContext,
    session_id: SessionId,
    stream: ReliablePathStream,
    target: TargetAddr,
) -> Result<(), RuntimeError> {
    let stream_id = stream.stream_id;
    let result = async {
        let outbound_stream = match outbound::connect_tcp(
            &context.outbound,
            &context.outbound_dns,
            &target,
            context.outbound_connect_timeout,
        )
        .await
        {
            Ok(stream) => stream,
            Err(err) => {
                emit_response_control_frame(
                    &stream,
                    Frame::StreamReset {
                        stream_id,
                        reason: ResetReason::Refused,
                    },
                )?;
                stream.close().await;
                return Err(RuntimeError::OutboundConnect(err));
            }
        };
        emit_response_control_frame(
            &stream,
            Frame::StreamMaxData {
                stream_id,
                max_offset: reliable_stream_initial_advertised_window_bytes(
                    stream.underlay,
                    stream.lane,
                    context.mux_limits,
                ),
            },
        )?;
        relay_reliable_stream(
            outbound_stream,
            stream,
            context.mux_limits,
            context.performance,
            session_id,
        )
        .await
        .map(|_| ())
    }
    .await;
    context.reliable_streams.close(session_id, stream_id);
    result
}
