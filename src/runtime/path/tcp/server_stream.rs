//! Product-stream lifecycle on one server TCP carrier.
//!
//! Attachment membership and product frame routing stay together so carrier
//! drain decisions consult one stream authority.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::outbound::{self, TargetProtocol};
use crate::protocol::{
    Frame, PathCapabilities, PathId, PathMetrics, SessionId, StreamDemandHint, StreamId,
    StreamOpenRole, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::path::server_context::ServerPathContext;
#[cfg(feature = "lab-diagnostics")]
use crate::runtime::relay::io::stream_ack_contiguous_frontier;
use crate::runtime::relay::io::{
    reliable_relay_buffer_len, reliable_stream_initial_advertised_window_bytes,
};
use crate::runtime::stream::{
    ServerCarrierPathRegistration, ServerReliablePathAttachment, ServerReliableStreamOpen,
    ServerReliableStreamOpenRequest, run_server_reliable_stream,
};
use std::collections::HashSet;

pub(super) struct ServerTcpStreamState {
    attached: HashSet<StreamId>,
}

impl ServerTcpStreamState {
    pub(super) fn new() -> Self {
        Self {
            attached: HashSet::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.attached.is_empty()
    }

    pub(super) async fn open(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        commands: &ReliablePathCommandSender,
        session_id: SessionId,
        path_id: PathId,
        path_capabilities: PathCapabilities,
        startup_metrics: Option<PathMetrics>,
        stream_id: StreamId,
        target: TargetAddr,
        demand: StreamDemandHint,
        role: StreamOpenRole,
    ) -> Result<Option<Frame>, RuntimeError> {
        outbound::validate_target(&target)?;
        context.outbound.ensure_supports(TargetProtocol::Tcp)?;
        let lane = crate::runtime::stream::flow_lane_from_stream_demand_hint(demand);
        let response = match context.reliable_streams.open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane,
                attachment: ServerReliablePathAttachment {
                    path_registration: path_registration.clone(),
                    commands: commands.clone(),
                    max_frame_payload_bytes: reliable_relay_buffer_len(context.mux_limits),
                    role,
                    initial_metrics: startup_metrics,
                },
            },
            context.mux_limits,
            context.max_reliable_streams,
        )? {
            ServerReliableStreamOpen::New(stream) => {
                self.attached.insert(stream_id);
                let stream_context = context.reliable_stream_context();
                tokio::spawn(async move {
                    if let Err(err) =
                        run_server_reliable_stream(stream_context, session_id, stream, target).await
                    {
                        eprintln!("warning: server reliable stream failed: {err}");
                    }
                });
                None
            }
            ServerReliableStreamOpen::Existing => {
                if role != StreamOpenRole::Validation {
                    self.attached.insert(stream_id);
                }
                context
                    .reliable_streams
                    .route_frame(
                        session_id,
                        stream_id,
                        Frame::PathStatus {
                            path_id,
                            status: crate::protocol::PathStatus::Active,
                            capabilities: path_capabilities,
                        },
                    )
                    .await?;
                Some(Frame::StreamMaxData {
                    stream_id,
                    max_offset: reliable_stream_initial_advertised_window_bytes(
                        UnderlayProtocol::Tcp,
                        lane,
                        context.mux_limits,
                    ),
                })
            }
            ServerReliableStreamOpen::DuplicateLiveIgnored => None,
            ServerReliableStreamOpen::Rejected => Some(Frame::StreamReset {
                stream_id,
                reason: crate::protocol::ResetReason::Refused,
            }),
        };
        Ok(response)
    }

    pub(super) async fn route_frame(
        &self,
        context: &ServerPathContext,
        session_id: SessionId,
        path_id: PathId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = path_id;
        #[cfg(feature = "lab-diagnostics")]
        if let Frame::StreamAck {
            complete, ranges, ..
        } = &frame
        {
            lab_diagnostic(
                "server_tcp_stream_ack_ingress",
                format_args!(
                    "stream_id={} path_id={} complete={} ranges={} frontier={} largest_end={}",
                    stream_id.0,
                    path_id.0,
                    complete,
                    ranges.len(),
                    stream_ack_contiguous_frontier(*complete, ranges),
                    ranges.last().map_or(0, |range| range.end),
                ),
            );
        }
        context
            .reliable_streams
            .route_frame(session_id, stream_id, frame)
            .await?;
        Ok(())
    }

    pub(super) fn detach(
        &mut self,
        context: &ServerPathContext,
        commands: &ReliablePathCommandSender,
        session_id: SessionId,
        path_id: PathId,
        stream_id: StreamId,
    ) {
        self.attached.remove(&stream_id);
        context.reliable_streams.detach_path(
            session_id,
            stream_id,
            UnderlayProtocol::Tcp,
            path_id,
            commands,
        );
    }
}
