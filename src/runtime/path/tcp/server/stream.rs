//! Product-stream lifecycle on one server TCP carrier.
//!
//! Attachment membership and product frame routing stay together so carrier
//! drain decisions consult one stream authority.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET, reliable_relay_buffer_len,
};
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::stream_ack_contiguous_frontier;
use crate::protocol::{
    Frame, PathId, SessionId, StreamDemandHint, StreamId, StreamReturnPlan, TargetAddr,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    ServerCarrierPathRegistration, ServerStreamFrameRoute, ServerStreamOpenOutcome,
    ServerStreamOpenRequest, ServerStreamPathAttachment,
};
use std::collections::HashSet;

pub(in crate::runtime::path::tcp) struct ServerTcpStreamState {
    attached: HashSet<StreamId>,
}

impl ServerTcpStreamState {
    pub(in crate::runtime::path::tcp) fn new() -> Self {
        Self {
            attached: HashSet::new(),
        }
    }

    pub(in crate::runtime::path::tcp) fn is_empty(&self) -> bool {
        self.attached.is_empty()
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime::path::tcp) async fn open(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        commands: &ReliablePathCommandSender,
        session_id: SessionId,
        stream_id: StreamId,
        target: TargetAddr,
        demand: StreamDemandHint,
        return_plan: StreamReturnPlan,
    ) -> Result<Option<Frame>, RuntimeError> {
        let response = match context
            .reliable_streams
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target,
                initial_demand: demand,
                return_plan,
                attachment: ServerStreamPathAttachment {
                    path_registration: path_registration.clone(),
                    commands: commands.clone(),
                    max_frame_payload_bytes: reliable_relay_buffer_len(context.mux_limits),
                },
                mux_limits: context.mux_limits,
            })
            .await?
        {
            ServerStreamOpenOutcome::New(_) => {
                self.attached.insert(stream_id);
                None
            }
            ServerStreamOpenOutcome::Existing(_) => {
                self.attached.insert(stream_id);
                Some(Frame::StreamMaxData {
                    stream_id,
                    // This frame accepts an attachment; it does not create a
                    // second receive window for the logical stream.
                    max_offset: RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET,
                })
            }
            // OPEN_STREAM rejection is attachment-local. STREAM_RESET is
            // reserved for terminating the logical MPP stream, while
            // STREAM_DETACH retires only this carrier's attachment.
            ServerStreamOpenOutcome::DuplicateLiveIgnored | ServerStreamOpenOutcome::Rejected => {
                Some(Frame::StreamDetach { stream_id })
            }
            // Silent policy drop creates no registry entry and emits no MPP
            // frame. The surrounding TCP carrier remains usable by siblings.
            ServerStreamOpenOutcome::Dropped => None,
        };
        Ok(response)
    }

    pub(in crate::runtime::path::tcp) async fn route_frame(
        &self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
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
                    stream_ack_contiguous_frontier(ranges),
                    ranges.last().map_or(0, |range| range.end),
                ),
            );
        }
        context
            .reliable_streams
            .route_frame(path_registration, stream_id, frame)
            .await?;
        Ok(())
    }

    pub(in crate::runtime::path::tcp) fn try_route_frame(
        &self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        path_id: PathId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<ServerStreamFrameRoute, RuntimeError> {
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
                    stream_ack_contiguous_frontier(ranges),
                    ranges.last().map_or(0, |range| range.end),
                ),
            );
        }
        context
            .reliable_streams
            .try_route_frame(path_registration, stream_id, frame)
    }

    pub(in crate::runtime::path::tcp) fn detach(
        &mut self,
        context: &ServerPathContext,
        path_registration: &ServerCarrierPathRegistration,
        stream_id: StreamId,
    ) -> Result<(), RuntimeError> {
        self.attached.remove(&stream_id);
        context
            .reliable_streams
            .detach_path(path_registration, stream_id)
    }
}
