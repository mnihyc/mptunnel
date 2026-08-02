//! Application-datagram lifecycle carried by one server TCP session.
//!
//! The carrier owns flow membership and wire replies; accepted target workers
//! and their policy lifetime arrive through the neutral datagram port.

use crate::product::PrincipalPermit;
use crate::protocol::frame::datagram_feedback_range;
use crate::protocol::{DatagramFlowId, DatagramId, Frame, SessionId, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::path::{
    AcceptedServerDatagramFlow, ServerDatagramOpenRequest, ServerDatagramRequest,
    ServerDatagramSendOutcome,
};

pub(in crate::runtime::path::tcp) enum ServerTcpDatagramEffect {
    None,
    Reply(Frame),
    ReplyAndSkipCommandPoll(Frame),
}

pub(in crate::runtime::path::tcp) struct ServerTcpDatagramState {
    flows: Vec<AcceptedServerDatagramFlow>,
}

impl ServerTcpDatagramState {
    pub(in crate::runtime::path::tcp) fn new() -> Self {
        Self { flows: Vec::new() }
    }

    pub(in crate::runtime::path::tcp) fn is_empty(&self) -> bool {
        self.flows.is_empty()
    }

    pub(in crate::runtime::path::tcp) async fn open(
        &mut self,
        context: &ServerPathContext,
        commands: &ReliablePathCommandSender,
        session_id: SessionId,
        principal_permit: PrincipalPermit,
        flow_id: DatagramFlowId,
        target: TargetAddr,
    ) -> Result<ServerTcpDatagramEffect, RuntimeError> {
        if self.flows.iter().any(|flow| flow.flow_id() == flow_id) {
            return Err(RuntimeError::Protocol("duplicate TCP datagram flow"));
        }
        if self.flows.len() >= context.max_udp_flows_per_session {
            return Ok(ServerTcpDatagramEffect::ReplyAndSkipCommandPoll(
                Frame::DatagramClose { flow_id },
            ));
        }
        let flow = match context
            .datagrams
            .open(ServerDatagramOpenRequest {
                session_id,
                principal_permit,
                flow_id,
                target,
                commands: commands.clone(),
            })
            .await
        {
            Ok(flow) => flow,
            Err(failure) => return Err(failure.into_error()),
        };
        self.flows.push(flow);
        Ok(ServerTcpDatagramEffect::None)
    }

    pub(in crate::runtime::path::tcp) async fn handle_data(
        &mut self,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
        ttl_ms: u32,
        payload: bytes::Bytes,
    ) -> Result<ServerTcpDatagramEffect, RuntimeError> {
        if ttl_ms == 0 {
            return Err(RuntimeError::Protocol("expired TCP datagram received"));
        }
        let flow = self
            .flows
            .iter()
            .find(|flow| flow.flow_id() == flow_id)
            .ok_or(RuntimeError::Protocol("unknown TCP datagram flow"))?;
        let effect = match flow
            .send(ServerDatagramRequest {
                datagram_id,
                ttl_ms,
                payload,
            })
            .await?
        {
            ServerDatagramSendOutcome::Accepted => {
                let received = datagram_feedback_range(datagram_id)
                    .ok_or(RuntimeError::Protocol("datagram feedback range overflow"))?;
                ServerTcpDatagramEffect::Reply(Frame::DatagramFeedback {
                    flow_id,
                    received: vec![received],
                })
            }
            ServerDatagramSendOutcome::Full => {
                crate::observability::process_event!(
                    Warn,
                    "tcp_datagram",
                    "worker_queue_full",
                    "TCP datagram worker queue full; dropping request"
                );
                ServerTcpDatagramEffect::None
            }
            ServerDatagramSendOutcome::Closed => {
                self.remove(flow_id);
                ServerTcpDatagramEffect::Reply(Frame::DatagramClose { flow_id })
            }
        };
        Ok(effect)
    }

    pub(in crate::runtime::path::tcp) fn handle_feedback(
        &self,
        flow_id: DatagramFlowId,
        received: Vec<crate::protocol::OffsetRange>,
    ) -> Result<(), RuntimeError> {
        let flow = self
            .flows
            .iter()
            .find(|flow| flow.flow_id() == flow_id)
            .ok_or(RuntimeError::Protocol("unknown TCP datagram flow feedback"))?;
        flow.acknowledge_response(received);
        Ok(())
    }

    pub(in crate::runtime::path::tcp) fn remove(&mut self, flow_id: DatagramFlowId) {
        self.flows.retain(|flow| flow.flow_id() != flow_id);
    }
}
