//! Application-datagram lifecycle carried by one server TCP session.
//!
//! Flow membership and target-side UDP workers stay together so closing one
//! flow releases its realtime registration without changing carrier lifetime.

use crate::outbound::{self, TargetProtocol};
use crate::protocol::{DatagramFlowId, DatagramId, Frame, SessionId, TargetAddr};
use crate::runtime::datagram::{
    ServerDatagramFlow, ServerDatagramRequest, datagram_ack_range,
    spawn_server_datagram_flow_worker,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::path::server_context::ServerPathContext;
use crate::runtime::stream::ServerRealtimeFlowRegistration;
use tokio::sync::mpsc;

pub(super) enum ServerTcpDatagramEffect {
    None,
    Reply(Frame),
    ReplyAndSkipCommandPoll(Frame),
    ReplyThenError {
        frame: Frame,
        error: RuntimeError,
        registration: ServerRealtimeFlowRegistration,
    },
}

pub(super) struct ServerTcpDatagramState {
    flows: Vec<ServerDatagramFlow>,
}

impl ServerTcpDatagramState {
    pub(super) fn new() -> Self {
        Self { flows: Vec::new() }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.flows.is_empty()
    }

    pub(super) async fn open(
        &mut self,
        context: &ServerPathContext,
        commands: &ReliablePathCommandSender,
        session_id: SessionId,
        flow_id: DatagramFlowId,
        target: TargetAddr,
    ) -> Result<ServerTcpDatagramEffect, RuntimeError> {
        if self.flows.iter().any(|flow| flow.flow_id == flow_id) {
            return Err(RuntimeError::Protocol("duplicate TCP datagram flow"));
        }
        if self.flows.len() >= context.max_udp_flows_per_session {
            return Ok(ServerTcpDatagramEffect::ReplyAndSkipCommandPoll(
                Frame::DatagramClose { flow_id },
            ));
        }
        outbound::validate_target(&target)?;
        context.outbound.ensure_supports(TargetProtocol::Udp)?;
        let realtime_registration = context.reliable_streams.register_realtime_flow(session_id);
        let outbound_socket = match outbound::connect_udp(
            &context.outbound,
            &context.outbound_dns,
            &target,
            context.outbound_connect_timeout,
        )
        .await
        {
            Ok(socket) => socket,
            Err(err) => {
                return Ok(ServerTcpDatagramEffect::ReplyThenError {
                    frame: Frame::DatagramClose { flow_id },
                    error: RuntimeError::OutboundConnect(err),
                    registration: realtime_registration,
                });
            }
        };
        let requests = spawn_server_datagram_flow_worker(
            flow_id,
            outbound_socket,
            commands.clone(),
            context.mux_limits,
        );
        self.flows.push(ServerDatagramFlow {
            flow_id,
            requests,
            _realtime_registration: realtime_registration,
        });
        Ok(ServerTcpDatagramEffect::None)
    }

    pub(super) fn handle_data(
        &mut self,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
        ttl_ms: u32,
        payload: bytes::Bytes,
    ) -> Result<ServerTcpDatagramEffect, RuntimeError> {
        if ttl_ms == 0 {
            return Err(RuntimeError::Protocol("expired TCP datagram received"));
        }
        let requests = self
            .flows
            .iter()
            .find(|flow| flow.flow_id == flow_id)
            .map(|flow| flow.requests.clone())
            .ok_or(RuntimeError::Protocol("unknown TCP datagram flow"))?;
        let effect = match requests.try_send(ServerDatagramRequest {
            datagram_id,
            ttl_ms,
            payload,
        }) {
            Ok(()) => ServerTcpDatagramEffect::Reply(Frame::DatagramFeedback {
                flow_id,
                received: vec![datagram_ack_range(datagram_id)?],
            }),
            Err(mpsc::error::TrySendError::Full(_)) => {
                eprintln!("warning: TCP datagram worker queue full; dropping request");
                ServerTcpDatagramEffect::None
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.remove(flow_id);
                ServerTcpDatagramEffect::Reply(Frame::DatagramClose { flow_id })
            }
        };
        Ok(effect)
    }

    pub(super) fn remove(&mut self, flow_id: DatagramFlowId) {
        self.flows.retain(|flow| flow.flow_id != flow_id);
    }
}
