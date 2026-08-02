//! One product-datagram attachment to the shared TCP path actor.

use super::policy::{DatagramPathSendError, datagram_remaining_ttl_ms};
use super::tcp::tcp_datagram_response_timeout;
use super::{
    DatagramClientFlow, DatagramSessionEvent, ReceivedDatagram, SentDatagram, SentDatagramEvidence,
    datagram_feedback_range, datagram_id_is_in_ranges,
};
use crate::model::capacity::TRANSPORT_TIMER_GRANULARITY;
use crate::model::path::CarrierPathInstanceId;
use crate::mux::MuxLimits;
use crate::protocol::{DatagramFlowId, DatagramId, Frame, OffsetRange, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::tcp::client::{ClientTcpDatagramAttachment, ClientTcpDatagramInbound};
use crate::runtime::path::{ClientPathContext, PathDeliveryStats, RelayPathLoadLease};
use crate::scheduler::PathSnapshot;
use bytes::Bytes;
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

pub(in crate::runtime) struct TcpDatagramClientSession {
    attachment: ClientTcpDatagramAttachment,
    flows: Vec<DatagramClientFlow>,
    mux_limits: MuxLimits,
    pub(in crate::runtime) path_index: usize,
    path_snapshot: PathSnapshot,
    stats: PathDeliveryStats,
    sent_datagrams: SentDatagramEvidence,
    last_datagram_rtt: Option<Duration>,
    response_rttvar: Option<Duration>,
    pub(in crate::runtime) connection_usable: bool,
    _load_lease: RelayPathLoadLease,
}

impl TcpDatagramClientSession {
    pub(in crate::runtime) async fn open(
        context: &ClientPathContext,
        path_index: usize,
        open_deadline: tokio::time::Instant,
        load_lease: RelayPathLoadLease,
    ) -> Result<Self, RuntimeError> {
        context
            .tcp_paths
            .get(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let frame_queue = (context.mux_limits.max_datagram_queue_bytes
            / context.mux_limits.max_payload_bytes.max(1))
        .max(1);
        let attachment = context
            .tcp_sessions
            .get(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?
            .open_datagram_attachment(open_deadline, frame_queue)
            .await?;
        let path_snapshot = attachment.path_snapshot();
        Ok(Self {
            attachment,
            flows: Vec::new(),
            mux_limits: context.mux_limits,
            path_index,
            path_snapshot,
            stats: PathDeliveryStats::default(),
            sent_datagrams: SentDatagramEvidence::new(context.mux_limits),
            last_datagram_rtt: None,
            response_rttvar: None,
            connection_usable: true,
            _load_lease: load_lease,
        })
    }

    pub(in crate::runtime) fn attachment_id(&self) -> u64 {
        self.attachment.id()
    }

    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.attachment.path_instance_id()
    }

    pub(in crate::runtime) async fn send_to(
        &mut self,
        target: TargetAddr,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
        payload: Bytes,
        fallback_deadline: tokio::time::Instant,
        product_deadline: tokio::time::Instant,
    ) -> Result<(), DatagramPathSendError> {
        if payload.len() > self.mux_limits.max_payload_bytes {
            return Err(DatagramPathSendError::PayloadLimitExceeded {
                limit: self.mux_limits.max_payload_bytes,
            });
        }
        match self.ensure_flow(flow_id, target, fallback_deadline).await {
            Ok(()) => {}
            Err(RuntimeError::ReliablePathSessionClosed) => {
                self.connection_usable = false;
                return Err(DatagramPathSendError::runtime(
                    RuntimeError::ReliablePathSessionClosed,
                ));
            }
            Err(RuntimeError::PathOpenTimedOut) => return Err(DatagramPathSendError::Timeout),
            Err(err) => return Err(DatagramPathSendError::runtime(err)),
        }

        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout);
        }
        let request_len = payload.len();
        let request_key = (flow_id, datagram_id);
        let sent_at = Instant::now();
        self.expire_datagrams_without_feedback(sent_at);
        self.sent_datagrams.insert(
            request_key,
            SentDatagram {
                sent_at,
                bytes: request_len,
                ttl: Duration::from_millis(u64::from(ttl_ms)),
            },
        );
        let frame = Frame::DatagramData {
            flow_id,
            datagram_id,
            ttl_ms,
            payload,
        };
        match self
            .attachment
            .send_frame(frame, fallback_deadline, Some(product_deadline))
            .await
        {
            Ok(()) => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "tcp_datagram_request_written",
                    format_args!(
                        "path_index={} flow_id={} datagram_id={} payload_bytes={}",
                        self.path_index, flow_id.0, datagram_id.0, request_len,
                    ),
                );
                Ok(())
            }
            Err(RuntimeError::ReliablePathSessionClosed) => {
                self.sent_datagrams.remove(&request_key);
                self.connection_usable = false;
                Err(DatagramPathSendError::runtime(
                    RuntimeError::ReliablePathSessionClosed,
                ))
            }
            Err(RuntimeError::PathOpenTimedOut) => {
                self.sent_datagrams.remove(&request_key);
                Err(DatagramPathSendError::Timeout)
            }
            Err(err) => {
                self.sent_datagrams.remove(&request_key);
                Err(DatagramPathSendError::runtime(err))
            }
        }
    }

    pub(in crate::runtime) async fn next_frame(
        &mut self,
    ) -> Result<ClientTcpDatagramInbound, RuntimeError> {
        self.attachment.next_frame().await
    }

    pub(in crate::runtime) async fn handle_frame(
        &mut self,
        inbound: ClientTcpDatagramInbound,
    ) -> Result<DatagramSessionEvent, RuntimeError> {
        let ClientTcpDatagramInbound { frame, received_at } = inbound;
        match frame {
            Frame::DatagramFeedback { flow_id, received } => {
                self.handle_datagram_feedback(flow_id, &received)?;
                Ok(DatagramSessionEvent::Feedback { flow_id, received })
            }
            Frame::DatagramData {
                flow_id,
                datagram_id,
                ttl_ms,
                payload,
            } => {
                if ttl_ms == 0 {
                    return Err(RuntimeError::Protocol(
                        "expired TCP response datagram received",
                    ));
                }
                let Some(expires_at) = received_tcp_datagram_expires_at(received_at, ttl_ms) else {
                    return Ok(DatagramSessionEvent::Control);
                };
                self.stats.record_payload_bytes(payload.len());
                Ok(DatagramSessionEvent::Received(ReceivedDatagram {
                    flow_id,
                    datagram_id,
                    expires_at,
                    payload,
                }))
            }
            Frame::DatagramClose { .. } => Err(RuntimeError::Protocol("TCP datagram flow closed")),
            _ => Err(RuntimeError::Protocol(
                "unexpected shared TCP datagram frame",
            )),
        }
    }

    pub(in crate::runtime) async fn acknowledge(
        &mut self,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
        write_deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        self.attachment
            .send_frame(
                Frame::DatagramFeedback {
                    flow_id,
                    received: vec![datagram_feedback_range(datagram_id)?],
                },
                write_deadline,
                None,
            )
            .await
    }

    pub(in crate::runtime) fn has_flow(&self, flow_id: DatagramFlowId) -> bool {
        self.flows.iter().any(|flow| flow.flow_id == flow_id)
    }

    async fn ensure_flow(
        &mut self,
        flow_id: DatagramFlowId,
        target: TargetAddr,
        open_deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        if let Some(flow) = self.flows.iter().find(|flow| flow.flow_id == flow_id) {
            return if flow.target == target {
                Ok(())
            } else {
                Err(RuntimeError::Protocol("TCP datagram flow target changed"))
            };
        }
        if self.flows.iter().any(|flow| flow.target == target) {
            return Err(RuntimeError::Protocol(
                "TCP datagram target rebound to another flow",
            ));
        }
        self.attachment
            .open_flow(flow_id, target.clone(), open_deadline)
            .await?;
        self.flows.push(DatagramClientFlow { target, flow_id });
        Ok(())
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        let close_deadline =
            tokio::time::Instant::now() + self.mux_limits.tcp_path_heartbeat_timeout;
        let result = self.attachment.close(close_deadline).await;
        self.flows.clear();
        result
    }

    pub(in crate::runtime) fn response_timeout(&self, ttl_ms: u32) -> Duration {
        tcp_datagram_response_timeout(
            self.path_snapshot,
            self.last_datagram_rtt,
            self.response_rttvar,
            ttl_ms,
        )
    }

    pub(in crate::runtime) fn delivery_stats(&self) -> PathDeliveryStats {
        self.stats
    }

    fn handle_datagram_feedback(
        &mut self,
        flow_id: DatagramFlowId,
        ranges: &[OffsetRange],
    ) -> Result<(), RuntimeError> {
        let now = Instant::now();
        self.expire_datagrams_without_feedback(now);
        let feedback_keys = self
            .sent_datagrams
            .keys()
            .copied()
            .filter(|(pending_flow_id, datagram_id)| {
                *pending_flow_id == flow_id && datagram_id_is_in_ranges(*datagram_id, ranges)
            })
            .collect::<Vec<_>>();
        for key in feedback_keys {
            if let Some(sent) = self.sent_datagrams.remove(&key) {
                self.observe_datagram_response(sent, now);
            }
        }
        Ok(())
    }

    fn expire_datagrams_without_feedback(&mut self, now: Instant) -> u64 {
        self.sent_datagrams.expire(now)
    }

    fn observe_datagram_response(&mut self, sent: SentDatagram, now: Instant) {
        let rtt = now
            .duration_since(sent.sent_at)
            .max(TRANSPORT_TIMER_GRANULARITY);
        let previous_srtt = self.last_datagram_rtt;
        let sample_var = previous_srtt
            .map(|previous| previous.abs_diff(rtt))
            .unwrap_or_else(|| rtt.div_f64(2.0));
        self.response_rttvar = Some(match self.response_rttvar {
            Some(previous) => previous.mul_f64(0.75) + sample_var.mul_f64(0.25),
            None => sample_var,
        });
        self.last_datagram_rtt = Some(match previous_srtt {
            Some(previous) => previous.mul_f64(0.875) + rtt.mul_f64(0.125),
            None => rtt,
        });
    }
}

fn received_tcp_datagram_expires_at(
    received_at: tokio::time::Instant,
    ttl_ms: u32,
) -> Option<tokio::time::Instant> {
    let expires_at = received_at + Duration::from_millis(u64::from(ttl_ms));
    (expires_at > tokio::time::Instant::now()).then_some(expires_at)
}

#[cfg(test)]
#[path = "tests_tcp_session.rs"]
mod tests;
