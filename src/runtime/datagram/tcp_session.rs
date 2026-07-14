//! One TCP carrier session for product datagram request/response.

use super::policy::{
    DatagramPathSendError, datagram_remaining_ttl_ms, datagram_response_deadline_budget,
};
use super::tcp::tcp_datagram_response_timeout;
use super::{DatagramClientFlow, SentDatagram, datagram_ack_range, datagram_id_is_in_ranges};
use crate::model::capacity::TRANSPORT_TIMER_GRANULARITY;
use crate::mux::MuxLimits;
use crate::mux::datagram::DatagramFlow;
use crate::protocol::{
    DatagramFlowId, DatagramId, Frame, IngressKind, OffsetRange, OutboundPolicy, PathId, TargetAddr,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::random_session_id;
use crate::runtime::path::tcp::client_connection::{
    ClientTcpCarrierConnection, ClientTcpHeartbeatTimeoutDisposition, connect_client_tcp_carrier,
};
use crate::runtime::path::{ClientPathContext, PathDeliveryStats};
use crate::scheduler::PathSnapshot;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

pub(in crate::runtime) struct TcpDatagramClientSession {
    connection: ClientTcpCarrierConnection,
    flows: Vec<DatagramClientFlow>,
    next_flow_id: u64,
    mux_limits: MuxLimits,
    pub(in crate::runtime) path_index: usize,
    path_id: PathId,
    path_snapshot: PathSnapshot,
    stats: PathDeliveryStats,
    sent_datagrams: HashMap<(DatagramFlowId, DatagramId), SentDatagram>,
    last_datagram_rtt: Option<Duration>,
    response_rttvar: Option<Duration>,
    pub(in crate::runtime) connection_usable: bool,
}

impl TcpDatagramClientSession {
    pub(in crate::runtime) async fn open(
        context: &ClientPathContext,
        path_index: usize,
        open_deadline: tokio::time::Instant,
    ) -> Result<Self, RuntimeError> {
        let path = context
            .tcp_paths
            .get(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let path_snapshot = context
            .tcp_path_snapshot(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?;
        let session_id = random_session_id()?;
        let security = context.tcp_path_security(path_index)?;
        let connection = connect_client_tcp_carrier(
            path,
            path_index,
            session_id,
            security,
            context.codec_limits,
            context.mux_limits,
            open_deadline,
        )
        .await?;
        Ok(Self {
            connection,
            flows: Vec::new(),
            next_flow_id: 0,
            mux_limits: context.mux_limits,
            path_index,
            path_id: PathId(path_index as u16),
            path_snapshot,
            stats: PathDeliveryStats::default(),
            sent_datagrams: HashMap::new(),
            last_datagram_rtt: None,
            response_rttvar: None,
            connection_usable: true,
        })
    }

    pub(in crate::runtime) async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        fallback_deadline: tokio::time::Instant,
        product_deadline: tokio::time::Instant,
    ) -> Result<Bytes, DatagramPathSendError> {
        if payload.len() > self.mux_limits.max_payload_bytes {
            return Err(DatagramPathSendError::MtuExceeded {
                limit: self.mux_limits.max_payload_bytes,
            });
        }
        let setup = async {
            self.connection
                .tick_heartbeat(ClientTcpHeartbeatTimeoutDisposition::KeepCarrierAlive)
                .await?;
            self.ensure_flow(target).await
        };
        let flow_id = match tokio::time::timeout_at(fallback_deadline, setup).await {
            Ok(Ok(flow_id)) => flow_id,
            Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err, false)),
            Err(_) => {
                self.connection_usable = false;
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout: Duration::ZERO,
                });
            }
        };
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout {
                path_was_acked: false,
                response_timeout: Duration::ZERO,
            });
        }
        let response_timeout = self.response_timeout(ttl_ms);
        let frame = {
            let flow = self
                .flows
                .iter_mut()
                .find(|flow| flow.flow_id == flow_id)
                .ok_or_else(|| {
                    DatagramPathSendError::runtime(
                        RuntimeError::Protocol("missing TCP datagram flow"),
                        false,
                    )
                })?;
            flow.flow.enqueue(0, ttl_ms, payload).map_err(|err| {
                DatagramPathSendError::runtime(RuntimeError::Datagram(err), false)
            })?;
            flow.flow.pop_frame(0).ok_or_else(|| {
                DatagramPathSendError::runtime(
                    RuntimeError::Protocol("datagram expired before TCP send"),
                    false,
                )
            })?
        };
        let (request_datagram_id, request_len) = match &frame {
            Frame::DatagramData {
                datagram_id,
                payload,
                ..
            } => (*datagram_id, payload.len()),
            _ => {
                return Err(DatagramPathSendError::runtime(
                    RuntimeError::Protocol("unexpected queued datagram frame"),
                    false,
                ));
            }
        };
        let request_key = (flow_id, request_datagram_id);
        let sent_at = Instant::now();
        let mut request_acked = false;
        let response_budget = datagram_response_deadline_budget(response_timeout, ttl_ms)
            .max(response_timeout)
            .min(fallback_deadline.saturating_duration_since(tokio::time::Instant::now()));
        let mut response_deadline = sent_at + response_budget;
        let product_response_deadline = product_deadline.into_std();
        self.sent_datagrams.insert(
            request_key,
            SentDatagram {
                sent_at,
                bytes: request_len,
                ttl: Duration::from_millis(u64::from(ttl_ms)),
            },
        );
        let write_request = async {
            self.connection.writer.write_frame(&frame).await?;
            self.connection.writer.flush().await
        };
        match tokio::time::timeout_at(fallback_deadline, write_request).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                return Err(DatagramPathSendError::runtime(
                    RuntimeError::Encrypted(err),
                    false,
                ));
            }
            Err(_) => {
                self.sent_datagrams.remove(&request_key);
                self.connection_usable = false;
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout,
                });
            }
        }
        loop {
            let now = Instant::now();
            if now >= response_deadline {
                self.sent_datagrams.remove(&request_key);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "tcp_datagram_response_timeout",
                    format_args!(
                        "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} response_budget_ms={} request_acked={}",
                        self.path_id.0,
                        self.path_index,
                        flow_id.0,
                        request_datagram_id.0,
                        response_timeout.as_millis(),
                        response_budget.as_millis(),
                        request_acked,
                    ),
                );
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: request_acked,
                    response_timeout,
                });
            }
            let wait_for = response_deadline.saturating_duration_since(now);
            let received = match tokio::time::timeout(wait_for, self.connection.frames.recv()).await
            {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(err))) => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Encrypted(err),
                        request_acked,
                    ));
                }
                Ok(None) => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::ReliablePathSessionClosed,
                        request_acked,
                    ));
                }
                Err(_) => {
                    self.sent_datagrams.remove(&request_key);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "tcp_datagram_response_timeout",
                        format_args!(
                            "path_id={} path_index={} flow_id={} datagram_id={} response_timeout_ms={} response_budget_ms={} request_acked={}",
                            self.path_id.0,
                            self.path_index,
                            flow_id.0,
                            request_datagram_id.0,
                            response_timeout.as_millis(),
                            response_budget.as_millis(),
                            request_acked,
                        ),
                    );
                    return Err(DatagramPathSendError::Timeout {
                        path_was_acked: request_acked,
                        response_timeout,
                    });
                }
            };
            self.connection.refresh_liveness();
            match received {
                Frame::DatagramFeedback { flow_id, received } => {
                    if flow_id == request_key.0
                        && datagram_id_is_in_ranges(request_datagram_id, &received)
                    {
                        request_acked = true;
                        response_deadline = product_response_deadline;
                    }
                    self.handle_datagram_feedback(flow_id, &received)
                        .map_err(|err| DatagramPathSendError::runtime(err, request_acked))?;
                }
                Frame::DatagramData {
                    flow_id: response_flow_id,
                    datagram_id,
                    payload,
                    ..
                } if response_flow_id == flow_id && datagram_id == request_datagram_id => {
                    let now = Instant::now();
                    let lost = self.expire_unacked_datagrams(now);
                    if let Some(sent) = self.sent_datagrams.remove(&request_key) {
                        self.observe_datagram_response(sent, now, lost);
                    }
                    let feedback = Frame::DatagramFeedback {
                        flow_id,
                        received: vec![
                            datagram_ack_range(datagram_id)
                                .map_err(|err| DatagramPathSendError::runtime(err, true))?,
                        ],
                    };
                    let send_feedback = async {
                        self.connection.writer.write_frame(&feedback).await?;
                        self.connection.writer.flush().await
                    };
                    if !matches!(
                        tokio::time::timeout_at(product_deadline, send_feedback).await,
                        Ok(Ok(()))
                    ) {
                        self.connection_usable = false;
                    }
                    self.stats.record_payload_bytes(request_len);
                    self.stats.record_payload_bytes(payload.len());
                    return Ok(payload);
                }
                Frame::DatagramData {
                    flow_id: response_flow_id,
                    datagram_id,
                    ..
                } if response_flow_id == flow_id => {
                    let feedback =
                        Frame::DatagramFeedback {
                            flow_id,
                            received: vec![datagram_ack_range(datagram_id).map_err(|err| {
                                DatagramPathSendError::runtime(err, request_acked)
                            })?],
                        };
                    let send_feedback = async {
                        self.connection.writer.write_frame(&feedback).await?;
                        self.connection.writer.flush().await
                    };
                    let io_deadline = if request_acked {
                        product_deadline
                    } else {
                        fallback_deadline
                    };
                    match tokio::time::timeout_at(io_deadline, send_feedback).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            return Err(DatagramPathSendError::runtime(
                                RuntimeError::Encrypted(err),
                                request_acked,
                            ));
                        }
                        Err(_) => {
                            self.connection_usable = false;
                            return Err(DatagramPathSendError::Timeout {
                                path_was_acked: request_acked,
                                response_timeout,
                            });
                        }
                    }
                }
                Frame::DatagramClose {
                    flow_id: closed_flow_id,
                } if closed_flow_id == flow_id => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Protocol("TCP datagram flow closed"),
                        request_acked,
                    ));
                }
                Frame::Ping { nonce } => {
                    let send_pong = async {
                        self.connection
                            .writer
                            .write_frame(&Frame::Pong { nonce })
                            .await?;
                        self.connection.writer.flush().await
                    };
                    let io_deadline = if request_acked {
                        product_deadline
                    } else {
                        fallback_deadline
                    };
                    match tokio::time::timeout_at(io_deadline, send_pong).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            return Err(DatagramPathSendError::runtime(
                                RuntimeError::Encrypted(err),
                                request_acked,
                            ));
                        }
                        Err(_) => {
                            self.connection_usable = false;
                            return Err(DatagramPathSendError::Timeout {
                                path_was_acked: request_acked,
                                response_timeout,
                            });
                        }
                    }
                }
                Frame::Pong { nonce } => {
                    self.connection.clear_matching_heartbeat(nonce);
                }
                Frame::PathStatus { .. } | Frame::SessionReady => {}
                Frame::PathClose { .. } => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::ReliablePathSessionClosed,
                        request_acked,
                    ));
                }
                Frame::SessionClose { reason } => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::RemoteClosed(reason),
                        request_acked,
                    ));
                }
                _ => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Protocol("unexpected TCP datagram frame"),
                        request_acked,
                    ));
                }
            }
        }
    }

    async fn ensure_flow(&mut self, target: TargetAddr) -> Result<DatagramFlowId, RuntimeError> {
        if let Some(flow) = self.flows.iter().find(|flow| flow.target == target) {
            return Ok(flow.flow_id);
        }
        let flow_id = DatagramFlowId(self.next_flow_id);
        self.next_flow_id = self
            .next_flow_id
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("TCP datagram flow id overflow"))?;
        self.connection
            .writer
            .write_frame(&Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
                ingress: IngressKind::Socks5,
                outbound: OutboundPolicy::Direct,
            })
            .await?;
        self.connection.writer.flush().await?;
        self.flows.push(DatagramClientFlow {
            target,
            flow: DatagramFlow::new(flow_id, self.mux_limits),
            flow_id,
        });
        Ok(flow_id)
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        for flow in &self.flows {
            self.connection
                .writer
                .write_frame(&Frame::DatagramClose {
                    flow_id: flow.flow_id,
                })
                .await?;
        }
        self.flows.clear();
        self.connection.close(self.path_id).await
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
        self.expire_unacked_datagrams(now);
        let acked_keys = self
            .sent_datagrams
            .keys()
            .copied()
            .filter(|(pending_flow_id, datagram_id)| {
                *pending_flow_id == flow_id && datagram_id_is_in_ranges(*datagram_id, ranges)
            })
            .collect::<Vec<_>>();
        for key in acked_keys {
            if let Some(sent) = self.sent_datagrams.remove(&key) {
                self.observe_datagram_response(sent, now, 0);
            }
        }
        Ok(())
    }

    fn expire_unacked_datagrams(&mut self, now: Instant) -> u64 {
        let expired = self
            .sent_datagrams
            .iter()
            .filter_map(|(key, sent)| {
                (now.duration_since(sent.sent_at) >= sent.ttl).then_some(*key)
            })
            .collect::<Vec<_>>();
        let lost = expired.len() as u64;
        for key in expired {
            self.sent_datagrams.remove(&key);
        }
        lost
    }

    fn observe_datagram_response(&mut self, sent: SentDatagram, now: Instant, _lost: u64) {
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
