//! One QUIC carrier session for product datagram request/response.

use super::policy::{
    DatagramPathSendError, datagram_remaining_ttl_ms, datagram_response_deadline_budget,
};
use super::session::{
    DatagramClientFlow, SentDatagram, datagram_ack_range, datagram_id_is_in_ranges,
};
use crate::config::SecurityConfig;
use crate::model::capacity::{PathRateSample, QUIC_TIMER_GRANULARITY};
use crate::mux::MuxLimits;
use crate::mux::datagram::{DatagramError, DatagramFlow};
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    CloseReason, DatagramFlowId, DatagramId, Frame, IngressKind, OffsetRange, OutboundPolicy,
    PathId, PathMetrics, SessionId, TargetAddr,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::identity::{random_session_id, random_u64};
use crate::runtime::path::commands::reliable_stream_frame_queue;
use crate::runtime::path::model::{PathDeliveryStats, UdpDatagramPathObservation};
use crate::runtime::path::quic::client::{
    ClientUdpDatagramStream, ClientUdpPathSessionHandle, ClientUdpPathSessionRuntime,
};
use crate::runtime::path::quic::io::{udp_path_finish_stream, udp_path_write_frame};
use crate::runtime::path::{
    ClientPathContext, ClientPathHealth, ClientPathHealthRecord, ClientPathState,
};
use crate::transport::PathSpec;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

pub(in crate::runtime) struct UdpDatagramClientSession {
    _path_session: ClientUdpPathSessionHandle,
    stream: ClientUdpDatagramStream,
    flows: Vec<DatagramClientFlow>,
    next_flow_id: u64,
    mux_limits: MuxLimits,
    pub(in crate::runtime) path_index: usize,
    path_id: PathId,
    stats: PathDeliveryStats,
    sent_datagrams: HashMap<(DatagramFlowId, DatagramId), SentDatagram>,
    last_datagram_rtt: Option<Duration>,
    last_feedback_observation: Option<UdpDatagramPathObservation>,
    mtu_payload_bytes: usize,
    pub(in crate::runtime) connection_usable: bool,
}

impl UdpDatagramClientSession {
    pub(in crate::runtime) async fn open(
        path: &PathSpec,
        path_index: usize,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let session_id = random_session_id()?;
        Self::open_for_session(
            path,
            path_index,
            session_id,
            security,
            codec_limits,
            mux_limits,
            handshake_timeout,
        )
        .await
    }

    pub(in crate::runtime) async fn open_for_session(
        path: &PathSpec,
        path_index: usize,
        session_id: SessionId,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let state = ClientPathState::new(ClientPathHealth {
            tcp: Vec::new(),
            udp: vec![ClientPathHealthRecord::default(); path_index.saturating_add(1)],
        });
        let path_session = ClientUdpPathSessionHandle::new(ClientUdpPathSessionRuntime {
            path: path.clone(),
            path_index,
            session_id,
            security,
            codec_limits,
            mux_limits,
            stream_frame_queue: reliable_stream_frame_queue(mux_limits),
            state,
        });
        Self::open_from_udp_session(path_session, path_index, mux_limits, handshake_timeout).await
    }

    pub(in crate::runtime) async fn open_from_udp_session(
        path_session: ClientUdpPathSessionHandle,
        path_index: usize,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let stream = tokio::time::timeout(handshake_timeout, path_session.open_datagram_stream())
            .await
            .map_err(|_| {
                RuntimeError::Protocol("QUIC UDP path datagram stream open timed out")
            })??;
        let path_id = stream.path_id;
        Ok(Self {
            _path_session: path_session,
            stream,
            flows: Vec::new(),
            next_flow_id: 0,
            mux_limits,
            path_index,
            path_id,
            stats: PathDeliveryStats::default(),
            sent_datagrams: HashMap::new(),
            last_datagram_rtt: None,
            last_feedback_observation: None,
            mtu_payload_bytes: mux_limits.max_payload_bytes,
            connection_usable: true,
        })
    }

    pub(in crate::runtime) async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        fallback_deadline: tokio::time::Instant,
        product_deadline: tokio::time::Instant,
        response_timeout: Duration,
    ) -> Result<Bytes, DatagramPathSendError> {
        let flow_id =
            match tokio::time::timeout_at(fallback_deadline, self.ensure_flow(target)).await {
                Ok(Ok(flow_id)) => flow_id,
                Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err, false)),
                Err(_) => {
                    return Err(DatagramPathSendError::Timeout {
                        path_was_acked: false,
                        response_timeout,
                    });
                }
            };
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout {
                path_was_acked: false,
                response_timeout,
            });
        }
        let frame = {
            let flow = self
                .flows
                .iter_mut()
                .find(|flow| flow.flow_id == flow_id)
                .ok_or_else(|| {
                    DatagramPathSendError::runtime(
                        RuntimeError::Protocol("missing UDP datagram flow"),
                        false,
                    )
                })?;
            flow.flow.enqueue(0, ttl_ms, payload).map_err(|err| {
                DatagramPathSendError::runtime(RuntimeError::Datagram(err), false)
            })?;
            flow.flow.pop_frame(0).ok_or_else(|| {
                DatagramPathSendError::runtime(
                    RuntimeError::Protocol("datagram expired before send"),
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
        self.last_feedback_observation = None;
        let request_started_at = Instant::now();
        let mut request_acked = false;
        let response_budget = datagram_response_deadline_budget(response_timeout, ttl_ms)
            .min(fallback_deadline.saturating_duration_since(tokio::time::Instant::now()));
        let mut response_deadline = request_started_at + response_budget;
        let product_response_deadline = product_deadline.into_std();

        self.sent_datagrams.insert(
            request_key,
            SentDatagram {
                sent_at: request_started_at,
                bytes: request_len,
                ttl: Duration::from_millis(u64::from(ttl_ms)),
            },
        );
        match tokio::time::timeout_at(
            fallback_deadline,
            udp_path_write_frame(
                &mut self.stream.send,
                &frame,
                self.stream.runtime.codec_limits,
            ),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err, false)),
            Err(_) => {
                self.sent_datagrams.remove(&request_key);
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout,
                });
            }
        }

        loop {
            let now = Instant::now();
            if now >= response_deadline {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "udp_datagram_response_timeout",
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
                self.sent_datagrams.remove(&request_key);
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: request_acked,
                    response_timeout,
                });
            }
            let wait_for = response_deadline.saturating_duration_since(now);
            let received = match tokio::time::timeout(wait_for, self.stream.frames.recv()).await {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(err))) => {
                    return Err(DatagramPathSendError::runtime(err, request_acked));
                }
                Ok(None) => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::ReliablePathSessionClosed,
                        request_acked,
                    ));
                }
                Err(_) => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "udp_datagram_response_timeout",
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
                    self.sent_datagrams.remove(&request_key);
                    return Err(DatagramPathSendError::Timeout {
                        path_was_acked: request_acked,
                        response_timeout,
                    });
                }
            };
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
                    let request_ack = datagram_ack_range(request_datagram_id)
                        .map_err(|err| DatagramPathSendError::runtime(err, true))?;
                    self.handle_datagram_feedback(flow_id, &[request_ack])
                        .map_err(|err| DatagramPathSendError::runtime(err, true))?;
                    let feedback = Frame::DatagramFeedback {
                        flow_id,
                        received: vec![
                            datagram_ack_range(datagram_id)
                                .map_err(|err| DatagramPathSendError::runtime(err, true))?,
                        ],
                    };
                    if !matches!(
                        tokio::time::timeout_at(
                            product_deadline,
                            udp_path_write_frame(
                                &mut self.stream.send,
                                &feedback,
                                self.stream.runtime.codec_limits,
                            ),
                        )
                        .await,
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
                    let io_deadline = if request_acked {
                        product_deadline
                    } else {
                        fallback_deadline
                    };
                    match tokio::time::timeout_at(
                        io_deadline,
                        udp_path_write_frame(
                            &mut self.stream.send,
                            &feedback,
                            self.stream.runtime.codec_limits,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            return Err(DatagramPathSendError::runtime(err, request_acked));
                        }
                        Err(_) => {
                            return Err(DatagramPathSendError::Timeout {
                                path_was_acked: request_acked,
                                response_timeout,
                            });
                        }
                    }
                }
                Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                    self.observe_remote_path_metrics(metrics);
                }
                Frame::SessionReady => {}
                Frame::RxRateHint { path_id, .. } if path_id == self.path_id => {}
                Frame::DatagramClose {
                    flow_id: closed_flow_id,
                } if closed_flow_id == flow_id => {
                    return Err(DatagramPathSendError::runtime(
                        RuntimeError::Protocol("datagram flow closed"),
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
                        RuntimeError::Protocol("unexpected UDP datagram frame"),
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
            .ok_or(RuntimeError::Protocol("UDP datagram flow id overflow"))?;
        udp_path_write_frame(
            &mut self.stream.send,
            &Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
                ingress: IngressKind::Socks5,
                outbound: OutboundPolicy::Direct,
            },
            self.stream.runtime.codec_limits,
        )
        .await?;
        self.flows.push(DatagramClientFlow {
            target,
            flow: DatagramFlow::new(flow_id, self.mux_limits),
            flow_id,
        });
        Ok(flow_id)
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        for flow in &self.flows {
            udp_path_write_frame(
                &mut self.stream.send,
                &Frame::DatagramClose {
                    flow_id: flow.flow_id,
                },
                self.stream.runtime.codec_limits,
            )
            .await?;
        }
        self.flows.clear();
        let _ = udp_path_finish_stream(&mut self.stream.send);
        Ok(())
    }

    pub(in crate::runtime) async fn ping(
        &mut self,
        probe_timeout: Duration,
    ) -> Result<(), RuntimeError> {
        let nonce = random_u64()?;
        udp_path_write_frame(
            &mut self.stream.send,
            &Frame::Ping { nonce },
            self.stream.runtime.codec_limits,
        )
        .await?;
        match tokio::time::timeout(probe_timeout, self.stream.frames.recv())
            .await
            .map_err(|_| RuntimeError::Protocol("UDP path probe ping timed out"))?
            .ok_or(RuntimeError::ReliablePathSessionClosed)??
        {
            Frame::Pong {
                nonce: received_nonce,
            } if received_nonce == nonce => Ok(()),
            Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
            _ => Err(RuntimeError::Protocol("unexpected UDP path probe frame")),
        }
    }

    pub(in crate::runtime) async fn close_session(&mut self) -> Result<(), RuntimeError> {
        udp_path_write_frame(
            &mut self.stream.send,
            &Frame::SessionClose {
                reason: CloseReason::Normal,
            },
            self.stream.runtime.codec_limits,
        )
        .await?;
        let _ = udp_path_finish_stream(&mut self.stream.send);
        Ok(())
    }

    pub(in crate::runtime) fn delivery_stats(&self) -> PathDeliveryStats {
        self.stats
    }

    pub(in crate::runtime) fn mtu_payload_bytes(&self) -> usize {
        self.mtu_payload_bytes
    }

    pub(in crate::runtime) async fn probe_mtu(
        &mut self,
        payload_bytes: usize,
    ) -> Result<usize, RuntimeError> {
        if payload_bytes > self.mux_limits.max_payload_bytes {
            return Err(RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                actual: payload_bytes,
                limit: self.mux_limits.max_payload_bytes,
            }));
        }
        self.mtu_payload_bytes = self.mux_limits.max_payload_bytes;
        Ok(self.mtu_payload_bytes)
    }

    pub(in crate::runtime) fn take_feedback_observation(
        &mut self,
    ) -> Option<UdpDatagramPathObservation> {
        self.last_feedback_observation.take()
    }

    fn handle_datagram_feedback(
        &mut self,
        flow_id: DatagramFlowId,
        ranges: &[OffsetRange],
    ) -> Result<(), RuntimeError> {
        let now = Instant::now();
        let lost = self.expire_unacked_datagrams(now);
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
                self.observe_datagram_ack(sent, now, lost);
            }
        }
        Ok(())
    }

    fn observe_remote_path_metrics(&mut self, metrics: PathMetrics) {
        self.last_feedback_observation = Some(UdpDatagramPathObservation {
            rtt: Duration::from_micros(u64::from(metrics.srtt_us)),
            jitter: Duration::from_micros(u64::from(metrics.jitter_us)),
            loss_rate: metrics
                .loss_observed
                .then(|| (f64::from(metrics.loss_ppm) / 1_000_000.0).clamp(0.0, 1.0))
                .unwrap_or(0.0),
            rate_sample: PathRateSample::new(
                metrics.delivery_rate_bps.max(8) / 8,
                Duration::from_secs(1),
            ),
        });
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

    fn observe_datagram_ack(&mut self, sent: SentDatagram, now: Instant, lost: u64) {
        let rtt = now.duration_since(sent.sent_at).max(QUIC_TIMER_GRANULARITY);
        let jitter = self
            .last_datagram_rtt
            .map(|previous| previous.abs_diff(rtt))
            .unwrap_or(Duration::ZERO);
        self.last_datagram_rtt = Some(rtt);
        let delivered = 1_u64;
        let total = delivered.saturating_add(lost).max(1);
        self.last_feedback_observation = Some(UdpDatagramPathObservation {
            rtt,
            jitter,
            loss_rate: lost as f64 / total as f64,
            rate_sample: PathRateSample::new(sent.bytes as u64, rtt),
        });
    }
}

/// Opens the QUIC path stream and records only carrier-open latency here.
pub(super) async fn open_udp_datagram_session_on_path(
    context: &ClientPathContext,
    path_index: usize,
    handshake_timeout: Duration,
) -> Result<UdpDatagramClientSession, RuntimeError> {
    let path_session = context
        .udp_sessions
        .get(path_index)
        .cloned()
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let started_at = Instant::now();
    let session = UdpDatagramClientSession::open_from_udp_session(
        path_session,
        path_index,
        context.mux_limits,
        handshake_timeout,
    )
    .await?;
    context.mark_udp_path_open_success(path_index, started_at.elapsed());
    Ok(session)
}
