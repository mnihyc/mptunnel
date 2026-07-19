//! One QUIC carrier session for product datagram request/response.

use super::policy::{DatagramPathSendError, datagram_remaining_ttl_ms};
use super::{
    DatagramClientFlow, DatagramSessionEvent, ReceivedDatagram, SentDatagram, SentDatagramEvidence,
    datagram_feedback_range, datagram_id_is_in_ranges,
};
use crate::config::SecurityConfig;
use crate::model::capacity::{PathRateSample, QUIC_TIMER_GRANULARITY};
use crate::model::path::CarrierPathInstanceId;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    DatagramFlowId, DatagramId, Frame, OffsetRange, PathId, PathMetrics, PathUsage, SessionId,
    TargetAddr, UnderlayProtocol,
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
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusSnapshotSource};
#[cfg(test)]
use crate::transport::SystemCarrierNetworkProvider;
use crate::transport::{CarrierNetworkProvider, CarrierPathIdentity, PathSpec};
use bytes::Bytes;
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

pub(in crate::runtime) struct UdpDatagramClientSession {
    _path_session: ClientUdpPathSessionHandle,
    stream: ClientUdpDatagramStream,
    flows: Vec<DatagramClientFlow>,
    mux_limits: MuxLimits,
    pub(in crate::runtime) path_index: usize,
    path_id: PathId,
    stats: PathDeliveryStats,
    sent_datagrams: SentDatagramEvidence,
    last_datagram_rtt: Option<Duration>,
    last_feedback_observation: Option<UdpDatagramPathObservation>,
    pub(in crate::runtime) connection_usable: bool,
}

impl UdpDatagramClientSession {
    #[cfg(test)]
    pub(in crate::runtime) async fn open(
        path: &PathSpec,
        path_index: usize,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let open_deadline = tokio::time::Instant::now() + handshake_timeout;
        Self::open_with_provider(
            path,
            path_index,
            security,
            codec_limits,
            mux_limits,
            open_deadline,
            std::sync::Arc::new(SystemCarrierNetworkProvider),
        )
        .await
    }

    pub(in crate::runtime) async fn open_with_provider(
        path: &PathSpec,
        path_index: usize,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        open_deadline: tokio::time::Instant,
        carrier_network: std::sync::Arc<dyn CarrierNetworkProvider>,
    ) -> Result<Self, RuntimeError> {
        let session_id = random_session_id()?;
        Self::open_for_session_with_provider(
            path,
            path_index,
            session_id,
            security,
            codec_limits,
            mux_limits,
            open_deadline,
            carrier_network,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) async fn open_for_session_with_provider(
        path: &PathSpec,
        path_index: usize,
        session_id: SessionId,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        open_deadline: tokio::time::Instant,
        carrier_network: std::sync::Arc<dyn CarrierNetworkProvider>,
    ) -> Result<Self, RuntimeError> {
        let state = ClientPathState::new(ClientPathHealth {
            tcp: Vec::new(),
            udp: vec![ClientPathHealthRecord::default(); path_index.saturating_add(1)],
        });
        let peer_status = PeerStatusBroker::new(false);
        let path_session = ClientUdpPathSessionHandle::new(ClientUdpPathSessionRuntime {
            paths: std::sync::Arc::new(vec![path.clone()]),
            config_index: 0,
            path_index,
            carrier_identity: CarrierPathIdentity {
                group_ordinal: 0,
                path_ordinal: path_index,
            },
            session_id,
            security: std::sync::Arc::new(vec![security]),
            codec_limits,
            mux_limits,
            stream_frame_queue: reliable_stream_frame_queue(mux_limits),
            state,
            carrier_network,
            peer_status,
            peer_status_snapshot: PeerStatusSnapshotSource::new(Vec::new),
        });
        Self::open_from_udp_session(path_session, path_index, mux_limits, open_deadline).await
    }

    pub(in crate::runtime) async fn open_from_udp_session(
        path_session: ClientUdpPathSessionHandle,
        path_index: usize,
        mux_limits: MuxLimits,
        open_deadline: tokio::time::Instant,
    ) -> Result<Self, RuntimeError> {
        let stream = path_session.open_datagram_stream(open_deadline).await?;
        let path_id = stream.path_id;
        Ok(Self {
            _path_session: path_session,
            stream,
            flows: Vec::new(),
            mux_limits,
            path_index,
            path_id,
            stats: PathDeliveryStats::default(),
            sent_datagrams: SentDatagramEvidence::new(mux_limits),
            last_datagram_rtt: None,
            last_feedback_observation: None,
            connection_usable: true,
        })
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
        match tokio::time::timeout_at(fallback_deadline, self.ensure_flow(flow_id, target)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err)),
            Err(_) => {
                return Err(DatagramPathSendError::Timeout);
            }
        };
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout);
        }
        let request_len = payload.len();
        let frame = Frame::DatagramData {
            flow_id,
            datagram_id,
            ttl_ms,
            payload,
        };
        let request_key = (flow_id, datagram_id);
        self.expire_datagrams_without_feedback(Instant::now());
        self.last_feedback_observation = None;
        let request_started_at = Instant::now();

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
            Ok(Ok(())) => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "udp_datagram_request_written",
                    format_args!(
                        "path_id={} path_index={} flow_id={} datagram_id={} payload_bytes={}",
                        self.path_id.0, self.path_index, flow_id.0, datagram_id.0, request_len,
                    ),
                );
            }
            Ok(Err(err)) => {
                self.sent_datagrams.remove(&request_key);
                return Err(DatagramPathSendError::runtime(err));
            }
            Err(_) => {
                self.sent_datagrams.remove(&request_key);
                return Err(DatagramPathSendError::Timeout);
            }
        }
        Ok(())
    }

    pub(in crate::runtime) async fn next_frame(&mut self) -> Result<Frame, RuntimeError> {
        self.stream
            .frames
            .recv()
            .await
            .ok_or(RuntimeError::ReliablePathSessionClosed)?
    }

    pub(in crate::runtime) async fn handle_frame(
        &mut self,
        frame: Frame,
    ) -> Result<DatagramSessionEvent, RuntimeError> {
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
                        "expired QUIC response datagram received",
                    ));
                }
                self.stats.record_payload_bytes(payload.len());
                Ok(DatagramSessionEvent::Received(ReceivedDatagram {
                    flow_id,
                    datagram_id,
                    expires_at: tokio::time::Instant::now()
                        + Duration::from_millis(u64::from(ttl_ms)),
                    payload,
                }))
            }
            Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                self.observe_remote_path_metrics(metrics);
                Ok(DatagramSessionEvent::Control)
            }
            Frame::PathStatus {
                path_id,
                sequence,
                usage,
            } => {
                apply_client_udp_datagram_path_status(
                    &self.stream.runtime.state,
                    self.path_index,
                    self.stream.path_instance_id,
                    self.path_id,
                    path_id,
                    sequence,
                    usage,
                )?;
                Ok(DatagramSessionEvent::Control)
            }
            Frame::Ping { nonce } => {
                udp_path_write_frame(
                    &mut self.stream.send,
                    &Frame::Pong { nonce },
                    self.stream.runtime.codec_limits,
                )
                .await?;
                Ok(DatagramSessionEvent::Control)
            }
            Frame::SessionReady | Frame::Pong { .. } => Ok(DatagramSessionEvent::Control),
            Frame::DatagramClose { .. } => Err(RuntimeError::Protocol("datagram flow closed")),
            Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
            _ => Err(RuntimeError::Protocol("unexpected UDP datagram frame")),
        }
    }

    pub(in crate::runtime) async fn acknowledge(
        &mut self,
        flow_id: DatagramFlowId,
        datagram_id: DatagramId,
    ) -> Result<(), RuntimeError> {
        udp_path_write_frame(
            &mut self.stream.send,
            &Frame::DatagramFeedback {
                flow_id,
                received: vec![datagram_feedback_range(datagram_id)?],
            },
            self.stream.runtime.codec_limits,
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
    ) -> Result<(), RuntimeError> {
        if let Some(flow) = self.flows.iter().find(|flow| flow.flow_id == flow_id) {
            return if flow.target == target {
                Ok(())
            } else {
                Err(RuntimeError::Protocol("QUIC datagram flow target changed"))
            };
        }
        if self.flows.iter().any(|flow| flow.target == target) {
            return Err(RuntimeError::Protocol(
                "QUIC datagram target rebound to another flow",
            ));
        }
        udp_path_write_frame(
            &mut self.stream.send,
            &Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
            },
            self.stream.runtime.codec_limits,
        )
        .await?;
        self.flows.push(DatagramClientFlow { target, flow_id });
        Ok(())
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

    pub(in crate::runtime) async fn ping_until(
        &mut self,
        probe_deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        let nonce = random_u64()?;
        let ping = async {
            udp_path_write_frame(
                &mut self.stream.send,
                &Frame::Ping { nonce },
                self.stream.runtime.codec_limits,
            )
            .await?;
            loop {
                let frame = self
                    .stream
                    .frames
                    .recv()
                    .await
                    .ok_or(RuntimeError::ReliablePathSessionClosed)??;
                match frame {
                    Frame::Pong {
                        nonce: received_nonce,
                    } if received_nonce == nonce => break Ok(()),
                    Frame::PathStatus {
                        path_id,
                        sequence,
                        usage,
                    } => {
                        apply_client_udp_datagram_path_status(
                            &self.stream.runtime.state,
                            self.path_index,
                            self.stream.path_instance_id,
                            self.path_id,
                            path_id,
                            sequence,
                            usage,
                        )?;
                    }
                    Frame::SessionReady => {}
                    Frame::SessionClose { reason } => {
                        break Err(RuntimeError::RemoteClosed(reason));
                    }
                    _ => break Err(RuntimeError::Protocol("unexpected UDP path probe frame")),
                }
            }
        };
        tokio::time::timeout_at(probe_deadline, ping)
            .await
            .map_err(|_| RuntimeError::Protocol("UDP path probe ping timed out"))??;
        Ok(())
    }

    pub(in crate::runtime) fn delivery_stats(&self) -> PathDeliveryStats {
        self.stats
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
        let lost = self.expire_datagrams_without_feedback(now);
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
                self.observe_datagram_feedback(sent, now, lost);
            }
        }
        Ok(())
    }

    fn observe_remote_path_metrics(&mut self, metrics: PathMetrics) {
        self.last_feedback_observation = Some(UdpDatagramPathObservation {
            rtt: Duration::from_micros(u64::from(metrics.srtt_us)),
            jitter: Duration::from_micros(u64::from(metrics.jitter_us)),
            loss_rate: if metrics.loss_observed {
                (f64::from(metrics.loss_ppm) / 1_000_000.0).clamp(0.0, 1.0)
            } else {
                0.0
            },
            rate_sample: PathRateSample::new(
                metrics.delivery_rate_bps.max(8) / 8,
                Duration::from_secs(1),
            ),
        });
    }

    fn expire_datagrams_without_feedback(&mut self, now: Instant) -> u64 {
        self.sent_datagrams.expire(now)
    }

    fn observe_datagram_feedback(&mut self, sent: SentDatagram, now: Instant, lost: u64) {
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

fn apply_client_udp_datagram_path_status(
    state: &ClientPathState,
    path_index: usize,
    path_instance_id: CarrierPathInstanceId,
    expected_path_id: PathId,
    status_path_id: PathId,
    sequence: u64,
    usage: PathUsage,
) -> Result<bool, RuntimeError> {
    if status_path_id != expected_path_id {
        return Err(RuntimeError::Protocol(
            "QUIC path usage advertisement path mismatch",
        ));
    }
    Ok(state.update_peer_path_usage(
        UnderlayProtocol::Udp,
        path_index,
        path_instance_id,
        sequence,
        usage,
    ))
}

#[cfg(test)]
#[path = "quic_session_test.rs"]
mod tests;

/// Opens the QUIC path stream and records only carrier-open latency here.
pub(super) async fn open_udp_datagram_session_on_path(
    context: &ClientPathContext,
    path_index: usize,
    open_deadline: tokio::time::Instant,
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
        open_deadline,
    )
    .await?;
    context.mark_udp_path_open_success(path_index, started_at.elapsed());
    Ok(session)
}
