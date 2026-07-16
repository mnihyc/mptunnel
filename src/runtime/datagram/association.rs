//! Cross-underlay product datagram selection and failover.

use super::policy::{DatagramTimeoutAction, datagram_remaining_ttl_ms, datagram_timeout_action};
use super::quic::UdpDatagramClientAssociation;
use super::tcp::TcpDatagramClientAssociation;
use crate::model::capacity::PATH_OPEN_SCORE_BYTES;
use crate::model::path::RelayPathKey;
use crate::protocol::{TargetAddr, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::telemetry::{ProductFlowCounter, ProductFlowLease};
use crate::scheduler::{TrafficClass, path_is_backup};
use bytes::Bytes;
use std::time::Duration;

pub(in crate::runtime) struct DatagramClientAssociation {
    context: ClientPathContext,
    udp: Option<Box<UdpDatagramClientAssociation>>,
    tcp: Option<Box<TcpDatagramClientAssociation>>,
    telemetry_flow: Option<ProductFlowLease>,
    telemetry_counter: Option<ProductFlowCounter>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct DatagramUnderlayCandidate {
    key: RelayPathKey,
    eta_ms: f64,
}

pub(in crate::runtime) enum DatagramUnderlaySendError {
    Timeout {
        path_was_acked: bool,
        product_attempts: usize,
        source: RuntimeError,
    },
    Runtime {
        path_was_acked: bool,
        product_attempts: usize,
        source: RuntimeError,
    },
    PayloadLimitExceeded {
        product_attempts: usize,
        source: RuntimeError,
    },
}

impl DatagramUnderlaySendError {
    fn into_runtime(self) -> RuntimeError {
        match self {
            Self::Timeout { source, .. }
            | Self::Runtime { source, .. }
            | Self::PayloadLimitExceeded { source, .. } => source,
        }
    }
}

impl DatagramClientAssociation {
    pub(in crate::runtime) async fn new(context: ClientPathContext) -> Result<Self, RuntimeError> {
        if context.udp_paths.is_empty() && context.tcp_paths.is_empty() {
            return Err(RuntimeError::NoDatagramPath);
        }
        Ok(Self {
            context,
            udp: None,
            tcp: None,
            telemetry_flow: None,
            telemetry_counter: None,
        })
    }

    fn ensure_product_flow(&mut self) {
        if self.telemetry_counter.is_none() {
            let flow = self
                .context
                .telemetry
                .open_local_datagram_flow(Some(self.context.session_id));
            self.telemetry_counter = Some(flow.counter());
            self.telemetry_flow = Some(flow);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn select_underlay(
        context: &ClientPathContext,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Option<UnderlayProtocol> {
        datagram_underlay_candidates(context, payload_bytes, ttl_ms)
            .first()
            .map(|candidate| candidate.key.underlay)
    }

    pub(in crate::runtime) async fn send_to_fresh_datagram_with_route_hint(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
        route_hint: Option<RelayPathKey>,
    ) -> Result<Bytes, RuntimeError> {
        self.ensure_product_flow();
        self.telemetry_counter
            .as_ref()
            .expect("datagram telemetry counter initialized")
            .record_datagram_to_peer(payload.len() as u64);
        let mut last_retryable_error = None;
        let product_deadline =
            tokio::time::Instant::now() + Duration::from_millis(u64::from(ttl_ms));
        let mut remaining_product_attempts = 2usize;
        let mut candidates = datagram_underlay_candidates(&self.context, payload.len(), ttl_ms);
        if let Some(route_hint) = route_hint
            && let Some(position) = candidates
                .iter()
                .position(|candidate| candidate.key == route_hint)
        {
            let hinted = candidates.remove(position);
            candidates.insert(0, hinted);
        }
        let candidate_count = candidates.len();
        for (position, candidate) in candidates.into_iter().enumerate() {
            if remaining_product_attempts == 0 {
                break;
            }
            let remaining_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
            if remaining_ttl_ms == 0 {
                return Err(RuntimeError::DatagramResponseTimedOut);
            }
            if candidate.eta_ms > f64::from(remaining_ttl_ms) {
                continue;
            }
            let has_unattempted_alternative = position + 1 < candidate_count;
            let attempt_limit = if has_unattempted_alternative {
                1
            } else {
                remaining_product_attempts
            };
            match candidate.key.underlay {
                UnderlayProtocol::Tcp => {
                    let result = self
                        .send_to_tcp(
                            target.clone(),
                            payload.clone(),
                            product_deadline,
                            attempt_limit,
                            has_unattempted_alternative,
                        )
                        .await;
                    match result {
                        Ok(response) => {
                            self.telemetry_counter
                                .as_ref()
                                .expect("datagram telemetry counter initialized")
                                .record_datagram_from_peer(response.len() as u64);
                            return Ok(response);
                        }
                        Err(DatagramUnderlaySendError::Timeout {
                            path_was_acked,
                            product_attempts,
                            source,
                        }) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            match datagram_timeout_action(
                                path_was_acked,
                                has_unattempted_alternative && remaining_product_attempts > 0,
                            ) {
                                DatagramTimeoutAction::RetryAlternative => {
                                    last_retryable_error = Some(source);
                                }
                                DatagramTimeoutAction::TerminalProductExpiry => {
                                    return Err(source);
                                }
                            }
                        }
                        Err(DatagramUnderlaySendError::Runtime {
                            path_was_acked,
                            product_attempts,
                            source,
                        }) if !path_was_acked && datagram_underlay_error_is_retryable(&source) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            if !has_unattempted_alternative || remaining_product_attempts == 0 {
                                return Err(source);
                            }
                            last_retryable_error = Some(source);
                        }
                        Err(DatagramUnderlaySendError::PayloadLimitExceeded {
                            product_attempts,
                            source,
                        }) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            if !has_unattempted_alternative || remaining_product_attempts == 0 {
                                return Err(source);
                            }
                            last_retryable_error = Some(source);
                        }
                        Err(err) => return Err(err.into_runtime()),
                    }
                }
                UnderlayProtocol::Udp => {
                    let result = self
                        .send_to_udp(
                            target.clone(),
                            payload.clone(),
                            product_deadline,
                            attempt_limit,
                            has_unattempted_alternative,
                        )
                        .await;
                    match result {
                        Ok(response) => {
                            self.telemetry_counter
                                .as_ref()
                                .expect("datagram telemetry counter initialized")
                                .record_datagram_from_peer(response.len() as u64);
                            return Ok(response);
                        }
                        Err(DatagramUnderlaySendError::Timeout {
                            path_was_acked,
                            product_attempts,
                            source,
                        }) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            match datagram_timeout_action(
                                path_was_acked,
                                has_unattempted_alternative && remaining_product_attempts > 0,
                            ) {
                                DatagramTimeoutAction::RetryAlternative => {
                                    last_retryable_error = Some(source);
                                }
                                DatagramTimeoutAction::TerminalProductExpiry => {
                                    return Err(source);
                                }
                            }
                        }
                        Err(DatagramUnderlaySendError::Runtime {
                            path_was_acked,
                            product_attempts,
                            source,
                        }) if !path_was_acked && datagram_underlay_error_is_retryable(&source) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            if !has_unattempted_alternative || remaining_product_attempts == 0 {
                                return Err(source);
                            }
                            last_retryable_error = Some(source);
                        }
                        Err(DatagramUnderlaySendError::PayloadLimitExceeded {
                            product_attempts,
                            source,
                        }) => {
                            remaining_product_attempts =
                                remaining_product_attempts.saturating_sub(product_attempts);
                            if !has_unattempted_alternative || remaining_product_attempts == 0 {
                                return Err(source);
                            }
                            last_retryable_error = Some(source);
                        }
                        Err(err) => return Err(err.into_runtime()),
                    }
                }
            }
        }
        Err(last_retryable_error.unwrap_or(RuntimeError::NoDatagramPath))
    }

    async fn send_to_udp(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        product_deadline: tokio::time::Instant,
        attempt_limit: usize,
        has_unattempted_outer_alternative: bool,
    ) -> Result<Bytes, DatagramUnderlaySendError> {
        if self.udp.is_none() {
            self.udp = Some(Box::new(UdpDatagramClientAssociation::new(
                self.context.clone(),
            )));
        }
        let udp = self
            .udp
            .as_mut()
            .ok_or(DatagramUnderlaySendError::Runtime {
                path_was_acked: false,
                product_attempts: 0,
                source: RuntimeError::NoSchedulableUdpPath,
            })?;
        udp.send_to(
            target,
            payload,
            product_deadline,
            attempt_limit,
            has_unattempted_outer_alternative,
        )
        .await
    }

    async fn send_to_tcp(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        product_deadline: tokio::time::Instant,
        attempt_limit: usize,
        has_unattempted_outer_alternative: bool,
    ) -> Result<Bytes, DatagramUnderlaySendError> {
        if self.tcp.is_none() {
            self.tcp = Some(Box::new(
                TcpDatagramClientAssociation::open_best(
                    self.context.clone(),
                    payload.len(),
                    product_deadline,
                    has_unattempted_outer_alternative,
                )
                .await
                .map_err(|source| DatagramUnderlaySendError::Runtime {
                    path_was_acked: false,
                    product_attempts: 0,
                    source,
                })?,
            ));
        }
        let remaining_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if remaining_ttl_ms == 0 {
            return Err(DatagramUnderlaySendError::Timeout {
                path_was_acked: false,
                product_attempts: 0,
                source: RuntimeError::DatagramResponseTimedOut,
            });
        }
        let (result, session_usable) = {
            let tcp = self
                .tcp
                .as_mut()
                .ok_or(DatagramUnderlaySendError::Runtime {
                    path_was_acked: false,
                    product_attempts: 0,
                    source: RuntimeError::NoSchedulableTcpPath,
                })?;
            let result = tcp
                .send_to_with_carrier_recovery(
                    target,
                    payload,
                    product_deadline,
                    attempt_limit,
                    has_unattempted_outer_alternative,
                )
                .await;
            (result, tcp.session.connection_usable)
        };
        match result {
            Ok((response, reusable)) => {
                if !reusable {
                    self.tcp = None;
                }
                Ok(response)
            }
            Err(err) => {
                if !session_usable {
                    self.tcp = None;
                }
                Err(err)
            }
        }
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        let udp_result = if let Some(udp) = &mut self.udp {
            udp.close().await
        } else {
            Ok(())
        };
        let tcp_result = if let Some(tcp) = &mut self.tcp {
            tcp.close().await
        } else {
            Ok(())
        };
        let result = udp_result.and(tcp_result);
        if result.is_ok() {
            self.telemetry_counter = None;
            if let Some(flow) = self.telemetry_flow.take() {
                flow.complete();
            }
        }
        result
    }
}

fn datagram_underlay_candidates(
    context: &ClientPathContext,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<DatagramUnderlayCandidate> {
    if ttl_ms == 0 {
        return Vec::new();
    }
    let payload_bytes = payload_bytes.max(PATH_OPEN_SCORE_BYTES);
    let freshness_budget_ms = f64::from(ttl_ms);
    let mut candidates = Vec::new();

    if let Some(path_index) = context
        .ordered_tcp_path_indices(TrafficClass::RealtimeDatagram, payload_bytes)
        .first()
        .copied()
    {
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: path_index,
        };
        if let Some(eta_ms) =
            context.reliable_relay_path_eta_ms(key, TrafficClass::RealtimeDatagram, payload_bytes)
            && eta_ms <= freshness_budget_ms
        {
            candidates.push(DatagramUnderlayCandidate { key, eta_ms });
        }
    }

    if let Some(candidate) = context
        .ordered_udp_path_candidates_for_ttl(payload_bytes, ttl_ms)
        .first()
        .copied()
    {
        candidates.push(DatagramUnderlayCandidate {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: candidate.path_index,
            },
            eta_ms: candidate.eta_ms,
        });
    }

    candidates.sort_by(|left, right| {
        let left_backup = context
            .reliable_path_snapshot(left.key)
            .is_some_and(path_is_backup);
        let right_backup = context
            .reliable_path_snapshot(right.key)
            .is_some_and(path_is_backup);
        left_backup
            .cmp(&right_backup)
            .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
            .then_with(|| {
                context
                    .relay_path_config_ordinal(left.key)
                    .cmp(&context.relay_path_config_ordinal(right.key))
            })
            .then_with(|| left.key.index.cmp(&right.key.index))
            .then_with(|| left.key.underlay.cmp(&right.key.underlay))
    });
    candidates
}

pub(in crate::runtime) fn datagram_underlay_candidate_keys(
    context: &ClientPathContext,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<RelayPathKey> {
    datagram_underlay_candidates(context, payload_bytes, ttl_ms)
        .into_iter()
        .map(|candidate| candidate.key)
        .collect()
}

pub(in crate::runtime) fn datagram_underlay_error_is_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    matches!(
        err,
        RuntimeError::NoTcpPath
            | RuntimeError::NoSchedulableTcpPath
            | RuntimeError::NoSchedulableUdpPath
            | RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Udp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::ReliablePathSessionClosed
    )
}

pub(in crate::runtime) fn runtime_error_is_datagram_response_timeout(err: &RuntimeError) -> bool {
    matches!(err, RuntimeError::DatagramResponseTimedOut)
}
