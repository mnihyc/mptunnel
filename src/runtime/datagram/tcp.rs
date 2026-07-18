//! TCP datagram carrier selection and recovery.

use super::association::{DatagramUnderlaySendError, runtime_error_is_datagram_response_timeout};
use super::policy::{
    DatagramPathSendError, datagram_remaining_ttl_ms, datagram_response_deadline_budget,
};
use super::tcp_session::TcpDatagramClientSession;
use crate::model::capacity::{DATAGRAM_FEEDBACK_DELAY_BUDGET, TRANSPORT_TIMER_GRANULARITY};
use crate::model::path::RelayPathKey;
use crate::model::timing::{
    path_open_pto, path_open_pto_multiplier, path_open_serialized_exchanges,
    transport_pto_from_snapshot,
};
use crate::mux::datagram::DatagramError;
use crate::protocol::{TargetAddr, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::scheduler::{PathSnapshot, TrafficClass};
use bytes::Bytes;
use std::time::{Duration, Instant};

pub(in crate::runtime) struct TcpDatagramClientAssociation {
    context: ClientPathContext,
    pub(in crate::runtime) session: TcpDatagramClientSession,
}

impl TcpDatagramClientAssociation {
    pub(in crate::runtime) async fn open_best(
        context: ClientPathContext,
        payload_bytes: usize,
        product_deadline: tokio::time::Instant,
        has_unattempted_alternative: bool,
        excluded_path_index: Option<usize>,
    ) -> Result<Self, RuntimeError> {
        if context.tcp_paths.is_empty() {
            return Err(RuntimeError::NoTcpPath);
        }
        // When recovery reserved a distinct ranked path, reopening the failed
        // path would spend that alternative's product time. Initial and
        // single-path opens pass no exclusion.
        let candidates = context
            .ordered_tcp_path_indices(TrafficClass::RealtimeDatagram, payload_bytes)
            .into_iter()
            .filter(|path_index| Some(*path_index) != excluded_path_index)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(RuntimeError::NoSchedulableTcpPath);
        }
        let candidate_count = candidates.len();
        let mut last_retryable_error = None;
        for (position, path_index) in candidates.into_iter().enumerate() {
            let remaining = product_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let key = RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: path_index,
            };
            let snapshot = context.tcp_path_snapshot(path_index);
            let eta_ms = context
                .reliable_relay_path_eta_ms(key, TrafficClass::RealtimeDatagram, payload_bytes)
                .unwrap_or(f64::INFINITY);
            if eta_ms > remaining.as_secs_f64() * 1000.0 {
                continue;
            }
            let path_budget = tcp_datagram_path_open_timeout(
                snapshot,
                has_unattempted_alternative || position + 1 < candidate_count,
                remaining,
            );
            if path_budget.is_zero() {
                break;
            }
            let open_deadline = tokio::time::Instant::now() + path_budget;
            let started_at = Instant::now();
            match TcpDatagramClientSession::open(&context, path_index, open_deadline).await {
                Ok(session) => {
                    context.mark_tcp_path_open_success(
                        path_index,
                        started_at.elapsed(),
                        TrafficClass::RealtimeDatagram,
                    );
                    return Ok(Self { context, session });
                }
                Err(err) if tcp_datagram_error_is_path_retryable(&err) => {
                    context.mark_tcp_path_failure(path_index);
                    last_retryable_error = Some(err);
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableTcpPath))
    }

    pub(in crate::runtime) async fn send_to_with_carrier_recovery(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        product_deadline: tokio::time::Instant,
        attempt_limit: usize,
        has_unattempted_outer_alternative: bool,
    ) -> Result<(Bytes, bool), DatagramUnderlaySendError> {
        let initial_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        let retry_deadline = tokio::time::Instant::now()
            + self
                .adaptive_retry_budget(initial_ttl_ms)
                .min(product_deadline.saturating_duration_since(tokio::time::Instant::now()));
        let mut product_attempts = 0usize;
        loop {
            let remaining_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
            if remaining_ttl_ms == 0 || product_attempts >= attempt_limit {
                return Err(DatagramUnderlaySendError::Timeout {
                    feedback_received: false,
                    product_attempts,
                    source: RuntimeError::DatagramResponseTimedOut,
                });
            }
            let has_tcp_alternative = product_attempts + 1 < attempt_limit
                && self
                    .context
                    .ordered_tcp_path_indices(TrafficClass::RealtimeDatagram, payload.len())
                    .into_iter()
                    .any(|path_index| path_index != self.session.path_index);
            let remaining = Duration::from_millis(u64::from(remaining_ttl_ms));
            let attempt_budget = if has_unattempted_outer_alternative || has_tcp_alternative {
                remaining / 2
            } else {
                remaining
            };
            let has_unattempted_alternative =
                has_unattempted_outer_alternative || has_tcp_alternative;
            let attempt_deadline = tokio::time::Instant::now() + attempt_budget;
            product_attempts = product_attempts.saturating_add(1);
            match self
                .session
                .send_to(
                    target.clone(),
                    payload.clone(),
                    attempt_deadline,
                    product_deadline,
                    has_unattempted_alternative,
                )
                .await
            {
                Ok(response) => {
                    let reusable = self.session.connection_usable;
                    if !reusable {
                        let path_index = self.session.path_index;
                        self.context
                            .mark_tcp_path_delivery(path_index, self.session.delivery_stats());
                        self.context
                            .release_tcp_path_load(path_index, TrafficClass::RealtimeDatagram);
                        self.context.mark_tcp_path_failure(path_index);
                    }
                    return Ok((response, reusable));
                }
                Err(DatagramPathSendError::Timeout {
                    feedback_received,
                    response_timeout: _,
                }) => {
                    if feedback_received {
                        if !self.session.connection_usable {
                            let failed_path_index = self.session.path_index;
                            self.context.mark_tcp_path_delivery(
                                failed_path_index,
                                self.session.delivery_stats(),
                            );
                            self.context.release_tcp_path_load(
                                failed_path_index,
                                TrafficClass::RealtimeDatagram,
                            );
                            self.context.mark_tcp_path_failure(failed_path_index);
                        }
                        return Err(DatagramUnderlaySendError::Timeout {
                            feedback_received,
                            product_attempts,
                            source: RuntimeError::DatagramResponseTimedOut,
                        });
                    }
                    let failed_path_index = self.session.path_index;
                    self.session.connection_usable = false;
                    self.context
                        .mark_tcp_path_delivery(failed_path_index, self.session.delivery_stats());
                    self.context
                        .release_tcp_path_load(failed_path_index, TrafficClass::RealtimeDatagram);
                    self.context.mark_tcp_path_failure(failed_path_index);
                    if has_unattempted_outer_alternative
                        || product_attempts >= attempt_limit
                        || tokio::time::Instant::now() >= retry_deadline
                    {
                        return Err(DatagramUnderlaySendError::Timeout {
                            feedback_received,
                            product_attempts,
                            source: RuntimeError::DatagramResponseTimedOut,
                        });
                    }
                    match Self::open_best(
                        self.context.clone(),
                        payload.len(),
                        product_deadline,
                        false,
                        has_tcp_alternative.then_some(failed_path_index),
                    )
                    .await
                    {
                        Ok(replacement) => {
                            self.session = replacement.session;
                        }
                        Err(_) => {
                            return Err(DatagramUnderlaySendError::Timeout {
                                feedback_received,
                                product_attempts,
                                source: RuntimeError::DatagramResponseTimedOut,
                            });
                        }
                    }
                }
                Err(DatagramPathSendError::Runtime {
                    feedback_received,
                    source,
                }) if !feedback_received && tcp_datagram_error_is_path_retryable(&source) => {
                    let failed_path_index = self.session.path_index;
                    self.session.connection_usable = false;
                    self.context
                        .mark_tcp_path_delivery(failed_path_index, self.session.delivery_stats());
                    self.context
                        .release_tcp_path_load(failed_path_index, TrafficClass::RealtimeDatagram);
                    self.context.mark_tcp_path_failure(failed_path_index);
                    if has_unattempted_outer_alternative
                        || product_attempts >= attempt_limit
                        || tokio::time::Instant::now() >= retry_deadline
                    {
                        return Err(DatagramUnderlaySendError::Runtime {
                            feedback_received,
                            product_attempts,
                            source,
                        });
                    }
                    let replacement = Self::open_best(
                        self.context.clone(),
                        payload.len(),
                        product_deadline,
                        false,
                        has_tcp_alternative.then_some(failed_path_index),
                    )
                    .await
                    .map_err(|source| DatagramUnderlaySendError::Runtime {
                        feedback_received: false,
                        product_attempts,
                        source,
                    })?;
                    self.session = replacement.session;
                }
                Err(DatagramPathSendError::Runtime {
                    feedback_received,
                    source,
                }) => {
                    let failed_path_index = self.session.path_index;
                    self.session.connection_usable = false;
                    self.context
                        .mark_tcp_path_delivery(failed_path_index, self.session.delivery_stats());
                    self.context
                        .release_tcp_path_load(failed_path_index, TrafficClass::RealtimeDatagram);
                    self.context.mark_tcp_path_failure(failed_path_index);
                    return Err(DatagramUnderlaySendError::Runtime {
                        feedback_received,
                        product_attempts,
                        source,
                    });
                }
                Err(DatagramPathSendError::PayloadLimitExceeded { limit }) => {
                    return Err(DatagramUnderlaySendError::Runtime {
                        feedback_received: false,
                        product_attempts,
                        source: RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                            actual: payload.len(),
                            limit,
                        }),
                    });
                }
            }
        }
    }

    fn adaptive_retry_budget(&self, ttl_ms: u32) -> Duration {
        datagram_response_deadline_budget(self.session.response_timeout(ttl_ms), ttl_ms, false)
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        let close_result = self.session.close().await;
        self.context
            .mark_tcp_path_delivery(self.session.path_index, self.session.delivery_stats());
        self.context
            .release_tcp_path_load(self.session.path_index, TrafficClass::RealtimeDatagram);
        close_result
    }
}

pub(in crate::runtime) fn tcp_datagram_response_timeout(
    snapshot: PathSnapshot,
    response_srtt: Option<Duration>,
    response_rttvar: Option<Duration>,
    ttl_ms: u32,
) -> Duration {
    let ttl = Duration::from_millis(u64::from(ttl_ms));
    if ttl.is_zero() {
        return ttl;
    }
    let initial_response_pto = transport_pto_from_snapshot(Some(snapshot));
    let srtt = response_srtt.unwrap_or(initial_response_pto);
    let rttvar = response_rttvar.unwrap_or_else(|| {
        Duration::from_secs_f64((snapshot.jitter_ms.max(snapshot.srtt_ms.max(1.0) / 8.0)) / 1000.0)
    });
    let loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
    (srtt + rttvar.mul_f64(4.0) + DATAGRAM_FEEDBACK_DELAY_BUDGET)
        .mul_f64(loss_gain)
        .max(TRANSPORT_TIMER_GRANULARITY.min(ttl))
        .min(ttl)
}

pub(in crate::runtime) fn tcp_datagram_path_open_timeout(
    snapshot: Option<PathSnapshot>,
    has_unattempted_alternative: bool,
    remaining_ttl: Duration,
) -> Duration {
    // A new TCP carrier needs its own conservative retransmission budget;
    // a prior probe RTT cannot prove that this connection has opened.
    let fresh_carrier_pto = path_open_pto(snapshot, false);
    if has_unattempted_alternative {
        fresh_carrier_pto
            .saturating_mul(path_open_serialized_exchanges(snapshot))
            .min(remaining_ttl / 2)
    } else {
        fresh_carrier_pto
            .saturating_mul(path_open_pto_multiplier(snapshot))
            .min(remaining_ttl)
    }
}

pub(in crate::runtime) fn tcp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::ReliablePathSessionClosed
    )
}
