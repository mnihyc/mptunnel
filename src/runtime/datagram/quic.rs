//! QUIC datagram path selection, pacing, and failover.

use super::UDP_PATH_HANDSHAKE_TIMEOUT;
use super::association::{DatagramUnderlaySendError, runtime_error_is_datagram_response_timeout};
use super::policy::{
    DatagramPathSendError, DatagramTimeoutAction, datagram_remaining_ttl_ms,
    datagram_timeout_action,
};
use super::quic_session::{UdpDatagramClientSession, open_udp_datagram_session_on_path};
use crate::model::capacity::{QUIC_PERSISTENT_CONGESTION_THRESHOLD, QUIC_TIMER_GRANULARITY};
use crate::model::timing::default_transport_pto;
use crate::mux::datagram::DatagramError;
use crate::protocol::{RateHint, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::model::{
    UdpDatagramPathObservation, UdpPathRuntimeModel, path_is_endpoint_only,
    udp_observation_has_datagram_feedback,
};
use crate::scheduler::{PathSnapshot, path_within_adaptive_lead_hysteresis};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

pub(in crate::runtime) struct UdpDatagramClientAssociation {
    pub(in crate::runtime) context: ClientPathContext,
    pub(in crate::runtime) paths: Vec<UdpDatagramAssociationPath>,
    pub(in crate::runtime) suppressed_paths: HashMap<usize, Instant>,
    pub(in crate::runtime) last_successful_path: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::runtime) struct UdpPathCandidate {
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) eta_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct UdpAssociationCandidateScore {
    path_index: usize,
    completion_ms: f64,
    eta_ms: f64,
    opens_new_session: bool,
    rank: usize,
    snapshot: PathSnapshot,
}

fn udp_association_candidate_order(
    left: &UdpAssociationCandidateScore,
    right: &UdpAssociationCandidateScore,
) -> std::cmp::Ordering {
    left.completion_ms
        .total_cmp(&right.completion_ms)
        .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
        .then_with(|| left.opens_new_session.cmp(&right.opens_new_session))
        .then_with(|| left.rank.cmp(&right.rank))
}

pub(in crate::runtime) struct UdpDatagramAssociationPath {
    pub(in crate::runtime) session: UdpDatagramClientSession,
    pub(in crate::runtime) pacer: UdpDatagramPacer,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpDatagramPacer {
    pub(in crate::runtime) next_send_at: Instant,
}

impl UdpDatagramPacer {
    fn new() -> Self {
        Self {
            next_send_at: Instant::now(),
        }
    }

    pub(in crate::runtime) fn ready_at(self) -> Instant {
        self.next_send_at
    }

    pub(in crate::runtime) async fn wait_for_send(
        &mut self,
        model: UdpPathRuntimeModel,
        payload_bytes: usize,
    ) {
        let now = Instant::now();
        if self.next_send_at > now {
            tokio::time::sleep(self.next_send_at.duration_since(now)).await;
        }
        self.next_send_at = Instant::now() + model.pacing_interval(payload_bytes);
    }
}

impl UdpDatagramClientAssociation {
    pub(in crate::runtime) fn new(context: ClientPathContext) -> Self {
        Self {
            context,
            paths: Vec::new(),
            suppressed_paths: HashMap::new(),
            last_successful_path: None,
        }
    }

    pub(in crate::runtime) async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        product_deadline: tokio::time::Instant,
        attempt_limit: usize,
        has_unattempted_outer_alternative: bool,
    ) -> Result<Bytes, DatagramUnderlaySendError> {
        if payload.len() > self.context.mux_limits.max_payload_bytes {
            return Err(DatagramUnderlaySendError::Runtime {
                path_was_acked: false,
                product_attempts: 0,
                source: RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                    actual: payload.len(),
                    limit: self.context.mux_limits.max_payload_bytes,
                }),
            });
        }
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        let candidates = self
            .context
            .ordered_udp_path_candidates_for_ttl(payload.len(), ttl_ms);
        if candidates.is_empty() {
            #[cfg(feature = "lab-diagnostics")]
            {
                let now = Instant::now();
                let observations = self
                    .context
                    .health()
                    .lock()
                    .expect("client path health lock")
                    .udp
                    .iter()
                    .enumerate()
                    .map(|(index, record)| {
                        let observation = record.observation_at(now);
                        format!(
                            "{}:{:?}:srtt={:?}:rate={:?}:carrier_rate={:?}:flows={}:failed={:?}",
                            index,
                            observation.state,
                            observation.measured_srtt_ms,
                            observation.measured_rate_bps,
                            observation.carrier_delivery_rate_bps,
                            observation.active_flows,
                            record.failed_until.map(|deadline| deadline
                                .saturating_duration_since(now)
                                .as_millis())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                lab_diagnostic(
                    "udp_datagram_no_candidates",
                    format_args!(
                        "udp_paths={} payload_bytes={} ttl_ms={} observations={}",
                        self.context.udp_paths.len(),
                        payload.len(),
                        ttl_ms,
                        observations
                    ),
                );
            }
            return Err(DatagramUnderlaySendError::Runtime {
                path_was_acked: false,
                product_attempts: 0,
                source: RuntimeError::NoSchedulableUdpPath,
            });
        }

        self.prune_suppressed_paths();
        let mut attempted = HashSet::new();
        let mut product_attempts = 0usize;
        let mut last_retryable_error = None;
        loop {
            let remaining_ttl_ms = datagram_remaining_ttl_ms(product_deadline);
            if remaining_ttl_ms == 0 || product_attempts >= attempt_limit {
                return Err(DatagramUnderlaySendError::Timeout {
                    path_was_acked: false,
                    product_attempts,
                    source: RuntimeError::DatagramResponseTimedOut,
                });
            }
            let Some(path_index) = self.select_path_candidate(
                &candidates,
                &attempted,
                payload.len(),
                remaining_ttl_ms,
            ) else {
                break;
            };
            attempted.insert(path_index);
            product_attempts = product_attempts.saturating_add(1);
            let has_unattempted_internal_alternative = product_attempts < attempt_limit
                && candidates
                    .iter()
                    .any(|candidate| !attempted.contains(&candidate.path_index));
            let has_unattempted_alternative =
                has_unattempted_outer_alternative || has_unattempted_internal_alternative;
            let remaining = Duration::from_millis(u64::from(remaining_ttl_ms));
            let fallback_deadline = if has_unattempted_alternative {
                tokio::time::Instant::now() + remaining / 2
            } else {
                product_deadline
            };
            match self
                .send_to_path(
                    path_index,
                    target.clone(),
                    payload.clone(),
                    fallback_deadline,
                    product_deadline,
                    has_unattempted_alternative,
                )
                .await
            {
                Ok(response) => {
                    self.last_successful_path = Some(path_index);
                    return Ok(response);
                }
                Err(DatagramPathSendError::MtuExceeded { limit }) => {
                    let source = RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                        actual: payload.len(),
                        limit,
                    });
                    product_attempts = product_attempts.saturating_sub(1);
                    if !has_unattempted_internal_alternative {
                        return Err(DatagramUnderlaySendError::PathMtuExceeded {
                            product_attempts,
                            source,
                        });
                    }
                    last_retryable_error = Some(source);
                }
                Err(DatagramPathSendError::Timeout {
                    path_was_acked,
                    response_timeout,
                }) => {
                    match datagram_timeout_action(
                        path_was_acked,
                        has_unattempted_internal_alternative,
                    ) {
                        DatagramTimeoutAction::RetryAlternative => {
                            self.remove_path(path_index);
                            self.suppress_path_after_timeout(
                                path_index,
                                response_timeout,
                                remaining_ttl_ms,
                            );
                            self.context.mark_udp_path_failure(path_index);
                            last_retryable_error = Some(RuntimeError::DatagramResponseTimedOut);
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "udp_datagram_unacked_timeout_retry_alternative",
                                format_args!(
                                    "path_index={} response_timeout_ms={} ttl_ms={}",
                                    path_index,
                                    response_timeout.as_millis(),
                                    remaining_ttl_ms
                                ),
                            );
                            continue;
                        }
                        DatagramTimeoutAction::TerminalProductExpiry => {
                            if path_was_acked {
                                self.context.mark_udp_path_feedback(
                                    path_index,
                                    UdpDatagramPathObservation {
                                        rtt: response_timeout,
                                        jitter: Duration::ZERO,
                                        loss_rate: 1.0,
                                        rate_sample: None,
                                    },
                                );
                            } else {
                                self.remove_path(path_index);
                                self.suppress_path_after_timeout(
                                    path_index,
                                    response_timeout,
                                    remaining_ttl_ms,
                                );
                                self.context.mark_udp_path_failure(path_index);
                            }
                        }
                    }
                    return Err(DatagramUnderlaySendError::Timeout {
                        path_was_acked,
                        product_attempts,
                        source: RuntimeError::DatagramResponseTimedOut,
                    });
                }
                Err(DatagramPathSendError::Runtime {
                    path_was_acked,
                    source,
                }) if udp_datagram_error_is_path_retryable(&source) => {
                    self.remove_path(path_index);
                    self.suppress_path_after_timeout(
                        path_index,
                        default_transport_pto(),
                        remaining_ttl_ms,
                    );
                    self.context.mark_udp_path_failure(path_index);
                    if path_was_acked || !has_unattempted_internal_alternative {
                        return Err(DatagramUnderlaySendError::Runtime {
                            path_was_acked,
                            product_attempts,
                            source,
                        });
                    }
                    last_retryable_error = Some(source);
                }
                Err(DatagramPathSendError::Runtime {
                    path_was_acked,
                    source,
                }) => {
                    return Err(DatagramUnderlaySendError::Runtime {
                        path_was_acked,
                        product_attempts,
                        source,
                    });
                }
            }
        }
        Err(DatagramUnderlaySendError::Runtime {
            path_was_acked: false,
            product_attempts,
            source: last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath),
        })
    }

    fn path_session_is_open(&self, path_index: usize) -> bool {
        self.paths
            .iter()
            .any(|path| path.session.path_index == path_index)
    }

    pub(in crate::runtime) async fn close(&mut self) -> Result<(), RuntimeError> {
        let mut close_error = None;
        while let Some(mut path) = self.paths.pop() {
            let close_result = path.session.close().await;
            self.context.mark_udp_datagram_path_delivery(
                path.session.path_index,
                path.session.delivery_stats(),
            );
            self.context.release_udp_path_load(path.session.path_index);
            if close_error.is_none()
                && let Err(err) = close_result
            {
                close_error = Some(err);
            }
        }
        match close_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub(in crate::runtime) fn select_path_candidate(
        &self,
        candidates: &[UdpPathCandidate],
        attempted: &HashSet<usize>,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Option<usize> {
        let now = Instant::now();
        let freshness_budget_ms = f64::from(ttl_ms);
        let mut viable = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| !attempted.contains(&candidate.path_index))
            .filter_map(|(rank, candidate)| {
                let open_ready_at = self
                    .paths
                    .iter()
                    .find(|path| path.session.path_index == candidate.path_index)
                    .map(|path| path.pacer.ready_at());
                let has_open_session = open_ready_at.is_some();
                let eta_ms = self.context.udp_path_eta_for_ttl(
                    candidate.path_index,
                    payload_bytes,
                    ttl_ms,
                    has_open_session,
                )?;
                let model = self
                    .context
                    .udp_path_runtime_model(candidate.path_index, ttl_ms)?;
                if !model.accepts_or_can_probe(payload_bytes) {
                    return None;
                }
                let snapshot = self.context.udp_path_snapshot(candidate.path_index)?;
                let ready_at = open_ready_at.unwrap_or(now);
                let ready_delay_ms = ready_at.saturating_duration_since(now).as_secs_f64() * 1000.0;
                let completion_ms = eta_ms + ready_delay_ms;
                (completion_ms <= freshness_budget_ms).then_some(UdpAssociationCandidateScore {
                    path_index: candidate.path_index,
                    completion_ms,
                    eta_ms,
                    opens_new_session: !has_open_session,
                    rank,
                    snapshot,
                })
            })
            .collect::<Vec<_>>();
        let evidenced_paths = viable
            .iter()
            .filter_map(|candidate| {
                self.path_has_datagram_feedback_or_hint(candidate.path_index)
                    .then_some(candidate.path_index)
            })
            .collect::<Vec<_>>();
        if evidenced_paths
            .iter()
            .any(|path_index| !self.path_is_temporarily_suppressed(*path_index, now))
        {
            viable.retain(|candidate| evidenced_paths.contains(&candidate.path_index));
        }
        let best_candidate = viable
            .iter()
            .min_by(|left, right| udp_association_candidate_order(left, right))
            .copied();
        if let Some(path_index) = self.last_successful_path
            && let Some(best) = best_candidate
            && let Some(candidate) = viable.iter().find(|candidate| {
                candidate.path_index == path_index
                    && !self.path_is_temporarily_suppressed(candidate.path_index, now)
            })
            && path_within_adaptive_lead_hysteresis(
                candidate.completion_ms,
                candidate.snapshot,
                best.completion_ms,
                best.snapshot,
                payload_bytes,
            )
        {
            return Some(candidate.path_index);
        }
        if evidenced_paths.is_empty()
            && self.context.udp_paths.iter().all(path_is_endpoint_only)
            && let Some(candidate) = viable
                .iter()
                .filter(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now))
                .min_by(|left, right| left.path_index.cmp(&right.path_index))
        {
            return Some(candidate.path_index);
        }
        let has_unsuppressed = viable
            .iter()
            .any(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now));
        if has_unsuppressed {
            viable.retain(|candidate| {
                !self.path_is_temporarily_suppressed(candidate.path_index, now)
            });
        }
        viable
            .into_iter()
            .min_by(udp_association_candidate_order)
            .map(|candidate| candidate.path_index)
    }

    pub(in crate::runtime) fn suppress_path_after_timeout(
        &mut self,
        path_index: usize,
        response_timeout: Duration,
        ttl_ms: u32,
    ) {
        let ttl = Duration::from_millis(u64::from(ttl_ms));
        let duration = response_timeout
            .max(QUIC_TIMER_GRANULARITY)
            .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
            .min(ttl);
        self.suppressed_paths
            .insert(path_index, Instant::now() + duration);
    }

    fn prune_suppressed_paths(&mut self) {
        let now = Instant::now();
        self.suppressed_paths
            .retain(|_, suppressed_until| *suppressed_until > now);
    }

    fn path_is_temporarily_suppressed(&self, path_index: usize, now: Instant) -> bool {
        self.suppressed_paths
            .get(&path_index)
            .is_some_and(|suppressed_until| *suppressed_until > now)
    }

    fn path_has_datagram_feedback_or_hint(&self, path_index: usize) -> bool {
        let Some(path) = self.context.udp_paths.get(path_index) else {
            return false;
        };
        let now = Instant::now();
        let Some(observation) = self
            .context
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get(path_index)
            .map(|record| record.observation_at(now))
        else {
            return false;
        };
        udp_observation_has_datagram_feedback(&observation)
            || path.metadata.initial_srtt_ms.is_some()
            || path.metadata.initial_jitter_ms.is_some()
            || path.metadata.initial_rate != RateHint::Unknown
    }

    async fn send_to_path(
        &mut self,
        path_index: usize,
        target: TargetAddr,
        payload: Bytes,
        fallback_deadline: tokio::time::Instant,
        product_deadline: tokio::time::Instant,
        has_unattempted_alternative: bool,
    ) -> Result<Bytes, DatagramPathSendError> {
        let ttl_ms = datagram_remaining_ttl_ms(product_deadline);
        if ttl_ms == 0 {
            return Err(DatagramPathSendError::Timeout {
                path_was_acked: false,
                response_timeout: Duration::ZERO,
            });
        }
        let model = self
            .context
            .udp_path_runtime_model(path_index, ttl_ms)
            .ok_or_else(|| {
                DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath, false)
            })?;
        if !model.accepts_or_can_probe(payload.len()) {
            return Err(DatagramPathSendError::MtuExceeded {
                limit: model.mtu_payload_bytes,
            });
        }
        let path_session_was_open = self.path_session_is_open(path_index);
        let association_had_open_path = !self.paths.is_empty();
        let open_started_at = tokio::time::Instant::now();
        let handshake_timeout = udp_datagram_path_open_timeout(
            association_had_open_path,
            has_unattempted_alternative,
            model,
            ttl_ms,
        )
        .min(fallback_deadline.saturating_duration_since(open_started_at));
        let open_deadline = (open_started_at + handshake_timeout).min(fallback_deadline);
        let setup_owns_remaining_product_budget = !association_had_open_path
            && !has_unattempted_alternative
            && fallback_deadline == product_deadline
            && product_deadline.saturating_duration_since(open_started_at)
                <= UDP_PATH_HANDSHAKE_TIMEOUT;
        let response_timeout = udp_datagram_first_response_timeout(
            path_session_was_open,
            association_had_open_path,
            has_unattempted_alternative,
            model,
            ttl_ms,
        );
        let position = match self.ensure_path_session(path_index, open_deadline).await {
            Err(RuntimeError::PathOpenTimedOut) if setup_owns_remaining_product_budget => {
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout,
                });
            }
            result => result.map_err(|err| DatagramPathSendError::runtime(err, false))?,
        };
        let current_mtu = self
            .paths
            .get(position)
            .ok_or_else(|| {
                DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath, false)
            })?
            .session
            .mtu_payload_bytes();
        if payload.len() > current_mtu {
            let probe_result = {
                let path = self.paths.get_mut(position).ok_or_else(|| {
                    DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath, false)
                })?;
                tokio::time::timeout_at(fallback_deadline, path.session.probe_mtu(payload.len()))
                    .await
            };
            match probe_result {
                Ok(Ok(probed_mtu)) => {
                    self.context.mark_udp_path_mtu(path_index, probed_mtu);
                }
                Ok(Err(err)) if udp_datagram_error_is_path_retryable(&err) => {
                    self.context.mark_udp_path_mtu(path_index, current_mtu);
                    return Err(DatagramPathSendError::MtuExceeded { limit: current_mtu });
                }
                Ok(Err(err)) => return Err(DatagramPathSendError::runtime(err, false)),
                Err(_) => {
                    self.context.mark_udp_path_mtu(path_index, current_mtu);
                    return Err(DatagramPathSendError::MtuExceeded { limit: current_mtu });
                }
            }
        }
        let (observation_path_index, observation, result, connection_usable) = {
            let path = self.paths.get_mut(position).ok_or_else(|| {
                DatagramPathSendError::runtime(RuntimeError::NoSchedulableUdpPath, false)
            })?;
            if tokio::time::timeout_at(
                fallback_deadline,
                path.pacer.wait_for_send(model, payload.len()),
            )
            .await
            .is_err()
            {
                return Err(DatagramPathSendError::Timeout {
                    path_was_acked: false,
                    response_timeout,
                });
            }
            let result = path
                .session
                .send_to(
                    target,
                    payload,
                    fallback_deadline,
                    product_deadline,
                    response_timeout,
                )
                .await;
            let observation = path.session.take_feedback_observation();
            (
                path.session.path_index,
                observation,
                result,
                path.session.connection_usable,
            )
        };
        if let Some(observation) = observation {
            self.context
                .mark_udp_path_feedback(observation_path_index, observation);
        }

        match result {
            Ok(response) => {
                if !connection_usable {
                    self.remove_path(path_index);
                    self.context.mark_udp_path_failure(path_index);
                }
                Ok(response)
            }
            Err(DatagramPathSendError::Timeout {
                path_was_acked,
                response_timeout,
            }) => Err(DatagramPathSendError::Timeout {
                path_was_acked,
                response_timeout,
            }),
            Err(err) => Err(err),
        }
    }

    async fn ensure_path_session(
        &mut self,
        path_index: usize,
        open_deadline: tokio::time::Instant,
    ) -> Result<usize, RuntimeError> {
        if let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        {
            return Ok(position);
        }
        let session =
            open_udp_datagram_session_on_path(&self.context, path_index, open_deadline).await?;
        self.paths.push(UdpDatagramAssociationPath {
            session,
            pacer: UdpDatagramPacer::new(),
        });
        Ok(self.paths.len() - 1)
    }

    fn remove_path(&mut self, path_index: usize) {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        else {
            return;
        };
        let path = self.paths.swap_remove(position);
        self.context.mark_udp_datagram_path_delivery(
            path.session.path_index,
            path.session.delivery_stats(),
        );
        self.context.release_udp_path_load(path.session.path_index);
    }
}

pub(in crate::runtime) fn udp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    if runtime_error_is_datagram_response_timeout(err) {
        return false;
    }
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::Protocol(_)
    )
}

pub(in crate::runtime) fn udp_datagram_path_open_timeout(
    association_has_open_path: bool,
    has_unattempted_alternative: bool,
    model: UdpPathRuntimeModel,
    ttl_ms: u32,
) -> Duration {
    let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
    if !association_has_open_path && !has_unattempted_alternative {
        return UDP_PATH_HANDSHAKE_TIMEOUT.min(ttl_timeout);
    }
    let response_timeout = if association_has_open_path {
        model.response_timeout
    } else {
        model.response_timeout
    };
    response_timeout
        .max(QUIC_TIMER_GRANULARITY)
        .min(UDP_PATH_HANDSHAKE_TIMEOUT)
        .min(ttl_timeout)
}

pub(in crate::runtime) fn udp_datagram_first_response_timeout(
    path_session_was_open: bool,
    association_had_open_path: bool,
    has_unattempted_alternative: bool,
    model: UdpPathRuntimeModel,
    ttl_ms: u32,
) -> Duration {
    if path_session_was_open || association_had_open_path {
        return model.response_timeout;
    }
    udp_datagram_path_open_timeout(false, has_unattempted_alternative, model, ttl_ms)
        .max(model.response_timeout)
}
