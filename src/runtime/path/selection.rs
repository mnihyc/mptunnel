//! Path snapshots, ranking, and atomic selection.
//!
//! Selection consumes immutable evidence and keeps snapshot-rank-reserve
//! operations under one health lock when admission must be race-free.

use super::set::ClientPathContext;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::admission::{
    BulkPathCandidate, bulk_scheduling_horizon_bytes, bulk_striping_admitted_candidates,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, ReliableOriginalDataOutput, ReliableStreamSourceAdmission,
    relay_lane_startup_chunk_bytes, reliable_stream_source_admission,
};
use crate::model::path::{RelayPathInstance, RelayPathKey, RelayPathProofEpoch};
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::health::ClientPathHealthRecord;
use crate::runtime::path::model::{
    ClientPathObservation, UdpPathCandidate, UdpPathRuntimeModel, apply_bulk_latency_isolation,
    bulk_candidate_has_bulk_rate_evidence, bulk_candidate_has_fresh_native_carrier_rate_evidence,
    bulk_path_candidate, configured_order_path_indices, configured_order_path_scores,
    endpoint_only_reliable_startup_should_preserve_configured_order, health_observations,
    ordered_path_scores, ordered_path_scores_for_ttl, path_allows_automatic_bulk_use,
    path_can_be_auto_discovered, path_is_endpoint_only, path_metrics_from_snapshot, path_snapshot,
    path_startup_snapshot, reliable_reservation_should_use_endpoint_only_startup_order,
    reliable_stream_path_candidates, reliable_stream_startup_path_scores,
    udp_datagram_payload_limit_bytes, udp_observation_has_datagram_feedback,
    udp_path_has_realtime_model, udp_reliable_stream_loss_reinjection_penalty_ms,
};
use crate::runtime::path::state::RelayPathLoadLease;
use crate::scheduler::{self, PathSnapshot, PathState as SchedulerPathState, TrafficClass};
use crate::transport::RateHint;
use smallvec::SmallVec;
use std::time::Instant;

/// One coherent path-owner sample used by request scheduling.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReliableRequestPathEvidence {
    pub(in crate::runtime) key: RelayPathKey,
    pub(in crate::runtime) shared_snapshot: Option<PathSnapshot>,
    pub(in crate::runtime) tcp: Option<ReliableRequestTcpPathEvidence>,
    pub(in crate::runtime) has_bulk_model_evidence: bool,
    pub(in crate::runtime) has_fresh_native_carrier_rate_evidence: bool,
    pub(in crate::runtime) fresh_proof: Option<RelayPathProofEpoch>,
    pub(in crate::runtime) config_ordinal: usize,
    pub(in crate::runtime) member_ordinal: u16,
}

/// TCP-only priors stay typed so a QUIC observation cannot carry TCP state.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReliableRequestTcpPathEvidence {
    pub(in crate::runtime) startup_snapshot: PathSnapshot,
    pub(in crate::runtime) rate_hint_unknown: bool,
}

/// Coherent request-path evidence captured under one path-state lock.
#[derive(Debug)]
pub(in crate::runtime) struct ReliableRequestPathBatchObservation {
    pub(in crate::runtime) paths: SmallVec<[ReliableRequestPathEvidence; 4]>,
    pub(in crate::runtime) bulk_candidates: SmallVec<[BulkPathCandidate; 4]>,
    pub(in crate::runtime) latency_pressure: bool,
}

impl ClientPathContext {
    fn sort_reliable_path_candidates(
        &self,
        candidates: &mut [BulkPathCandidate],
        lane: TrafficClass,
        payload_bytes: usize,
        evidence_free_startup: bool,
    ) {
        if evidence_free_startup && lane.is_latency_sensitive() {
            for candidate in candidates
                .iter_mut()
                .filter(|candidate| candidate.snapshot.peer_usage.is_some())
            {
                if let Some(score) = scheduler::score_path(candidate.snapshot, lane, payload_bytes)
                {
                    // Authenticated readiness qualifies exact-instance timing
                    // for latency ranking, but never promotes capacity.
                    candidate.eta_ms = score.eta_ms;
                }
            }
        }
        candidates.sort_by(|left, right| {
            let common = scheduler::path_is_backup(left.snapshot)
                .cmp(&scheduler::path_is_backup(right.snapshot));
            if evidence_free_startup && lane.is_latency_sensitive() {
                let left_authenticated = left.snapshot.peer_usage.is_some();
                let right_authenticated = right.snapshot.peer_usage.is_some();
                common
                    // A latency-sensitive application flow is not a carrier probe:
                    // prefer authenticated service over an establishing path.
                    .then_with(|| right_authenticated.cmp(&left_authenticated))
                    .then_with(|| {
                        if left_authenticated && right_authenticated {
                            left.eta_ms.total_cmp(&right.eta_ms)
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    })
                    .then_with(|| {
                        left.snapshot
                            .active_latency_sensitive_flows
                            .cmp(&right.snapshot.active_latency_sensitive_flows)
                    })
                    .then_with(|| left.snapshot.active_flows.cmp(&right.snapshot.active_flows))
                    .then_with(|| self.relay_path_key_order(left.key, right.key))
            } else if evidence_free_startup {
                common
                    .then_with(|| {
                        left.snapshot
                            .active_latency_sensitive_flows
                            .cmp(&right.snapshot.active_latency_sensitive_flows)
                    })
                    .then_with(|| left.snapshot.active_flows.cmp(&right.snapshot.active_flows))
                    .then_with(|| self.relay_path_key_order(left.key, right.key))
            } else {
                common
                    .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                    .then_with(|| self.relay_path_key_order(left.key, right.key))
            }
        });
    }

    pub(in crate::runtime) fn relay_path_allows_automatic_bulk_use(
        &self,
        key: RelayPathKey,
    ) -> bool {
        match key.underlay {
            UnderlayProtocol::Tcp => self.tcp_path_spec(key.index),
            UnderlayProtocol::Udp => self.udp_paths.get(key.index),
        }
        .is_some_and(path_allows_automatic_bulk_use)
    }

    pub(in crate::runtime) fn automatic_bulk_path_count(
        &self,
        underlay: UnderlayProtocol,
        service_index: Option<usize>,
    ) -> usize {
        let paths = match underlay {
            UnderlayProtocol::Tcp => &self.tcp_paths,
            UnderlayProtocol::Udp => &self.udp_paths,
        };
        paths
            .iter()
            .enumerate()
            .filter(|(index, path)| {
                Some(*index) != service_index && path_allows_automatic_bulk_use(path)
            })
            .count()
    }

    pub(in crate::runtime) fn ordered_tcp_path_indices(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations = self.tcp_health_observations_for_lane(lane);
        if endpoint_only_reliable_startup_should_preserve_configured_order(
            &self.tcp_paths,
            &observations,
            lane,
        ) {
            let mut candidates =
                configured_order_path_indices(&self.tcp_paths, &observations, lane, payload_bytes);
            candidates.sort_by(|left, right| {
                self.relay_path_key_order(
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index: *left,
                    },
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index: *right,
                    },
                )
            });
            return candidates;
        }
        let mut scores = reliable_stream_startup_path_scores(
            &self.tcp_paths,
            &observations,
            lane,
            payload_bytes,
        );
        scores.sort_by(|left, right| {
            let left_snapshot = path_snapshot(
                &self.tcp_paths[left.0],
                left.0,
                observations.get(left.0).copied().unwrap_or_default(),
            );
            let right_snapshot = path_snapshot(
                &self.tcp_paths[right.0],
                right.0,
                observations.get(right.0).copied().unwrap_or_default(),
            );
            scheduler::path_is_backup(left_snapshot)
                .cmp(&scheduler::path_is_backup(right_snapshot))
                .then_with(|| left.1.total_cmp(&right.1))
                .then_with(|| {
                    self.relay_path_key_order(
                        RelayPathKey {
                            underlay: UnderlayProtocol::Tcp,
                            index: left.0,
                        },
                        RelayPathKey {
                            underlay: UnderlayProtocol::Tcp,
                            index: right.0,
                        },
                    )
                })
        });
        scores.into_iter().map(|(index, _)| index).collect()
    }

    pub(in crate::runtime) fn try_reserve_relay_path_load_if_unchanged(
        &self,
        key: RelayPathKey,
        lane: TrafficClass,
        expected_active_flows: u32,
        expected_active_latency_sensitive_flows: u32,
    ) -> Option<RelayPathLoadLease> {
        let now = Instant::now();
        let mut health = self.state.health().lock().expect("client path health lock");
        let current = health.path_record_mut(key)?;
        // Topology and carrier credit are revalidated by the sender. This
        // conditional claim fences only the shared load that influenced the
        // score; a still-attached Suspect path remains usable as before.
        if current.active_flows != expected_active_flows
            || current.active_latency_sensitive_flows != expected_active_latency_sensitive_flows
        {
            return None;
        }
        if !current.reserve_load(lane, now) {
            return None;
        }
        drop(health);
        Some(RelayPathLoadLease::new(
            self.state.clone(),
            key,
            lane,
            self.tcp_carrier_groups.clone(),
        ))
    }

    pub(in crate::runtime) fn reserve_reliable_stream_path(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
        excluded: &[RelayPathKey],
    ) -> Option<RelayPathLoadLease> {
        let now = Instant::now();
        let mut health = self.state.health().lock().expect("client path health lock");
        let mut tcp_observations = health_observations(&health.tcp, now);
        apply_bulk_latency_isolation(&mut tcp_observations, lane, self.mux_limits);
        let mut udp_observations = health_observations(&health.udp, now);
        apply_bulk_latency_isolation(&mut udp_observations, lane, self.mux_limits);
        let mut candidates = reliable_stream_path_candidates(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
            lane,
            payload_bytes,
        );
        candidates.retain(|candidate| !excluded.contains(&candidate.key));
        let evidence_free_startup = reliable_reservation_should_use_endpoint_only_startup_order(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
        );
        self.sort_reliable_path_candidates(
            &mut candidates,
            lane,
            payload_bytes,
            evidence_free_startup,
        );
        #[cfg(feature = "lab-diagnostics")]
        for (rank, candidate) in candidates.iter().enumerate() {
            lab_diagnostic(
                "reliable_stream_initial_path_candidate",
                format_args!(
                    "lane={:?} payload_bytes={} rank={} path_underlay={:?} path_index={} eta_ms={:.3} state={:?} active_flows={} active_latency_flows={} queue_bytes={} data_level_queue_bytes={} bytes_in_flight={} inflight_limit={} delivery_rate_bps={:.0} pacing_rate_bps={:.0} app_limited={} liveness_evidence={} path_proof_evidence={} ack_data_evidence={} bulk_rate_evidence={} sender_delivery_evidence={}",
                    lane,
                    payload_bytes,
                    rank,
                    candidate.key.underlay,
                    candidate.key.index,
                    candidate.eta_ms,
                    candidate.snapshot.state,
                    candidate.snapshot.active_flows,
                    candidate.snapshot.active_latency_sensitive_flows,
                    candidate.snapshot.queue_bytes,
                    candidate.snapshot.data_level_queue_bytes,
                    candidate.snapshot.bytes_in_flight,
                    candidate.snapshot.carrier_inflight_limit_bytes,
                    candidate.snapshot.delivery_rate_bps,
                    candidate.snapshot.pacing_rate_bps,
                    candidate.snapshot.app_limited,
                    candidate.has_liveness_evidence,
                    candidate.has_path_proof_evidence,
                    candidate.has_ack_data_evidence,
                    candidate.has_bulk_rate_evidence,
                    candidate.has_sender_delivery_evidence,
                ),
            );
        }
        let selected = candidates.first()?.key;
        let reserved = match selected.underlay {
            UnderlayProtocol::Tcp => health.tcp.get_mut(selected.index)?.reserve_load(lane, now),
            UnderlayProtocol::Udp => health.udp.get_mut(selected.index)?.reserve_load(lane, now),
        };
        if !reserved {
            return None;
        }
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "reliable_stream_initial_path_selected",
            format_args!(
                "lane={:?} payload_bytes={} path_underlay={:?} path_index={} candidate_count={}",
                lane,
                payload_bytes,
                selected.underlay,
                selected.index,
                candidates.len(),
            ),
        );
        Some(RelayPathLoadLease::new(
            self.state.clone(),
            selected,
            lane,
            self.tcp_carrier_groups.clone(),
        ))
    }

    pub(in crate::runtime) fn ordered_reliable_path_keys(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let now = Instant::now();
        let health = self.state.health().lock().expect("client path health lock");
        let mut tcp_observations = health_observations(&health.tcp, now);
        apply_bulk_latency_isolation(&mut tcp_observations, lane, self.mux_limits);
        let mut udp_observations = health_observations(&health.udp, now);
        apply_bulk_latency_isolation(&mut udp_observations, lane, self.mux_limits);
        let mut candidates = reliable_stream_path_candidates(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
            lane,
            payload_bytes,
        );
        self.sort_reliable_path_candidates(&mut candidates, lane, payload_bytes, false);
        candidates
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
    }

    pub(in crate::runtime) fn ordered_tcp_reinjection_path_indices(
        &self,
        current_path_index: Option<usize>,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations = self.tcp_health_observations_for_lane(lane);
        let scores = ordered_path_scores(&self.tcp_paths, &observations, lane, payload_bytes);
        if lane != TrafficClass::Throughput {
            return scores.into_iter().map(|(index, _)| index).collect();
        }
        let current_eta = current_path_index.and_then(|current_path_index| {
            scores
                .iter()
                .find_map(|(index, eta)| (*index == current_path_index).then_some(*eta))
        });
        let has_active_survivor = scores.iter().any(|(index, _)| {
            Some(*index) != current_path_index
                && observations
                    .get(*index)
                    .is_some_and(|observation| observation.state == SchedulerPathState::Active)
        });
        scores
            .into_iter()
            .filter(|(index, eta)| {
                Some(*index) != current_path_index
                    && current_eta.is_none_or(|current| *eta < current)
                    && (!has_active_survivor
                        || observations.get(*index).is_some_and(|observation| {
                            observation.state == SchedulerPathState::Active
                        }))
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub(in crate::runtime) fn ordered_udp_stream_reinjection_path_indices(
        &self,
        current_path_index: Option<usize>,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let now = Instant::now();
        let mut observations = health_observations(
            &self
                .state
                .health()
                .lock()
                .expect("client path health lock")
                .udp,
            now,
        );
        apply_bulk_latency_isolation(&mut observations, lane, self.mux_limits);
        let scores = if endpoint_only_reliable_startup_should_preserve_configured_order(
            &self.udp_paths,
            &observations,
            lane,
        ) {
            configured_order_path_scores(&self.udp_paths, &observations, lane, payload_bytes)
        } else {
            ordered_path_scores(&self.udp_paths, &observations, lane, payload_bytes)
        };
        scores
            .into_iter()
            .filter(|(index, _)| Some(*index) != current_path_index)
            .filter(|(index, _)| {
                lane != TrafficClass::Throughput
                    || !observations
                        .iter()
                        .enumerate()
                        .any(|(candidate, observation)| {
                            Some(candidate) != current_path_index
                                && observation.state == SchedulerPathState::Active
                        })
                    || observations
                        .get(*index)
                        .is_some_and(|observation| observation.state == SchedulerPathState::Active)
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub(in crate::runtime) fn ordered_reliable_reinjection_path_keys(
        &self,
        current_tcp_path_index: Option<usize>,
        current_udp_path_index: Option<usize>,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut candidates = self
            .ordered_tcp_reinjection_path_indices(current_tcp_path_index, lane, payload_bytes)
            .into_iter()
            .map(|index| RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index,
            })
            .chain(
                self.ordered_udp_stream_reinjection_path_indices(
                    current_udp_path_index,
                    lane,
                    payload_bytes,
                )
                .into_iter()
                .map(|index| RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index,
                }),
            )
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            let left_backup = self
                .reliable_path_snapshot(*left)
                .is_some_and(scheduler::path_is_backup);
            let right_backup = self
                .reliable_path_snapshot(*right)
                .is_some_and(scheduler::path_is_backup);
            let left_eta = self
                .reliable_relay_path_eta_ms(*left, lane, payload_bytes)
                .unwrap_or(f64::INFINITY);
            let right_eta = self
                .reliable_relay_path_eta_ms(*right, lane, payload_bytes)
                .unwrap_or(f64::INFINITY);
            left_backup
                .cmp(&right_backup)
                .then_with(|| left_eta.total_cmp(&right_eta))
                .then_with(|| self.relay_path_key_order(*left, *right))
        });
        candidates
    }

    pub(in crate::runtime) fn ordered_reliable_bulk_striping_path_keys(
        &self,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        bulk_striping_admitted_candidates(
            self.ordered_reliable_bulk_path_candidates(payload_bytes),
            payload_bytes,
            self.mux_limits,
            |left, right| self.relay_path_key_order(left, right),
        )
        .into_iter()
        .map(|candidate| candidate.key)
        .collect()
    }

    pub(in crate::runtime) fn ordered_reliable_unproven_bulk_path_keys(
        &self,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let payload_bytes = payload_bytes
            .min(relay_lane_startup_chunk_bytes(
                TrafficClass::Latency,
                self.mux_limits,
            ))
            .max(PATH_OPEN_SCORE_BYTES);
        let mut candidates = self.ordered_reliable_bulk_path_candidates(payload_bytes);
        if candidates
            .iter()
            .any(|candidate| !scheduler::path_is_backup(candidate.snapshot))
        {
            candidates.retain(|candidate| !scheduler::path_is_backup(candidate.snapshot));
        }
        candidates.sort_by(|left, right| {
            scheduler::path_is_backup(left.snapshot)
                .cmp(&scheduler::path_is_backup(right.snapshot))
                .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                .then_with(|| self.relay_path_key_order(left.key, right.key))
        });
        let admitted = candidates
            .into_iter()
            .filter(|candidate| {
                !candidate.has_bulk_rate_evidence && candidate.snapshot.active_flows == 0
            })
            .collect::<Vec<_>>();
        admitted
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
    }

    fn ordered_reliable_bulk_path_candidates(
        &self,
        payload_bytes: usize,
    ) -> SmallVec<[BulkPathCandidate; 4]> {
        let now = Instant::now();
        let health = self.state.health().lock().expect("client path health lock");
        let tcp_observations = health_observations(&health.tcp, now);
        let udp_observations = health_observations(&health.udp, now);
        self.reliable_bulk_path_candidates_from_observations(
            payload_bytes,
            &tcp_observations,
            &udp_observations,
        )
    }

    fn reliable_bulk_path_candidates_from_observations(
        &self,
        payload_bytes: usize,
        tcp_observations: &[ClientPathObservation],
        udp_observations: &[ClientPathObservation],
    ) -> SmallVec<[BulkPathCandidate; 4]> {
        let scoring_payload_bytes = bulk_scheduling_horizon_bytes(payload_bytes, self.mux_limits);
        ordered_path_scores(
            &self.tcp_paths,
            tcp_observations,
            TrafficClass::Throughput,
            scoring_payload_bytes,
        )
        .into_iter()
        .filter_map(|(index, eta_ms)| {
            let path = self.tcp_paths.get(index)?;
            let observation = tcp_observations.get(index).copied().unwrap_or_default();
            let snapshot = path_snapshot(path, index, observation);
            path_can_be_auto_discovered(path, observation).then_some(bulk_path_candidate(
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index,
                },
                eta_ms,
                path,
                observation,
                snapshot,
            ))
        })
        .chain(
            ordered_path_scores(
                &self.udp_paths,
                udp_observations,
                TrafficClass::Throughput,
                scoring_payload_bytes,
            )
            .into_iter()
            .filter_map(|(index, eta_ms)| {
                let path = self.udp_paths.get(index)?;
                let observation = udp_observations.get(index).copied().unwrap_or_default();
                let snapshot = path_snapshot(path, index, observation);
                path_can_be_auto_discovered(path, observation).then_some(bulk_path_candidate(
                    RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index,
                    },
                    eta_ms
                        + udp_reliable_stream_loss_reinjection_penalty_ms(snapshot, payload_bytes),
                    path,
                    observation,
                    snapshot,
                ))
            }),
        )
        .collect::<SmallVec<[BulkPathCandidate; 4]>>()
    }

    /// Captures carrier health for one request decision under one state lock.
    pub(in crate::runtime) fn observe_reliable_request_paths<I>(
        &self,
        attached_paths: I,
        payload_bytes: usize,
        include_bulk_admission: bool,
    ) -> ReliableRequestPathBatchObservation
    where
        I: IntoIterator<Item = (RelayPathKey, Option<RelayPathProofEpoch>)>,
    {
        let now = Instant::now();
        let mut health = self.state.health().lock().expect("client path health lock");
        if include_bulk_admission {
            for record in health.tcp_records_mut() {
                record.maintain(now);
            }
            for record in &mut health.udp {
                record.maintain(now);
            }
        }
        // Full configured-path vectors are needed only for multipath admission.
        // Ordinary and single-path sends sample attached records directly.
        let bulk_observations = include_bulk_admission.then(|| {
            (
                health_observations(&health.tcp, now),
                health_observations(&health.udp, now),
            )
        });
        let bulk_candidates = bulk_observations
            .as_ref()
            .map(|(tcp, udp)| {
                self.reliable_bulk_path_candidates_from_observations(payload_bytes, tcp, udp)
            })
            .unwrap_or_default();
        let latency_pressure = bulk_observations.as_ref().is_some_and(|(_, _)| {
            health
                .tcp_records()
                .chain(&health.udp)
                .any(|observation| observation.active_latency_sensitive_flows > 0)
        });
        let paths = attached_paths
            .into_iter()
            .map(|(key, proof)| {
                let observed = match key.underlay {
                    UnderlayProtocol::Tcp => self
                        .tcp_path_spec(key.index)
                        .zip(health.tcp_record(key.index))
                        .map(|(path, record)| {
                            let observation = bulk_observations
                                .as_ref()
                                .and_then(|(tcp, _)| tcp.get(key.index))
                                .copied()
                                .unwrap_or_else(|| record.observation_at(now));
                            (path, observation, record)
                        }),
                    UnderlayProtocol::Udp => self
                        .udp_paths
                        .get(key.index)
                        .zip(health.udp.get(key.index))
                        .map(|(path, record)| {
                            let observation = bulk_observations
                                .as_ref()
                                .and_then(|(_, udp)| udp.get(key.index))
                                .copied()
                                .unwrap_or_else(|| record.observation_at(now));
                            (path, observation, record)
                        }),
                };
                let Some((path, observation, record)) = observed else {
                    return ReliableRequestPathEvidence {
                        key,
                        shared_snapshot: None,
                        tcp: None,
                        has_bulk_model_evidence: false,
                        has_fresh_native_carrier_rate_evidence: false,
                        fresh_proof: None,
                        config_ordinal: self.relay_path_config_ordinal(key),
                        member_ordinal: self.relay_path_member_ordinal(key),
                    };
                };
                let fresh_proof = proof.filter(|proof| {
                    include_bulk_admission
                        && observation.state == SchedulerPathState::Active
                        && !observation.manual_disabled
                        && record.path_proof_generation() == proof.proof_generation
                        && record
                            .successful_path_proof_acked_at(proof.proof_id, proof.attached_at, now)
                            .is_some()
                });
                ReliableRequestPathEvidence {
                    key,
                    shared_snapshot: Some(path_snapshot(path, key.index, observation)),
                    tcp: (key.underlay == UnderlayProtocol::Tcp).then(|| {
                        ReliableRequestTcpPathEvidence {
                            startup_snapshot: path_startup_snapshot(
                                path,
                                observation.wire_path_id.unwrap_or(PathId(key.index as u16)),
                            ),
                            rate_hint_unknown: path.metadata.initial_rate == RateHint::Unknown,
                        }
                    }),
                    has_bulk_model_evidence: include_bulk_admission
                        && bulk_candidate_has_bulk_rate_evidence(path, observation),
                    has_fresh_native_carrier_rate_evidence: include_bulk_admission
                        && fresh_proof.is_some()
                        && proof.is_some_and(|proof| {
                            bulk_candidate_has_fresh_native_carrier_rate_evidence(
                                path,
                                observation,
                                proof.attached_at,
                                now,
                            )
                        }),
                    fresh_proof,
                    config_ordinal: self.relay_path_config_ordinal(key),
                    member_ordinal: self.relay_path_member_ordinal(key),
                }
            })
            .collect();
        ReliableRequestPathBatchObservation {
            paths,
            bulk_candidates,
            latency_pressure,
        }
    }

    pub(in crate::runtime) fn tcp_health_observations_for_lane(
        &self,
        lane: TrafficClass,
    ) -> Vec<ClientPathObservation> {
        let now = Instant::now();
        let mut observations = health_observations(
            &self
                .state
                .health()
                .lock()
                .expect("client path health lock")
                .tcp,
            now,
        );
        apply_bulk_latency_isolation(&mut observations, lane, self.mux_limits);
        observations
    }

    pub(in crate::runtime) fn tcp_path_snapshot(&self, index: usize) -> Option<PathSnapshot> {
        let now = Instant::now();
        let path = self.tcp_path_spec(index)?;
        let observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .tcp_record(index)?
            .observation_at(now);
        Some(path_snapshot(path, index, observation))
    }

    pub(in crate::runtime) fn udp_path_snapshot(&self, index: usize) -> Option<PathSnapshot> {
        let now = Instant::now();
        let path = self.udp_paths.get(index)?;
        let observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get(index)?
            .observation_at(now);
        Some(path_snapshot(path, index, observation))
    }

    pub(in crate::runtime) fn reliable_path_snapshot(
        &self,
        key: RelayPathKey,
    ) -> Option<PathSnapshot> {
        match key.underlay {
            UnderlayProtocol::Tcp => self.tcp_path_snapshot(key.index),
            UnderlayProtocol::Udp => self.udp_path_snapshot(key.index),
        }
    }

    pub(in crate::runtime) fn reliable_path_snapshot_for_instance(
        &self,
        instance: RelayPathInstance,
    ) -> Option<PathSnapshot> {
        let path = match instance.key.underlay {
            UnderlayProtocol::Tcp => self.tcp_path_spec(instance.key.index),
            UnderlayProtocol::Udp => self.udp_paths.get(instance.key.index),
        }?;
        let observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .path_record(instance.key)?
            .observation_for_instance_at(instance.path_instance_id, Instant::now())?;
        Some(path_snapshot(path, instance.key.index, observation))
    }

    /// Projects exact attached carrier instances under one path-health lock.
    ///
    /// A newly authenticated instance can become available before its first
    /// health publication. That is an unmeasured output, not an absent one:
    /// use only the configured startup prior until exact-instance evidence is
    /// published. Evidence from a replacement occupying the same path key is
    /// never inherited by the older attachment.
    pub(in crate::runtime) fn reliable_stream_source_admission(
        &self,
        instances: impl IntoIterator<Item = (RelayPathInstance, bool)>,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> ReliableStreamSourceAdmission {
        let now = Instant::now();
        let health = self.state.health().lock().expect("client path health lock");
        reliable_stream_source_admission(
            instances.into_iter().filter_map(|(instance, stale)| {
                let path = match instance.key.underlay {
                    UnderlayProtocol::Tcp => self.tcp_path_spec(instance.key.index),
                    UnderlayProtocol::Udp => self.udp_paths.get(instance.key.index),
                }?;
                let observation = health
                    .path_record(instance.key)
                    .and_then(|record| {
                        record.observation_for_instance_at(instance.path_instance_id, now)
                    })
                    .unwrap_or_default();
                Some(ReliableOriginalDataOutput {
                    snapshot: path_snapshot(path, instance.key.index, observation),
                    stale,
                })
            }),
            lane,
            payload_bytes,
            self.mux_limits,
        )
    }

    pub(in crate::runtime) fn tcp_native_drain_observed(&self, index: usize) -> bool {
        self.state
            .health()
            .lock()
            .expect("client path health lock")
            .tcp_record(index)
            .is_some_and(|record| record.native_drain_observed)
    }

    pub(in crate::runtime) fn reliable_path_rtt_is_observed(&self, key: RelayPathKey) -> bool {
        let health = self.state.health().lock().expect("client path health lock");
        health.path_record(key).is_some_and(|record| {
            record.carrier_srtt_ms.is_some()
                || record.carrier_rttvar_ms.is_some()
                || record.measured_srtt_ms.is_some()
                || record.measured_jitter_ms.is_some()
        })
    }

    pub(in crate::runtime) fn reliable_relay_path_eta_ms(
        &self,
        key: RelayPathKey,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<f64> {
        self.reliable_path_snapshot(key).and_then(|snapshot| {
            scheduler::score_path(snapshot, lane, payload_bytes).map(|score| score.eta_ms)
        })
    }

    pub(in crate::runtime) fn relay_path_metrics(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<PathMetrics> {
        let now = Instant::now();
        let (path, observation) = match underlay {
            UnderlayProtocol::Tcp => {
                let path = self.tcp_path_spec(index)?;
                let observation = self
                    .state
                    .health()
                    .lock()
                    .expect("client path health lock")
                    .tcp_record(index)?
                    .observation_at(now);
                (path, observation)
            }
            UnderlayProtocol::Udp => {
                let path = self.udp_paths.get(index)?;
                let observation = self
                    .state
                    .health()
                    .lock()
                    .expect("client path health lock")
                    .udp
                    .get(index)?
                    .observation_at(now);
                (path, observation)
            }
        };
        let snapshot = path_snapshot(path, index, observation);
        Some(path_metrics_from_snapshot(
            snapshot,
            observation,
            PathMetricDirection::ClientToServer,
        ))
    }

    pub(in crate::runtime) fn ordered_udp_path_candidates_for_ttl(
        &self,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Vec<UdpPathCandidate> {
        if ttl_ms == 0 {
            return Vec::new();
        }
        let now = Instant::now();
        let observations = health_observations(
            &self
                .state
                .health()
                .lock()
                .expect("client path health lock")
                .udp,
            now,
        );
        if self.udp_paths.iter().all(path_is_endpoint_only)
            && !observations
                .iter()
                .any(udp_observation_has_datagram_feedback)
        {
            let freshness_budget_ms = f64::from(ttl_ms);
            return configured_order_path_indices(
                &self.udp_paths,
                &observations,
                TrafficClass::RealtimeDatagram,
                payload_bytes,
            )
            .into_iter()
            .find_map(|path_index| {
                let path = self.udp_paths.get(path_index)?;
                let observation = observations.get(path_index).copied()?;
                let eta_ms = scheduler::score_path(
                    path_snapshot(path, path_index, observation),
                    TrafficClass::RealtimeDatagram,
                    payload_bytes,
                )?
                .eta_ms;
                (eta_ms <= freshness_budget_ms).then_some(UdpPathCandidate { path_index, eta_ms })
            })
            .into_iter()
            .collect();
        }
        let mut candidates = ordered_path_scores_for_ttl(
            &self.udp_paths,
            &observations,
            TrafficClass::RealtimeDatagram,
            payload_bytes,
            ttl_ms,
        )
        .into_iter()
        .map(|(path_index, eta_ms)| UdpPathCandidate { path_index, eta_ms })
        .collect::<Vec<_>>();
        if candidates
            .iter()
            .any(|candidate| self.udp_path_candidate_has_realtime_model(*candidate, &observations))
        {
            candidates.retain(|candidate| {
                self.udp_path_candidate_has_realtime_model(*candidate, &observations)
            });
        }
        candidates
    }

    pub(in crate::runtime) fn udp_path_candidate_has_realtime_model(
        &self,
        candidate: UdpPathCandidate,
        observations: &[ClientPathObservation],
    ) -> bool {
        let Some(path) = self.udp_paths.get(candidate.path_index) else {
            return false;
        };
        observations
            .get(candidate.path_index)
            .copied()
            .is_some_and(|observation| udp_path_has_realtime_model(path, observation))
    }

    pub(in crate::runtime) fn udp_path_eta_for_ttl(
        &self,
        index: usize,
        payload_bytes: usize,
        ttl_ms: u32,
        discount_open_udp_session: bool,
    ) -> Option<f64> {
        if ttl_ms == 0 {
            return None;
        }
        let path = self.udp_paths.get(index)?;
        let now = Instant::now();
        let mut observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get(index)?
            .observation_at(now);
        if discount_open_udp_session {
            observation.active_flows = observation.active_flows.saturating_sub(1);
        }
        let score = scheduler::score_path(
            path_snapshot(path, index, observation),
            TrafficClass::RealtimeDatagram,
            payload_bytes,
        )?;
        let freshness_budget_ms = f64::from(ttl_ms);
        (score.eta_ms <= freshness_budget_ms).then_some(score.eta_ms)
    }

    pub(in crate::runtime) fn udp_path_runtime_model(
        &self,
        index: usize,
        ttl_ms: u32,
    ) -> Option<UdpPathRuntimeModel> {
        if ttl_ms == 0 {
            return None;
        }
        let path = self.udp_paths.get(index)?;
        let now = Instant::now();
        let observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get(index)?
            .observation_at(now);
        let snapshot = path_snapshot(path, index, observation);
        scheduler::score_path(snapshot, TrafficClass::RealtimeDatagram, 1)?;
        Some(UdpPathRuntimeModel::from_snapshot(
            snapshot,
            ttl_ms,
            udp_datagram_payload_limit_bytes(path, self.mux_limits.max_payload_bytes),
        ))
    }

    pub(in crate::runtime) fn relay_path_has_bulk_model_evidence(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> bool {
        let now = Instant::now();
        let health = self.state.health().lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => {
                let Some(path) = self.tcp_path_spec(index) else {
                    return false;
                };
                health
                    .tcp_record(index)
                    .map(|record| {
                        bulk_candidate_has_bulk_rate_evidence(path, record.observation_at(now))
                    })
                    .unwrap_or(false)
            }
            UnderlayProtocol::Udp => {
                let Some(path) = self.udp_paths.get(index) else {
                    return false;
                };
                health
                    .udp
                    .get(index)
                    .map(|record| {
                        bulk_candidate_has_bulk_rate_evidence(path, record.observation_at(now))
                    })
                    .unwrap_or(false)
            }
        }
    }

    pub(in crate::runtime) fn relay_path_instance_has_bulk_model_evidence(
        &self,
        instance: RelayPathInstance,
    ) -> bool {
        let path = match instance.key.underlay {
            UnderlayProtocol::Tcp => self.tcp_path_spec(instance.key.index),
            UnderlayProtocol::Udp => self.udp_paths.get(instance.key.index),
        };
        let Some(path) = path else {
            return false;
        };
        let observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .path_record(instance.key)
            .and_then(|record| {
                record.observation_for_instance_at(instance.path_instance_id, Instant::now())
            });
        observation
            .map(|observation| bulk_candidate_has_bulk_rate_evidence(path, observation))
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_relay_path_instance_for_test(
        &self,
        instance: RelayPathInstance,
    ) {
        self.state.install_peer_path_usage(
            instance.key.underlay,
            instance.key.index,
            instance.path_instance_id,
            0,
            crate::protocol::PathUsage::Available,
        );
    }

    pub(in crate::runtime) fn relay_path_has_fresh_proof(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        proof_id: u64,
        attached_at: Instant,
    ) -> bool {
        self.relay_path_has_fresh_proof_as_of(
            underlay,
            index,
            proof_id,
            attached_at,
            Instant::now(),
        )
    }

    pub(in crate::runtime) fn relay_path_has_fresh_proof_as_of(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        proof_id: u64,
        attached_at: Instant,
        now: Instant,
    ) -> bool {
        self.relay_path_fresh_proof_acked_as_of(underlay, index, proof_id, attached_at, now)
            .is_some()
    }

    pub(in crate::runtime) fn relay_path_proof_generation(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<u64> {
        let health = self.state.health().lock().expect("client path health lock");
        health
            .path_record(RelayPathKey { underlay, index })
            .map(ClientPathHealthRecord::path_proof_generation)
    }

    /// Revalidates the exact proof epoch at the apply linearization point.
    pub(in crate::runtime) fn relay_path_proof_epoch_is_current(
        &self,
        key: RelayPathKey,
        proof: RelayPathProofEpoch,
    ) -> bool {
        let now = Instant::now();
        let health = self.state.health().lock().expect("client path health lock");
        health.path_record(key).is_some_and(|record| {
            let observation = record.observation_at(now);
            observation.state == SchedulerPathState::Active
                && !observation.manual_disabled
                && record.path_proof_generation() == proof.proof_generation
                && record
                    .successful_path_proof_acked_at(proof.proof_id, proof.attached_at, now)
                    .is_some()
        })
    }

    pub(in crate::runtime) fn relay_path_fresh_proof_acked_as_of(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        proof_id: u64,
        attached_at: Instant,
        now: Instant,
    ) -> Option<Instant> {
        let health = self.state.health().lock().expect("client path health lock");
        health
            .path_record(RelayPathKey { underlay, index })
            .and_then(|record| {
                let observation = record.observation_at(now);
                (observation.state == SchedulerPathState::Active && !observation.manual_disabled)
                    .then(|| record.successful_path_proof_acked_at(proof_id, attached_at, now))
                    .flatten()
            })
    }

    pub(in crate::runtime) fn reliable_relay_has_latency_pressure(&self) -> bool {
        let now = Instant::now();
        let health = self.state.health().lock().expect("client path health lock");
        let tcp_pressure = health
            .tcp_records()
            .any(|record| record.observation_at(now).active_latency_sensitive_flows > 0);
        tcp_pressure
            || health
                .udp
                .iter()
                .any(|record| record.observation_at(now).active_latency_sensitive_flows > 0)
    }
}

#[cfg(test)]
#[path = "tests_selection.rs"]
mod tests;
