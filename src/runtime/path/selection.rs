//! Path snapshots, ranking, and atomic selection.
//!
//! Selection consumes immutable evidence and keeps snapshot-rank-reserve
//! operations under one health lock when admission must be race-free.

use super::set::ClientPathContext;
use super::*;
use crate::model::admission::{
    BulkPathCandidate, bulk_service_horizon_payload_bytes, bulk_striping_admitted_subflows,
};
use crate::model::capacity::relay_lane_startup_chunk_bytes;

impl ClientPathContext {
    pub(in crate::runtime) fn relay_path_allows_automatic_bulk_use(
        &self,
        key: RelayPathKey,
    ) -> bool {
        match key.underlay {
            UnderlayProtocol::Tcp => self.tcp_paths.get(key.index),
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
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations = self.tcp_health_observations_for_lane(lane);
        if endpoint_only_reliable_startup_should_preserve_configured_order(
            &self.tcp_paths,
            &observations,
            lane,
        ) {
            return configured_order_path_indices(
                &self.tcp_paths,
                &observations,
                lane,
                payload_bytes,
            );
        }
        ordered_reliable_path_indices(&self.tcp_paths, &observations, lane, payload_bytes)
    }

    pub(in crate::runtime) fn try_reserve_relay_path_load_if_unchanged(
        &self,
        key: RelayPathKey,
        lane: FlowLane,
        expected_active_flows: u32,
        expected_active_latency_sensitive_flows: u32,
    ) -> Option<RelayPathLoadLease> {
        let mut health = self.state.health().lock().expect("client path health lock");
        let records = match key.underlay {
            UnderlayProtocol::Tcp => &mut health.tcp,
            UnderlayProtocol::Udp => &mut health.udp,
        };
        let Some(current) = records.get_mut(key.index) else {
            return None;
        };
        if current.manual_disabled
            || current.state != SchedulerPathState::Active
            || current.active_flows != expected_active_flows
            || current.active_latency_sensitive_flows != expected_active_latency_sensitive_flows
        {
            return None;
        }
        current.reserve_load(lane);
        drop(health);
        Some(RelayPathLoadLease::new(self.state.clone(), key, lane))
    }

    pub(in crate::runtime) fn reserve_reliable_stream_path(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
        excluded: &[RelayPathKey],
    ) -> Option<RelayPathKey> {
        let mut health = self.state.health().lock().expect("client path health lock");
        let mut tcp_observations = health_observations(&mut health.tcp);
        apply_bulk_latency_isolation(&mut tcp_observations, lane, self.mux_limits);
        let mut udp_observations = health_observations(&mut health.udp);
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
        if reliable_reservation_should_use_endpoint_only_startup_order(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
            lane,
        ) {
            candidates.sort_by(|left, right| {
                left.snapshot
                    .active_latency_sensitive_flows
                    .cmp(&right.snapshot.active_latency_sensitive_flows)
                    .then_with(|| left.snapshot.active_flows.cmp(&right.snapshot.active_flows))
                    .then_with(|| self.relay_path_key_order(left.key, right.key))
            });
        } else {
            candidates.sort_by(|left, right| {
                left.eta_ms
                    .total_cmp(&right.eta_ms)
                    .then_with(|| self.relay_path_key_order(left.key, right.key))
            });
        }
        #[cfg(feature = "lab-diagnostics")]
        for (rank, candidate) in candidates.iter().enumerate() {
            lab_diagnostic(
                "reliable_stream_initial_path_candidate",
                format_args!(
                    "lane={:?} payload_bytes={} rank={} path_underlay={:?} path_index={} eta_ms={:.3} state={:?} active_flows={} active_latency_flows={} queue_bytes={} product_queue_bytes={} bytes_in_flight={} inflight_limit={} delivery_rate_bps={:.0} pacing_rate_bps={:.0} app_limited={} liveness_evidence={} path_proof_evidence={} ack_data_evidence={} bulk_rate_evidence={} sender_delivery_evidence={}",
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
                    candidate.snapshot.product_queue_bytes,
                    candidate.snapshot.bytes_in_flight,
                    candidate.snapshot.inflight_limit_bytes,
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
        match selected.underlay {
            UnderlayProtocol::Tcp => health.tcp.get_mut(selected.index)?.reserve_load(lane),
            UnderlayProtocol::Udp => health.udp.get_mut(selected.index)?.reserve_load(lane),
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
        Some(selected)
    }

    pub(in crate::runtime) fn ordered_reliable_path_keys(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut health = self.state.health().lock().expect("client path health lock");
        let mut tcp_observations = health_observations(&mut health.tcp);
        apply_bulk_latency_isolation(&mut tcp_observations, lane, self.mux_limits);
        let mut udp_observations = health_observations(&mut health.udp);
        apply_bulk_latency_isolation(&mut udp_observations, lane, self.mux_limits);
        let mut candidates = reliable_stream_path_candidates(
            &self.tcp_paths,
            &tcp_observations,
            &self.udp_paths,
            &udp_observations,
            lane,
            payload_bytes,
        );
        candidates.sort_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| self.relay_path_key_order(left.key, right.key))
        });
        candidates
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
    }

    pub(in crate::runtime) fn ordered_tcp_repair_path_indices(
        &self,
        current_path_index: Option<usize>,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations = self.tcp_health_observations_for_lane(lane);
        let scores = ordered_path_scores(&self.tcp_paths, &observations, lane, payload_bytes);
        if !matches!(lane, FlowLane::Throughput | FlowLane::Background) {
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

    pub(in crate::runtime) fn ordered_udp_stream_repair_path_indices(
        &self,
        current_path_index: Option<usize>,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let mut observations = health_observations(
            &mut self
                .state
                .health()
                .lock()
                .expect("client path health lock")
                .udp,
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
                !matches!(lane, FlowLane::Throughput | FlowLane::Background)
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

    pub(in crate::runtime) fn ordered_reliable_repair_path_keys(
        &self,
        current_tcp_path_index: Option<usize>,
        current_udp_path_index: Option<usize>,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut candidates = self
            .ordered_tcp_repair_path_indices(current_tcp_path_index, lane, payload_bytes)
            .into_iter()
            .map(|index| RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index,
            })
            .chain(
                self.ordered_udp_stream_repair_path_indices(
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
            let left_eta = self
                .reliable_relay_path_eta_ms(*left, lane, payload_bytes)
                .unwrap_or(f64::INFINITY);
            let right_eta = self
                .reliable_relay_path_eta_ms(*right, lane, payload_bytes)
                .unwrap_or(f64::INFINITY);
            left_eta
                .total_cmp(&right_eta)
                .then_with(|| self.relay_path_key_order(*left, *right))
        });
        candidates
    }

    pub(in crate::runtime) fn ordered_reliable_bulk_striping_path_keys(
        &self,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut candidates = self.ordered_reliable_bulk_path_candidates(payload_bytes);
        let has_bulk_rate_evidence = candidates
            .iter()
            .any(|candidate| candidate.has_bulk_rate_evidence);
        let has_active_bulk_work = candidates.iter().any(bulk_candidate_has_active_bulk_work);
        if has_bulk_rate_evidence {
            candidates.retain(|candidate| candidate.has_bulk_rate_evidence);
        } else if has_active_bulk_work {
            candidates.retain(bulk_candidate_has_active_bulk_work);
        } else if !bulk_candidates_span_underlays(&candidates)
            && candidates
                .iter()
                .any(|candidate| candidate.snapshot.active_flows > 0)
        {
            candidates.retain(|candidate| candidate.snapshot.active_flows > 0);
        }
        candidates.sort_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| self.relay_path_key_order(left.key, right.key))
        });
        if !has_bulk_rate_evidence && !has_active_bulk_work {
            candidates.truncate(1);
        }
        bulk_striping_admitted_subflows(candidates, payload_bytes, self.mux_limits)
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
    }

    pub(in crate::runtime) fn ordered_reliable_bulk_validation_path_keys(
        &self,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let payload_bytes = payload_bytes
            .min(relay_lane_startup_chunk_bytes(
                FlowLane::Latency,
                self.mux_limits,
            ))
            .max(PATH_OPEN_SCORE_BYTES);
        let mut candidates = self.ordered_reliable_bulk_path_candidates(payload_bytes);
        candidates.sort_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| self.relay_path_key_order(left.key, right.key))
        });
        let admitted = candidates
            .into_iter()
            .filter(|candidate| {
                !candidate.has_bulk_rate_evidence && candidate.snapshot.active_flows == 0
            })
            .collect::<Vec<_>>();
        carrier_diverse_bulk_validation_order(admitted)
            .into_iter()
            .map(|candidate| candidate.key)
            .collect()
    }

    fn ordered_reliable_bulk_path_candidates(
        &self,
        payload_bytes: usize,
    ) -> Vec<BulkPathCandidate> {
        let mut health = self.state.health().lock().expect("client path health lock");
        let tcp_observations = health_observations(&mut health.tcp);
        let udp_observations = health_observations(&mut health.udp);
        let scoring_payload_bytes =
            bulk_service_horizon_payload_bytes(payload_bytes, self.mux_limits);
        ordered_path_scores(
            &self.tcp_paths,
            &tcp_observations,
            FlowLane::Throughput,
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
                &udp_observations,
                FlowLane::Throughput,
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
                    eta_ms + udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes),
                    path,
                    observation,
                    snapshot,
                ))
            }),
        )
        .collect::<Vec<_>>()
    }

    pub(in crate::runtime) fn tcp_health_observations_for_lane(
        &self,
        lane: FlowLane,
    ) -> Vec<ClientPathObservation> {
        let mut observations = health_observations(
            &mut self
                .state
                .health()
                .lock()
                .expect("client path health lock")
                .tcp,
        );
        apply_bulk_latency_isolation(&mut observations, lane, self.mux_limits);
        observations
    }

    pub(in crate::runtime) fn tcp_path_snapshot(&self, index: usize) -> Option<PathSnapshot> {
        let path = self.tcp_paths.get(index)?;
        let observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)?
            .observe(Instant::now());
        Some(path_snapshot(path, index, observation))
    }

    pub(in crate::runtime) fn udp_path_snapshot(&self, index: usize) -> Option<PathSnapshot> {
        let path = self.udp_paths.get(index)?;
        let observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
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

    pub(in crate::runtime) fn reliable_path_rtt_is_observed(&self, key: RelayPathKey) -> bool {
        let health = self.state.health().lock().expect("client path health lock");
        let record = match key.underlay {
            UnderlayProtocol::Tcp => health.tcp.get(key.index),
            UnderlayProtocol::Udp => health.udp.get(key.index),
        };
        record.is_some_and(|record| {
            record.carrier_srtt_ms.is_some()
                || record.carrier_rttvar_ms.is_some()
                || record.measured_srtt_ms.is_some()
                || record.measured_jitter_ms.is_some()
        })
    }

    pub(in crate::runtime) fn reliable_relay_path_eta_ms(
        &self,
        key: RelayPathKey,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<f64> {
        self.reliable_path_snapshot(key).and_then(|snapshot| {
            scheduler::score_path(snapshot, lane, payload_bytes, SchedulerPolicy::default())
                .map(|score| score.eta_ms)
        })
    }

    pub(in crate::runtime) fn relay_path_metrics(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<PathMetrics> {
        let (path, observation) = match underlay {
            UnderlayProtocol::Tcp => {
                let path = self.tcp_paths.get(index)?;
                let observation = self
                    .state
                    .health()
                    .lock()
                    .expect("client path health lock")
                    .tcp
                    .get_mut(index)?
                    .observe(Instant::now());
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
                    .get_mut(index)?
                    .observe(Instant::now());
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
        let observations = health_observations(
            &mut self
                .state
                .health()
                .lock()
                .expect("client path health lock")
                .udp,
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
                FlowLane::RealtimeDatagram,
                payload_bytes,
            )
            .into_iter()
            .find_map(|path_index| {
                let path = self.udp_paths.get(path_index)?;
                let observation = observations.get(path_index).copied()?;
                let eta_ms = scheduler::score_path(
                    path_snapshot(path, path_index, observation),
                    FlowLane::RealtimeDatagram,
                    payload_bytes,
                    SchedulerPolicy::default(),
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
            FlowLane::RealtimeDatagram,
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
        let mut observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
        if discount_open_udp_session {
            observation.active_flows = observation.active_flows.saturating_sub(1);
        }
        let score = scheduler::score_path(
            path_snapshot(path, index, observation),
            FlowLane::RealtimeDatagram,
            payload_bytes,
            SchedulerPolicy::default(),
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
        let observation = self
            .state
            .health()
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
        let snapshot = path_snapshot(path, index, observation);
        scheduler::score_path(
            snapshot,
            FlowLane::RealtimeDatagram,
            1,
            SchedulerPolicy::default(),
        )?;
        Some(UdpPathRuntimeModel::from_snapshot(
            snapshot,
            ttl_ms,
            udp_mtu_payload_bytes(path, observation, self.mux_limits.max_payload_bytes),
            observation.measured_mtu_payload_bytes.is_some(),
            udp_probe_ceiling_payload_bytes(self.mux_limits.max_payload_bytes),
        ))
    }

    pub(in crate::runtime) fn relay_path_has_bulk_model_evidence(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> bool {
        let mut health = self.state.health().lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => {
                let Some(path) = self.tcp_paths.get(index) else {
                    return false;
                };
                health
                    .tcp
                    .get_mut(index)
                    .map(|record| {
                        bulk_candidate_has_bulk_rate_evidence(path, record.observe(Instant::now()))
                    })
                    .unwrap_or(false)
            }
            UnderlayProtocol::Udp => {
                let Some(path) = self.udp_paths.get(index) else {
                    return false;
                };
                health
                    .udp
                    .get_mut(index)
                    .map(|record| {
                        bulk_candidate_has_bulk_rate_evidence(path, record.observe(Instant::now()))
                    })
                    .unwrap_or(false)
            }
        }
    }

    pub(in crate::runtime) fn relay_path_has_native_bulk_model_evidence_since(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        valid_after: Instant,
    ) -> bool {
        if underlay != UnderlayProtocol::Udp {
            return false;
        }
        let now = Instant::now();
        let mut health = self.state.health().lock().expect("client path health lock");
        health
            .udp
            .get_mut(index)
            .map(|record| {
                let observation = record.observe(now);
                bulk_candidate_has_fresh_native_carrier_rate_evidence(observation, valid_after, now)
            })
            .unwrap_or(false)
    }

    pub(in crate::runtime) fn relay_path_has_fresh_proof(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        proof_id: u64,
        attached_at: Instant,
    ) -> bool {
        self.relay_path_fresh_proof_acked_at(underlay, index, proof_id, attached_at)
            .is_some()
    }

    pub(in crate::runtime) fn relay_path_proof_generation(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
    ) -> Option<u64> {
        let health = self.state.health().lock().expect("client path health lock");
        match underlay {
            UnderlayProtocol::Tcp => health.tcp.get(index),
            UnderlayProtocol::Udp => health.udp.get(index),
        }
        .map(ClientPathHealthRecord::path_proof_generation)
    }

    pub(in crate::runtime) fn relay_path_fresh_proof_acked_at(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        proof_id: u64,
        attached_at: Instant,
    ) -> Option<Instant> {
        let mut health = self.state.health().lock().expect("client path health lock");
        let record = match underlay {
            UnderlayProtocol::Tcp => health.tcp.get_mut(index),
            UnderlayProtocol::Udp => health.udp.get_mut(index),
        };
        record.and_then(|record| {
            let observation = record.observe(Instant::now());
            (observation.state == SchedulerPathState::Active && !observation.manual_disabled)
                .then(|| record.successful_path_proof_acked_at(proof_id, attached_at))
                .flatten()
        })
    }

    pub(in crate::runtime) fn reliable_relay_has_latency_pressure(&self) -> bool {
        let mut health = self.state.health().lock().expect("client path health lock");
        let tcp_pressure = health.tcp.iter_mut().any(|record| {
            record
                .observe(Instant::now())
                .active_latency_sensitive_flows
                > 0
        });
        tcp_pressure
            || health.udp.iter_mut().any(|record| {
                record
                    .observe(Instant::now())
                    .active_latency_sensitive_flows
                    > 0
            })
    }
}
