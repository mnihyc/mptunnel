use super::*;

/// One carrier output attached to a response stream.
///
/// It owns carrier command access and sender-evidence fields for this stream on
/// this path. Product repair and ordering identity stay in `ResponseStreamBinding`.
#[derive(Clone)]
pub(super) struct ResponseStreamOutputEntry {
    pub(super) key: CarrierPathKey,
    pub(super) commands: TcpPathSessionCommandSender,
    pub(super) bytes_in_flight: u64,
    pub(super) product_queue_bytes: u64,
    pub(super) delivery_samples: u32,
    pub(super) last_delivery_at: Option<Instant>,
    pub(super) validation_credit_bytes: u64,
    pub(super) path_metrics: Option<ServerPathMetricsEntry>,
}

pub(super) struct ResponseStreamOutputs {
    pub(super) entries: Vec<ResponseStreamOutputEntry>,
    pub(super) next_index: usize,
}

/// Product byte range currently assigned to a carrier path.
///
/// STREAM_ACK releases this ledger entry from product flight; carrier ACKs only
/// update carrier/path evidence and must not release product repair state.
#[derive(Debug, Clone, Copy)]
pub(super) struct CarrierPathFlight {
    pub(super) key: CarrierPathKey,
    pub(super) end: u64,
    pub(super) bytes: usize,
    pub(super) stream_ack_proves_path: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CarrierPathFlightDebt {
    pub(super) key: CarrierPathKey,
    pub(super) bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CarrierPathAckedHole {
    pub(super) key: CarrierPathKey,
    pub(super) end: u64,
    pub(super) bytes: u64,
    pub(super) stream_ack_proves_path: bool,
}

#[derive(Debug, Default)]
pub(super) struct ResponseAckOrderingState {
    pub(super) contiguous_frontier: u64,
    pub(super) acked_holes: BTreeMap<u64, Vec<CarrierPathAckedHole>>,
}

pub(super) struct ResponseAckOrderingUpdate {
    pub(super) changed: bool,
    pub(super) contiguous_frontier: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) acked_hole_bytes: u64,
    pub(super) newly_contiguous: Vec<CarrierPathAckedHole>,
}

impl ResponseAckOrderingState {
    pub(super) fn apply_ack(
        &mut self,
        ranges: &[OffsetRange],
        released: &[(u64, CarrierPathFlight)],
    ) -> ResponseAckOrderingUpdate {
        let previous_frontier = self.contiguous_frontier;
        let previous_hole_bytes = self.acked_hole_bytes();
        let mut newly_contiguous = Vec::new();

        for (offset, flight) in released {
            let hole = CarrierPathAckedHole {
                key: flight.key,
                end: flight.end,
                bytes: flight.bytes as u64,
                stream_ack_proves_path: flight.stream_ack_proves_path,
            };
            if hole.end <= self.contiguous_frontier {
                newly_contiguous.push(hole);
            } else {
                self.acked_holes.entry(*offset).or_default().push(hole);
            }
        }

        self.advance_contiguous_frontier(ranges);
        let frontier = self.contiguous_frontier;
        self.acked_holes.retain(|_, holes| {
            holes.retain(|hole| {
                if hole.end <= frontier {
                    newly_contiguous.push(*hole);
                    false
                } else {
                    true
                }
            });
            !holes.is_empty()
        });
        let acked_hole_bytes = self.acked_hole_bytes();

        ResponseAckOrderingUpdate {
            changed: previous_frontier != self.contiguous_frontier
                || previous_hole_bytes != acked_hole_bytes
                || !newly_contiguous.is_empty(),
            contiguous_frontier: self.contiguous_frontier,
            acked_hole_bytes,
            newly_contiguous,
        }
    }

    fn advance_contiguous_frontier(&mut self, ranges: &[OffsetRange]) {
        let ranges = normalized_offset_ranges(ranges);
        loop {
            let mut next_frontier = self.contiguous_frontier;
            for range in &ranges {
                if range.start > next_frontier {
                    break;
                }
                if range.end > next_frontier {
                    next_frontier = range.end;
                }
            }
            for (offset, holes) in self.acked_holes.range(..=next_frontier) {
                if *offset > next_frontier {
                    break;
                }
                for hole in holes {
                    if hole.end > next_frontier {
                        next_frontier = hole.end;
                    }
                }
            }
            if next_frontier == self.contiguous_frontier {
                break;
            }
            self.contiguous_frontier = next_frontier;
        }
    }

    pub(super) fn acked_hole_bytes(&self) -> u64 {
        self.acked_holes
            .values()
            .flat_map(|holes| holes.iter())
            .map(|hole| hole.bytes)
            .sum()
    }
}

fn server_stream_ordering_debt_bytes(
    lower_flights: &[CarrierPathFlightDebt],
    candidate: CarrierPathKey,
) -> u64 {
    lower_flights
        .iter()
        .filter_map(|flight| (flight.key != candidate).then_some(flight.bytes))
        .sum()
}

fn server_total_lower_flight_debt_bytes(lower_flights: &[CarrierPathFlightDebt]) -> u64 {
    lower_flights.iter().map(|flight| flight.bytes).sum()
}

fn server_admission_ordering_debt_bytes(
    lower_flights: &[CarrierPathFlightDebt],
    candidate: CarrierPathKey,
    role: BulkAdmissionRole,
) -> u64 {
    if role == BulkAdmissionRole::ActiveDataPath {
        server_total_lower_flight_debt_bytes(lower_flights)
    } else {
        server_stream_ordering_debt_bytes(lower_flights, candidate)
    }
}

fn server_oldest_lower_flight_owner(
    lower_flights: &[CarrierPathFlightDebt],
) -> Option<CarrierPathKey> {
    lower_flights.first().map(|flight| flight.key)
}

fn server_bulk_admission_role(
    lead_key: CarrierPathKey,
    candidate: CarrierPathKey,
    lower_flight_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
) -> BulkAdmissionRole {
    if lower_flight_owner == Some(candidate) || (candidate == lead_key && ordering_debt == 0) {
        BulkAdmissionRole::ActiveDataPath
    } else if let Some(owner) = lower_flight_owner {
        bulk_additional_admission_role(owner.underlay, candidate.underlay)
    } else {
        bulk_additional_admission_role(lead_key.underlay, candidate.underlay)
    }
}

fn server_bulk_role_for_output_count(
    role: BulkAdmissionRole,
    output_count: usize,
) -> BulkAdmissionRole {
    if role == BulkAdmissionRole::ActiveDataPath && output_count <= 1 {
        BulkAdmissionRole::ActiveSingleCarrier
    } else {
        role
    }
}

fn server_bulk_lead_candidate_suppression(
    key: CarrierPathKey,
    snapshot: PathSnapshot,
    eta_ms: f64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    output_count: usize,
) -> Option<&'static str> {
    let lower_flight_owner = server_oldest_lower_flight_owner(lower_flights);
    let role = if lower_flight_owner.is_none() || lower_flight_owner == Some(key) {
        BulkAdmissionRole::ActiveDataPath
    } else {
        bulk_additional_admission_role(lower_flight_owner.expect("checked").underlay, key.underlay)
    };
    let role = server_bulk_role_for_output_count(role, output_count);
    let ordering_debt = if role == BulkAdmissionRole::ActiveDataPath {
        server_total_lower_flight_debt_bytes(lower_flights)
    } else {
        server_stream_ordering_debt_bytes(lower_flights, key)
    };
    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
        best_snapshot: snapshot,
        best_eta_ms: eta_ms,
        candidate_snapshot: snapshot,
        candidate_eta_ms: eta_ms,
        payload_bytes,
        mux_limits,
        role,
        stream_ordering_debt_bytes: ordering_debt,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ServerPathMetricsSource {
    PeerHint,
    LocalSender,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ServerPathMetricsEntry {
    pub(super) metrics: PathMetrics,
    pub(super) source: ServerPathMetricsSource,
}

#[derive(Clone)]
pub(super) struct CarrierPathSendTarget {
    pub(super) key: CarrierPathKey,
    pub(super) commands: TcpPathSessionCommandSender,
}

pub(super) struct CarrierPathBulkChoice {
    pub(super) primary: CarrierPathSendTarget,
    pub(super) validation_duplicate: Option<CarrierPathSendTarget>,
}

pub(super) enum CarrierPathSendChoice {
    Single(CarrierPathSendTarget),
    Bulk(CarrierPathBulkChoice),
}

impl ResponseStreamOutputs {
    pub(super) fn read_backpressure_snapshot(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let now = Instant::now();
        if !relay_lane_is_bulk(lane) {
            return self.entries.last().map(|entry| {
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now)
            });
        }
        let active_key = self.entries.last().map(|entry| entry.key);
        self.entries
            .iter()
            .filter(|entry| {
                Some(entry.key) == active_key || server_output_has_sender_evidence(entry)
            })
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (eta_ms, snapshot)
            })
            .min_by(|left, right| left.0.total_cmp(&right.0))
            .map(|(_, snapshot)| snapshot)
    }

    pub(super) fn next_commands(&mut self) -> Option<CarrierPathSendTarget> {
        if self.entries.is_empty() {
            return None;
        }
        self.next_index %= self.entries.len();
        let entry = self.entries[self.next_index].clone();
        self.next_index = (self.next_index + 1) % self.entries.len();
        Some(CarrierPathSendTarget {
            key: entry.key,
            commands: entry.commands,
        })
    }

    pub(super) fn data_commands(&self) -> Option<CarrierPathSendTarget> {
        self.entries
            .last()
            .cloned()
            .map(|entry| CarrierPathSendTarget {
                key: entry.key,
                commands: entry.commands,
            })
    }

    pub(super) fn repair_commands(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        avoid_keys: &[CarrierPathKey],
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<CarrierPathSendTarget> {
        let now = Instant::now();
        let active_key = self.entries.last().map(|entry| entry.key);
        let choose = |prefer_avoiding: bool| {
            self.entries
                .iter()
                .filter(|entry| !prefer_avoiding || !avoid_keys.contains(&entry.key))
                .map(|entry| {
                    let snapshot = server_bulk_output_snapshot(
                        entry,
                        session_id,
                        FlowLane::Latency,
                        lane_tracker,
                        mux_limits,
                        now,
                    );
                    let eta_ms = server_bulk_output_eta_ms(
                        entry.key,
                        snapshot,
                        active_key,
                        FlowLane::Latency,
                        payload_bytes,
                        mux_limits,
                    );
                    (eta_ms, entry)
                })
                .min_by(|left, right| left.0.total_cmp(&right.0))
                .map(|(_, entry)| CarrierPathSendTarget {
                    key: entry.key,
                    commands: entry.commands.clone(),
                })
        };
        choose(true).or_else(|| choose(false))
    }

    pub(super) fn bulk_send_ready(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[CarrierPathFlightDebt],
    ) -> bool {
        self.select_bulk_output(
            session_id,
            lane_tracker,
            lane,
            payload_bytes,
            mux_limits,
            lower_flights,
            Instant::now(),
        )
        .is_some()
    }

    pub(super) fn bulk_commands(
        &mut self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[CarrierPathFlightDebt],
    ) -> Option<CarrierPathBulkChoice> {
        let now = Instant::now();
        #[cfg(feature = "lab-diagnostics")]
        {
            let active_key = self.entries.last().map(|entry| entry.key);
            let lead_candidate = self.bulk_lead_candidate(
                session_id,
                lane_tracker,
                lane,
                payload_bytes,
                mux_limits,
                now,
                active_key,
                lower_flights,
            );
            self.log_bulk_candidates(
                session_id,
                lane_tracker,
                lane,
                active_key,
                lead_candidate,
                payload_bytes,
                mux_limits,
                now,
                lower_flights,
            );
        }
        let (position, primary_eta_ms, primary_snapshot) = self.select_bulk_output(
            session_id,
            lane_tracker,
            lane,
            payload_bytes,
            mux_limits,
            lower_flights,
            now,
        )?;
        let entry = self.entries[position].clone();
        #[cfg(feature = "lab-diagnostics")]
        let snapshot =
            server_bulk_output_snapshot(&entry, session_id, lane, lane_tracker, mux_limits, now);
        self.next_index = (position + 1) % self.entries.len().max(1);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_bulk_output_selected",
            format_args!(
                "path_underlay={:?} path_id={} reason=admitted payload_bytes={} scoring_payload_bytes={} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} queue_bytes={} inflight_limit={} active_flows={} active_latency_sensitive_flows={}",
                entry.key.underlay,
                entry.key.path_id.0,
                payload_bytes,
                bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
                entry.delivery_samples,
                entry.validation_credit_bytes,
                entry.bytes_in_flight,
                snapshot.bytes_in_flight,
                snapshot.queue_bytes,
                snapshot.inflight_limit_bytes,
                snapshot.active_flows,
                snapshot.active_latency_sensitive_flows,
            ),
        );
        let validation_duplicate = self.validation_duplicate_for_bulk_choice(
            &entry,
            session_id,
            lane_tracker,
            lane,
            primary_eta_ms,
            primary_snapshot,
            payload_bytes,
            mux_limits,
            lower_flights,
            now,
        );
        Some(CarrierPathBulkChoice {
            primary: CarrierPathSendTarget {
                key: entry.key,
                commands: entry.commands,
            },
            validation_duplicate,
        })
    }

    fn select_bulk_output(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[CarrierPathFlightDebt],
        now: Instant,
    ) -> Option<(usize, f64, PathSnapshot)> {
        let active_key = self.entries.last().map(|entry| entry.key);
        let lower_flight_owner = server_oldest_lower_flight_owner(lower_flights);
        let attached_lower_flight_owner =
            lower_flight_owner.filter(|owner| self.entries.iter().any(|entry| entry.key == *owner));
        if let Some(owner) = attached_lower_flight_owner
            && self
                .lower_frontier_owner_service_suppression(
                    session_id,
                    lane_tracker,
                    lane,
                    owner,
                    payload_bytes,
                    mux_limits,
                    lower_flights,
                    now,
                )
                .is_some()
        {
            return None;
        }
        let has_sender_evidence_candidate =
            self.entries.iter().any(server_output_has_sender_evidence);
        let normal_candidates = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                server_output_can_carry_primary_bulk(
                    entry,
                    active_key,
                    payload_bytes,
                    lower_flights,
                    attached_lower_flight_owner,
                    has_sender_evidence_candidate,
                )
            })
            .map(|(position, entry)| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (position, eta_ms, snapshot)
            })
            .collect::<Vec<_>>();
        let lead_candidate = normal_candidates
            .iter()
            .filter(|(position, eta_ms, snapshot)| {
                let key = self.entries[*position].key;
                server_bulk_lead_candidate_suppression(
                    key,
                    *snapshot,
                    *eta_ms,
                    payload_bytes,
                    mux_limits,
                    lower_flights,
                    self.entries.len(),
                )
                .is_none()
            })
            .min_by(|left, right| left.1.total_cmp(&right.1))
            .map(|(position, eta_ms, snapshot)| (self.entries[*position].key, *eta_ms, *snapshot));
        normal_candidates
            .into_iter()
            .filter(|(position, eta_ms, snapshot)| {
                lead_candidate.is_some_and(|(lead_key, best_eta_ms, best_snapshot)| {
                    let key = self.entries[*position].key;
                    let cross_path_ordering_debt =
                        server_stream_ordering_debt_bytes(lower_flights, key);
                    let owns_lower_frontier = lower_flight_owner == Some(key);
                    let role = server_bulk_admission_role(
                        lead_key,
                        key,
                        lower_flight_owner,
                        cross_path_ordering_debt,
                    );
                    let role = server_bulk_role_for_output_count(role, self.entries.len());
                    let admission_ordering_debt =
                        server_admission_ordering_debt_bytes(lower_flights, key, role);
                    let (baseline_snapshot, baseline_eta_ms) = if owns_lower_frontier
                        && matches!(
                            role,
                            BulkAdmissionRole::ActiveDataPath
                                | BulkAdmissionRole::ActiveSingleCarrier
                        ) {
                        (*snapshot, *eta_ms)
                    } else {
                        (best_snapshot, best_eta_ms)
                    };
                    bulk_candidate_admission_suppression(
                        baseline_snapshot,
                        baseline_eta_ms,
                        *snapshot,
                        *eta_ms,
                        payload_bytes,
                        mux_limits,
                        role,
                    )
                    .or_else(|| {
                        bulk_candidate_admission_suppression_with_ordering_debt(
                            BulkAdmissionCheck {
                                best_snapshot: baseline_snapshot,
                                best_eta_ms: baseline_eta_ms,
                                candidate_snapshot: *snapshot,
                                candidate_eta_ms: *eta_ms,
                                payload_bytes,
                                mux_limits,
                                role,
                                stream_ordering_debt_bytes: admission_ordering_debt,
                            },
                        )
                    })
                    .is_none()
                })
            })
            .min_by(|left, right| left.1.total_cmp(&right.1))
    }

    pub(super) fn lower_frontier_owner_service_suppression(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        owner: CarrierPathKey,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[CarrierPathFlightDebt],
        now: Instant,
    ) -> Option<&'static str> {
        let active_key = self.entries.last().map(|entry| entry.key);
        let owner_entry = self.entries.iter().find(|entry| entry.key == owner)?;
        let owner_snapshot = server_bulk_output_snapshot(
            owner_entry,
            session_id,
            lane,
            lane_tracker,
            mux_limits,
            now,
        );
        let owner_eta_ms = server_bulk_output_eta_ms(
            owner,
            owner_snapshot,
            active_key,
            lane,
            payload_bytes,
            mux_limits,
        );
        let alternate = self
            .entries
            .iter()
            .filter(|entry| entry.key != owner && server_output_has_sender_evidence(entry))
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (entry.key, eta_ms, snapshot)
            })
            .filter(|(_, eta_ms, snapshot)| {
                bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                    best_snapshot: *snapshot,
                    best_eta_ms: *eta_ms,
                    candidate_snapshot: *snapshot,
                    candidate_eta_ms: *eta_ms,
                    payload_bytes,
                    mux_limits,
                    role: BulkAdmissionRole::ActiveDataPath,
                    stream_ordering_debt_bytes: 0,
                })
                .is_none()
            })
            .min_by(|left, right| left.1.total_cmp(&right.1));
        let (_, alternate_eta_ms, alternate_snapshot) = alternate?;
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: alternate_snapshot,
            best_eta_ms: alternate_eta_ms,
            candidate_snapshot: owner_snapshot,
            candidate_eta_ms: owner_eta_ms,
            payload_bytes,
            mux_limits,
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes: server_total_lower_flight_debt_bytes(lower_flights),
        })
    }

    fn validation_duplicate_for_bulk_choice(
        &self,
        entry: &ResponseStreamOutputEntry,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        primary_eta_ms: f64,
        primary_snapshot: PathSnapshot,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[CarrierPathFlightDebt],
        now: Instant,
    ) -> Option<CarrierPathSendTarget> {
        let active_key = self.entries.last().map(|entry| entry.key);
        let lower_flight_owner = server_oldest_lower_flight_owner(lower_flights);
        self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, validation)| {
                validation.key != entry.key
                    && validation.key.underlay == UnderlayProtocol::Udp
                    && !server_output_has_sender_evidence(validation)
                    && validation.validation_credit_bytes >= payload_bytes as u64
            })
            .map(|(validation_position, validation)| {
                let validation_snapshot = server_bulk_output_snapshot(
                    validation,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                (
                    validation_position,
                    validation.key,
                    server_bulk_output_eta_ms(
                        validation.key,
                        validation_snapshot,
                        active_key,
                        lane,
                        payload_bytes,
                        mux_limits,
                    ),
                    validation_snapshot,
                )
            })
            .filter(|(_, validation_key, validation_eta_ms, validation_snapshot)| {
                let cross_path_ordering_debt =
                    server_stream_ordering_debt_bytes(lower_flights, *validation_key);
                let owns_lower_frontier = lower_flight_owner == Some(*validation_key);
                let role = server_bulk_admission_role(
                    entry.key,
                    *validation_key,
                    lower_flight_owner,
                    cross_path_ordering_debt,
                );
                let role = server_bulk_role_for_output_count(role, self.entries.len());
                let admission_ordering_debt =
                    server_admission_ordering_debt_bytes(lower_flights, *validation_key, role);
                let (baseline_snapshot, baseline_eta_ms) =
                    if owns_lower_frontier
                        && matches!(
                            role,
                            BulkAdmissionRole::ActiveDataPath
                                | BulkAdmissionRole::ActiveSingleCarrier
                        )
                    {
                        (*validation_snapshot, *validation_eta_ms)
                    } else {
                        (primary_snapshot, primary_eta_ms)
                    };
                bulk_candidate_admission_suppression(
                    baseline_snapshot,
                    baseline_eta_ms,
                    *validation_snapshot,
                    *validation_eta_ms,
                    payload_bytes,
                    mux_limits,
                    role,
                )
                .or_else(|| {
                    bulk_candidate_admission_suppression_with_ordering_debt(
                        BulkAdmissionCheck {
                            best_snapshot: baseline_snapshot,
                            best_eta_ms: baseline_eta_ms,
                            candidate_snapshot: *validation_snapshot,
                            candidate_eta_ms: *validation_eta_ms,
                            payload_bytes,
                            mux_limits,
                            role,
                            stream_ordering_debt_bytes: admission_ordering_debt,
                        },
                    )
                })
                .is_none()
            })
            .min_by(|left, right| left.2.total_cmp(&right.2))
            .map(|(validation_position, _, _, _)| {
                let validation = self.entries[validation_position].clone();
                #[cfg(feature = "lab-diagnostics")]
                {
                    let validation_snapshot = server_bulk_output_snapshot(
                        &validation,
                        session_id,
                        lane,
                        lane_tracker,
                        mux_limits,
                        now,
                    );
                    lab_diagnostic(
                        "server_bulk_output_selected",
                        format_args!(
                            "path_underlay={:?} path_id={} reason=validation_duplicate payload_bytes={} scoring_payload_bytes={} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} queue_bytes={} inflight_limit={} active_flows={} active_latency_sensitive_flows={}",
                            validation.key.underlay,
                            validation.key.path_id.0,
                            payload_bytes,
                            bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
                            validation.delivery_samples,
                            validation.validation_credit_bytes,
                            validation.bytes_in_flight,
                            validation_snapshot.bytes_in_flight,
                            validation_snapshot.queue_bytes,
                            validation_snapshot.inflight_limit_bytes,
                            validation_snapshot.active_flows,
                            validation_snapshot.active_latency_sensitive_flows,
                        ),
                    );
                }
                CarrierPathSendTarget {
                    key: validation.key,
                    commands: validation.commands,
                }
            })
    }

    #[cfg(feature = "lab-diagnostics")]
    fn bulk_lead_candidate(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
        active_key: Option<CarrierPathKey>,
        lower_flights: &[CarrierPathFlightDebt],
    ) -> Option<(CarrierPathKey, f64, PathSnapshot)> {
        let has_sender_evidence_candidate =
            self.entries.iter().any(server_output_has_sender_evidence);
        let attached_lower_flight_owner = server_oldest_lower_flight_owner(lower_flights)
            .filter(|owner| self.entries.iter().any(|entry| entry.key == *owner));
        self.entries
            .iter()
            .filter(|entry| {
                server_output_can_carry_primary_bulk(
                    entry,
                    active_key,
                    payload_bytes,
                    lower_flights,
                    attached_lower_flight_owner,
                    has_sender_evidence_candidate,
                )
            })
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (entry.key, eta_ms, snapshot)
            })
            .filter(|(key, eta_ms, snapshot)| {
                server_bulk_lead_candidate_suppression(
                    *key,
                    *snapshot,
                    *eta_ms,
                    payload_bytes,
                    mux_limits,
                    lower_flights,
                    self.entries.len(),
                )
                .is_none()
            })
            .min_by(|left, right| left.1.total_cmp(&right.1))
    }

    #[cfg(feature = "lab-diagnostics")]
    fn log_bulk_candidates(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        active_key: Option<CarrierPathKey>,
        lead_candidate: Option<(CarrierPathKey, f64, PathSnapshot)>,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
        lower_flights: &[CarrierPathFlightDebt],
    ) {
        let attached_lower_flight_owner = server_oldest_lower_flight_owner(lower_flights)
            .filter(|owner| self.entries.iter().any(|entry| entry.key == *owner));
        for entry in &self.entries {
            let snapshot =
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now);
            let eta_ms = server_bulk_output_eta_ms(
                entry.key,
                snapshot,
                active_key,
                lane,
                payload_bytes,
                mux_limits,
            );
            let validation_ordering_debt =
                server_stream_ordering_debt_bytes(lower_flights, entry.key);
            let has_sender_evidence_candidate =
                self.entries.iter().any(server_output_has_sender_evidence);
            let reason = if Some(entry.key) != active_key
                && attached_lower_flight_owner.is_some_and(|owner| owner != entry.key)
            {
                "waiting_for_lower_frontier_owner"
            } else if Some(entry.key) != active_key
                && !server_output_has_sender_evidence(entry)
                && active_key.is_some_and(|active| active.underlay != entry.key.underlay)
            {
                "cross_underlay_validation_needs_sender_evidence"
            } else if Some(entry.key) != active_key
                && !server_output_has_sender_evidence(entry)
                && !server_output_has_primary_validation_credit(entry, payload_bytes)
            {
                "validation_credit_exhausted"
            } else if Some(entry.key) != active_key
                && !server_output_has_sender_evidence(entry)
                && has_sender_evidence_candidate
            {
                "validation_without_sender_evidence"
            } else if Some(entry.key) != active_key
                && !server_output_has_sender_evidence(entry)
                && validation_ordering_debt > 0
            {
                "validation_would_expand_ordering_debt"
            } else if let Some((lead_key, best_eta_ms, best_snapshot)) = lead_candidate {
                let cross_path_ordering_debt =
                    server_stream_ordering_debt_bytes(lower_flights, entry.key);
                let role = server_bulk_admission_role(
                    lead_key,
                    entry.key,
                    server_oldest_lower_flight_owner(lower_flights),
                    cross_path_ordering_debt,
                );
                let role = server_bulk_role_for_output_count(role, self.entries.len());
                let admission_ordering_debt =
                    server_admission_ordering_debt_bytes(lower_flights, entry.key, role);
                let owns_lower_frontier =
                    server_oldest_lower_flight_owner(lower_flights) == Some(entry.key);
                let (baseline_snapshot, baseline_eta_ms) = if owns_lower_frontier
                    && matches!(
                        role,
                        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier
                    ) {
                    (snapshot, eta_ms)
                } else {
                    (best_snapshot, best_eta_ms)
                };
                if let Some(suppression) =
                    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                        best_snapshot: baseline_snapshot,
                        best_eta_ms: baseline_eta_ms,
                        candidate_snapshot: snapshot,
                        candidate_eta_ms: eta_ms,
                        payload_bytes,
                        mux_limits,
                        role,
                        stream_ordering_debt_bytes: admission_ordering_debt,
                    })
                {
                    suppression
                } else if entry.key == lead_key
                    && server_output_has_primary_validation_credit(entry, payload_bytes)
                    && !server_output_has_sender_evidence(entry)
                {
                    "validation_lead_admitted"
                } else if entry.key == lead_key {
                    "lead_admitted"
                } else if server_output_has_sender_evidence(entry) {
                    "delivery_evidence_admitted"
                } else {
                    "validation_candidate"
                }
            } else {
                "validation_candidate_no_admitted_baseline"
            };
            lab_diagnostic(
                "server_bulk_output_candidate",
                format_args!(
                    "path_underlay={:?} path_id={} active={} reason={} payload_bytes={} scoring_payload_bytes={} eta_ms={:.3} confidence={:.3} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} stream_ordering_debt={} queue_bytes={} command_pending_bytes={} inflight_limit={} active_flows={} active_latency_sensitive_flows={} srtt_ms={:.3} delivery_rate_mbps={:.3}",
                    entry.key.underlay,
                    entry.key.path_id.0,
                    Some(entry.key) == active_key,
                    reason,
                    payload_bytes,
                    bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
                    eta_ms,
                    snapshot.confidence,
                    entry.delivery_samples,
                    entry.validation_credit_bytes,
                    entry.bytes_in_flight,
                    snapshot.bytes_in_flight,
                    server_admission_ordering_debt_bytes(
                        lower_flights,
                        entry.key,
                        server_bulk_role_for_output_count(
                            server_bulk_admission_role(
                                lead_candidate
                                    .map(|candidate| candidate.0)
                                    .unwrap_or(entry.key),
                                entry.key,
                                server_oldest_lower_flight_owner(lower_flights),
                                server_stream_ordering_debt_bytes(lower_flights, entry.key),
                            ),
                            self.entries.len(),
                        )
                    ),
                    snapshot.queue_bytes,
                    entry.commands.pending_bytes(),
                    snapshot.inflight_limit_bytes,
                    snapshot.active_flows,
                    snapshot.active_latency_sensitive_flows,
                    snapshot.srtt_ms,
                    snapshot.delivery_rate_bps / 1_000_000.0,
                ),
            );
        }
    }
}

pub(super) fn server_bulk_output_snapshot(
    entry: &ResponseStreamOutputEntry,
    session_id: SessionId,
    lane: FlowLane,
    lane_tracker: &ServerPathLaneTracker,
    mux_limits: MuxLimits,
    now: Instant,
) -> PathSnapshot {
    let local_sender_metrics = entry.path_metrics.and_then(|path_metrics| {
        (path_metrics.source == ServerPathMetricsSource::LocalSender).then_some(path_metrics)
    });
    let validation_hint_metrics = entry
        .path_metrics
        .and_then(|path_metrics| (entry.delivery_samples == 0).then_some(path_metrics));
    let model_metrics = local_sender_metrics.or(validation_hint_metrics);
    let srtt_ms = model_metrics.map_or_else(
        || default_path_srtt_ms(entry.key.underlay),
        |path_metrics| f64::from(path_metrics.metrics.srtt_us.max(1)) / 1000.0,
    );
    let jitter_ms = model_metrics.map_or(0.0, |path_metrics| {
        f64::from(path_metrics.metrics.jitter_us) / 1000.0
    });
    let loss_rate = model_metrics
        .map_or(0.0, |path_metrics| {
            f64::from(path_metrics.metrics.loss_ppm) / 1_000_000.0
        })
        .clamp(0.0, 1.0);
    let model_rate_bps = model_metrics.map(server_path_metrics_rate_bps);
    let local_sender_rate_bps = local_sender_metrics.map(server_path_metrics_rate_bps);
    let rate_bps = match entry.key.underlay {
        UnderlayProtocol::Udp => local_sender_rate_bps,
        UnderlayProtocol::Tcp => model_rate_bps,
    }
    .unwrap_or_else(|| default_path_rate_bps(entry.key.underlay))
    .max(1.0);
    let mut snapshot = PathSnapshot::new(entry.key.path_id, entry.key.underlay, srtt_ms, rate_bps);
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    if let Some(path_metrics) = model_metrics {
        snapshot.pacing_rate_bps =
            (path_metrics.metrics.pacing_rate_bps.max(1) as f64).max(snapshot.delivery_rate_bps);
        snapshot.app_limited = path_metrics.metrics.app_limited;
    }
    let metric_queue_bytes =
        model_metrics.map_or(0, |path_metrics| path_metrics.metrics.queue_bytes);
    snapshot.queue_bytes = metric_queue_bytes.saturating_add(entry.commands.pending_bytes());
    snapshot.product_queue_bytes = entry.product_queue_bytes;
    snapshot.bytes_in_flight = match entry.key.underlay {
        UnderlayProtocol::Udp => {
            local_sender_metrics.map_or(0, |path_metrics| path_metrics.metrics.bytes_in_flight)
        }
        UnderlayProtocol::Tcp => entry.bytes_in_flight,
    };
    snapshot.product_bytes_in_flight = entry.bytes_in_flight;
    snapshot.inflight_limit_bytes =
        model_metrics.map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes);
    snapshot.confidence = server_output_confidence(entry, now);
    let lane_load = lane_tracker.snapshot(session_id, entry.key);
    let session_lane_load = lane_tracker.session_snapshot(session_id);
    snapshot.active_flows = lane_load.active_flows;
    snapshot.active_latency_sensitive_flows = lane_load.active_latency_sensitive_flows;
    snapshot.session_active_latency_sensitive_flows =
        session_lane_load.active_latency_sensitive_flows;
    let known_bulk_flows = lane_load
        .active_flows
        .saturating_sub(lane_load.active_latency_sensitive_flows);
    if relay_lane_is_bulk(lane)
        && lane_load.active_latency_sensitive_flows > 0
        && known_bulk_flows > 0
    {
        let latency_headroom =
            adaptive_reliable_relay_inflight_bytes(Some(snapshot), FlowLane::Latency, mux_limits)
                as u64;
        let protected_queue =
            latency_headroom.saturating_mul(u64::from(lane_load.active_latency_sensitive_flows));
        snapshot.queue_bytes = snapshot.queue_bytes.saturating_add(protected_queue);
    }
    snapshot
}

pub(super) fn server_bulk_output_eta_ms(
    key: CarrierPathKey,
    snapshot: PathSnapshot,
    active_key: Option<CarrierPathKey>,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> f64 {
    let queued_bits = snapshot
        .queue_bytes
        .saturating_add(snapshot.product_queue_bytes)
        .saturating_add(snapshot.bytes_in_flight)
        .saturating_mul(8) as f64;
    let scoring_payload_bytes = if relay_lane_is_bulk(lane) {
        bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
    } else {
        payload_bytes
    };
    let payload_bits = scoring_payload_bytes as f64 * 8.0;
    let mut eta_ms = snapshot.srtt_ms / 2.0;
    let effective_rate_bps = if relay_lane_is_bulk(lane) {
        snapshot
            .delivery_rate_bps
            .max(snapshot.pacing_rate_bps)
            .max(1.0)
    } else {
        snapshot.delivery_rate_bps.max(1.0)
    };
    eta_ms += (queued_bits + payload_bits) / effective_rate_bps * 1000.0;
    eta_ms += snapshot.jitter_ms;
    eta_ms += snapshot.loss_rate.clamp(0.0, 1.0) * 500.0;
    if key.underlay == UnderlayProtocol::Udp && relay_lane_is_bulk(lane) {
        eta_ms += udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes);
    }
    eta_ms += (1.0 - snapshot.confidence.clamp(0.0, 1.0)) * snapshot.srtt_ms;
    if Some(key) != active_key && snapshot.confidence < 0.5 {
        eta_ms += snapshot.srtt_ms;
        if snapshot.bytes_in_flight > 0 {
            eta_ms += snapshot.srtt_ms;
        }
    }
    eta_ms
}

fn server_output_confidence(entry: &ResponseStreamOutputEntry, now: Instant) -> f64 {
    let delivery_confidence = (f64::from(entry.delivery_samples) / 8.0).clamp(0.0, 1.0);
    let metric_confidence = match entry.path_metrics {
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::LocalSender,
            metrics,
        }) if metrics.has_ack_derived_data_sample => {
            let source_confidence =
                f64::from(metrics.confidence_ppm).clamp(0.0, 1_000_000.0) / 1_000_000.0;
            let sample_confidence = (f64::from(metrics.data_sample_count) / 8.0).clamp(0.0, 1.0);
            source_confidence * sample_confidence
        }
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::PeerHint,
            ..
        }) => 0.1,
        _ => 0.0,
    };
    let freshness_confidence = entry
        .last_delivery_at
        .map(|seen| {
            let age = now.saturating_duration_since(seen).as_secs_f64();
            (1.0 - age / 30.0).clamp(0.0, 1.0) * 0.25
        })
        .unwrap_or(0.0);
    delivery_confidence
        .max(metric_confidence)
        .max(freshness_confidence)
        .clamp(0.1, 1.0)
}

fn server_path_metrics_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    let delivery_rate_bps = path_metrics.metrics.delivery_rate_bps.max(1) as f64;
    let pacing_rate_bps = path_metrics.metrics.pacing_rate_bps.max(1) as f64;
    if path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.app_limited
    {
        delivery_rate_bps.max(pacing_rate_bps)
    } else {
        delivery_rate_bps
    }
}

pub(super) fn server_output_has_sender_evidence(entry: &ResponseStreamOutputEntry) -> bool {
    entry.delivery_samples > 0
        || matches!(
            entry.path_metrics,
            Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics: PathMetrics {
                    delivery_rate_bps: 1..,
                    has_ack_derived_data_sample: true,
                    ..
                },
            })
        )
}

fn server_output_has_primary_validation_credit(
    entry: &ResponseStreamOutputEntry,
    payload_bytes: usize,
) -> bool {
    entry.validation_credit_bytes >= payload_bytes as u64
}

fn server_output_can_carry_primary_bulk(
    entry: &ResponseStreamOutputEntry,
    active_key: Option<CarrierPathKey>,
    payload_bytes: usize,
    lower_flights: &[CarrierPathFlightDebt],
    attached_lower_flight_owner: Option<CarrierPathKey>,
    has_sender_evidence_candidate: bool,
) -> bool {
    if let Some(owner) = attached_lower_flight_owner
        && entry.key != owner
    {
        return false;
    }
    if Some(entry.key) == active_key || server_output_has_sender_evidence(entry) {
        return true;
    }
    if let Some(active) = active_key
        && active.underlay != entry.key.underlay
    {
        return false;
    }
    if has_sender_evidence_candidate {
        return false;
    }
    server_output_has_primary_validation_credit(entry, payload_bytes)
        && server_stream_ordering_debt_bytes(lower_flights, entry.key) == 0
}

pub(super) fn record_server_sender_decision(
    session_id: SessionId,
    stream_id: StreamId,
    key: CarrierPathKey,
    frame: &Frame,
    lane: FlowLane,
    reason: &'static str,
) {
    #[cfg(feature = "lab-diagnostics")]
    lab_sender_service_decision(
        "server",
        Some(session_id.0),
        stream_id.0,
        reason,
        sender_service_frame_kind(frame),
        reliable_stream_frame_payload_bytes(frame),
        format_args!(
            "path_underlay={:?} path_id={} lane={:?} pacing_bytes={}",
            key.underlay,
            key.path_id.0,
            lane,
            frame_pacing_bytes(frame),
        ),
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (session_id, stream_id, key, frame, lane, reason);
}

#[cfg(feature = "lab-diagnostics")]
pub(super) fn sender_service_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}
