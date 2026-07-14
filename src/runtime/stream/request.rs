//! Exact request-stream flight ownership.
//!
//! The stream owns product offsets and attachment-fenced flights so path
//! selection can observe ordering debt without owning or mutating it.

use crate::model::admission::bulk_service_feed_reservoir_payload_bytes;
use crate::model::capacity::{
    QUIC_TIMER_GRANULARITY, reliable_relay_buffer_len, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::multipath::{FlowSubflowSet, PathAdmissionDecision, SubflowAdmissionInput};
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::request::evidence::{
    RequestPathRateEvidence, RequestPerFlowRateModel, RequestTcpAckTurnoverModel,
};
use crate::model::work::CarrierWorkKind;
use crate::mux::MuxLimits;
use crate::protocol::frame::{normalized_offset_ranges, reliable_stream_frame_extent};
use crate::protocol::{Frame, OffsetRange, UnderlayProtocol};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

/// Request startup-subflow evidence and its post-enqueue admission commit.
///
/// Planning clones an epoch; only a successful carrier enqueue installs the
/// candidate, so stale selection cannot leave partial product membership.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestStartupState {
    pub(in crate::runtime) epoch: Option<FlowSubflowSet<RelayPathInstance>>,
    pub(in crate::runtime) acked_bytes: HashMap<RelayPathInstance, u64>,
    pub(in crate::runtime) first_sent_at: HashMap<RelayPathInstance, Instant>,
    pub(in crate::runtime) rate_evidence: HashSet<RelayPathInstance>,
    pub(in crate::runtime) receipt_proofs: HashMap<RelayPathInstance, (u64, u64)>,
    pub(in crate::runtime) attempted_subflows: HashSet<RelayPathInstance>,
}

#[derive(Debug)]
pub(in crate::runtime) struct RequestStartupAdmission {
    next_epoch: FlowSubflowSet<RelayPathInstance>,
    candidate: RelayPathInstance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RequestAckClockOperation {
    Pending {
        service: RelayPathInstance,
        candidate: RelayPathInstance,
    },
    Owner {
        candidate: RelayPathInstance,
        target_bytes: u64,
    },
}

impl RequestAckClockOperation {
    pub(in crate::runtime) fn candidate(self) -> RelayPathInstance {
        match self {
            Self::Pending { candidate, .. } | Self::Owner { candidate, .. } => candidate,
        }
    }
}

/// Evidence owned by one exact request-path attachment.
///
/// Exact instances, rather than configured path indexes, fence evidence across
/// reconnects. Keeping one record per instance also makes partial cleanup an
/// explicit state transition instead of a collection of unrelated map edits.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestSubflowState {
    rate_evidence: Option<RequestPathRateEvidence>,
    per_flow_rate: Option<RequestPerFlowRateModel>,
    tcp_ack_turnover: Option<RequestTcpAckTurnoverModel>,
    rate_proven: bool,
    ack_clock_first_window: bool,
    ack_clock_proven: bool,
    window_turnover_proven: bool,
    ack_clock_calibration_bytes: Option<u64>,
    ack_clock_calibration_target: Option<u64>,
    tcp_capacity_proven: bool,
    graduated: bool,
}

impl RequestSubflowState {
    #[cfg(test)]
    pub(in crate::runtime) fn rate_evidence(&self) -> Option<&RequestPathRateEvidence> {
        self.rate_evidence.as_ref()
    }

    pub(in crate::runtime) fn rate_evidence_mut(
        &mut self,
        observed_at: Instant,
    ) -> &mut RequestPathRateEvidence {
        self.rate_evidence
            .get_or_insert_with(|| RequestPathRateEvidence::new(observed_at))
    }

    pub(in crate::runtime) fn per_flow_rate(&self) -> Option<RequestPerFlowRateModel> {
        self.per_flow_rate
    }

    pub(in crate::runtime) fn set_per_flow_rate(&mut self, model: RequestPerFlowRateModel) {
        self.per_flow_rate = Some(model);
    }

    pub(in crate::runtime) fn tcp_ack_turnover(&self) -> Option<RequestTcpAckTurnoverModel> {
        self.tcp_ack_turnover
    }

    pub(in crate::runtime) fn set_tcp_ack_turnover(&mut self, model: RequestTcpAckTurnoverModel) {
        self.tcp_ack_turnover = Some(model);
    }

    pub(in crate::runtime) fn rate_proven(&self) -> bool {
        self.rate_proven
    }

    pub(in crate::runtime) fn mark_rate_proven(&mut self) -> bool {
        !std::mem::replace(&mut self.rate_proven, true)
    }

    pub(in crate::runtime) fn ack_clock_first_window(&self) -> bool {
        self.ack_clock_first_window
    }

    pub(in crate::runtime) fn mark_ack_clock_first_window(&mut self) -> bool {
        !std::mem::replace(&mut self.ack_clock_first_window, true)
    }

    pub(in crate::runtime) fn ack_clock_proven(&self) -> bool {
        self.ack_clock_proven
    }

    pub(in crate::runtime) fn mark_ack_clock_proven(&mut self) -> bool {
        !std::mem::replace(&mut self.ack_clock_proven, true)
    }

    pub(in crate::runtime) fn window_turnover_proven(&self) -> bool {
        self.window_turnover_proven
    }

    pub(in crate::runtime) fn mark_window_turnover_proven(&mut self) {
        self.window_turnover_proven = true;
    }

    pub(in crate::runtime) fn ack_clock_calibration_bytes(&self) -> Option<u64> {
        self.ack_clock_calibration_bytes
    }

    pub(in crate::runtime) fn set_ack_clock_calibration_bytes(&mut self, bytes: u64) {
        self.ack_clock_calibration_bytes = Some(bytes);
    }

    pub(in crate::runtime) fn ack_clock_calibration_target(&self) -> Option<u64> {
        self.ack_clock_calibration_target
    }

    pub(in crate::runtime) fn set_ack_clock_calibration_target(&mut self, bytes: u64) {
        self.ack_clock_calibration_target = Some(bytes);
    }

    pub(in crate::runtime) fn tcp_capacity_proven(&self) -> bool {
        self.tcp_capacity_proven
    }

    pub(in crate::runtime) fn mark_tcp_capacity_proven(&mut self) {
        self.tcp_capacity_proven = true;
    }

    pub(in crate::runtime) fn clear_tcp_capacity_proven(&mut self) {
        self.tcp_capacity_proven = false;
    }

    pub(in crate::runtime) fn graduated(&self) -> bool {
        self.graduated
    }

    pub(in crate::runtime) fn mark_graduated(&mut self) {
        self.graduated = true;
    }

    pub(in crate::runtime) fn clear_graduated(&mut self) {
        self.graduated = false;
    }

    pub(in crate::runtime) fn has_product_evidence(&self) -> bool {
        self.rate_evidence.is_some()
            || self.per_flow_rate.is_some()
            || self.rate_proven
            || self.ack_clock_proven
    }

    /// Revoke TCP admission evidence while retaining a completed flow model.
    ///
    /// A flow model is receiver-proven product history; carrier proof expiry
    /// must not erase it. All incomplete calibration authority is discarded.
    pub(in crate::runtime) fn revoke_tcp_capacity(&mut self) {
        self.tcp_capacity_proven = false;
        self.graduated = false;
        self.rate_proven = false;
        self.ack_clock_first_window = false;
        self.ack_clock_proven = false;
        self.window_turnover_proven = false;
        self.rate_evidence = None;
        self.tcp_ack_turnover = None;
        self.ack_clock_calibration_bytes = None;
        self.ack_clock_calibration_target = None;
    }
}

/// Exact-instance subflow records for one request stream.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestSubflows {
    entries: HashMap<RelayPathInstance, RequestSubflowState>,
}

impl RequestSubflows {
    pub(in crate::runtime) fn get(
        &self,
        instance: RelayPathInstance,
    ) -> Option<&RequestSubflowState> {
        self.entries.get(&instance)
    }

    pub(in crate::runtime) fn get_mut(
        &mut self,
        instance: RelayPathInstance,
    ) -> &mut RequestSubflowState {
        self.entries.entry(instance).or_default()
    }

    pub(in crate::runtime) fn get_existing_mut(
        &mut self,
        instance: RelayPathInstance,
    ) -> Option<&mut RequestSubflowState> {
        self.entries.get_mut(&instance)
    }

    pub(in crate::runtime) fn retain_live(&mut self, live: &HashSet<RelayPathInstance>) {
        self.entries.retain(|instance, _| live.contains(instance));
    }

    pub(in crate::runtime) fn iter(
        &self,
    ) -> impl Iterator<Item = (RelayPathInstance, &RequestSubflowState)> {
        self.entries
            .iter()
            .map(|(instance, state)| (*instance, state))
    }
}

/// Single-task request product state.
///
/// The client relay serializes this aggregate, so request offsets, evidence,
/// Service identity, and repair history stay lock-free. Per-path evidence is
/// keyed once in `subflows`, preventing partial membership cleanup.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestStreamState {
    pub(in crate::runtime) flights: RequestFlightLedger,
    pub(in crate::runtime) ordered_service: Option<RelayPathInstance>,
    pub(in crate::runtime) startup: RequestStartupState,
    pub(in crate::runtime) subflows: RequestSubflows,
    pub(in crate::runtime) ack_clock_operation: Option<RequestAckClockOperation>,
    pub(in crate::runtime) membership_generation: Option<u64>,
    pub(in crate::runtime) missing_owner_repair_attempts: HashMap<RelayPathInstance, Instant>,
}

impl RequestStreamState {
    pub(in crate::runtime) fn ordered_service_key(&self) -> Option<RelayPathKey> {
        self.ordered_service.map(|service| service.key)
    }

    pub(in crate::runtime) fn reset_subflow_epoch(&mut self) {
        self.startup.reset_epoch();
        if matches!(
            self.ack_clock_operation,
            Some(RequestAckClockOperation::Pending { .. })
        ) {
            self.ack_clock_operation = None;
        }
    }
}

impl RequestStartupState {
    pub(in crate::runtime) fn plan_admission(
        &self,
        mux_limits: MuxLimits,
        service: RelayPathInstance,
        candidate: RelayPathInstance,
        payload_bytes: usize,
    ) -> Option<RequestStartupAdmission> {
        if service.key.underlay != UnderlayProtocol::Tcp
            || candidate.key.underlay != UnderlayProtocol::Tcp
        {
            return None;
        }
        let startup_credit =
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits))
                .unwrap_or(usize::MAX);
        let mut next_epoch = self
            .epoch
            .as_ref()
            .filter(|epoch| epoch.matches_envelope(service, startup_credit, 0, Duration::ZERO))
            .cloned()
            .unwrap_or_else(|| FlowSubflowSet::new(0, service, startup_credit, 0, Duration::ZERO));
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };
        (next_epoch.admit_subflow_owner(input).decision == PathAdmissionDecision::AdmitSubflow)
            .then_some(RequestStartupAdmission {
                next_epoch,
                candidate,
            })
    }

    pub(in crate::runtime) fn commit_admission(&mut self, admission: RequestStartupAdmission) {
        self.epoch = Some(admission.next_epoch);
        self.attempted_subflows.insert(admission.candidate);
    }

    pub(in crate::runtime) fn reset_epoch(&mut self) {
        self.epoch = None;
        self.acked_bytes.clear();
        self.first_sent_at.clear();
        self.rate_evidence.clear();
        self.receipt_proofs.clear();
    }

    pub(in crate::runtime) fn retain_live(&mut self, live: &HashSet<RelayPathInstance>) {
        self.attempted_subflows
            .retain(|instance| live.contains(instance));
        self.acked_bytes
            .retain(|instance, _| live.contains(instance));
        self.first_sent_at
            .retain(|instance, _| live.contains(instance));
        self.rate_evidence
            .retain(|instance| live.contains(instance));
        self.receipt_proofs
            .retain(|instance, _| live.contains(instance));
    }
}

/// Product read-ahead authority for one exact request Service epoch.
///
/// TCP and QUIC supply different evidence below this owner. Both grow the same
/// bounded product window only after their evidence becomes product-authoritative.
#[derive(Debug)]
pub(in crate::runtime) struct RequestOutstandingWindow {
    service_epoch_instance: Option<RelayPathInstance>,
    product_limit_bytes: usize,
    growth_epoch_at: Instant,
    acked_in_epoch: usize,
}

impl RequestOutstandingWindow {
    pub(in crate::runtime) fn new() -> Self {
        Self::new_at(Instant::now())
    }

    fn new_at(now: Instant) -> Self {
        Self {
            service_epoch_instance: None,
            product_limit_bytes: 0,
            growth_epoch_at: now,
            acked_in_epoch: 0,
        }
    }

    pub(in crate::runtime) fn limit_bytes(
        &mut self,
        service_instance: Option<RelayPathInstance>,
        lane: crate::scheduler::FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> usize {
        self.limit_bytes_at(
            service_instance,
            lane,
            payload_bytes,
            mux_limits,
            Instant::now(),
        )
    }

    pub(in crate::runtime) fn resolved_service_instance(
        &self,
        ordered_service: Option<RelayPathInstance>,
        pre_owner_active: Option<RelayPathInstance>,
    ) -> Option<RelayPathInstance> {
        ordered_service.or_else(|| {
            self.service_epoch_instance
                .is_none()
                .then_some(pre_owner_active)
                .flatten()
        })
    }

    fn limit_bytes_at(
        &mut self,
        service_instance: Option<RelayPathInstance>,
        lane: crate::scheduler::FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
    ) -> usize {
        let resource_ceiling = request_outstanding_resource_ceiling(mux_limits);
        let startup_reservoir = if lane.is_bulk() {
            bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        } else {
            // Flow classification expects one full source queue; a smaller
            // bound would make latency probing a stop-and-wait prerequisite.
            reliable_relay_buffer_len(mux_limits)
        }
        .min(resource_ceiling)
        .max(1);
        if service_instance.is_none() && self.service_epoch_instance.is_some() {
            if !lane.is_bulk() && self.product_limit_bytes > startup_reservoir {
                self.product_limit_bytes = startup_reservoir;
                self.growth_epoch_at = now;
                self.acked_in_epoch = 0;
            }
            return self.product_limit_bytes.min(resource_ceiling).max(1);
        }
        if let Some(instance) = service_instance
            && self.service_epoch_instance != Some(instance)
        {
            // A Service handoff starts a fresh ACK epoch even when both
            // associations use the same carrier protocol.
            self.service_epoch_instance = Some(instance);
            self.product_limit_bytes = 0;
            self.growth_epoch_at = now;
            self.acked_in_epoch = 0;
        }
        let lane_demoted = !lane.is_bulk() && self.product_limit_bytes > startup_reservoir;
        if lane_demoted || self.product_limit_bytes < startup_reservoir {
            self.product_limit_bytes = startup_reservoir;
            self.growth_epoch_at = now;
            self.acked_in_epoch = 0;
        }
        self.product_limit_bytes.min(resource_ceiling).max(1)
    }

    pub(in crate::runtime) fn record_acked(
        &mut self,
        released_bytes: usize,
        owner_instance: RelayPathInstance,
        service_instance: Option<RelayPathInstance>,
        owner_capable: bool,
        lane: crate::scheduler::FlowLane,
        growth_interval: Duration,
        mux_limits: MuxLimits,
    ) {
        self.record_acked_at(
            released_bytes,
            owner_instance,
            service_instance,
            owner_capable,
            lane,
            growth_interval,
            mux_limits,
            Instant::now(),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn record_acked_at(
        &mut self,
        released_bytes: usize,
        owner_instance: RelayPathInstance,
        service_instance: Option<RelayPathInstance>,
        owner_capable: bool,
        lane: crate::scheduler::FlowLane,
        growth_interval: Duration,
        mux_limits: MuxLimits,
        now: Instant,
    ) {
        let Some(service_instance) = service_instance else {
            return;
        };
        if released_bytes == 0
            || !owner_capable
            || service_instance.key.underlay != owner_instance.key.underlay
            || self.service_epoch_instance != Some(service_instance)
            || !lane.is_bulk()
        {
            return;
        }
        let resource_ceiling = request_outstanding_resource_ceiling(mux_limits);
        if self.product_limit_bytes == 0 || self.product_limit_bytes >= resource_ceiling {
            return;
        }
        let growth_interval = growth_interval.max(QUIC_TIMER_GRANULARITY);
        if now.saturating_duration_since(self.growth_epoch_at) > growth_interval {
            self.growth_epoch_at = now;
            self.acked_in_epoch = 0;
            return;
        }
        self.acked_in_epoch = self.acked_in_epoch.saturating_add(released_bytes);
        let durable_product_floor =
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits))
                .unwrap_or(usize::MAX)
                .min(self.product_limit_bytes);
        let growth_threshold = self
            .product_limit_bytes
            .div_ceil(2)
            .max(durable_product_floor)
            .max(1);
        if self.acked_in_epoch < growth_threshold {
            return;
        }
        self.product_limit_bytes = self
            .product_limit_bytes
            .saturating_mul(2)
            .min(resource_ceiling)
            .max(1);
        self.growth_epoch_at = now;
        self.acked_in_epoch = 0;
    }

    pub(in crate::runtime) fn record_tcp_ack_clock_turnover(
        &mut self,
        turnover_bytes: usize,
        service_instance: Option<RelayPathInstance>,
        lane: crate::scheduler::FlowLane,
        mux_limits: MuxLimits,
    ) {
        let Some(service_instance) = service_instance else {
            return;
        };
        if service_instance.key.underlay != UnderlayProtocol::Tcp
            || self.service_epoch_instance != Some(service_instance)
            || !lane.is_bulk()
        {
            return;
        }
        let resource_ceiling = request_outstanding_resource_ceiling(mux_limits);
        if self.product_limit_bytes == 0 || self.product_limit_bytes >= resource_ceiling {
            return;
        }
        let durable_product_floor =
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits))
                .unwrap_or(usize::MAX)
                .min(self.product_limit_bytes);
        let next_limit = request_tcp_product_limit_for_turnover(
            self.product_limit_bytes,
            turnover_bytes,
            durable_product_floor,
            resource_ceiling,
        );
        if next_limit > self.product_limit_bytes {
            self.product_limit_bytes = next_limit;
            self.growth_epoch_at = Instant::now();
            self.acked_in_epoch = 0;
        }
    }
}

fn request_outstanding_resource_ceiling(mux_limits: MuxLimits) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    mux_limits
        .max_repair_bytes
        .min(mux_limits.max_path_flight_bytes)
        .min(stream_window)
        .max(1)
}

fn request_tcp_product_limit_for_turnover(
    current_limit: usize,
    turnover_bytes: usize,
    durable_product_floor: usize,
    resource_ceiling: usize,
) -> usize {
    let mut limit = current_limit.max(1).min(resource_ceiling.max(1));
    while limit < resource_ceiling {
        let threshold = limit.div_ceil(2).max(durable_product_floor).max(1);
        if turnover_bytes < threshold {
            break;
        }
        limit = limit.saturating_mul(2).min(resource_ceiling).max(1);
    }
    limit
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestPathRelease {
    pub(in crate::runtime) key: RelayPathKey,
    pub(in crate::runtime) instance: RelayPathInstance,
    pub(in crate::runtime) bytes: usize,
    pub(in crate::runtime) sent_at: Instant,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) elapsed: Duration,
    pub(in crate::runtime) path_proving: bool,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestFlightLedger {
    // OwnerData identifies the ordered path; RepairData remains a duplicate.
    // Exact attachment instances fence ACK evidence across path replacement.
    flights: BTreeMap<u64, Vec<RequestFlight>>,
}

impl RequestFlightLedger {
    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_frame(
        &mut self,
        key: RelayPathKey,
        frame: &Frame,
    ) -> usize {
        self.record_owner_frame_instance(RelayPathInstance { key, id: 0 }, frame)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_repair_frame(
        &mut self,
        key: RelayPathKey,
        frame: &Frame,
    ) -> usize {
        self.record_repair_frame_instance(RelayPathInstance { key, id: 0 }, frame)
    }

    pub(in crate::runtime) fn record_owner_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_product_frame(instance, frame, CarrierWorkKind::OwnerData)
    }

    pub(in crate::runtime) fn record_repair_frame_instance(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) -> usize {
        self.record_product_frame(instance, frame, CarrierWorkKind::RepairData)
    }

    fn record_product_frame(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) -> usize {
        debug_assert!(kind.carries_product_offsets());
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return 0;
        };
        self.flights.entry(offset).or_default().push(RequestFlight {
            instance,
            end,
            bytes,
            sent_at: Instant::now(),
            kind,
        });
        bytes
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(
        &mut self,
        ranges: &[OffsetRange],
    ) -> Vec<RequestPathRelease> {
        if ranges.is_empty() || self.flights.is_empty() {
            return Vec::new();
        }

        let original_flights = std::mem::take(&mut self.flights)
            .into_iter()
            .flat_map(|(start, flights)| flights.into_iter().map(move |flight| (start, flight)))
            .collect::<Vec<_>>();
        let ambiguous_intervals = request_ambiguous_flight_intervals(&original_flights);
        let now = Instant::now();
        let mut released = Vec::new();
        for (start, flight) in original_flights.iter().copied() {
            let split = split_flight_interval_by_ack(start, flight.end, ranges);
            for (acked_start, acked_end) in split.acked {
                let bytes = flight_interval_bytes(acked_start, acked_end);
                if bytes == 0 {
                    continue;
                }
                released.push(RequestPathRelease {
                    key: flight.instance.key,
                    instance: flight.instance,
                    bytes,
                    sent_at: flight.sent_at,
                    elapsed: now.saturating_duration_since(flight.sent_at),
                    path_proving: flight.kind.is_ordering_owner()
                        && !flight_intervals_overlap(&ambiguous_intervals, acked_start, acked_end),
                });
            }
            for (retained_start, retained_end) in split.retained {
                let bytes = flight_interval_bytes(retained_start, retained_end);
                if bytes == 0 {
                    continue;
                }
                self.flights
                    .entry(retained_start)
                    .or_default()
                    .push(RequestFlight {
                        end: retained_end,
                        bytes,
                        ..flight
                    });
            }
        }
        released
    }

    pub(in crate::runtime) fn drain_all(&mut self) -> Vec<RequestPathRelease> {
        let mut released = Vec::new();
        for flights in std::mem::take(&mut self.flights).into_values() {
            for flight in flights {
                released.push(RequestPathRelease {
                    key: flight.instance.key,
                    instance: flight.instance,
                    bytes: flight.bytes,
                    sent_at: flight.sent_at,
                    elapsed: Instant::now().saturating_duration_since(flight.sent_at),
                    path_proving: false,
                });
            }
        }
        released
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_product_flights_for_test(&mut self, age: Duration) {
        let sent_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        for flights in self.flights.values_mut() {
            for flight in flights {
                flight.sent_at = sent_at;
            }
        }
    }

    pub(in crate::runtime) fn sent_keys_for_frame(&self, frame: &Frame) -> Vec<RelayPathKey> {
        let Some((offset, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let mut keys = Vec::new();
        if let Some(flights) = self.flights.get(&offset) {
            for flight in flights {
                if flight.end >= end && !keys.contains(&flight.instance.key) {
                    keys.push(flight.instance.key);
                }
            }
        }
        keys
    }

    pub(in crate::runtime) fn has_missing_ordering_owner_before_offset(
        &self,
        offset: u64,
        live_instances: &[RelayPathInstance],
    ) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights.iter().any(|flight| {
                flight.kind.is_ordering_owner() && !live_instances.contains(&flight.instance)
            })
        })
    }

    pub(in crate::runtime) fn ordering_owner_keys_for_frame(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
    ) -> Vec<RelayPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let mut owner_keys = Vec::new();
        for (offset, flights) in self.flights.range(..end) {
            if *offset > start {
                break;
            }
            for flight in flights {
                if flight.kind.is_ordering_owner()
                    && flight.end >= end
                    && live_instances.contains(&flight.instance)
                    && !owner_keys.contains(&flight.instance.key)
                {
                    owner_keys.push(flight.instance.key);
                }
            }
        }
        owner_keys
    }

    pub(in crate::runtime) fn ordering_owner_underlay_for_frame(
        &self,
        frame: &Frame,
    ) -> Option<UnderlayProtocol> {
        let owner_keys = self.ordering_owner_keys_for_frame_any_instance(frame);
        let underlay = owner_keys.first()?.underlay;
        owner_keys
            .iter()
            .all(|key| key.underlay == underlay)
            .then_some(underlay)
    }

    pub(in crate::runtime) fn ordering_owner_keys_for_frame_any_instance(
        &self,
        frame: &Frame,
    ) -> Vec<RelayPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let mut owner_keys = Vec::new();
        for (offset, flights) in self.flights.range(..end) {
            if *offset > start {
                break;
            }
            for flight in flights {
                if !flight.kind.is_ordering_owner() || flight.end < end {
                    continue;
                }
                if !owner_keys.contains(&flight.instance.key) {
                    owner_keys.push(flight.instance.key);
                }
            }
        }
        owner_keys
    }

    pub(in crate::runtime) fn live_owner_tail_repair_owner_keys(
        &self,
        frame: &Frame,
        live_instances: &[RelayPathInstance],
        first_repair_after: Duration,
        repeat_repair_after: Duration,
    ) -> Vec<RelayPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let now = Instant::now();
        let expected_owner_keys = self.ordering_owner_keys_for_frame(frame, live_instances);
        let mut owner_keys = Vec::new();
        for (offset, flights) in self.flights.range(..end) {
            if *offset > start {
                break;
            }
            for flight in flights {
                if !flight.kind.is_ordering_owner()
                    || flight.end < end
                    || !live_instances.contains(&flight.instance)
                    || now.saturating_duration_since(flight.sent_at) < first_repair_after
                {
                    continue;
                }
                if expected_owner_keys.contains(&flight.instance.key)
                    && !owner_keys.contains(&flight.instance.key)
                {
                    owner_keys.push(flight.instance.key);
                }
            }
        }
        if owner_keys.is_empty() {
            return owner_keys;
        }
        let recent_distinct_repair = self.flights.range(..end).any(|(offset, flights)| {
            *offset < end
                && flights.iter().any(|flight| {
                    flight.end > start
                        && flight.kind == CarrierWorkKind::RepairData
                        && live_instances.contains(&flight.instance)
                        && !owner_keys.contains(&flight.instance.key)
                        && now.saturating_duration_since(flight.sent_at) < repeat_repair_after
                })
        });
        if recent_distinct_repair {
            Vec::new()
        } else {
            owner_keys
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn latest_unacked_ranges_for_path(
        &self,
        key: RelayPathKey,
    ) -> Vec<OffsetRange> {
        let mut ranges = Vec::new();
        for (offset, flights) in &self.flights {
            let Some(latest) = latest_ordering_owner(flights) else {
                continue;
            };
            if latest.instance.key == key {
                ranges.push(OffsetRange {
                    start: *offset,
                    end: latest.end,
                });
            }
        }
        normalized_offset_ranges(&ranges)
    }

    pub(in crate::runtime) fn latest_unacked_ranges_for_path_instance(
        &self,
        instance: RelayPathInstance,
    ) -> Vec<OffsetRange> {
        let ranges = self
            .flights
            .iter()
            .filter_map(|(offset, flights)| {
                latest_ordering_owner(flights)
                    .filter(|owner| owner.instance == instance)
                    .map(|owner| OffsetRange {
                        start: *offset,
                        end: owner.end,
                    })
            })
            .collect::<Vec<_>>();
        normalized_offset_ranges(&ranges)
    }

    pub(in crate::runtime) fn ordering_owner_instances(&self) -> Vec<RelayPathInstance> {
        let mut instances = Vec::new();
        for flights in self.flights.values() {
            for flight in flights {
                if flight.kind.is_ordering_owner() && !instances.contains(&flight.instance) {
                    instances.push(flight.instance);
                }
            }
        }
        instances
    }

    pub(in crate::runtime) fn ordering_debt_bytes_before_offset(
        &self,
        key: RelayPathKey,
        offset: u64,
    ) -> u64 {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| {
                let latest = latest_ordering_owner(flights)?;
                (latest.instance.key != key).then_some(latest.bytes as u64)
            })
            .sum()
    }

    pub(in crate::runtime) fn ordering_owner_bytes_before_offset(&self, offset: u64) -> u64 {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| latest_ordering_owner(flights))
            .map(|owner| owner.bytes as u64)
            .sum()
    }

    pub(in crate::runtime) fn has_ordering_owner_flights_for_instance(
        &self,
        instance: RelayPathInstance,
    ) -> bool {
        self.flights.values().any(|flights| {
            flights
                .iter()
                .any(|flight| flight.instance == instance && flight.kind.is_ordering_owner())
        })
    }

    pub(in crate::runtime) fn has_foreign_ordering_owner_before_offset(
        &self,
        offset: u64,
        allowed: &[RelayPathKey],
    ) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights.iter().any(|flight| {
                flight.kind.is_ordering_owner() && !allowed.contains(&flight.instance.key)
            })
        })
    }

    pub(in crate::runtime) fn foreign_ordering_owner_debt_before_offset(
        &self,
        offset: u64,
        allowed: &[RelayPathKey],
    ) -> (usize, u64) {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| {
                flights
                    .iter()
                    .find(|flight| {
                        flight.kind.is_ordering_owner() && !allowed.contains(&flight.instance.key)
                    })
                    .map(|flight| flight.bytes as u64)
            })
            .fold((0usize, 0u64), |(ranges, bytes), flight_bytes| {
                (ranges.saturating_add(1), bytes.saturating_add(flight_bytes))
            })
    }

    pub(in crate::runtime) fn has_repair_flights_before_offset(&self, offset: u64) -> bool {
        self.flights.range(..offset).any(|(_, flights)| {
            flights
                .iter()
                .any(|flight| flight.kind == CarrierWorkKind::RepairData)
        })
    }

    pub(in crate::runtime) fn oldest_lower_flight_owner_before_offset(
        &self,
        offset: u64,
    ) -> Option<RelayPathKey> {
        self.flights.range(..offset).find_map(|(_, flights)| {
            latest_ordering_owner(flights).map(|flight| flight.instance.key)
        })
    }
}

fn latest_ordering_owner(flights: &[RequestFlight]) -> Option<&RequestFlight> {
    flights
        .iter()
        .rev()
        .find(|flight| flight.kind.is_ordering_owner())
}

fn request_ambiguous_flight_intervals(flights: &[(u64, RequestFlight)]) -> Vec<(u64, u64)> {
    let mut events = BTreeMap::<u64, i64>::new();
    for (start, flight) in flights {
        *events.entry(*start).or_default() += 1;
        *events.entry(flight.end).or_default() -= 1;
    }
    let mut intervals = Vec::new();
    let mut active = 0_i64;
    let mut previous = None;
    for (position, delta) in events {
        if let Some(previous) = previous
            && previous < position
            && active > 1
        {
            intervals.push((previous, position));
        }
        active += delta;
        previous = Some(position);
    }
    intervals
}

fn flight_intervals_overlap(intervals: &[(u64, u64)], start: u64, end: u64) -> bool {
    let position = intervals.partition_point(|(_, interval_end)| *interval_end <= start);
    intervals
        .get(position)
        .is_some_and(|(interval_start, _)| *interval_start < end)
}

struct FlightIntervalSplit {
    acked: Vec<(u64, u64)>,
    retained: Vec<(u64, u64)>,
}

fn split_flight_interval_by_ack(
    start: u64,
    end: u64,
    ranges: &[OffsetRange],
) -> FlightIntervalSplit {
    let mut acked = Vec::new();
    let mut retained = Vec::new();
    let mut cursor = start;
    for range in ranges {
        if range.end <= cursor {
            continue;
        }
        if range.start >= end {
            break;
        }
        let ack_start = cursor.max(range.start);
        if cursor < ack_start {
            retained.push((cursor, ack_start));
        }
        let ack_end = end.min(range.end);
        if ack_start < ack_end {
            acked.push((ack_start, ack_end));
            cursor = ack_end;
        }
        if cursor >= end {
            break;
        }
    }
    if cursor < end {
        retained.push((cursor, end));
    }
    FlightIntervalSplit { acked, retained }
}

fn flight_interval_bytes(start: u64, end: u64) -> usize {
    usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX)
}

#[derive(Debug, Clone, Copy)]
struct RequestFlight {
    instance: RelayPathInstance,
    end: u64,
    bytes: usize,
    sent_at: Instant,
    kind: CarrierWorkKind,
}

#[cfg(test)]
#[path = "request_test.rs"]
mod tests;
