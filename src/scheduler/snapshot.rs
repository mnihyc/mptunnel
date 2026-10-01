//! Producer projections for path decisions. Native and peer timing are private
//! and typed; completion policy receives only its restricted input view.

use super::policy::{PathRateScope, PathState};
use crate::model::advisory_score::DirectionalTiming;
use crate::model::path::PathPolicy;
use crate::model::service_rate::DirectionalServiceRate;
use crate::model::timing::{PathTiming, PeerTiming, TransportTiming};
use crate::protocol::{PathId, PathUsage, UnderlayProtocol};

#[derive(Debug, Clone, Copy)]
pub struct PathSnapshot {
    pub id: PathId,
    pub underlay: UnderlayProtocol,
    pub state: PathState,
    pub policy: PathPolicy,
    /// The receiver's directional preference for data sent by this endpoint.
    /// Local health and endpoint-local policy remain independent inputs.
    pub peer_usage: Option<PathUsage>,
    timing: PathTiming,
    pub delivery_rate_bps: f64,
    /// Exact rate input for RFC 10.2 advisory ranking. Generic snapshots and
    /// legacy fixtures may omit it; an exact production action must bind the
    /// carrier instance and original-sender direction before ranking.
    pub(crate) scheduling_service_rate: Option<DirectionalServiceRate>,
    /// Exact coherent timing input for RFC 10.2 advisory ranking.
    ///
    /// Legacy snapshots may omit it. This private typed sidecar does not alter
    /// legacy `score_path`; exact-action owners opt in only after their full
    /// transaction model is migrated.
    pub(crate) directional_timing: Option<DirectionalTiming>,
    pub rate_scope: PathRateScope,
    /// Qualified native carrier delivery capacity, when distinct from the
    /// product flow's completion rate.
    pub carrier_delivery_rate_bps: Option<f64>,
    pub product_progress_rate_bps: Option<f64>,
    /// Exact product ACK accounting satisfies the transport-specific durable
    /// sample threshold; a point rate alone is not admission evidence.
    pub has_durable_product_progress: bool,
    pub loss_rate: f64,
    /// Bytes waiting in the carrier-owned writer/socket queue.
    pub queue_bytes: u64,
    /// MPP bytes waiting above the carrier queue.
    pub data_level_queue_bytes: u64,
    /// Bytes reported in flight by the native carrier.
    pub bytes_in_flight: u64,
    /// MPP transmissions awaiting a Data ACK on this path.
    pub data_level_bytes_in_flight: u64,
    pub active_flows: u32,
    pub active_latency_sensitive_flows: u32,
    pub session_active_latency_sensitive_flows: u32,
    pub pacing_rate_bps: f64,
    /// Native carrier congestion-window or inflight credit; zero is unknown.
    pub carrier_inflight_limit_bytes: u64,
    /// Explicit MPP per-path scheduling window; zero requests model derivation.
    pub data_level_limit_bytes: u64,
    pub confidence: f64,
    pub app_limited: bool,
}

impl PathSnapshot {
    pub(crate) fn set_timing(&mut self, timing: PathTiming) {
        self.timing = timing;
    }

    pub(crate) fn transport_timing(self) -> TransportTiming {
        self.timing.transport()
    }
    pub(crate) fn peer_timing(self) -> PeerTiming {
        self.timing.peer()
    }

    #[cfg(test)]
    pub(crate) fn set_rtt_for_test(&mut self, srtt_ms: f64) {
        self.timing = PathTiming::startup(srtt_ms, self.timing.peer().rttvar_ms());
    }

    #[cfg(test)]
    pub(crate) fn set_rttvar_for_test(&mut self, rttvar_ms: f64) {
        self.timing = PathTiming::startup(self.timing.peer().srtt_ms(), rttvar_ms);
    }

    pub fn new(
        id: PathId,
        underlay: UnderlayProtocol,
        srtt_ms: f64,
        delivery_rate_bps: f64,
    ) -> Self {
        Self {
            id,
            underlay,
            state: PathState::Active,
            policy: PathPolicy::default(),
            peer_usage: None,
            timing: PathTiming::startup(srtt_ms, 0.0),
            delivery_rate_bps,
            scheduling_service_rate: None,
            directional_timing: None,
            rate_scope: PathRateScope::PathCapacity,
            carrier_delivery_rate_bps: None,
            product_progress_rate_bps: None,
            has_durable_product_progress: false,
            loss_rate: 0.0,
            queue_bytes: 0,
            data_level_queue_bytes: 0,
            bytes_in_flight: 0,
            data_level_bytes_in_flight: 0,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            session_active_latency_sensitive_flows: 0,
            pacing_rate_bps: delivery_rate_bps,
            carrier_inflight_limit_bytes: 0,
            data_level_limit_bytes: 0,
            confidence: 1.0,
            app_limited: false,
        }
    }

    pub(crate) fn with_scheduling_service_rate(
        mut self,
        service_rate: DirectionalServiceRate,
    ) -> Self {
        self.scheduling_service_rate = Some(service_rate);
        self
    }

    pub(crate) fn scheduling_service_rate(self) -> Option<DirectionalServiceRate> {
        self.scheduling_service_rate
    }

    pub(crate) fn with_directional_timing(mut self, timing: DirectionalTiming) -> Self {
        self.directional_timing = Some(timing);
        self
    }

    #[cfg(test)]
    pub(crate) fn directional_timing(self) -> Option<DirectionalTiming> {
        self.directional_timing
    }
}

/// Inputs admitted to completion ranking. The private borrow exposes only the
/// selected facts; native RTT, pacing, diagnostic rates and admission credit
/// are inaccessible to policy. It avoids rebuilding a second field bundle
/// for every score and cannot outlive the immutable producer snapshot.
#[derive(Clone, Copy)]
pub(super) struct CompletionPath<'a>(&'a PathSnapshot);

impl CompletionPath<'_> {
    #[inline]
    pub(super) fn id(self) -> PathId {
        self.0.id
    }

    #[inline]
    pub(super) fn state(self) -> PathState {
        self.0.state
    }

    #[inline]
    pub(super) fn policy(self) -> PathPolicy {
        self.0.policy
    }

    #[inline]
    pub(super) fn peer_usage(self) -> Option<PathUsage> {
        self.0.peer_usage
    }

    #[inline]
    pub(super) fn timing(self) -> PeerTiming {
        self.0.timing.peer()
    }

    #[inline]
    pub(super) fn delivery_rate_bps(self) -> f64 {
        self.0.delivery_rate_bps
    }

    #[inline]
    pub(super) fn rate_scope(self) -> PathRateScope {
        self.0.rate_scope
    }

    #[inline]
    pub(super) fn loss_rate(self) -> f64 {
        self.0.loss_rate
    }

    #[inline]
    pub(super) fn queue_bytes(self) -> u64 {
        self.0.queue_bytes
    }

    #[inline]
    pub(super) fn data_level_queue_bytes(self) -> u64 {
        self.0.data_level_queue_bytes
    }

    #[inline]
    pub(super) fn bytes_in_flight(self) -> u64 {
        self.0.bytes_in_flight
    }

    #[inline]
    pub(super) fn data_level_bytes_in_flight(self) -> u64 {
        self.0.data_level_bytes_in_flight
    }

    #[inline]
    pub(super) fn active_flows(self) -> u32 {
        self.0.active_flows
    }

    #[inline]
    pub(super) fn active_latency_sensitive_flows(self) -> u32 {
        self.0.active_latency_sensitive_flows
    }

    #[inline]
    pub(super) fn confidence(self) -> f64 {
        self.0.confidence
    }
}

impl PathSnapshot {
    #[inline]
    pub(super) fn completion(&self) -> CompletionPath<'_> {
        CompletionPath(self)
    }
}
