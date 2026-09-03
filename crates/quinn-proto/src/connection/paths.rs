use std::{cmp, mem, net::SocketAddr};

use tracing::trace;

use super::{
    mtud::MtuDiscovery,
    pacing::Pacer,
    spaces::{PacketSpace, SentPacket},
};
use crate::{congestion, packet::SpaceId, Duration, Instant, TransportConfig, TIMER_GRANULARITY};

#[cfg(feature = "qlog")]
use qlog::events::quic::MetricsUpdated;

/// Description of a particular network path
pub(super) struct PathData {
    pub(super) remote: SocketAddr,
    pub(super) rtt: RttEstimator,
    /// Whether we're enabling ECN on outgoing packets
    pub(super) sending_ecn: bool,
    /// Congestion controller state
    pub(super) congestion: Box<dyn congestion::Controller>,
    /// Pacing state
    pub(super) pacing: Pacer,
    pub(super) challenge: Option<u64>,
    pub(super) challenge_pending: bool,
    /// Whether we're certain the peer can both send and receive on this address
    ///
    /// Initially equal to `use_stateless_retry` for servers, and becomes false again on every
    /// migration. Always true for clients.
    pub(super) validated: bool,
    /// Total size of all UDP datagrams sent on this path
    pub(super) total_sent: u64,
    /// Total size of all UDP datagrams received on this path
    pub(super) total_recvd: u64,
    /// The state of the MTU discovery process
    pub(super) mtud: MtuDiscovery,
    /// Packet number of the first packet sent after an RTT sample was collected on this path
    ///
    /// Used in persistent congestion determination.
    pub(super) first_packet_after_rtt_sample: Option<(SpaceId, u64)>,
    pub(super) in_flight: InFlight,
    /// Number of the first packet sent on this path
    ///
    /// Used to determine whether a packet was sent on an earlier path. Insufficient to determine if
    /// a packet was sent on a later path.
    first_packet: Option<u64>,

    /// Snapshot of the qlog recovery metrics
    #[cfg(feature = "qlog")]
    recovery_metrics: RecoveryMetrics,

    /// Tag uniquely identifying a path in a connection
    generation: u64,
    /// Identity of the congestion-controller model that owns packet callbacks.
    ///
    /// NAT rebinding preserves this value because it clones the same model. A fresh network path
    /// or explicit reset advances it so delayed evidence cannot mutate the replacement model.
    controller_epoch: u64,
}

impl PathData {
    pub(super) fn new(
        remote: SocketAddr,
        allow_mtud: bool,
        peer_max_udp_payload_size: Option<u16>,
        generation: u64,
        now: Instant,
        config: &TransportConfig,
    ) -> Self {
        let congestion = config
            .congestion_controller_factory
            .clone()
            .build(now, config.get_initial_mtu());
        Self::new_with_congestion(
            remote,
            allow_mtud,
            peer_max_udp_payload_size,
            generation,
            now,
            config,
            congestion,
        )
    }

    pub(super) fn for_new_network_path(
        remote: SocketAddr,
        previous: &Self,
        allow_mtud: bool,
        peer_max_udp_payload_size: Option<u16>,
        generation: u64,
        now: Instant,
        config: &TransportConfig,
    ) -> Self {
        let current_mtu = config.get_initial_mtu();
        let congestion = previous
            .congestion
            .fresh_path_box(now, current_mtu)
            .unwrap_or_else(|| {
                config
                    .congestion_controller_factory
                    .clone()
                    .build(now, current_mtu)
            });
        Self::new_with_congestion(
            remote,
            allow_mtud,
            peer_max_udp_payload_size,
            generation,
            now,
            config,
            congestion,
        )
    }

    fn new_with_congestion(
        remote: SocketAddr,
        allow_mtud: bool,
        peer_max_udp_payload_size: Option<u16>,
        generation: u64,
        now: Instant,
        config: &TransportConfig,
        congestion: Box<dyn congestion::Controller>,
    ) -> Self {
        Self {
            remote,
            rtt: RttEstimator::new(config.initial_rtt),
            sending_ecn: true,
            pacing: Pacer::new(
                config.initial_rtt,
                congestion.initial_window(),
                config.get_initial_mtu(),
                now,
            ),
            congestion,
            challenge: None,
            challenge_pending: false,
            validated: false,
            total_sent: 0,
            total_recvd: 0,
            mtud: config
                .mtu_discovery_config
                .as_ref()
                .filter(|_| allow_mtud)
                .map_or(
                    MtuDiscovery::disabled(config.get_initial_mtu(), config.min_mtu),
                    |mtud_config| {
                        MtuDiscovery::new(
                            config.get_initial_mtu(),
                            config.min_mtu,
                            peer_max_udp_payload_size,
                            mtud_config.clone(),
                        )
                    },
                ),
            first_packet_after_rtt_sample: None,
            in_flight: InFlight::new(),
            first_packet: None,
            #[cfg(feature = "qlog")]
            recovery_metrics: RecoveryMetrics::default(),
            generation,
            controller_epoch: generation,
        }
    }

    pub(super) fn from_previous(
        remote: SocketAddr,
        prev: &Self,
        generation: u64,
        now: Instant,
    ) -> Self {
        let congestion = prev.congestion.clone_box();
        let smoothed_rtt = prev.rtt.get();
        Self {
            remote,
            rtt: prev.rtt,
            pacing: Pacer::new(smoothed_rtt, congestion.window(), prev.current_mtu(), now),
            sending_ecn: true,
            congestion,
            challenge: None,
            challenge_pending: false,
            validated: false,
            total_sent: 0,
            total_recvd: 0,
            mtud: prev.mtud.clone(),
            first_packet_after_rtt_sample: prev.first_packet_after_rtt_sample,
            in_flight: InFlight::new(),
            first_packet: None,
            #[cfg(feature = "qlog")]
            recovery_metrics: prev.recovery_metrics.clone(),
            generation,
            controller_epoch: prev.controller_epoch,
        }
    }

    /// Publish this already-installed initial controller as the active path.
    /// The connection is not externally visible until this transaction has
    /// completed.
    pub(super) fn activate_initial(
        &mut self,
    ) -> Result<(), congestion::ControllerActivationTransitionError> {
        let Some(fence) = self.congestion.activation_fence() else {
            return Ok(());
        };
        let mut transition = match fence.begin_transition() {
            Ok(transition) => transition,
            Err(_) => {
                self.congestion.on_activation_terminal();
                return Err(congestion::ControllerActivationTransitionError::FencePoisoned);
            }
        };
        let activation = match transition.allocate() {
            Ok(activation) => activation,
            Err(exhausted) => {
                if exhausted.publish_terminal() {
                    self.congestion.on_activation_terminal();
                }
                return Err(congestion::ControllerActivationTransitionError::Exhausted);
            }
        };
        self.congestion.on_activated(activation);
        transition.publish();
        self.congestion.on_activation_published();
        Ok(())
    }

    /// Install `replacement` as the exact active path under its shared
    /// activation fence and return the previously active path.
    pub(super) fn replace_with_activated(
        &mut self,
        mut replacement: Self,
    ) -> Result<Self, congestion::ControllerActivationTransitionError> {
        let Some(fence) = self.require_shared_activation_fence(replacement.congestion.as_ref())?
        else {
            return Ok(mem::replace(self, replacement));
        };
        let mut transition = match fence.begin_transition() {
            Ok(transition) => transition,
            Err(_) => {
                replacement.congestion.on_activation_terminal();
                return Err(congestion::ControllerActivationTransitionError::FencePoisoned);
            }
        };
        let activation = match transition.allocate() {
            Ok(activation) => activation,
            Err(exhausted) => {
                if exhausted.publish_terminal() {
                    replacement.congestion.on_activation_terminal();
                }
                return Err(congestion::ControllerActivationTransitionError::Exhausted);
            }
        };
        replacement.congestion.on_activated(activation);
        let previous = mem::replace(self, replacement);
        transition.publish();
        self.congestion.on_activation_published();
        Ok(previous)
    }

    fn require_shared_activation_fence(
        &self,
        replacement: &dyn congestion::Controller,
    ) -> Result<
        Option<congestion::ControllerActivationFence>,
        congestion::ControllerActivationTransitionError,
    > {
        let current = self.congestion.activation_fence();
        let next = replacement.activation_fence();
        match (&current, &next) {
            (None, None) => Ok(None),
            (Some(current), Some(next)) if current.same_owner(next) => Ok(Some(next.clone())),
            _ => {
                if let Some(fence) = current.as_ref() {
                    let _ = fence.terminalize(
                        congestion::ControllerActivationTerminal::FenceMismatch,
                        || self.congestion.on_activation_terminal(),
                    );
                }
                if let Some(fence) = next.as_ref() {
                    let _ = fence.terminalize(
                        congestion::ControllerActivationTerminal::FenceMismatch,
                        || replacement.on_activation_terminal(),
                    );
                }
                Err(congestion::ControllerActivationTransitionError::FenceMismatch)
            }
        }
    }

    pub(super) fn terminalize_activation(&self, reason: congestion::ControllerActivationTerminal) {
        if let Some(fence) = self.congestion.activation_fence() {
            let _ = fence.terminalize(reason, || self.congestion.on_activation_terminal());
        }
    }

    /// Resets RTT, congestion control and MTU states.
    ///
    /// This is useful when it is known the underlying path has changed.
    pub(super) fn reset(
        &mut self,
        now: Instant,
        config: &TransportConfig,
        controller_epoch: u64,
    ) -> Result<(), congestion::ControllerActivationTransitionError> {
        let current_mtu = config.get_initial_mtu();
        let mut replacement = self
            .congestion
            .fresh_path_box(now, current_mtu)
            .unwrap_or_else(|| {
                config
                    .congestion_controller_factory
                    .clone()
                    .build(now, current_mtu)
            });
        let Some(fence) = self.require_shared_activation_fence(replacement.as_ref())? else {
            self.install_reset_state(replacement, config, controller_epoch);
            return Ok(());
        };
        let mut transition = match fence.begin_transition() {
            Ok(transition) => transition,
            Err(_) => {
                replacement.on_activation_terminal();
                return Err(congestion::ControllerActivationTransitionError::FencePoisoned);
            }
        };
        let activation = match transition.allocate() {
            Ok(activation) => activation,
            Err(exhausted) => {
                if exhausted.publish_terminal() {
                    replacement.on_activation_terminal();
                }
                return Err(congestion::ControllerActivationTransitionError::Exhausted);
            }
        };
        replacement.on_activated(activation);
        self.install_reset_state(replacement, config, controller_epoch);
        transition.publish();
        self.congestion.on_activation_published();
        Ok(())
    }

    fn install_reset_state(
        &mut self,
        congestion: Box<dyn congestion::Controller>,
        config: &TransportConfig,
        controller_epoch: u64,
    ) {
        self.rtt = RttEstimator::new(config.initial_rtt);
        self.congestion = congestion;
        self.controller_epoch = controller_epoch;
        self.first_packet_after_rtt_sample = None;
        self.mtud.reset(config.get_initial_mtu(), config.min_mtu);
    }

    /// Indicates whether we're a server that hasn't validated the peer's address and hasn't
    /// received enough data from the peer to permit sending `bytes_to_send` additional bytes
    pub(super) fn anti_amplification_blocked(&self, bytes_to_send: u64) -> bool {
        !self.validated && self.total_recvd * 3 < self.total_sent + bytes_to_send
    }

    /// Returns the path's current MTU
    pub(super) fn current_mtu(&self) -> u16 {
        self.mtud.current_mtu()
    }

    /// Account for transmission of `packet` with number `pn` in `space`
    pub(super) fn sent(&mut self, pn: u64, packet: SentPacket, space: &mut PacketSpace) {
        self.in_flight.insert(&packet);
        if self.first_packet.is_none() {
            self.first_packet = Some(pn);
        }
        if let Some(forgotten) = space.sent(pn, packet) {
            self.remove_in_flight(&forgotten);
        }
    }

    /// Remove `packet` with number `pn` from this path's congestion control counters, or return
    /// `false` if `pn` was sent before this path was established.
    pub(super) fn remove_in_flight(&mut self, packet: &SentPacket) -> bool {
        if packet.path_generation != self.generation {
            return false;
        }
        self.in_flight.remove(packet);
        true
    }

    #[cfg(feature = "qlog")]
    pub(super) fn qlog_recovery_metrics(&mut self, pto_count: u32) -> Option<MetricsUpdated> {
        let controller_metrics = self.congestion.metrics();

        let metrics = RecoveryMetrics {
            min_rtt: Some(self.rtt.min),
            smoothed_rtt: Some(self.rtt.get()),
            latest_rtt: Some(self.rtt.latest),
            rtt_variance: Some(self.rtt.var),
            pto_count: Some(pto_count),
            bytes_in_flight: Some(self.in_flight.bytes),
            packets_in_flight: Some(self.in_flight.ack_eliciting),

            congestion_window: Some(controller_metrics.congestion_window),
            ssthresh: controller_metrics.ssthresh,
            pacing_rate: controller_metrics.pacing_rate,
        };

        let event = metrics.to_qlog_event(&self.recovery_metrics);
        self.recovery_metrics = metrics;
        event
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn controller_epoch(&self) -> u64 {
        self.controller_epoch
    }
}

/// Congestion metrics as described in [`recovery_metrics_updated`].
///
/// [`recovery_metrics_updated`]: https://datatracker.ietf.org/doc/html/draft-ietf-quic-qlog-quic-events.html#name-recovery_metrics_updated
#[cfg(feature = "qlog")]
#[derive(Default, Clone, PartialEq)]
#[non_exhaustive]
struct RecoveryMetrics {
    pub min_rtt: Option<Duration>,
    pub smoothed_rtt: Option<Duration>,
    pub latest_rtt: Option<Duration>,
    pub rtt_variance: Option<Duration>,
    pub pto_count: Option<u32>,
    pub bytes_in_flight: Option<u64>,
    pub packets_in_flight: Option<u64>,
    pub congestion_window: Option<u64>,
    pub ssthresh: Option<u64>,
    pub pacing_rate: Option<u64>,
}

#[cfg(feature = "qlog")]
impl RecoveryMetrics {
    /// Retain only values that have been updated since the last snapshot.
    fn retain_updated(&self, previous: &Self) -> Self {
        macro_rules! keep_if_changed {
            ($name:ident) => {
                if previous.$name == self.$name {
                    None
                } else {
                    self.$name
                }
            };
        }

        Self {
            min_rtt: keep_if_changed!(min_rtt),
            smoothed_rtt: keep_if_changed!(smoothed_rtt),
            latest_rtt: keep_if_changed!(latest_rtt),
            rtt_variance: keep_if_changed!(rtt_variance),
            pto_count: keep_if_changed!(pto_count),
            bytes_in_flight: keep_if_changed!(bytes_in_flight),
            packets_in_flight: keep_if_changed!(packets_in_flight),
            congestion_window: keep_if_changed!(congestion_window),
            ssthresh: keep_if_changed!(ssthresh),
            pacing_rate: keep_if_changed!(pacing_rate),
        }
    }

    /// Emit a `MetricsUpdated` event containing only updated values
    fn to_qlog_event(&self, previous: &Self) -> Option<MetricsUpdated> {
        let updated = self.retain_updated(previous);

        if updated == Self::default() {
            return None;
        }

        Some(MetricsUpdated {
            min_rtt: updated.min_rtt.map(|rtt| rtt.as_secs_f32()),
            smoothed_rtt: updated.smoothed_rtt.map(|rtt| rtt.as_secs_f32()),
            latest_rtt: updated.latest_rtt.map(|rtt| rtt.as_secs_f32()),
            rtt_variance: updated.rtt_variance.map(|rtt| rtt.as_secs_f32()),
            pto_count: updated
                .pto_count
                .map(|count| count.try_into().unwrap_or(u16::MAX)),
            bytes_in_flight: updated.bytes_in_flight,
            packets_in_flight: updated.packets_in_flight,
            congestion_window: updated.congestion_window,
            ssthresh: updated.ssthresh,
            pacing_rate: updated.pacing_rate,
        })
    }
}

/// RTT estimation for a particular network path
#[derive(Copy, Clone)]
pub struct RttEstimator {
    /// The most recent RTT measurement made when receiving an ack for a previously unacked packet
    latest: Duration,
    /// The smoothed RTT of the connection, computed as described in RFC6298
    smoothed: Option<Duration>,
    /// The RTT variance, computed as described in RFC6298
    var: Duration,
    /// The minimum RTT seen in the connection, ignoring ack delay.
    min: Duration,
}

impl RttEstimator {
    pub(crate) fn new(initial_rtt: Duration) -> Self {
        Self {
            latest: initial_rtt,
            smoothed: None,
            var: initial_rtt / 2,
            min: initial_rtt,
        }
    }

    /// The current best RTT estimation.
    pub fn get(&self) -> Duration {
        self.smoothed.unwrap_or(self.latest)
    }

    /// Conservative estimate of RTT
    ///
    /// Takes the maximum of smoothed and latest RTT, as recommended
    /// in 6.1.2 of the recovery spec (draft 29).
    pub fn conservative(&self) -> Duration {
        self.get().max(self.latest)
    }

    /// Minimum RTT registered so far for this estimator.
    pub fn min(&self) -> Duration {
        self.min
    }

    /// Current RTT-variation estimate used by QUIC recovery.
    pub(crate) fn variance(&self) -> Duration {
        self.var
    }

    // PTO computed as described in RFC9002#6.2.1
    pub(crate) fn pto_base(&self) -> Duration {
        self.get() + cmp::max(4 * self.var, TIMER_GRANULARITY)
    }

    pub(crate) fn update(&mut self, ack_delay: Duration, rtt: Duration) {
        self.latest = rtt;
        // min_rtt ignores ack delay.
        self.min = cmp::min(self.min, self.latest);
        // Based on RFC6298.
        if let Some(smoothed) = self.smoothed {
            let adjusted_rtt = if self.min + ack_delay <= self.latest {
                self.latest - ack_delay
            } else {
                self.latest
            };
            let var_sample = smoothed.abs_diff(adjusted_rtt);
            self.var = (3 * self.var + var_sample) / 4;
            self.smoothed = Some((7 * smoothed + adjusted_rtt) / 8);
        } else {
            self.smoothed = Some(self.latest);
            self.var = self.latest / 2;
            self.min = self.latest;
        }
    }
}

#[derive(Default)]
pub(crate) struct PathResponses {
    pending: Vec<PathResponse>,
}

impl PathResponses {
    pub(crate) fn push(&mut self, packet: u64, token: u64, remote: SocketAddr) {
        /// Arbitrary permissive limit to prevent abuse
        const MAX_PATH_RESPONSES: usize = 16;
        let response = PathResponse {
            packet,
            token,
            remote,
        };
        let existing = self.pending.iter_mut().find(|x| x.remote == remote);
        if let Some(existing) = existing {
            // Update a queued response
            if existing.packet <= packet {
                *existing = response;
            }
            return;
        }
        if self.pending.len() < MAX_PATH_RESPONSES {
            self.pending.push(response);
        } else {
            // We don't expect to ever hit this with well-behaved peers, so we don't bother dropping
            // older challenges.
            trace!("ignoring excessive PATH_CHALLENGE");
        }
    }

    pub(crate) fn pop_off_path(&mut self, remote: SocketAddr) -> Option<(u64, SocketAddr)> {
        let response = *self.pending.last()?;
        if response.remote == remote {
            // We don't bother searching further because we expect that the on-path response will
            // get drained in the immediate future by a call to `pop_on_path`
            return None;
        }
        self.pending.pop();
        Some((response.token, response.remote))
    }

    pub(crate) fn pop_on_path(&mut self, remote: SocketAddr) -> Option<u64> {
        let response = *self.pending.last()?;
        if response.remote != remote {
            // We don't bother searching further because we expect that the off-path response will
            // get drained in the immediate future by a call to `pop_off_path`
            return None;
        }
        self.pending.pop();
        Some(response.token)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[derive(Copy, Clone)]
struct PathResponse {
    /// The packet number the corresponding PATH_CHALLENGE was received in
    packet: u64,
    token: u64,
    /// The address the corresponding PATH_CHALLENGE was received from
    remote: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::congestion::{
        Controller, ControllerActivationFence, ControllerActivationState,
        ControllerActivationTerminal, ControllerActivationTransitionError, ControllerFactory,
    };
    use std::any::Any;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct EpochController {
        lineage: u64,
        epoch: u64,
        supports_fresh_path: bool,
    }

    impl Controller for EpochController {
        fn on_congestion_event(
            &mut self,
            _now: Instant,
            _sent: Instant,
            _is_persistent_congestion: bool,
            _is_ecn: bool,
            _lost_bytes: u64,
            _largest_lost: u64,
            _space: crate::packet::SpaceId,
        ) {
        }

        fn on_mtu_update(&mut self, _new_mtu: u16) {}

        fn window(&self) -> u64 {
            12_000
        }

        fn clone_box(&self) -> Box<dyn Controller> {
            Box::new(self.clone())
        }

        fn fresh_path_box(&self, _now: Instant, _current_mtu: u16) -> Option<Box<dyn Controller>> {
            self.supports_fresh_path.then(|| {
                Box::new(Self {
                    lineage: self.lineage,
                    epoch: self.epoch + 1,
                    supports_fresh_path: true,
                }) as Box<dyn Controller>
            })
        }

        fn initial_window(&self) -> u64 {
            self.window()
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    struct EpochControllerFactory {
        builds: AtomicU64,
        supports_fresh_path: bool,
    }

    #[derive(Clone)]
    struct FencedController {
        fence: ControllerActivationFence,
    }

    impl Controller for FencedController {
        fn activation_fence(&self) -> Option<ControllerActivationFence> {
            Some(self.fence.clone())
        }

        fn on_congestion_event(
            &mut self,
            _now: Instant,
            _sent: Instant,
            _is_persistent_congestion: bool,
            _is_ecn: bool,
            _lost_bytes: u64,
            _largest_lost: u64,
            _space: crate::packet::SpaceId,
        ) {
        }

        fn on_mtu_update(&mut self, _new_mtu: u16) {}

        fn window(&self) -> u64 {
            12_000
        }

        fn clone_box(&self) -> Box<dyn Controller> {
            Box::new(self.clone())
        }

        fn initial_window(&self) -> u64 {
            self.window()
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    struct FencedControllerFactory {
        fence: ControllerActivationFence,
    }

    impl ControllerFactory for FencedControllerFactory {
        fn build(self: Arc<Self>, _now: Instant, _current_mtu: u16) -> Box<dyn Controller> {
            Box::new(FencedController {
                fence: self.fence.clone(),
            })
        }
    }

    impl ControllerFactory for EpochControllerFactory {
        fn build(self: Arc<Self>, _now: Instant, _current_mtu: u16) -> Box<dyn Controller> {
            Box::new(EpochController {
                lineage: self.builds.fetch_add(1, Ordering::Relaxed) + 1,
                epoch: 1,
                supports_fresh_path: self.supports_fresh_path,
            })
        }
    }

    fn controller(path: &PathData) -> EpochController {
        *path
            .congestion
            .clone_box()
            .into_any()
            .downcast::<EpochController>()
            .expect("epoch controller")
    }

    fn path(config: &TransportConfig, generation: u64, now: Instant) -> PathData {
        PathData::new(
            "127.0.0.1:443".parse().expect("test address"),
            false,
            None,
            generation,
            now,
            config,
        )
    }

    #[test]
    fn replacement_with_a_different_activation_fence_fails_closed() {
        let now = Instant::now();
        let current_fence = ControllerActivationFence::new();
        let replacement_fence = ControllerActivationFence::new();
        let mut current_config = TransportConfig::default();
        current_config.congestion_controller_factory(Arc::new(FencedControllerFactory {
            fence: current_fence.clone(),
        }));
        let mut replacement_config = TransportConfig::default();
        replacement_config.congestion_controller_factory(Arc::new(FencedControllerFactory {
            fence: replacement_fence.clone(),
        }));
        let mut current = path(&current_config, 0, now);
        current.activate_initial().expect("initial activation");
        let mut replacement = path(&replacement_config, 1, now);
        replacement
            .activate_initial()
            .expect("replacement's independent activation");
        let original_remote = current.remote;

        assert!(matches!(
            current.replace_with_activated(replacement),
            Err(ControllerActivationTransitionError::FenceMismatch)
        ));
        assert_eq!(
            current.remote, original_remote,
            "active pointer did not switch"
        );
        assert_eq!(
            current_fence
                .with_current(|state| state)
                .expect("current terminal fence"),
            ControllerActivationState::Terminal(ControllerActivationTerminal::FenceMismatch)
        );
        assert_eq!(
            replacement_fence
                .with_current(|state| state)
                .expect("replacement terminal fence"),
            ControllerActivationState::Terminal(ControllerActivationTerminal::FenceMismatch)
        );
    }

    #[test]
    fn nat_clone_keeps_epoch_but_new_network_path_and_reset_advance_it() {
        let factory = Arc::new(EpochControllerFactory {
            builds: AtomicU64::new(0),
            supports_fresh_path: true,
        });
        let mut config = TransportConfig::default();
        config.congestion_controller_factory(factory.clone());
        let now = Instant::now();
        let initial = path(&config, 0, now);
        assert_eq!(initial.controller_epoch(), 0);

        let rebound = PathData::from_previous(
            "127.0.0.1:8443".parse().expect("rebound address"),
            &initial,
            1,
            now,
        );
        assert_eq!(controller(&rebound).lineage, controller(&initial).lineage);
        assert_eq!(controller(&rebound).epoch, controller(&initial).epoch);
        assert_eq!(rebound.controller_epoch(), initial.controller_epoch());

        let mut migrated = PathData::for_new_network_path(
            "[::1]:443".parse().expect("new network address"),
            &rebound,
            false,
            None,
            2,
            now,
            &config,
        );
        assert_eq!(controller(&migrated).lineage, controller(&initial).lineage);
        assert_eq!(controller(&migrated).epoch, controller(&initial).epoch + 1);
        assert_eq!(migrated.controller_epoch(), 2);
        assert_eq!(factory.builds.load(Ordering::Relaxed), 1);

        migrated.reset(now, &config, 3).expect("controller reset");
        assert_eq!(controller(&migrated).lineage, controller(&initial).lineage);
        assert_eq!(controller(&migrated).epoch, controller(&initial).epoch + 2);
        assert_eq!(migrated.controller_epoch(), 3);
        assert_eq!(factory.builds.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn controllers_without_fresh_path_hook_keep_factory_replacement() {
        let factory = Arc::new(EpochControllerFactory {
            builds: AtomicU64::new(0),
            supports_fresh_path: false,
        });
        let mut config = TransportConfig::default();
        config.congestion_controller_factory(factory.clone());
        let now = Instant::now();
        let initial = path(&config, 0, now);
        let migrated = PathData::for_new_network_path(
            "[::1]:443".parse().expect("new network address"),
            &initial,
            false,
            None,
            1,
            now,
            &config,
        );

        assert_ne!(controller(&migrated).lineage, controller(&initial).lineage);
        assert_eq!(factory.builds.load(Ordering::Relaxed), 2);
    }
}

/// Summary statistics of packets that have been sent on a particular path, but which have not yet
/// been acked or deemed lost
pub(super) struct InFlight {
    /// Sum of the sizes of all sent packets considered "in flight" by congestion control
    ///
    /// The size does not include IP or UDP overhead. Packets only containing ACK frames do not
    /// count towards this to ensure congestion control does not impede congestion feedback.
    pub(super) bytes: u64,
    /// Number of packets in flight containing frames other than ACK and PADDING
    ///
    /// This can be 0 even when bytes is not 0 because PADDING frames cause a packet to be
    /// considered "in flight" by congestion control. However, if this is nonzero, bytes will always
    /// also be nonzero.
    pub(super) ack_eliciting: u64,
}

impl InFlight {
    fn new() -> Self {
        Self {
            bytes: 0,
            ack_eliciting: 0,
        }
    }

    fn insert(&mut self, packet: &SentPacket) {
        self.bytes += u64::from(packet.size);
        self.ack_eliciting += u64::from(packet.ack_eliciting);
    }

    /// Update counters to account for a packet becoming acknowledged, lost, or abandoned
    fn remove(&mut self, packet: &SentPacket) {
        self.bytes -= u64::from(packet.size);
        self.ack_eliciting -= u64::from(packet.ack_eliciting);
    }
}
