//! Logic for controlling the rate at which data is sent

use crate::connection::RttEstimator;
pub use crate::packet::SpaceId;
use crate::{Duration, Instant};
use std::any::Any;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, MutexGuard};

mod bbr;
mod bbr3;
mod cubic;
mod new_reno;

pub use bbr::{Bbr, BbrConfig};
pub use bbr3::{Bbr3, Bbr3Config};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};

/// Equality-only identity of one installed congestion-controller activation.
///
/// Values are transport-issued, nonzero, never reused within one activation
/// fence, and never equal to `u64::MAX`, which is reserved for terminal
/// exhaustion. The numeric value has no capacity, health, or ordering meaning
/// outside the fence implementation.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ControllerActivation(u64);

impl ControllerActivation {
    /// Opaque checked serial for binding this identity into an external
    /// authority stamp.
    ///
    /// The value is meaningful only for equality (and transport-issued
    /// non-reuse) within its owning [`ControllerActivationFence`]. It is not a
    /// rate, health score, or cross-fence order.
    pub fn opaque_serial(self) -> u64 {
        self.0
    }
}

/// Current state protected by a [`ControllerActivationFence`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ControllerActivationState {
    /// No controller has been installed through this fence yet.
    Uninitialized,
    /// The exact active controller carries this activation identity.
    Live(ControllerActivation),
    /// This fence is absorbing and cannot name a successor.
    Terminal(ControllerActivationTerminal),
}

/// Why an activation fence became absorbing.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ControllerActivationTerminal {
    /// The last live value was `u64::MAX - 1`; `u64::MAX` remains non-live.
    Exhausted,
    /// A replacement did not preserve the active controller's fence owner.
    FenceMismatch,
    /// An allocated transition returned without publishing its successor.
    AbandonedTransition,
    /// The owning QUIC connection is no longer live.
    ConnectionClosed,
}

/// A poisoned controller-activation transition fence.
///
/// Poison is fail-closed because a panic may have interrupted an active-path
/// pointer transition before its activation publication completed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ControllerActivationFencePoisoned;

/// Checked exhaustion of the controller-activation identity space.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ControllerActivationExhausted {
    publish_terminal: bool,
}

impl ControllerActivationExhausted {
    pub(crate) fn publish_terminal(self) -> bool {
        self.publish_terminal
    }
}

/// Failure to complete an exact active-controller transition.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ControllerActivationTransitionError {
    Exhausted,
    FencePoisoned,
    FenceMismatch,
}

/// Shared serialization fence for active-controller pointer transitions and
/// their activation publication.
///
/// Quinn holds this fence across install/reset/restore, local controller
/// activation, and publication of the new current identity. Consumers must use
/// [`Self::with_current`] for their final equality check and ownership transfer,
/// so a transport switch cannot linearize between those operations.
#[derive(Clone)]
pub struct ControllerActivationFence(Arc<ControllerActivationFenceInner>);

struct ControllerActivationFenceInner {
    state: Mutex<ControllerActivationState>,
}

impl std::fmt::Debug for ControllerActivationFence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControllerActivationFence")
            .finish_non_exhaustive()
    }
}

impl Default for ControllerActivationFence {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerActivationFence {
    /// Construct an uninitialized activation fence.
    pub fn new() -> Self {
        Self(Arc::new(ControllerActivationFenceInner {
            state: Mutex::new(ControllerActivationState::Uninitialized),
        }))
    }

    /// Inspect the exact current activation while excluding transport
    /// transitions for the complete duration of `inspect`.
    ///
    /// Code in `inspect` must not re-enter the owning Quinn connection, whose
    /// state lock is acquired before this fence on transport transitions.
    pub fn with_current<T>(
        &self,
        inspect: impl FnOnce(ControllerActivationState) -> T,
    ) -> Result<T, ControllerActivationFencePoisoned> {
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| ControllerActivationFencePoisoned)?;
        Ok(inspect(*state))
    }

    pub(crate) fn begin_transition(
        &self,
    ) -> Result<ControllerActivationTransition<'_>, ControllerActivationFencePoisoned> {
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| ControllerActivationFencePoisoned)?;
        Ok(ControllerActivationTransition {
            state,
            pending: None,
        })
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn terminalize(
        &self,
        reason: ControllerActivationTerminal,
        publish: impl FnOnce(),
    ) -> Result<bool, ControllerActivationFencePoisoned> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| ControllerActivationFencePoisoned)?;
        if matches!(*state, ControllerActivationState::Terminal(_)) {
            return Ok(false);
        }
        *state = ControllerActivationState::Terminal(reason);
        publish();
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn with_test_state(state: ControllerActivationState) -> Self {
        Self(Arc::new(ControllerActivationFenceInner {
            state: Mutex::new(state),
        }))
    }
}

pub(crate) struct ControllerActivationTransition<'a> {
    state: MutexGuard<'a, ControllerActivationState>,
    pending: Option<ControllerActivation>,
}

impl ControllerActivationTransition<'_> {
    pub(crate) fn allocate(
        &mut self,
    ) -> Result<ControllerActivation, ControllerActivationExhausted> {
        debug_assert!(self.pending.is_none(), "one activation per transition");
        let current = match *self.state {
            ControllerActivationState::Uninitialized => 0,
            ControllerActivationState::Live(activation) => activation.0,
            ControllerActivationState::Terminal(_) => {
                return Err(ControllerActivationExhausted {
                    publish_terminal: false,
                });
            }
        };
        let Some(next) = current.checked_add(1).filter(|next| *next < u64::MAX) else {
            *self.state =
                ControllerActivationState::Terminal(ControllerActivationTerminal::Exhausted);
            return Err(ControllerActivationExhausted {
                publish_terminal: true,
            });
        };
        let activation = ControllerActivation(next);
        self.pending = Some(activation);
        Ok(activation)
    }

    pub(crate) fn publish(&mut self) {
        let activation = self
            .pending
            .take()
            .expect("an activation must be allocated before publication");
        *self.state = ControllerActivationState::Live(activation);
    }
}

impl Drop for ControllerActivationTransition<'_> {
    fn drop(&mut self) {
        if self.pending.take().is_some() {
            // An allocated successor that was not published cannot leave the
            // predecessor live: the pointer transaction may already have
            // started. Fail closed instead of making the old identity appear
            // current again.
            *self.state = ControllerActivationState::Terminal(
                ControllerActivationTerminal::AbandonedTransition,
            );
        }
    }
}

/// Opaque identity of one congestion-controller recovery/undo transaction.
///
/// The transport retains this with declared-loss evidence so a late ACK can only undo the exact
/// model snapshot created by the corresponding RFC 9002 recovery episode.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RecoveryTransactionId(pub(crate) u64);

impl RecoveryTransactionId {
    /// Construct an identity unique within one controller instance.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }
}

/// Provenance of the most recently completed native delivery-rate sample.
///
/// The record is immutable once published. Its revision is local to one
/// concrete controller lineage: state clones retain it, while a fresh path
/// starts without a sample.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BandwidthSample {
    /// Checked, nonzero revision advanced for every completed sample,
    /// including samples whose delivery interval is invalid.
    pub revision: NonZeroU64,
    /// Whether the raw delivery-rate sample is finite and strictly positive
    /// over a valid, nonzero interval.
    pub valid: bool,
    /// Packet-number space of the packet selected by the native sampler.
    pub source_space: SpaceId,
    /// Packet number selected by the native sampler.
    pub source_packet_number: u64,
    /// Packet-timed round recorded when the selected packet was sent.
    pub source_round: u64,
    /// Exact application-limited state recorded when the selected packet was
    /// sent.
    pub app_limited: bool,
}

/// Common interface for different congestion controllers
pub trait Controller: Send + Sync {
    /// Optional shared fence for exact active-controller transitions.
    ///
    /// Controllers that opt in must return clones of one stable fence from all
    /// path-state clones and fresh replacements belonging to the same
    /// connection. Quinn does not call the activation hooks below when this is
    /// `None`.
    fn activation_fence(&self) -> Option<ControllerActivationFence> {
        None
    }

    /// Bind this concrete controller object to a newly allocated activation.
    ///
    /// Quinn calls this while holding the controller's activation fence and
    /// before publishing the fence's new current value.
    #[allow(unused_variables)]
    fn on_activated(&mut self, activation: ControllerActivation) {}

    /// Publish a durable/coalescing wake after activation and pointer state are
    /// coherent, but before releasing the activation fence.
    fn on_activation_published(&self) {}

    /// Publish a durable/coalescing wake for fail-closed terminal activation
    /// state. Checked exhaustion invokes this while the fence remains held.
    fn on_activation_terminal(&self) {}

    /// The application protocol has authenticated and may begin using
    /// congestion-controller evidence.
    ///
    /// Callers serialize this notification with packet callbacks. Controllers
    /// without application-aware instrumentation may ignore it.
    fn on_application_ready(&mut self) {}

    /// One or more packets were just sent
    #[allow(unused_variables)]
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {}

    /// Snapshot delivery state when an ack-eliciting packet enters flight.
    #[allow(unused_variables)]
    fn on_packet_sent(
        &mut self,
        now: Instant,
        bytes: u16,
        prior_in_flight: u64,
        packet_number: u64,
        space: SpaceId,
        app_limited: bool,
    ) -> Option<PacketDeliveryState> {
        None
    }

    /// The connection had data to send but was blocked by the congestion window.
    fn on_cwnd_limited(&mut self) {}

    /// Packet deliveries were confirmed
    ///
    /// `app_limited` indicates whether the connection was blocked on outgoing
    /// application data prior to receiving these acknowledgements.
    #[allow(unused_variables)]
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        packet_number: u64,
        space: SpaceId,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// Confirm delivery using the packet's send-time delivery snapshot when available.
    #[allow(unused_variables)]
    fn on_ack_with_packet_state(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        packet_number: u64,
        space: SpaceId,
        packet_state: Option<PacketDeliveryState>,
        rtt: &RttEstimator,
    ) {
        self.on_ack(now, sent, bytes, packet_number, space, app_limited, rtt);
    }

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those batches has completed.
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
        space: SpaceId,
    ) {
    }

    /// Packets were deemed lost or marked congested
    ///
    /// `in_persistent_congestion` indicates whether all packets sent within the persistent
    /// congestion threshold period ending when the most recent packet in this batch was sent were
    /// lost.
    /// `lost_bytes` indicates how many bytes were lost. This value will be 0 for ECN triggers.
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        is_ecn: bool,
        lost_bytes: u64,
        largest_lost: u64,
        space: SpaceId,
    );

    /// One packet was just lost.
    #[allow(unused_variables)]
    fn on_packet_lost(
        &mut self,
        lost_bytes: u16,
        packet_number: u64,
        space: SpaceId,
        now: Instant,
    ) -> Option<RecoveryTransactionId> {
        None
    }

    /// All retained packets from one recovery transaction were acknowledged late.
    #[allow(unused_variables)]
    fn on_spurious_congestion_event(&mut self, transaction: RecoveryTransactionId) -> bool {
        false
    }

    /// Retained evidence for a recovery transaction expired or became ambiguous.
    #[allow(unused_variables)]
    fn on_recovery_transaction_abandoned(&mut self, transaction: RecoveryTransactionId) {}

    /// A transport-valid CE interval makes any packet-loss undo snapshot ambiguous.
    fn on_validated_ecn_congestion_event(&mut self) {}

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// The peer's ACK-frequency parameters have changed.
    #[allow(unused_variables)]
    fn on_ack_frequency_update(
        &mut self,
        ack_eliciting_threshold: u64,
        requested_max_ack_delay: Duration,
    ) {
    }

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Retrieve implementation-specific metrics used to populate `qlog` traces when they are enabled
    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            pacing_rate: None,
            bandwidth_estimate: None,
            send_quantum: None,
        }
    }

    /// Provenance of the latest completed native delivery-rate sample.
    ///
    /// This is separate from [`ControllerMetrics`] because metrics are read on
    /// every transmit poll, while sample provenance changes only at the end of
    /// an ACK batch.
    fn latest_bandwidth_sample(&self) -> Option<BandwidthSample> {
        None
    }

    /// Legacy compatibility hook for downstream controllers.
    ///
    /// Quinn's pacer consumes [`ControllerMetrics::pacing_rate`] exclusively.
    fn pacing_rate(&self) -> Option<u64> {
        self.metrics().pacing_rate
    }

    /// Duplicate the controller's state
    fn clone_box(&self) -> Box<dyn Controller>;

    /// Construct fresh congestion state for a different network path.
    ///
    /// Controllers with connection-scoped instrumentation can opt in to keep
    /// that stable owner while discarding every path-specific model. The
    /// default preserves Quinn's existing behavior of asking the configured
    /// [`ControllerFactory`] to build the replacement.
    #[allow(unused_variables)]
    fn fresh_path_box(&self, now: Instant, current_mtu: u16) -> Option<Box<dyn Controller>> {
        None
    }

    /// Initial congestion window
    fn initial_window(&self) -> u64;

    /// Returns Self for use in down-casting to extract implementation details
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Per-packet state used by delivery-rate sampling.
#[derive(Debug, Clone, Copy)]
pub struct PacketDeliveryState {
    /// Bytes delivered before this packet was sent.
    pub delivered: u64,
    /// Time of the most recent delivery before this packet was sent.
    pub delivered_time: Instant,
    /// Nanoseconds from the current send interval's first transmission to this
    /// packet. A compact relative duration avoids storing a second `Instant`
    /// in every outstanding packet.
    pub send_elapsed_ns: u64,
}

/// Common congestion controller metrics
#[derive(Default)]
#[non_exhaustive]
pub struct ControllerMetrics {
    /// Congestion window (bytes)
    pub congestion_window: u64,
    /// Slow start threshold (bytes)
    pub ssthresh: Option<u64>,
    /// Pacing rate (bytes/s)
    pub pacing_rate: Option<u64>,
    /// Estimated sustainable path bandwidth (bytes/s)
    pub bandwidth_estimate: Option<u64>,
    /// Controller-selected maximum send batch (bytes)
    pub send_quantum: Option<u64>,
}

/// Constructs controllers on demand
pub trait ControllerFactory {
    /// Construct a fresh `Controller`
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller>;
}

const BASE_DATAGRAM_SIZE: u64 = 1200;

#[cfg(test)]
mod activation_tests {
    use super::*;

    #[test]
    fn activation_is_checked_nonzero_and_reserves_max_for_terminal() {
        let fence = ControllerActivationFence::with_test_state(ControllerActivationState::Live(
            ControllerActivation(u64::MAX - 2),
        ));
        let last_live = {
            let mut transition = fence.begin_transition().expect("transition fence");
            let activation = transition.allocate().expect("last live activation");
            assert_eq!(activation.0, u64::MAX - 1);
            transition.publish();
            activation
        };
        assert_eq!(
            fence.with_current(|state| state).expect("current state"),
            ControllerActivationState::Live(last_live)
        );

        let mut transition = fence.begin_transition().expect("terminal transition");
        assert_eq!(
            transition.allocate(),
            Err(ControllerActivationExhausted {
                publish_terminal: true,
            })
        );
        drop(transition);
        assert_eq!(
            fence.with_current(|state| state).expect("terminal state"),
            ControllerActivationState::Terminal(ControllerActivationTerminal::Exhausted)
        );
    }

    #[test]
    fn unpublished_allocation_fails_closed_instead_of_reviving_old_a() {
        let fence = ControllerActivationFence::new();
        {
            let mut transition = fence.begin_transition().expect("transition fence");
            let activation = transition.allocate().expect("first activation");
            assert_ne!(activation.0, 0);
            // Deliberately omit publication to model an interrupted nonpanic
            // branch in future transition code.
        }
        assert_eq!(
            fence.with_current(|state| state).expect("terminal state"),
            ControllerActivationState::Terminal(ControllerActivationTerminal::AbandonedTransition,)
        );
    }
}
