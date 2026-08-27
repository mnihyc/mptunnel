//! Logic for controlling the rate at which data is sent

use crate::connection::RttEstimator;
pub use crate::packet::SpaceId;
use crate::{Duration, Instant};
use std::any::Any;
use std::sync::Arc;

mod bbr;
mod bbr3;
mod cubic;
mod new_reno;

pub use bbr::{Bbr, BbrConfig};
pub use bbr3::{Bbr3, Bbr3Config};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};

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

/// Common interface for different congestion controllers
pub trait Controller: Send + Sync {
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
        self.on_ack(
            now,
            sent,
            bytes,
            packet_number,
            space,
            app_limited,
            rtt,
        );
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
