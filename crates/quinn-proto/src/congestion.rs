//! Logic for controlling the rate at which data is sent

use crate::connection::RttEstimator;
use crate::Instant;
use std::any::Any;
use std::sync::Arc;

mod bbr;
mod cubic;
mod new_reno;

pub use bbr::{Bbr, BbrConfig};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};

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
        app_limited: bool,
    ) -> Option<PacketDeliveryState> {
        None
    }

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
        packet_state: Option<PacketDeliveryState>,
        rtt: &RttEstimator,
    ) {
        self.on_ack(now, sent, bytes, app_limited, rtt);
    }

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those batches has completed.
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
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
        lost_bytes: u64,
    );

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Retrieve implementation-specific metrics used to populate `qlog` traces when they are enabled
    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            pacing_rate: None,
        }
    }

    /// Controller-selected pacing rate in bytes per second.
    fn pacing_rate(&self) -> Option<u64> {
        None
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
    /// Pacing rate (bits/s)
    pub pacing_rate: Option<u64>,
}

/// Constructs controllers on demand
pub trait ControllerFactory {
    /// Construct a fresh `Controller`
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller>;
}

const BASE_DATAGRAM_SIZE: u64 = 1200;
