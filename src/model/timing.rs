//! Carrier-neutral retransmission and setup timing.
//!
//! Runtime services provide immutable path snapshots. This model derives PTO,
//! proof freshness, and serialized setup budgets without observing a carrier.

use super::capacity::{
    QUIC_MAX_ACK_DELAY, QUIC_PERSISTENT_CONGESTION_THRESHOLD, QUIC_TIMER_GRANULARITY,
    RELIABLE_INITIAL_RTT,
};
use crate::protocol::UnderlayProtocol;
use crate::scheduler::PathSnapshot;
use std::time::Duration;

pub(crate) fn transport_pto_from_ms(srtt_ms: f64, rttvar_ms: f64) -> Duration {
    let srtt = Duration::from_secs_f64(srtt_ms.max(0.0) / 1000.0);
    let rttvar = Duration::from_secs_f64(rttvar_ms.max(0.0) / 1000.0);
    srtt + (rttvar * 4).max(QUIC_TIMER_GRANULARITY) + QUIC_MAX_ACK_DELAY
}

/// Product reinjection waits one PTO derived from the path that still owns the
/// original range; native TCP and QUIC recovery continue below this timer.
pub(crate) fn reliable_relay_tail_reinjection_delay(path: Option<PathSnapshot>) -> Duration {
    transport_pto_from_snapshot(path)
}

/// One connection-level ACK-gap observation remains valid for the carrier's
/// persistent-congestion horizon. Native recovery continues independently.
pub(crate) fn reliable_ack_gap_reinjection_batch_lifetime(path: Option<PathSnapshot>) -> Duration {
    transport_pto_from_snapshot(path).saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
}

/// Backoff for retrying a product sender whose bounded carrier queue is full.
pub(crate) fn sender_service_retry_delay(path: Option<PathSnapshot>) -> Duration {
    (transport_pto_from_snapshot(path) / 16)
        .max(Duration::from_millis(5))
        .min(QUIC_MAX_ACK_DELAY)
}

pub(crate) fn quic_bulk_proof_freshness_horizon(srtt: Duration, rttvar: Duration) -> Duration {
    // A rate proof loses placement rights at the same three-PTO boundary where
    // QUIC declares persistent congestion; reachability evidence is separate.
    transport_pto_from_ms(srtt.as_secs_f64() * 1000.0, rttvar.as_secs_f64() * 1000.0)
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
}

pub(crate) fn transport_pto_from_snapshot(path: Option<PathSnapshot>) -> Duration {
    // PTO follows carrier evidence, not product priority. Lane policy may gate
    // or size product reinjection, but does not rewrite this timing basis.
    path.map(|path| {
        let srtt_ms = path.srtt_ms.max(1.0);
        let rttvar_ms = path.jitter_ms.max(srtt_ms / 8.0);
        transport_pto_from_ms(srtt_ms, rttvar_ms)
    })
    .unwrap_or_else(default_transport_pto)
}

pub(crate) fn path_open_pto(path: Option<PathSnapshot>, rtt_is_observed: bool) -> Duration {
    let path_pto = transport_pto_from_snapshot(path);
    if rtt_is_observed {
        path_pto
    } else {
        path_pto.max(default_transport_pto())
    }
}

pub(crate) fn path_open_timeout(path: Option<PathSnapshot>, rtt_is_observed: bool) -> Duration {
    path_open_pto(path, rtt_is_observed).saturating_mul(path_open_pto_multiplier(path))
}

pub(crate) fn path_open_pto_multiplier(path: Option<PathSnapshot>) -> u32 {
    path_open_serialized_exchanges(path)
        .saturating_sub(1)
        .saturating_add(persistent_congestion_pto_backoff_multiplier())
}

pub(crate) fn persistent_congestion_pto_backoff_multiplier() -> u32 {
    (0..QUIC_PERSISTENT_CONGESTION_THRESHOLD).fold(0_u32, |total, exponent| {
        total.saturating_add(1_u32.checked_shl(exponent).unwrap_or(u32::MAX))
    })
}

pub(crate) fn path_open_serialized_exchanges(path: Option<PathSnapshot>) -> u32 {
    match path.map(|snapshot| snapshot.underlay) {
        Some(UnderlayProtocol::Udp) => 2,
        Some(UnderlayProtocol::Tcp) | None => 3,
    }
}

pub(crate) fn default_transport_pto() -> Duration {
    transport_pto_from_ms(
        RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0,
        RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0 / 2.0,
    )
}
