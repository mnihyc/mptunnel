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
use std::time::{Duration, Instant};

const TCP_MIN_RETRANSMISSION_TIMEOUT: Duration = Duration::from_millis(200);
const TCP_INITIAL_RETRANSMISSION_TIMEOUT: Duration = Duration::from_secs(1);
const MPTCP_STALE_LOSS_COUNT: u32 = 4;

/// Target-independent clocks for one exact original Product assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReliableDataAckGapTiming {
    pub(crate) assignment_at: Instant,
    pub(crate) loss_at: Option<Instant>,
    pub(crate) fallback_at: Instant,
}

impl ReliableDataAckGapTiming {
    /// Selects the next evaluation epoch for the currently measured target.
    /// A target may race at the loss boundary only when its current completion
    /// projection beats the live owner's comparable projection. The fallback
    /// is an authority epoch, not an owner-delivery estimate.
    pub(crate) fn target_deadline(
        self,
        alternate_completion: Option<Duration>,
        owner_completion: Option<Duration>,
        observed_at: Instant,
    ) -> Option<Instant> {
        let alternate_completion = alternate_completion?;
        if observed_at >= self.fallback_at {
            return Some(self.fallback_at);
        }
        if let (Some(loss_at), Some(owner_completion)) = (self.loss_at, owner_completion) {
            let launch_at = observed_at.max(loss_at);
            let alternate_delivery = launch_at.checked_add(alternate_completion);
            let owner_delivery = observed_at.checked_add(owner_completion);
            let alternate_wins = alternate_delivery
                .zip(owner_delivery)
                .is_some_and(|(alternate, owner)| alternate < owner);
            if alternate_wins {
                return Some(loss_at);
            }
        }
        Some(self.fallback_at)
    }
}

pub(crate) fn transport_pto_from_ms(srtt_ms: f64, rttvar_ms: f64) -> Duration {
    let srtt = Duration::from_secs_f64(srtt_ms.max(0.0) / 1000.0);
    let rttvar = Duration::from_secs_f64(rttvar_ms.max(0.0) / 1000.0);
    srtt + (rttvar * 4).max(QUIC_TIMER_GRANULARITY) + QUIC_MAX_ACK_DELAY
}

pub(crate) fn tcp_retransmission_timeout_from_ms(srtt_ms: f64, rttvar_ms: f64) -> Duration {
    let srtt = Duration::from_secs_f64(srtt_ms.max(0.0) / 1000.0);
    let rttvar = Duration::from_secs_f64(rttvar_ms.max(0.0) / 1000.0);
    (srtt + (rttvar * 4).max(QUIC_TIMER_GRANULARITY)).max(TCP_MIN_RETRANSMISSION_TIMEOUT)
}

pub(crate) fn tcp_retransmission_timeout_from_snapshot(path: Option<PathSnapshot>) -> Duration {
    path.map(|path| {
        let srtt_ms = path.srtt_ms.max(1.0);
        let rttvar_ms = path.jitter_ms.max(srtt_ms / 8.0);
        tcp_retransmission_timeout_from_ms(srtt_ms, rttvar_ms)
    })
    .unwrap_or(TCP_INITIAL_RETRANSMISSION_TIMEOUT)
}

/// Initial Data-level retransmission follows the recovery clock of the carrier
/// that owns the missing range. Once reinjection is accepted, its repeat clock
/// follows that selected carrier's immutable observation. Neither declares a
/// carrier stale.
pub(crate) fn reliable_data_retransmission_interval(
    underlay: Option<UnderlayProtocol>,
    path: Option<PathSnapshot>,
) -> Duration {
    match underlay.or_else(|| path.map(|path| path.underlay)) {
        Some(UnderlayProtocol::Tcp) => tcp_retransmission_timeout_from_snapshot(path),
        Some(UnderlayProtocol::Udp) | None => transport_pto_from_snapshot(path),
    }
}

/// Data ACK gaps use the carrier's established time-threshold loss model:
/// TCP RACK uses 5/4 SRTT and QUIC uses the RFC 9002 default 9/8 SRTT.
pub(crate) fn reliable_data_ack_loss_delay(
    underlay: Option<UnderlayProtocol>,
    path: Option<PathSnapshot>,
) -> Option<Duration> {
    let path = path?;
    let multiplier = match underlay.unwrap_or(path.underlay) {
        UnderlayProtocol::Tcp => 5.0 / 4.0,
        UnderlayProtocol::Udp => 9.0 / 8.0,
    };
    let delay_ms = (path.srtt_ms.max(1.0) * multiplier).max(1.0);
    delay_ms
        .is_finite()
        .then(|| Duration::from_secs_f64(delay_ms / 1000.0))
}

/// Derives absolute owner clocks from the exact assignment epoch and latest
/// observation. Runtime gap state may tighten these clocks, but never lets a
/// later observation restart them for the same assignment.
pub(crate) fn reliable_data_ack_gap_timing(
    original_assignment_at: Option<std::time::Instant>,
    underlay: Option<UnderlayProtocol>,
    original_path: Option<PathSnapshot>,
) -> Option<ReliableDataAckGapTiming> {
    let original_assignment_at = original_assignment_at?;
    let fallback_at = original_assignment_at.checked_add(reliable_data_retransmission_interval(
        underlay,
        original_path,
    ))?;
    let loss_at = reliable_data_ack_loss_delay(underlay, original_path)
        .and_then(|loss_delay| original_assignment_at.checked_add(loss_delay));
    Some(ReliableDataAckGapTiming {
        assignment_at: original_assignment_at,
        loss_at,
        fallback_at,
    })
}

/// Aggregates the immutable assignment epochs in one exact ranked frontier.
///
/// Every supplied span must contribute its own exact owner observation.  The
/// latest absolute loss/fallback boundary wins, so the ranked prefix is not
/// eligible until all of its OriginalData spans have matured.  Runtime cause
/// state retains the earliest observation for the same assignment set; this
/// helper deliberately performs no cross-evaluation mutation.
pub(crate) fn reliable_data_ack_gap_timing_for_assignments<I: Copy>(
    assignments: &[(I, Instant)],
    mut owner_path: impl FnMut(I) -> (UnderlayProtocol, Option<PathSnapshot>),
) -> Option<ReliableDataAckGapTiming> {
    let mut aggregate = None::<ReliableDataAckGapTiming>;
    for (owner, assignment_at) in assignments.iter().copied() {
        let (underlay, snapshot) = owner_path(owner);
        let timing = reliable_data_ack_gap_timing(Some(assignment_at), Some(underlay), snapshot)?;
        aggregate = Some(match aggregate {
            Some(current) => ReliableDataAckGapTiming {
                assignment_at: current.assignment_at.max(timing.assignment_at),
                loss_at: match (current.loss_at, timing.loss_at) {
                    (Some(current), Some(next)) => Some(current.max(next)),
                    _ => None,
                },
                fallback_at: current.fallback_at.max(timing.fallback_at),
            },
            None => timing,
        });
    }
    aggregate
}

/// An authoritative later Data ACK may start bounded repair at the owner's
/// time-threshold loss boundary when the currently measured alternate can
/// finish before the live owner's comparable projected delivery. Otherwise
/// the independent owner-recovery epoch remains the bounded liveness fallback.
#[cfg(test)]
pub(crate) fn reliable_data_ack_gap_reinjection_deadline(
    original_assignment_at: Option<std::time::Instant>,
    underlay: Option<UnderlayProtocol>,
    original_path: Option<PathSnapshot>,
    alternate_completion: Option<Duration>,
    owner_completion: Option<Duration>,
    observed_at: std::time::Instant,
) -> Option<std::time::Instant> {
    reliable_data_ack_gap_timing(original_assignment_at, underlay, original_path)?.target_deadline(
        alternate_completion,
        owner_completion,
        observed_at,
    )
}

/// Without a later ACK, connection-level recovery waits for the owning
/// carrier's RTO/PTO whenever an eligible alternate exists. The alternate's
/// completion estimate decides whether it can win earlier; it cannot erase the
/// exact owner fallback. Silence alone is not a RACK or QUIC loss declaration.
#[cfg(test)]
pub(crate) fn reliable_data_ack_recovery_deadline(
    original_assignment_at: Option<std::time::Instant>,
    underlay: Option<UnderlayProtocol>,
    original_path: Option<PathSnapshot>,
    alternate_completion: Option<Duration>,
) -> Option<std::time::Instant> {
    alternate_completion?;
    reliable_data_ack_gap_timing(original_assignment_at, underlay, original_path)
        .map(|timing| timing.fallback_at)
}

#[cfg(test)]
pub(crate) fn reliable_data_ack_gap_reinjection_ready(
    original_assignment_at: Option<std::time::Instant>,
    underlay: Option<UnderlayProtocol>,
    original_path: Option<PathSnapshot>,
    alternate_completion: Option<Duration>,
    owner_completion: Option<Duration>,
    now: std::time::Instant,
) -> bool {
    reliable_data_ack_gap_reinjection_deadline(
        original_assignment_at,
        underlay,
        original_path,
        alternate_completion,
        owner_completion,
        now,
    )
    .is_some_and(|deadline| now >= deadline)
}

/// Repeated recovery intervals without exact data progress make a path stale
/// for new assignments. Existing carrier recovery continues independently.
pub(crate) fn reliable_path_stale_interval(
    underlay: Option<UnderlayProtocol>,
    path: Option<PathSnapshot>,
) -> Duration {
    let interval = reliable_data_retransmission_interval(underlay, path);
    match underlay.or_else(|| path.map(|path| path.underlay)) {
        Some(UnderlayProtocol::Tcp) => interval.saturating_mul(MPTCP_STALE_LOSS_COUNT),
        Some(UnderlayProtocol::Udp) | None => {
            interval.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
        }
    }
}

/// Product reinjection waits one carrier recovery interval derived from the
/// path that still owns the original range.
pub(crate) fn reliable_relay_tail_reinjection_delay(path: Option<PathSnapshot>) -> Duration {
    reliable_data_retransmission_interval(None, path)
}

/// Backoff for retrying a product sender whose bounded carrier queue is full.
pub(crate) fn sender_service_retry_delay(path: Option<PathSnapshot>) -> Duration {
    (transport_pto_from_snapshot(path) / 16)
        .max(Duration::from_millis(5))
        .min(QUIC_MAX_ACK_DELAY)
}

pub(crate) fn transport_rate_sample_freshness_horizon(
    srtt: Duration,
    rttvar: Duration,
) -> Duration {
    // A rate sample loses placement rights at the same three-PTO boundary where
    // a carrier stops treating prior delivery as current path behavior.
    transport_pto_from_ms(srtt.as_secs_f64() * 1000.0, rttvar.as_secs_f64() * 1000.0)
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
}

pub(crate) fn quic_bulk_proof_freshness_horizon(srtt: Duration, rttvar: Duration) -> Duration {
    transport_rate_sample_freshness_horizon(srtt, rttvar)
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

pub(crate) fn path_open_serialized_exchanges(_path: Option<PathSnapshot>) -> u32 {
    // Both carriers perform three serialized network exchanges before a new
    // product stream is usable: transport establishment, authenticated path
    // join, and product stream acceptance. QUIC removes TCP head-of-line
    // recovery; it does not remove these application handshakes.
    3
}

pub(crate) fn default_transport_pto() -> Duration {
    transport_pto_from_ms(
        RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0,
        RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0 / 2.0,
    )
}

#[cfg(test)]
#[path = "tests_timing.rs"]
mod tests;
