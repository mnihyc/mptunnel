//! Carrier-neutral datagram deadlines, retry decisions, and errors.

use crate::model::capacity::{DATAGRAM_RESPONSE_DEADLINE_MULTIPLIER, TRANSPORT_TIMER_GRANULARITY};
use crate::runtime::error::RuntimeError;
use std::time::Duration;

pub(in crate::runtime) fn datagram_feedback_retry_budget(
    feedback_timeout: Duration,
    ttl_ms: u32,
    has_unattempted_alternative: bool,
) -> Duration {
    let ttl_budget = datagram_useful_ttl_budget(ttl_ms);
    if ttl_budget.is_zero() {
        return ttl_budget;
    }
    let feedback_timeout = feedback_timeout
        .max(TRANSPORT_TIMER_GRANULARITY)
        .min(ttl_budget);
    if has_unattempted_alternative {
        // One modeled pre-feedback response timeout leaves the remaining product
        // deadline for another path. The final attempt keeps the larger budget.
        feedback_timeout
    } else {
        feedback_timeout
            .saturating_mul(DATAGRAM_RESPONSE_DEADLINE_MULTIPLIER)
            .min(ttl_budget)
    }
}

fn datagram_useful_ttl_budget(ttl_ms: u32) -> Duration {
    let ttl = Duration::from_millis(u64::from(ttl_ms));
    if ttl.is_zero() {
        return ttl;
    }
    ttl
}

pub(in crate::runtime) fn datagram_remaining_ttl_ms(expires_at: tokio::time::Instant) -> u32 {
    let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return 0;
    }
    remaining.as_millis().max(1).min(u128::from(u32::MAX)) as u32
}

pub(in crate::runtime) enum DatagramPathSendError {
    PayloadLimitExceeded { limit: usize },
    Timeout,
    Runtime(RuntimeError),
}

impl DatagramPathSendError {
    pub(in crate::runtime) fn runtime(source: RuntimeError) -> Self {
        Self::Runtime(source)
    }
}
