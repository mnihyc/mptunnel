use super::{QUIC_TIMER_GRANULARITY, quic_capacity_receipt_rate_bps};
use std::time::Duration;

#[test]
fn quic_receipt_rate_rejects_empty_evidence() {
    assert_eq!(
        quic_capacity_receipt_rate_bps(0, QUIC_TIMER_GRANULARITY),
        None
    );
    assert_eq!(quic_capacity_receipt_rate_bps(1, Duration::ZERO), None);
}

#[test]
fn quic_receipt_rate_applies_timer_granularity_and_nearest_rounding() {
    assert_eq!(
        quic_capacity_receipt_rate_bps(1, Duration::from_micros(1)),
        Some(8_000)
    );
    assert_eq!(
        quic_capacity_receipt_rate_bps(1, Duration::from_millis(3)),
        Some(2_667)
    );
}

#[test]
fn quic_receipt_rate_saturates_to_the_evidence_type() {
    assert_eq!(
        quic_capacity_receipt_rate_bps(u64::MAX, QUIC_TIMER_GRANULARITY),
        Some(u64::MAX)
    );
}
