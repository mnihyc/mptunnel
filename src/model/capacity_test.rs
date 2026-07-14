use super::{
    QUIC_TIMER_GRANULARITY, quic_capacity_receipt_rate_bps, valid_quic_capacity_proof_geometry,
};
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

#[test]
fn quic_capacity_geometry_accepts_a_bounded_policy_tail() {
    assert!(valid_quic_capacity_proof_geometry(838, 500, 62, 400, 438));
    assert!(
        valid_quic_capacity_proof_geometry(900, 500, 62, 400, 438),
        "timing guard bytes may follow the complete proof minimum"
    );
    assert!(
        !valid_quic_capacity_proof_geometry(837, 500, 62, 400, 438),
        "the train cannot underfill warmup plus strict proof"
    );
    assert!(
        !valid_quic_capacity_proof_geometry(900, 500, 0, 400, 500),
        "accounting slack remains fixed by the sample floor"
    );
    assert!(
        !valid_quic_capacity_proof_geometry(900, 500, 62, 400, 1),
        "one byte cannot satisfy a representative sample floor"
    );
}
