use super::*;
use crate::model::work::CarrierWorkKind;

#[test]
fn only_original_connection_data_owns_a_sequence_range() {
    assert!(CarrierWorkKind::OriginalData.is_original_transmission());
    assert!(
        !CarrierWorkKind::ReinjectedData.is_original_transmission(),
        "a reinjected copy must not create a second range owner or delivery sample"
    );
}

#[test]
fn extra_traffic_budget_is_hard_percent_plus_startup_floor() {
    let budget = ExtraTrafficBudget::new(
        1_000_000,
        49_999,
        1024,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );

    assert_eq!(budget.limit_bytes(), 51_024);
    assert!(budget.can_spend(1025));
    assert!(!budget.can_spend(1026));
    assert_eq!(budget.remaining_bytes(), 1025);
}

#[test]
fn extra_traffic_ledger_keeps_original_and_duplicate_bytes_separate() {
    let mut ledger = ExtraTrafficLedger::default();
    ledger.record_delivered_data(1_000_000);
    ledger.record_reinjection(300);

    assert_eq!(ledger.delivered_data_bytes(), 1_000_000);
    assert_eq!(ledger.reinjected_bytes(), 300);
    assert_eq!(
        ledger
            .budget(
                1024,
                MppPerformanceConfig {
                    extra_traffic_hint_percent: 5
                }
            )
            .limit_bytes(),
        51_024
    );
}
