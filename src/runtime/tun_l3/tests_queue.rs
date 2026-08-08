use super::*;

#[test]
fn packet_budget_is_shared_and_byte_bounded() {
    let budget = IpPacketQueueBudget::new(1_500);
    let first = budget.try_reserve(1_000).expect("reserve first packet");
    assert_eq!(budget.available_bytes(), 500);
    assert!(matches!(
        budget.try_reserve(1),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    drop(first);
    assert_eq!(budget.available_bytes(), 1_500);
}
