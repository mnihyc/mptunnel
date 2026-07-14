use crate::runtime::path::tcp::client_state::DiscardedClientTcpCapacityReceipt;

#[test]
fn discarded_request_tcp_receipt_requires_the_exact_completed_epoch() {
    let discarded = DiscardedClientTcpCapacityReceipt {
        calibration_id: 17,
        train_payload_bytes: 3 * 1024 * 1024,
    };
    assert!(discarded.matches(17, 3 * 1024 * 1024));
    assert!(!discarded.matches(18, 3 * 1024 * 1024));
    assert!(!discarded.matches(17, 3 * 1024 * 1024 - 1));
}
