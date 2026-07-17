use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug, Default)]
struct TestCreditSource {
    limit_bytes: AtomicU64,
    committed_bytes: AtomicU64,
    closed: AtomicBool,
    notify: Arc<Notify>,
}

impl CarrierSendCreditSource for TestCreditSource {
    fn snapshot(&self) -> CarrierSendCreditSnapshot {
        CarrierSendCreditSnapshot {
            limit_bytes: self.limit_bytes.load(Ordering::Acquire),
            committed_bytes: self.committed_bytes.load(Ordering::Acquire),
            closed: self.closed.load(Ordering::Acquire),
        }
    }

    fn notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }
}

#[test]
fn one_quantum_may_cross_the_current_native_limit() {
    let source = Arc::new(TestCreditSource::default());
    source.limit_bytes.store(14_600, Ordering::Release);
    let credit = CarrierSendCredit::new(source);

    assert!(credit.try_reserve(64 * 1024).is_ok());
    assert_eq!(credit.reserved_bytes(), 64 * 1024);
    assert_eq!(credit.try_reserve(1), Err(CarrierSendCreditError::Blocked));
}

#[test]
fn native_committed_bytes_and_shared_reservations_both_close_credit() {
    let source = Arc::new(TestCreditSource::default());
    source.limit_bytes.store(128 * 1024, Ordering::Release);
    source.committed_bytes.store(96 * 1024, Ordering::Release);
    let first = CarrierSendCredit::new(source.clone());
    let second = first.clone();

    assert!(first.try_reserve(64 * 1024).is_ok());
    assert_eq!(second.try_reserve(1), Err(CarrierSendCreditError::Blocked));
    first.release(64 * 1024);
    source.committed_bytes.store(0, Ordering::Release);
    assert!(second.try_reserve(64 * 1024).is_ok());
}

#[test]
fn closed_carrier_rejects_credit_without_reserving_bytes() {
    let source = Arc::new(TestCreditSource::default());
    source.limit_bytes.store(64 * 1024, Ordering::Release);
    source.closed.store(true, Ordering::Release);
    let credit = CarrierSendCredit::new(source);

    assert_eq!(
        credit.try_reserve(1024),
        Err(CarrierSendCreditError::Closed)
    );
    assert_eq!(credit.reserved_bytes(), 0);
}
