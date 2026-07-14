use super::*;

#[test]
fn capacity_receive_tracker_accepts_only_exact_bounded_epoch() {
    let mut tracker = CapacityReceiveTracker::new(1024);

    tracker.record_data(7, 400).expect("first record");
    tracker.record_data(7, 624).expect("second record");
    assert_eq!(tracker.finish(7, 1024).expect("exact finish"), 1024);
}

#[test]
fn capacity_receive_tracker_rejects_over_limit_and_interleaving() {
    let mut over_limit = CapacityReceiveTracker::new(1024);
    over_limit.record_data(7, 900).expect("first record");
    assert!(matches!(
        over_limit.record_data(7, 125),
        Err(RuntimeError::Protocol(_))
    ));

    let mut interleaved = CapacityReceiveTracker::new(1024);
    interleaved.record_data(7, 400).expect("first token");
    assert!(matches!(
        interleaved.record_data(8, 400),
        Err(RuntimeError::Protocol(_))
    ));
}

#[test]
fn capacity_receive_tracker_rejects_mismatched_finish() {
    let mut tracker = CapacityReceiveTracker::new(1024);
    tracker.record_data(7, 400).expect("data record");

    assert!(matches!(
        tracker.finish(8, 400),
        Err(RuntimeError::Protocol(_))
    ));
}
