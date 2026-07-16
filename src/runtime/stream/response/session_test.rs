use super::{ServerSessionRegistration, ServerSessionTracker};
use crate::protocol::SessionId;
use std::sync::Arc;

#[test]
fn balanced_references_reclaim_session() {
    let tracker = ServerSessionTracker::default();
    let session_id = SessionId(7);

    tracker.attach_session(session_id);
    tracker.attach_session(session_id);
    assert_eq!(tracker.reference_count(session_id), 2);

    tracker.detach_session(session_id);
    assert_eq!(tracker.reference_count(session_id), 1);
    tracker.detach_session(session_id);
    assert_eq!(tracker.reference_count(session_id), 0);
}

#[test]
#[should_panic(expected = "detached unregistered server session")]
fn unbalanced_detach_is_an_invariant_violation() {
    ServerSessionTracker::default().detach_session(SessionId(9));
}

#[test]
fn registrations_hold_independent_session_references() {
    let tracker = Arc::new(ServerSessionTracker::default());
    let session_id = SessionId(11);

    let response = ServerSessionRegistration::new(tracker.clone(), session_id);
    let realtime = ServerSessionRegistration::new(tracker.clone(), session_id);
    assert_eq!(tracker.reference_count(session_id), 2);

    drop(response);
    assert_eq!(tracker.reference_count(session_id), 1);
    drop(realtime);
    assert_eq!(tracker.reference_count(session_id), 0);
}
