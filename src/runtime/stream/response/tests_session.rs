use super::{ServerSessionRegistration, ServerSessionTracker};
use crate::mux::MuxLimits;
use crate::product::PrincipalPermit;
use crate::protocol::{CloseReason, SessionId};
use crate::runtime::RuntimeError;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn balanced_references_reclaim_session() {
    let tracker = ServerSessionTracker::default();
    let session_id = SessionId(7);

    tracker
        .attach_authenticated_session(session_id, &PrincipalPermit::for_test("test-peer"))
        .expect("register authenticated carrier");
    tracker
        .attach_session(session_id)
        .expect("attach first product owner");
    tracker
        .attach_session(session_id)
        .expect("attach second product owner");
    assert_eq!(tracker.reference_count(session_id), 3);

    tracker.detach_session(session_id);
    assert_eq!(tracker.reference_count(session_id), 2);
    tracker.detach_session(session_id);
    assert_eq!(tracker.reference_count(session_id), 1);
    tracker.detach_session(session_id);
    assert_eq!(tracker.reference_count(session_id), 0);
}

#[tokio::test]
async fn terminal_retirement_is_exact_and_fences_same_id_rejoin() {
    let tracker = ServerSessionTracker::from_limits_and_retention(
        MuxLimits::default(),
        2,
        Duration::from_secs(60),
    );
    let retired = SessionId(21);
    let sibling = SessionId(22);
    let permit = PrincipalPermit::for_test("test-peer");
    tracker
        .attach_authenticated_session(retired, &permit)
        .expect("register retiring session");
    tracker
        .attach_authenticated_session(sibling, &permit)
        .expect("register unrelated session");
    let retirement = tracker
        .session_retirement(retired)
        .expect("subscribe retiring session");
    let sibling_retirement = tracker
        .session_retirement(sibling)
        .expect("subscribe unrelated session");

    assert!(tracker.retire_session(retired, CloseReason::Normal));
    assert_eq!(retirement.wait().await, CloseReason::Normal);
    assert!(!sibling_retirement.is_retired());
    assert!(!tracker.retire_session(retired, CloseReason::PolicyRejected));
    assert!(matches!(
        tracker.attach_authenticated_session(retired, &permit),
        Err(RuntimeError::RemoteClosed(CloseReason::Normal))
    ));
    tracker
        .attach_session(sibling)
        .expect("unrelated session remains admissible");

    tracker.detach_session(sibling);
    tracker.detach_session(sibling);
    tracker.detach_session(retired);
}

#[test]
fn terminal_zero_reference_tombstone_remains_inside_the_session_bound() {
    let tracker = ServerSessionTracker::from_limits_and_retention(
        MuxLimits::default(),
        1,
        Duration::from_secs(60),
    );
    let retired = SessionId(31);
    let permit = PrincipalPermit::for_test("test-peer");
    tracker
        .attach_authenticated_session(retired, &permit)
        .expect("register retiring session");
    assert!(tracker.retire_session(retired, CloseReason::Normal));
    tracker.detach_session(retired);

    assert_eq!(tracker.reference_count(retired), 0);
    assert!(matches!(
        tracker.attach_authenticated_session(retired, &permit),
        Err(RuntimeError::RemoteClosed(CloseReason::Normal))
    ));
    assert!(matches!(
        tracker.attach_authenticated_session(SessionId(32), &permit),
        Err(RuntimeError::Protocol(
            "server authenticated session limit reached"
        ))
    ));
}

#[test]
fn expired_terminal_tombstone_releases_capacity_and_same_id_fence() {
    let tracker = ServerSessionTracker::from_limits_and_retention(
        MuxLimits::default(),
        1,
        Duration::from_millis(10),
    );
    let retired = SessionId(41);
    let permit = PrincipalPermit::for_test("test-peer");
    tracker
        .attach_authenticated_session(retired, &permit)
        .expect("register retiring session");
    assert!(tracker.retire_session(retired, CloseReason::Normal));
    tracker.detach_session(retired);
    assert!(matches!(
        tracker.attach_authenticated_session(retired, &permit),
        Err(RuntimeError::RemoteClosed(CloseReason::Normal))
    ));

    std::thread::sleep(Duration::from_millis(25));

    tracker
        .attach_authenticated_session(retired, &permit)
        .expect("expired tombstone permits the same SessionId again");
    tracker.detach_session(retired);
    tracker
        .attach_authenticated_session(SessionId(42), &permit)
        .expect("expired tombstone releases global session capacity");
    tracker.detach_session(SessionId(42));
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

    tracker
        .attach_authenticated_session(session_id, &PrincipalPermit::for_test("test-peer"))
        .expect("register authenticated carrier");
    let response = ServerSessionRegistration::new(tracker.clone(), session_id);
    let realtime = ServerSessionRegistration::new(tracker.clone(), session_id);
    assert_eq!(tracker.reference_count(session_id), 3);

    drop(response);
    assert_eq!(tracker.reference_count(session_id), 2);
    drop(realtime);
    assert_eq!(tracker.reference_count(session_id), 1);
    tracker.detach_session(session_id);
    assert_eq!(tracker.reference_count(session_id), 0);
}
