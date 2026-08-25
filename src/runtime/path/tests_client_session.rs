use super::ClientSessionLifecycle;
use crate::protocol::CloseReason;
use crate::runtime::error::RuntimeError;

#[tokio::test]
async fn session_terminal_is_sticky_for_sibling_carrier_and_product_owners() {
    let lifecycle = ClientSessionLifecycle::new();
    let sibling_carrier = lifecycle.retirement();
    let product = lifecycle.retirement();

    assert_eq!(
        lifecycle.retire(CloseReason::PolicyRejected),
        CloseReason::PolicyRejected
    );
    assert_eq!(sibling_carrier.wait().await, CloseReason::PolicyRejected);
    assert_eq!(product.wait().await, CloseReason::PolicyRejected);
    assert_eq!(
        lifecycle.retire(CloseReason::Normal),
        CloseReason::PolicyRejected,
        "a later carrier cannot replace the first session terminal reason"
    );
    assert!(matches!(
        lifecycle.ensure_active(),
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
}

#[test]
fn terminal_session_refuses_reconnect_and_product_publication() {
    let lifecycle = ClientSessionLifecycle::new();
    let mut publications = 0;
    lifecycle
        .commit_if_active(|| publications += 1)
        .expect("active session admits its initial carrier");
    lifecycle.retire(CloseReason::Normal);

    assert_eq!(
        lifecycle.commit_if_active(|| publications += 1),
        Err(CloseReason::Normal)
    );
    assert_eq!(publications, 1, "no publication follows SESSION_CLOSE");
}

#[test]
fn active_commit_finishes_before_a_waiting_terminal_and_no_later_commit_passes() {
    let lifecycle = ClientSessionLifecycle::new();
    let commit_lifecycle = lifecycle.clone();
    let (commit_entered, commit_entered_rx) = std::sync::mpsc::sync_channel(0);
    let (release_commit, release_commit_rx) = std::sync::mpsc::sync_channel(0);
    let commit = std::thread::spawn(move || {
        commit_lifecycle.commit_if_active(|| {
            commit_entered
                .send(())
                .expect("test observes the active publication transaction");
            release_commit_rx
                .recv()
                .expect("test releases the active publication transaction");
            11
        })
    });
    commit_entered_rx
        .recv()
        .expect("publication owns the lifecycle commitment first");

    let retire_lifecycle = lifecycle.clone();
    let retirement =
        std::thread::spawn(move || retire_lifecycle.retire(CloseReason::PolicyRejected));
    release_commit
        .send(())
        .expect("allow the pre-terminal publication to finish");

    assert_eq!(commit.join().expect("publication thread"), Ok(11));
    assert_eq!(
        retirement.join().expect("retirement thread"),
        CloseReason::PolicyRejected
    );
    assert_eq!(
        lifecycle.commit_if_active(|| 12),
        Err(CloseReason::PolicyRejected),
        "the serialized terminal forbids every later publication"
    );
}

#[tokio::test]
async fn one_session_terminal_does_not_affect_an_unrelated_context() {
    let retired = ClientSessionLifecycle::new();
    let unrelated = ClientSessionLifecycle::new();
    retired.retire(CloseReason::PolicyRejected);

    assert_eq!(unrelated.reason(), None);
    assert_eq!(unrelated.commit_if_active(|| 7), Ok(7));
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(1),
            unrelated.retirement().wait(),
        )
        .await
        .is_err(),
        "an unrelated SessionId remains active"
    );
}
